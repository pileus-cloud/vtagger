"""
VTagger Umbrella Client.

Handles authenticated communication with the Umbrella Cost API:
  - Keycloak UM 2.0 authentication (Basic Auth + JWT)
  - Account listing
  - Streaming asset/resource fetches with dynamic tag columns
  - Virtual tag uploads via presigned URL and import monitoring
"""

import base64
import csv
import gzip
import io
import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from typing import Dict, Generator, List, Optional, Set, Tuple
from urllib.parse import urlencode

import httpx

from app.config import settings
from app.services.credential_manager import get_credentials, has_credentials
from app.services.agent_logger import log_timing


class UmbrellaClient:
    """Client for the Umbrella Cost Management API using Keycloak UM 2.0."""

    def __init__(self):
        self.base_url = settings.umbrella_api_base
        self.jwt_token: Optional[str] = None
        self.user_key: Optional[str] = None
        self.token_expiry: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Authentication (Keycloak UM 2.0)
    # ------------------------------------------------------------------

    def _ensure_authenticated(self):
        """Ensure we have a valid authentication token, re-authenticating if needed."""
        if self.jwt_token and self.token_expiry:
            buffer = timedelta(minutes=5)
            if datetime.now() < (self.token_expiry - buffer):
                return  # Token still valid

        if not self.authenticate():
            raise Exception("Authentication failed")

    def authenticate(self) -> bool:
        """
        Authenticate using Keycloak UM 2.0.

        Reads username/password from the credential manager (env vars, keyring,
        or encrypted config file) and exchanges them for a JWT bearer token.
        """
        creds = get_credentials()
        if creds is None:
            raise Exception(
                "Credentials not available. "
                "Set via environment variables (VTAGGER_USERNAME / VTAGGER_PASSWORD), "
                "keyring, or 'vtagger credentials set' CLI command."
            )

        username, password = creds

        # Basic Auth header
        basic_auth = base64.b64encode(f"{username}:{password}".encode()).decode()

        url = f"{self.base_url}/v1/authentication/token/generate"
        headers = {
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "username": username,
            "password": password,
        }

        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(url, json=payload, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    self.jwt_token = data.get("Authorization")
                    temp_apikey = data.get("apikey", "")
                    self.user_key = temp_apikey.split(":")[0] if temp_apikey else None
                    self.token_expiry = datetime.now() + timedelta(hours=1)
                    log_timing("AUTH: Authenticated successfully")
                    return True

                log_timing(f"AUTH: Failed - {response.status_code} {response.text}")
                return False

        except httpx.RequestError as exc:
            log_timing(f"AUTH: Request error - {exc}")
            return False

    def is_authenticated(self) -> bool:
        """Check whether we hold a valid (non-expired) token."""
        if not self.jwt_token or not self.token_expiry:
            return False
        buffer = timedelta(minutes=5)
        return datetime.now() < (self.token_expiry - buffer)

    def _build_headers(self, account_key: Optional[str] = None) -> Dict[str, str]:
        """Build request headers with JWT and optional apikey."""
        headers = {
            "Authorization": self.jwt_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if account_key is not None:
            headers["apikey"] = f"{self.user_key}:{account_key}:0"
        else:
            headers["apikey"] = f"{self.user_key}:-1:-1"
        return headers

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------

    def get_accounts(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch the list of cloud accounts from Umbrella.

        Returns:
            Tuple of (aggregate_accounts, individual_accounts).
        """
        self._ensure_authenticated()

        url = f"{self.base_url}/v1/user-management/accounts"
        headers = self._build_headers()

        with httpx.Client(timeout=30.0) as client:
            response = client.get(url, headers=headers)

            # Retry once on 401
            if response.status_code == 401:
                self.jwt_token = None
                self._ensure_authenticated()
                headers = self._build_headers()
                response = client.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(f"Failed to get accounts: {response.status_code}")

            accounts = response.json()

            aggregate_accounts = []
            individual_accounts = []
            for acc in accounts:
                if acc.get("isAllAccounts", False):
                    aggregate_accounts.append(acc)
                else:
                    individual_accounts.append(acc)

            log_timing(
                f"ACCOUNTS: {len(aggregate_accounts)} aggregate, "
                f"{len(individual_accounts)} individual"
            )
            return aggregate_accounts, individual_accounts

    # ------------------------------------------------------------------
    # Asset / Resource streaming
    # ------------------------------------------------------------------

    def fetch_assets_stream(
        self,
        account_key: str,
        start_date: str,
        end_date: str,
        batch_size: int = 1000,
        exclude_tagged: bool = True,
        max_pages: int = 0,
        progress_callback=None,
        filter_mode: str = "not_vtagged",
        tag_keys: Set[str] = None,
        vtag_filter_dimensions: List[str] = None,
    ) -> Generator[List[Dict], None, None]:
        """
        Stream asset rows from the Umbrella v2 export endpoint in batches.

        Parameters
        ----------
        account_key : str
            The Umbrella account key to query.
        start_date, end_date : str
            Date range in YYYY-MM-DD format.
        batch_size : int
            Number of rows per batch to yield.
        max_pages : int
            Stop after this many pages (0 = unlimited).
        progress_callback : callable, optional
            Called with (page_number, row_count) after each page.
        filter_mode : str
            "not_vtagged" - only resources missing vtags for the specified dimensions,
            "all" - every resource (no governance filter).
        tag_keys : set of str, optional
            Physical tag keys to include as dynamic columns.
        vtag_filter_dimensions : list of str, optional
            Virtual tag dimension names for the governance filter.

        Yields
        ------
        list of dict
            A batch of resource rows.
        """
        self._ensure_authenticated()

        headers = self._build_headers(account_key)
        url = f"{self.base_url.replace('/v1', '')}/v2/usage/assets"

        # ---- Build query parameters ----
        params = [
            ("startDate", start_date),
            ("endDate", end_date),
            ("isK8S", "0"),
            ("granLevel", "week"),
            ("columns", "resourceid"),
            ("columns", "linkedaccid"),
            ("columns", "payeraccount"),
        ]

        # Dynamic tag columns
        if tag_keys:
            for key in sorted(tag_keys):
                params.append(("columns", f"customtags:{key}"))

        # Standard cost columns
        params.append(("costType", "cost"))
        params.append(("isUnblended", "false"))

        next_token = None
        batch = []
        page_count = 0
        total_records = 0

        with httpx.Client(timeout=600.0) as client:
            while True:
                page_count += 1

                request_params = list(params)
                if next_token:
                    request_params.append(("token", next_token))

                query_string = urlencode(request_params)

                # Add governance tags filter for not_vtagged mode
                if filter_mode == "not_vtagged" and vtag_filter_dimensions:
                    for dim in vtag_filter_dimensions:
                        query_string += f"&filters%5Bgovernance_tags_keys%5D={dim}%3A%20no_tag"
                # filter_mode == "all" -> no governance filter

                full_url = f"{url}?{query_string}"

                try:
                    log_timing(f"FETCH: Page {page_count} for account {account_key}")
                    response = client.get(full_url, headers=headers)

                    # Handle token expiration
                    if response.status_code == 401:
                        log_timing("FETCH: Token expired, re-authenticating...")
                        self.jwt_token = None
                        self._ensure_authenticated()
                        headers = self._build_headers(account_key)
                        response = client.get(full_url, headers=headers)

                    if response.status_code != 200:
                        raise Exception(
                            f"Failed to fetch assets: {response.status_code}"
                        )

                except httpx.RequestError as exc:
                    log_timing(f"FETCH: Page {page_count} failed - {exc}")
                    raise

                result = response.json()
                data = result.get("data", [])
                total_records += len(data)

                log_timing(
                    f"FETCH: Page {page_count} returned {len(data)} rows "
                    f"(total: {total_records})"
                )

                if progress_callback:
                    progress_callback(page_count, total_records)

                for asset in data:
                    batch.append(asset)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

                next_token = result.get("nextToken")
                if not next_token:
                    log_timing(
                        f"FETCH: Complete - {total_records} records "
                        f"in {page_count} pages"
                    )
                    break

                if max_pages > 0 and page_count >= max_pages:
                    log_timing(
                        f"FETCH: Reached max_pages limit ({max_pages}), "
                        f"total: {total_records} records"
                    )
                    break

            # Yield remaining
            if batch:
                yield batch

    # ------------------------------------------------------------------
    # Virtual Tag Upload (presigned URL flow)
    # ------------------------------------------------------------------

    def upload_virtual_tags(
        self,
        csv_path: str,
        account_id: str,
        compressed: bool = True,
    ) -> str:
        """
        Upload virtual tags CSV to Umbrella via presigned URL.

        Parameters
        ----------
        csv_path : str
            Path to the CSV file on disk.
        account_id : str
            The cloud account ID (AWS or Azure).
        compressed : bool
            Whether to gzip-compress the upload.

        Returns
        -------
        str
            The upload ID for monitoring progress.
        """
        self._ensure_authenticated()

        # Find account_key for this account_id
        aggregate_accounts, individual_accounts = self.get_accounts()
        account_key = None

        for acc in individual_accounts + aggregate_accounts:
            if acc.get("accountId") == account_id or acc.get("accountName") == account_id:
                account_key = acc.get("accountKey")
                break

        if not account_key:
            raise Exception(f"Account not found: {account_id}")

        headers = self._build_headers(account_key)

        # Step 1: Get presigned upload URL
        url = (
            f"{self.base_url.replace('/v1', '')}"
            f"/v1/governance-tags/resources/import/generate-upload-url"
        )
        payload = {
            "accountId": account_id,
            "compressed": compressed,
            "mode": "replace",
        }

        with httpx.Client(timeout=300.0) as client:
            response = client.post(url, json=payload, headers=headers)

            if response.status_code != 200:
                raise Exception(
                    f"Failed to get upload URL: {response.status_code}"
                )

            result = response.json()
            upload_url = result.get("uploadUrl")
            upload_id = result.get("uploadId")

            if not upload_url or not upload_id:
                raise Exception("Invalid upload URL response")

            log_timing(f"UPLOAD: Got presigned URL for upload {upload_id}")

            # Step 2: Upload the file
            with open(csv_path, "rb") as f:
                file_data = f.read()

            upload_headers = {"Content-Type": "text/csv"}
            if compressed:
                upload_headers["Content-Encoding"] = "gzip"

            upload_response = client.put(
                upload_url, content=file_data, headers=upload_headers
            )

            if upload_response.status_code not in (200, 201):
                raise Exception(
                    f"Failed to upload file: {upload_response.status_code}"
                )

            log_timing(f"UPLOAD: File uploaded successfully (upload_id={upload_id})")
            return upload_id

    # ------------------------------------------------------------------
    # Import Monitoring
    # ------------------------------------------------------------------

    def monitor_import(self, upload_id: str) -> Generator[Dict, None, None]:
        """
        Poll the import status until it completes, fails, or is cancelled.

        Yields progress update dictionaries.
        """
        self._ensure_authenticated()

        # Get any account key for monitoring
        aggregate_accounts, individual_accounts = self.get_accounts()
        if not individual_accounts and not aggregate_accounts:
            raise Exception("No accounts available")

        account_key = (individual_accounts or aggregate_accounts)[0].get("accountKey")
        headers = self._build_headers(account_key)

        url = (
            f"{self.base_url.replace('/v1', '')}"
            f"/v1/governance-tags/resources/import/status/{upload_id}"
        )

        with httpx.Client(timeout=30.0) as client:
            while True:
                response = client.get(url, headers=headers)

                if response.status_code != 200:
                    raise Exception(
                        f"Failed to get import status: {response.status_code}"
                    )

                status = response.json()
                yield status

                state = status.get("state", "")
                if state in ("COMPLETED", "FAILED", "CANCELLED"):
                    break

                time.sleep(5)

    # ------------------------------------------------------------------
    # Date Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def get_week_date_range(week_number: int, year: int) -> Tuple[str, str]:
        """
        Get the Monday date for a specific ISO week number.

        For weekly granularity API calls, we send the same date for start and end.
        The API returns data for the entire week containing that date.

        Returns:
            Tuple of (start_date, end_date) as YYYY-MM-DD strings.
        """
        import datetime as dt

        week_date = dt.date.fromisocalendar(year, week_number, 1)  # Monday
        date_str = week_date.strftime("%Y-%m-%d")
        return date_str, date_str


# Global instance
umbrella_client = UmbrellaClient()
