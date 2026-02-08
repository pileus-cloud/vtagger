"""
VTagger Umbrella Client.

Handles authenticated communication with the Umbrella Cost API:
  - Keycloak OAuth2 token acquisition
  - Account listing
  - Streaming asset/resource fetches with dynamic tag columns
  - Virtual tag uploads and import monitoring

Ported from BPVtagger with the following changes:
  - Removed hardcoded customTagValue_4-8 column mappings
  - Removed get_linked_account_names() (BP-specific)
  - Added dynamic tag_keys parameter for column building
  - Added vtag_filter_dimensions parameter for selective sync filtering
"""

import csv
import io
import json
import time
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, Generator, List, Optional, Set

import requests

from app.config import settings
from app.services.agent_logger import log_timing


class UmbrellaClient:
    """Client for the Umbrella Cost Management API."""

    def __init__(self):
        self.base_url = settings.umbrella_api_base
        self.token: Optional[str] = None
        self.token_expiry: float = 0.0
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    # ------------------------------------------------------------------
    # Authentication (Keycloak)
    # ------------------------------------------------------------------

    def authenticate(self, login_key: str) -> bool:
        """
        Authenticate with the Umbrella API via Keycloak.

        The login_key is exchanged for an OAuth2 bearer token.
        Returns True on success.
        """
        token_url = f"{self.base_url}/auth/token"
        try:
            resp = self._session.post(
                token_url,
                json={"loginKey": login_key},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            self.token = data.get("access_token") or data.get("token")
            if not self.token:
                log_timing("AUTH: No token in response")
                return False

            # Default expiry: 55 minutes (Keycloak tokens typically last 60 min)
            expires_in = data.get("expires_in", 3300)
            self.token_expiry = time.time() + expires_in

            self._session.headers["Authorization"] = f"Bearer {self.token}"
            log_timing("AUTH: Authenticated successfully")
            return True

        except requests.RequestException as exc:
            log_timing(f"AUTH: Authentication failed - {exc}")
            return False

    def is_authenticated(self) -> bool:
        """Check whether we hold a valid (non-expired) token."""
        return self.token is not None and time.time() < self.token_expiry

    def _ensure_auth(self):
        """Raise if not authenticated."""
        if not self.is_authenticated():
            raise RuntimeError("Not authenticated - call authenticate() first")

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------

    def get_accounts(self) -> List[Dict]:
        """
        Fetch the list of cloud accounts from Umbrella.

        Returns a list of account dicts with at minimum:
            accountKey, accountId, accountName, provider
        """
        self._ensure_auth()
        url = f"{self.base_url}/accounts"
        try:
            resp = self._session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            accounts = data if isinstance(data, list) else data.get("accounts", [])
            log_timing(f"ACCOUNTS: Fetched {len(accounts)} accounts")
            return accounts
        except requests.RequestException as exc:
            log_timing(f"ACCOUNTS: Failed - {exc}")
            raise

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
        Stream asset rows from the Umbrella export endpoint in batches.

        Parameters
        ----------
        account_key : str
            The Umbrella account key to query.
        start_date, end_date : str
            Date range in YYYY-MM-DD format.
        batch_size : int
            Number of rows per page.
        exclude_tagged : bool
            Legacy flag; kept for backwards compatibility.
        max_pages : int
            Stop after this many pages (0 = unlimited).
        progress_callback : callable, optional
            Called with (page_number, row_count) after each page.
        filter_mode : str
            "not_vtagged" to filter resources that have no virtual tags yet,
            "all" to return every resource.
        tag_keys : set of str, optional
            Physical tag keys to include as dynamic columns.
        vtag_filter_dimensions : list of str, optional
            Virtual tag dimension names for the governance filter.

        Yields
        ------
        list of dict
            A batch of resource rows.
        """
        self._ensure_auth()

        url = f"{self.base_url}/exports/resources/{account_key}"

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
        params.append(("columns", "costType"))
        params.append(("columns", "isUnblended"))

        # Pagination
        params.append(("pageSize", str(batch_size)))

        # ---- Build governance tags filter ----
        if filter_mode == "not_vtagged" and vtag_filter_dimensions:
            filter_parts = [f"{dim}: no_tag" for dim in vtag_filter_dimensions]
            filter_value = ",".join(filter_parts)
            params.append(("governance_tags_keys", filter_value))
        # If filter_mode == "not_vtagged" but no dimensions specified, skip filter
        # If filter_mode == "all", no governance filter applied

        page = 1
        total_fetched = 0

        while True:
            page_params = list(params) + [("page", str(page))]
            query_string = urllib.parse.urlencode(page_params)
            full_url = f"{url}?{query_string}"

            try:
                log_timing(f"FETCH: Page {page} for account {account_key}")
                resp = self._session.get(full_url, timeout=120)
                resp.raise_for_status()
            except requests.RequestException as exc:
                log_timing(f"FETCH: Page {page} failed - {exc}")
                raise

            data = resp.json()

            # The API may return rows under "data", "resources", or at top level
            rows = data if isinstance(data, list) else data.get("data") or data.get("resources", [])

            if not rows:
                log_timing(f"FETCH: No more rows at page {page}")
                break

            # Normalise tag columns: "customtags:KeyName" -> "Tag: KeyName"
            normalised_rows = []
            for row in rows:
                normalised = {}
                for k, v in row.items():
                    if k.startswith("customtags:"):
                        tag_name = k[len("customtags:"):]
                        normalised[f"Tag: {tag_name}"] = v if v else "no tag"
                    else:
                        normalised[k] = v
                normalised_rows.append(normalised)

            total_fetched += len(normalised_rows)
            log_timing(f"FETCH: Page {page} returned {len(normalised_rows)} rows (total: {total_fetched})")

            if progress_callback:
                progress_callback(page, total_fetched)

            yield normalised_rows

            # Check for end of data
            if len(rows) < batch_size:
                break

            # Check max pages limit
            if max_pages and page >= max_pages:
                log_timing(f"FETCH: Reached max_pages limit ({max_pages})")
                break

            page += 1

        log_timing(f"FETCH: Stream complete - {total_fetched} total rows across {page} pages")

    # ------------------------------------------------------------------
    # Virtual Tag Upload
    # ------------------------------------------------------------------

    def upload_virtual_tags(self, account_key: str, vtag_csv_content: str) -> Dict:
        """
        Upload a CSV of virtual tag assignments to Umbrella.

        Parameters
        ----------
        account_key : str
            The Umbrella account key.
        vtag_csv_content : str
            CSV content with columns: resourceId, vtagName, vtagValue, ...

        Returns
        -------
        dict
            API response with import job status.
        """
        self._ensure_auth()

        url = f"{self.base_url}/imports/vtags/{account_key}"

        # Count rows for logging
        reader = csv.reader(io.StringIO(vtag_csv_content))
        row_count = sum(1 for _ in reader) - 1  # subtract header

        log_timing(f"UPLOAD: Uploading {row_count} virtual tag rows to account {account_key}")

        try:
            resp = self._session.post(
                url,
                data=vtag_csv_content,
                headers={
                    "Content-Type": "text/csv",
                    "Authorization": f"Bearer {self.token}",
                },
                timeout=120,
            )
            resp.raise_for_status()
            result = resp.json()
            log_timing(f"UPLOAD: Success - {result}")
            return result

        except requests.RequestException as exc:
            log_timing(f"UPLOAD: Failed - {exc}")
            raise

    # ------------------------------------------------------------------
    # Import Monitoring
    # ------------------------------------------------------------------

    def monitor_import(self, account_key: str, import_id: str, poll_interval: int = 10, max_wait: int = 600) -> Dict:
        """
        Poll the import status until it completes or times out.

        Parameters
        ----------
        account_key : str
            The Umbrella account key.
        import_id : str
            The import job ID returned by upload_virtual_tags.
        poll_interval : int
            Seconds between status checks.
        max_wait : int
            Maximum seconds to wait.

        Returns
        -------
        dict
            Final import status.
        """
        self._ensure_auth()

        url = f"{self.base_url}/imports/vtags/{account_key}/{import_id}/status"
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > max_wait:
                log_timing(f"MONITOR: Timed out after {max_wait}s for import {import_id}")
                return {"status": "timeout", "import_id": import_id, "elapsed": elapsed}

            try:
                resp = self._session.get(url, timeout=30)
                resp.raise_for_status()
                status = resp.json()
            except requests.RequestException as exc:
                log_timing(f"MONITOR: Status check failed - {exc}")
                time.sleep(poll_interval)
                continue

            import_status = status.get("status", "").lower()
            log_timing(f"MONITOR: Import {import_id} status: {import_status} ({elapsed:.0f}s)")

            if import_status in ("completed", "complete", "success"):
                return status

            if import_status in ("failed", "error"):
                return status

            time.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Date Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def get_week_date_range(reference_date: Optional[str] = None) -> tuple:
        """
        Get the start and end dates for the week containing the reference date.

        Weeks run Monday to Sunday. If no reference_date is given, uses
        the current date.

        Parameters
        ----------
        reference_date : str, optional
            A date string in YYYY-MM-DD format.

        Returns
        -------
        tuple of (str, str)
            (week_start, week_end) in YYYY-MM-DD format.
        """
        if reference_date:
            dt = datetime.strptime(reference_date, "%Y-%m-%d")
        else:
            dt = datetime.now()

        # Monday = 0 in weekday()
        week_start = dt - timedelta(days=dt.weekday())
        week_end = week_start + timedelta(days=6)

        return week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")


# Global instance
umbrella_client = UmbrellaClient()
