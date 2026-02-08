"""
VTagger Upload Service.

Converts tagged JSONL output to CSV format and uploads vtags
to the Umbrella API. Handles grouping by payer account and
dynamic dimension columns.

Ported from BPVtagger with dynamic vtag columns from dimensions dict.
"""

import csv
import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.config import settings
from app.database import execute_query, execute_write, get_db
from app.services.agent_logger import log_timing


class VtagUploadService:
    """Handles conversion and upload of vtag data."""

    def __init__(self):
        self._cancelled = False

    def cancel(self):
        """Cancel the current upload operation."""
        self._cancelled = True

    def is_cancelled(self) -> bool:
        """Check if cancellation was requested."""
        return self._cancelled

    def convert_jsonl_to_csv(
        self,
        jsonl_file: str,
        csv_file: Optional[str] = None,
    ) -> str:
        """
        Convert a JSONL tagged output file to CSV format for upload.

        Dynamic vtag columns: iterates over all keys in record["dimensions"].
        For each dim_name, dim_value: if dim_value != "Unallocated",
        creates vtag entry as "dim_name:dim_value".

        Args:
            jsonl_file: Path to input JSONL file.
            csv_file: Optional output CSV path. Defaults to same name with .csv extension.

        Returns:
            Path to the generated CSV file.
        """
        if csv_file is None:
            csv_file = jsonl_file.rsplit(".", 1)[0] + "_upload.csv"

        log_timing(f"Converting JSONL to CSV: {jsonl_file}")

        # First pass: discover all dimension names and collect records
        all_dim_names = set()
        records = []

        with open(jsonl_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    records.append(record)
                    dims = record.get("dimensions", {})
                    all_dim_names.update(dims.keys())
                except json.JSONDecodeError:
                    continue

        sorted_dim_names = sorted(all_dim_names)

        # Write CSV with dynamic vtag columns
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)

            # Header: resourceid, linkedaccid, payeraccount, vtags
            writer.writerow(["resourceid", "linkedaccid", "payeraccount", "vtags"])

            for record in records:
                resource_id = record.get("resourceid", "")
                linked_acc = record.get("linkedaccid", "")
                payer_acc = record.get("payeraccount", "")

                dims = record.get("dimensions", {})
                vtags = []
                for dim_name, dim_value in dims.items():
                    if dim_value and dim_value != "Unallocated":
                        vtags.append(f"{dim_name}:{dim_value}")

                vtag_str = "|".join(vtags) if vtags else ""

                writer.writerow([resource_id, linked_acc, payer_acc, vtag_str])

        log_timing(f"CSV generated: {csv_file} ({len(records)} records)")
        return csv_file

    def group_jsonl_by_payer_account(
        self,
        jsonl_file: str,
        output_dir: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Group JSONL records by payer account and write separate CSV files.

        Dynamic vtag columns from dimensions dict.

        Args:
            jsonl_file: Path to input JSONL file.
            output_dir: Directory for output CSVs. Defaults to same directory as input.

        Returns:
            Dict mapping payer_account -> csv_file_path.
        """
        if output_dir is None:
            output_dir = os.path.dirname(jsonl_file)

        log_timing(f"Grouping JSONL by payer account: {jsonl_file}")

        # Group records by payer account
        groups: Dict[str, List[Dict]] = defaultdict(list)

        with open(jsonl_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    payer = record.get("payeraccount", "unknown")
                    groups[payer].append(record)
                except json.JSONDecodeError:
                    continue

        # Write a CSV per payer account
        output_files = {}
        base_name = os.path.splitext(os.path.basename(jsonl_file))[0]

        for payer, records in groups.items():
            safe_payer = payer.replace("/", "_").replace("\\", "_")
            csv_path = os.path.join(output_dir, f"{base_name}_{safe_payer}.csv")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["resourceid", "linkedaccid", "payeraccount", "vtags"])

                for record in records:
                    resource_id = record.get("resourceid", "")
                    linked_acc = record.get("linkedaccid", "")
                    payer_acc = record.get("payeraccount", "")

                    dims = record.get("dimensions", {})
                    vtags = []
                    for dim_name, dim_value in dims.items():
                        if dim_value and dim_value != "Unallocated":
                            vtags.append(f"{dim_name}:{dim_value}")

                    vtag_str = "|".join(vtags) if vtags else ""
                    writer.writerow([resource_id, linked_acc, payer_acc, vtag_str])

            output_files[payer] = csv_path
            log_timing(f"  Payer {payer}: {len(records)} records -> {csv_path}")

        log_timing(f"Grouped into {len(output_files)} payer account files")
        return output_files

    def upload_vtags(
        self,
        umbrella_client,
        account_key: str,
        csv_file: str,
        description: str = "",
    ) -> Dict[str, Any]:
        """
        Upload a CSV vtag file to the Umbrella API.

        Args:
            umbrella_client: Client for Umbrella API.
            account_key: Umbrella account key.
            csv_file: Path to CSV file to upload.
            description: Optional description for the upload.

        Returns:
            Dict with upload results.
        """
        self._cancelled = False
        log_timing(f"Uploading vtags: {csv_file}")

        # Count records in CSV
        vtag_count = 0
        with open(csv_file, "r") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for _ in reader:
                vtag_count += 1

        # Create upload record
        upload_id = execute_write(
            """INSERT INTO vtag_uploads
            (upload_date, file_name, vtag_count, status, created_at)
            VALUES (?, ?, ?, 'uploading', CURRENT_TIMESTAMP)""",
            (
                datetime.now().strftime("%Y-%m-%d"),
                os.path.basename(csv_file),
                vtag_count,
            ),
        )

        try:
            if self._cancelled:
                execute_write(
                    """UPDATE vtag_uploads SET status = 'cancelled',
                    updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
                    (upload_id,),
                )
                return {"upload_id": upload_id, "status": "cancelled"}

            # Read CSV content for upload
            with open(csv_file, "r") as f:
                csv_content = f.read()

            # Upload via umbrella client
            response = umbrella_client.upload_virtual_tags(
                account_key=account_key,
                vtag_csv_content=csv_content,
            )

            # Update upload record
            status = "completed" if response.get("status", "").lower() in ("completed", "complete", "success") else "submitted"
            execute_write(
                """UPDATE vtag_uploads SET
                status = ?,
                api_response = ?,
                updated_at = CURRENT_TIMESTAMP
                WHERE id = ?""",
                (
                    status,
                    json.dumps(response),
                    upload_id,
                ),
            )

            result = {
                "upload_id": upload_id,
                "status": status,
                "vtag_count": vtag_count,
                "file_name": os.path.basename(csv_file),
                "api_response": response,
            }

            log_timing(f"Upload {status}: {vtag_count} vtags")
            return result

        except Exception as e:
            execute_write(
                """UPDATE vtag_uploads SET
                status = 'error',
                error_message = ?,
                updated_at = CURRENT_TIMESTAMP
                WHERE id = ?""",
                (str(e), upload_id),
            )
            log_timing(f"Upload error: {e}")
            raise

    def upload_from_jsonl(
        self,
        umbrella_client,
        account_key: str,
        jsonl_file: str,
        group_by_payer: bool = False,
        description: str = "",
    ) -> Dict[str, Any]:
        """
        Convert JSONL to CSV and upload to Umbrella.

        Args:
            umbrella_client: Client for Umbrella API.
            account_key: Umbrella account key.
            jsonl_file: Path to JSONL file.
            group_by_payer: If True, group by payer account and upload separately.
            description: Optional description for the upload.

        Returns:
            Dict with upload results.
        """
        self._cancelled = False

        if group_by_payer:
            csv_files = self.group_jsonl_by_payer_account(jsonl_file)
            results = {}
            for payer, csv_path in csv_files.items():
                if self._cancelled:
                    break
                results[payer] = self.upload_vtags(
                    umbrella_client=umbrella_client,
                    account_key=account_key,
                    csv_file=csv_path,
                    description=f"{description} (payer: {payer})",
                )
            return {"status": "completed", "payer_uploads": results}
        else:
            csv_file = self.convert_jsonl_to_csv(jsonl_file)
            return self.upload_vtags(
                umbrella_client=umbrella_client,
                account_key=account_key,
                csv_file=csv_file,
                description=description,
            )

    def list_uploads(
        self, limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List recent vtag uploads."""
        rows = execute_query(
            """SELECT id, upload_date, file_name, vtag_count, status,
            error_message, created_at, updated_at
            FROM vtag_uploads
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?""",
            (limit, offset),
        )
        return [dict(r) for r in rows]

    def get_upload(self, upload_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific upload record."""
        rows = execute_query(
            "SELECT * FROM vtag_uploads WHERE id = ?", (upload_id,)
        )
        if rows:
            result = dict(rows[0])
            if result.get("api_response"):
                try:
                    result["api_response"] = json.loads(result["api_response"])
                except (json.JSONDecodeError, TypeError):
                    pass
            return result
        return None


# Global instance
vtag_upload_service = VtagUploadService()
