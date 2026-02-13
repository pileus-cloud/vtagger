"""Download all assets for ISO week 1 of 2026 (Dec 29, 2025 - Jan 4, 2026) to CSV."""
import csv
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from app.services.umbrella_client import umbrella_client

OUTPUT = os.path.expanduser("~/Downloads/all_assets_week1_2026.csv")

# ISO week 1 of 2026: Monday Dec 29, 2025 - Sunday Jan 4, 2026
START_DATE = "2025-12-29"
END_DATE = "2026-01-04"

def main():
    print(f"Authenticating...")
    umbrella_client._ensure_authenticated()

    # Get the payer account key (932213950603)
    agg, ind = umbrella_client.get_accounts()
    all_accounts = ind + agg

    # Use payer account 9350 (932213950603)
    account_key = None
    for acc in all_accounts:
        if acc.get("accountId") == "932213950603":
            account_key = str(acc["accountKey"])
            break

    if not account_key:
        # Fallback to first account
        account_key = str(all_accounts[0]["accountKey"])
        print(f"Using fallback account key: {account_key}")
    else:
        print(f"Using account key: {account_key} (932213950603)")

    print(f"Fetching ALL assets for {START_DATE} to {END_DATE} (filter_mode=all)...")

    total = 0
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = None

        for batch in umbrella_client.fetch_assets_stream(
            account_key=account_key,
            start_date=START_DATE,
            end_date=END_DATE,
            batch_size=5000,
            filter_mode="all",
            progress_callback=lambda page, count: print(f"  Page {page}: {count} rows so far..."),
        ):
            for asset in batch:
                if writer is None:
                    # Use all columns from first record
                    fieldnames = list(asset.keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                writer.writerow(asset)
                total += 1

    print(f"\nDone! {total} assets written to {OUTPUT}")


if __name__ == "__main__":
    main()
