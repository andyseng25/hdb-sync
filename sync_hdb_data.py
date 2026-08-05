"""
Automated sync script, runs on GitHub's servers on a schedule.

Checks the last few months of HDB resale data (to catch transactions
that register late, after their month was first published), computes
PSF and remaining lease in months, and pushes any new records to the
secured WordPress endpoint. Already-known records are silently
skipped by the endpoint itself (via the unique api_id key), so
re-checking recent months on every run is always safe, never creates
duplicates.

Reads secrets from environment variables, set as GitHub Actions
secrets. Never hardcode keys in this file.
"""

import os
import json
import requests
from datetime import datetime, timezone

DATASET_ID = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
API_URL = "https://data.gov.sg/api/action/datastore_search"
SITE_URL = "https://andyseng.me"

DATA_GOV_API_KEY = os.environ["DATA_GOV_API_KEY"]
WP_IMPORT_KEY = os.environ["WP_IMPORT_KEY"]

MONTHS_TO_CHECK = 3   # re-check the last N months to catch late registrations
BATCH_SIZE = 300      # records sent to WordPress per request, avoids PHP timeouts
SQM_TO_SQFT = 10.7639


def recent_months(n):
    months = []
    today = datetime.now(timezone.utc)
    year, month = today.year, today.month
    for _ in range(n):
        months.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return months


def parse_lease_to_months(text):
    years, months = 0, 0
    parts = text.split()
    for i, part in enumerate(parts):
        if part.startswith("year"):
            years = int(parts[i - 1])
        if part.startswith("month"):
            months = int(parts[i - 1])
    return years * 12 + months


def fetch_month(month_str):
    records = []
    offset = 0
    limit = 1000
    headers = {"x-api-key": DATA_GOV_API_KEY}
    filters = json.dumps({"month": month_str})

    while True:
        params = {
            "resource_id": DATASET_ID,
            "limit": limit,
            "offset": offset,
            "filters": filters,
        }
        response = requests.get(API_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data.get("success"):
            raise RuntimeError(f"API reported failure: {data}")

        batch = data["result"]["records"]

        # Safety check: confirm the filter actually worked, don't trust it blindly.
        for r in batch:
            if r["month"] != month_str:
                raise RuntimeError(
                    f"Filter mismatch: asked for {month_str}, got {r['month']}. "
                    "Stopping rather than risk importing the wrong data."
                )

        records.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return records


def enrich(record):
    floor_area_sqm = float(record["floor_area_sqm"])
    resale_price = float(record["resale_price"])
    psf = resale_price / (floor_area_sqm * SQM_TO_SQFT)
    lease_months = parse_lease_to_months(record["remaining_lease"])

    return {
        "api_id": int(record["_id"]),
        "month": record["month"],
        "town": record["town"],
        "flat_type": record["flat_type"],
        "block": record["block"],
        "street_name": record["street_name"],
        "storey_range": record["storey_range"],
        "floor_area_sqm": floor_area_sqm,
        "flat_model": record["flat_model"],
        "lease_commence_date": int(float(record["lease_commence_date"])),
        "remaining_lease": record["remaining_lease"],
        "resale_price": resale_price,
        "psf": psf,
        "remaining_lease_months": lease_months,
    }


def send_batch(records):
    url = f"{SITE_URL}/wp-json/hdb/v1/import"
    headers = {"x-hdb-import-key": WP_IMPORT_KEY}
    response = requests.post(url, json=records, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def main():
    months = recent_months(MONTHS_TO_CHECK)
    print(f"Checking months: {', '.join(months)}")

    all_records = []
    for month_str in months:
        records = fetch_month(month_str)
        print(f"  {month_str}: {len(records)} records from API")
        all_records.extend(records)

    enriched = [enrich(r) for r in all_records]

    total_inserted = 0
    total_skipped = 0

    for i in range(0, len(enriched), BATCH_SIZE):
        batch = enriched[i:i + BATCH_SIZE]
        result = send_batch(batch)
        total_inserted += result["inserted"]
        total_skipped += result["skipped_duplicates"]
        print(f"  Batch {i // BATCH_SIZE + 1}: inserted {result['inserted']}, "
              f"skipped {result['skipped_duplicates']}")

    print(f"\nDone. New records inserted: {total_inserted}. "
          f"Already known: {total_skipped}.")

    if total_inserted == 0:
        print("No new transactions found. Normal between HDB's monthly releases.")


if __name__ == "__main__":
    main()
