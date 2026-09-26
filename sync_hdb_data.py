"""
Automated sync script, runs on GitHub's servers on a schedule.

Checks the last few months of HDB resale data (to catch transactions
that register late, after their month was first published), computes
PSF and remaining lease in months, and pushes any new records to the
secured WordPress endpoint. Already-known records are silently
skipped by the endpoint itself (via the unique fingerprint key), so
re-checking recent months on every run is always safe, never creates
duplicates.

v2 (26 Sep 2026): dedup key changed from data.gov.sg's _id to a
fingerprint of the deal's own details. The _id is only a row number
and gets reassigned when data.gov.sg reloads its sorted table, which
caused new deals to be skipped as "duplicates" (e.g. the $1.701M and
$1.72M Pinnacle@Duxton deals landed on IDs already held by old
Aljunied Cres deals). Identical twin deals in the same month get a
running counter (#1, #2) so neither is dropped.

Reads secrets from environment variables, set as GitHub Actions
secrets. Never hardcode keys in this file.
"""

import os
import sys
import json
import time
import hashlib
import requests
from collections import defaultdict
from datetime import datetime, timezone

DATASET_ID = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
API_URL = "https://data.gov.sg/api/action/datastore_search"
SITE_URL = "https://andyseng.me"

DATA_GOV_API_KEY = os.environ["DATA_GOV_API_KEY"]
WP_IMPORT_KEY = os.environ["WP_IMPORT_KEY"]

MONTHS_TO_CHECK = 3   # re-check the last N months to catch late registrations

# ONE-TIME USE: set to True, commit, run the workflow manually once to
# rebuild the whole table since Jan 2017, then set back to False and
# commit again. Leaving it True makes every scheduled run a slow full
# re-import (harmless, no duplicates, but wasteful).
FULL_RESYNC = True
FIRST_MONTH = "2017-01"
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


def all_months_since(first):
    months = []
    year, month = int(first[:4]), int(first[5:7])
    today = datetime.now(timezone.utc)
    while (year, month) <= (today.year, today.month):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            month = 1
            year += 1
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
        response = None
        for attempt in range(5):
            response = requests.get(API_URL, params=params, headers=headers, timeout=30)
            if response.status_code != 429:
                break
            wait = 10 * (attempt + 1)
            print(f"    Rate limited by data.gov.sg, waiting {wait}s...")
            time.sleep(wait)
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


def base_key(record):
    """The deal's own details, normalised so formatting quirks
    (e.g. "107.00" vs "107.0") can't make one deal look like two."""
    return "|".join([
        record["month"].strip(),
        record["town"].strip().upper(),
        record["flat_type"].strip().upper(),
        record["block"].strip().upper(),
        record["street_name"].strip().upper(),
        record["storey_range"].strip().upper(),
        f"{float(record['floor_area_sqm']):.2f}",
        record["flat_model"].strip().upper(),
        str(int(float(record["lease_commence_date"]))),
        record["remaining_lease"].strip().lower(),
        f"{float(record['resale_price']):.2f}",
    ])


def add_fingerprints(records):
    """Fingerprint = sha256 of the deal's details plus a running count,
    so two genuinely identical deals in the same month stay separate.
    Must be called on a FULL month of records, which fetch_month returns."""
    seen = defaultdict(int)
    for r in records:
        key = base_key(r)
        seen[key] += 1
        r["_fingerprint"] = hashlib.sha256(
            f"{key}#{seen[key]}".encode("utf-8")
        ).hexdigest()
    return records


def enrich(record):
    floor_area_sqm = float(record["floor_area_sqm"])
    resale_price = float(record["resale_price"])
    psf = resale_price / (floor_area_sqm * SQM_TO_SQFT)
    lease_months = parse_lease_to_months(record["remaining_lease"])

    return {
        "fingerprint": record["_fingerprint"],
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


def fetch_db_counts():
    url = f"{SITE_URL}/wp-json/hdb/v1/counts"
    headers = {"x-hdb-import-key": WP_IMPORT_KEY}
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def verify(api_counts):
    """Compare what data.gov.sg has against what WordPress holds, for
    every month this run covered. Fails the whole run (red X in GitHub,
    email alert) if any month is short, so a gap can never go unnoticed."""
    db_counts = fetch_db_counts()
    missing, extra = [], []
    for month, api_n in api_counts.items():
        db_n = db_counts.get(month, 0)
        if db_n < api_n:
            missing.append((month, api_n, db_n))
        elif db_n > api_n:
            extra.append((month, api_n, db_n))

    print(f"\nVERIFY: checked {len(api_counts)} months against WordPress.")
    for month, api_n, db_n in extra:
        print(f"  NOTE {month}: API {api_n}, site {db_n} "
              f"(site has {db_n - api_n} more, likely an HDB correction)")
    if missing:
        for month, api_n, db_n in missing:
            print(f"  MISSING {month}: API {api_n}, site {db_n} "
                  f"({api_n - db_n} deals short)")
        print("VERIFY FAILED. Re-run the workflow; missing deals will be added.")
        sys.exit(1)
    print("VERIFY PASSED. Every month on the site matches data.gov.sg.")


def main():
    if FULL_RESYNC:
        months = all_months_since(FIRST_MONTH)
        print(f"FULL RESYNC: {len(months)} months, {months[0]} to {months[-1]}")
    else:
        months = recent_months(MONTHS_TO_CHECK)
        print(f"Checking months: {', '.join(months)}")

    all_records = []
    api_counts = {}
    for month_str in months:
        records = add_fingerprints(fetch_month(month_str))
        print(f"  {month_str}: {len(records)} records from API")
        api_counts[month_str] = len(records)
        all_records.extend(records)
        if FULL_RESYNC:
            time.sleep(1)  # be polite to data.gov.sg on the big run

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

    # Guard: every month we meant to cover must have been fetched.
    not_fetched = [m for m in months if m not in api_counts]
    if not_fetched:
        print(f"FETCH GAP: these months were never fetched: {not_fetched}")
        sys.exit(1)

    verify(api_counts)


if __name__ == "__main__":
    main()
