"""
Automated sync script for URA private residential transactions, District 13
only (postal sectors 34-37: Macpherson, Braddell, Potong Pasir, Bidadari,
Joo Seng, parts of Bartley). Runs on GitHub's servers on a schedule, same
pattern as sync_hdb_data.py.

Sends to the standalone "URA District 13 Private Property" plugin, not the
HDB plugin. Different endpoint, different secret, no shared code between
the two on the WordPress side.

v3 note: the first live run against eservice.ura.gov.sg/v1 returned a
non-JSON response. Switching to the older www.ura.gov.sg endpoint got a
clean HTTP 403 with a CloudFront "request blocked" page, meaning an edge
layer in front of URA is filtering the request before it reaches URA's
own service, most likely on request fingerprint (default Python UA) or
origin IP (GitHub's cloud ranges), not the AccessKey itself. This version
sends realistic browser headers to test the fingerprint theory first,
since it's the cheaper thing to rule out before assuming a geography
block that would need a different fix entirely.

Reads secrets from environment variables, set as GitHub Actions secrets.
Never hardcode keys in this file.
"""

import os
import json
import hashlib
import requests

TOKEN_URL = "https://www.ura.gov.sg/uraDataService/insertNewToken.action"
DATA_URL = "https://www.ura.gov.sg/uraDataService/invokeUraDS"
SITE_URL = "https://andyseng.me"
IMPORT_ENDPOINT = f"{SITE_URL}/wp-json/ura/v1/import"

URA_ACCESS_KEY = os.environ["URA_ACCESS_KEY"]
URA_WP_IMPORT_KEY = os.environ["URA_WP_IMPORT_KEY"]

# Defaults to dry (safe) if not set. The workflow file controls this
# explicitly, see sync-ura-d13.yml.
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"

TARGET_DISTRICT = "13"
MAX_BATCHES = 6          # safety cap, see fetch_all() for why this isn't hardcoded to a fixed count
BATCH_SIZE = 300         # records sent to WordPress per request, matches the HDB script
SQM_TO_SQFT = 10.7639

LANDED_KEYWORDS = ("terrace", "semi-detached", "detached", "bungalow")

SALE_TYPE_LABELS = {
    "1": "New Sale",
    "2": "Sub Sale",
    "3": "Resale",
}

# Default python-requests UA is an easy, common bot-filter trigger,
# separate from any IP/geography question. Testing that first.
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-SG,en;q=0.9",
}


def _debug_body(response, label):
    """Print enough of a failed response to diagnose it, without ever
    printing the AccessKey or Token we sent, only what came back."""
    print(f"{label} status code: {response.status_code}")
    body = response.text or "(empty body)"
    print(f"{label} response body (first 500 chars):\n{body[:500]}")


def get_token():
    headers = {**BROWSER_HEADERS, "AccessKey": URA_ACCESS_KEY}
    response = requests.get(TOKEN_URL, headers=headers, timeout=30)

    if response.status_code != 200:
        _debug_body(response, "Token request")
        raise RuntimeError(
            f"Token request returned HTTP {response.status_code}, see body above. "
            "If the body looks like a CloudFront/WAF block page rather than "
            "anything mentioning your key, this is an edge block, not a bad "
            "AccessKey, and needs a different fix (see chat)."
        )

    try:
        data = response.json()
    except ValueError:
        _debug_body(response, "Token request")
        raise RuntimeError(
            "Token endpoint returned HTTP 200 but the body wasn't JSON. "
            "See the printed body above for what it actually sent."
        )

    if data.get("Status") != "Success":
        raise RuntimeError(f"Token request failed: {data}")
    return data["Result"]


def fetch_batch(batch_num, token):
    headers = {**BROWSER_HEADERS, "AccessKey": URA_ACCESS_KEY, "Token": token}
    params = {"service": "PMI_Resi_Transaction", "batch": batch_num}
    response = requests.get(DATA_URL, params=params, headers=headers, timeout=60)

    if response.status_code != 200:
        _debug_body(response, f"Batch {batch_num} request")
        return None

    try:
        data = response.json()
    except ValueError:
        _debug_body(response, f"Batch {batch_num} request")
        return None

    if data.get("Status") != "Success":
        print(f"  Batch {batch_num}: API reported failure: {data}")
        return None

    return data.get("Result", [])


def fetch_all(token):
    """
    URA splits the 60 month transaction history across a small number of
    batches, but the documented example only ever shows batch=1, the
    actual count isn't stated anywhere public. Rather than hardcode a
    guess, keep asking for the next batch number until one comes back
    empty or errors, capped at MAX_BATCHES so a bad response can't loop
    forever.
    """
    all_projects = []
    for batch_num in range(1, MAX_BATCHES + 1):
        result = fetch_batch(batch_num, token)
        if not result:
            print(f"  Batch {batch_num}: empty or unavailable, stopping.")
            break
        print(f"  Batch {batch_num}: {len(result)} project entries")
        all_projects.extend(result)
    return all_projects


def classify_category(property_type):
    text = property_type.lower()
    if any(keyword in text for keyword in LANDED_KEYWORDS):
        return "landed"
    return "condo"


def parse_contract_date(text):
    # Format is MMYY, e.g. "0715" -> July 2015. All URA data covers 2015
    # onward, so a fixed "20xx" prefix is safe here.
    month = int(text[0:2])
    year = 2000 + int(text[2:4])
    return f"{year:04d}-{month:02d}"


def make_txn_hash(project, district, street, propertyType, typeOfArea,
                   tenure, floorRange, contractDate, area, price,
                   typeOfSale, noOfUnits):
    raw = "|".join([
        project, district, street, propertyType, typeOfArea, tenure,
        floorRange, contractDate, str(area), str(price), typeOfSale,
        str(noOfUnits),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def enrich(project_entry, txn):
    district = txn.get("district", "")
    property_type = txn.get("propertyType", "")
    area_sqm = float(txn.get("area", 0))
    price = float(txn.get("price", 0))
    psf = price / (area_sqm * SQM_TO_SQFT) if area_sqm else 0
    contract_date = parse_contract_date(txn.get("contractDate", "0100"))
    type_of_sale_code = txn.get("typeOfSale", "")
    type_of_sale = SALE_TYPE_LABELS.get(type_of_sale_code, "Unknown")
    type_of_area = txn.get("typeOfArea", "")
    tenure = txn.get("tenure", "")
    floor_range = txn.get("floorRange", "")
    no_of_units = int(txn.get("noOfUnits", 1) or 1)
    project = project_entry.get("project", "")
    street = project_entry.get("street", "")
    market_segment = project_entry.get("marketSegment", "")

    txn_hash = make_txn_hash(
        project, district, street, property_type, type_of_area, tenure,
        floor_range, contract_date, area_sqm, price, type_of_sale_code,
        no_of_units,
    )

    return {
        "txn_hash": txn_hash,
        "project": project,
        "street": street,
        "district": district,
        "market_segment": market_segment,
        "property_type": property_type,
        "category": classify_category(property_type),
        "type_of_area": type_of_area,
        "area_sqm": area_sqm,
        "price": price,
        "psf": psf,
        "floor_range": floor_range,
        "tenure": tenure,
        "contract_date": contract_date,
        "type_of_sale": type_of_sale,
        "no_of_units": no_of_units,
    }


def filter_and_enrich(all_projects):
    records = []
    for project_entry in all_projects:
        for txn in project_entry.get("transaction", []):
            if txn.get("district") != TARGET_DISTRICT:
                continue
            records.append(enrich(project_entry, txn))

    # Safety check, belt and suspenders: confirm the filter actually
    # worked, don't trust it blindly. Same philosophy as sync_hdb_data.py.
    for r in records:
        if r["district"] != TARGET_DISTRICT:
            raise RuntimeError(
                f"Filter mismatch: kept a record for district {r['district']}. "
                "Stopping rather than risk importing the wrong data."
            )

    return records


def send_batch(records):
    headers = {"x-ura-import-key": URA_WP_IMPORT_KEY}
    response = requests.post(IMPORT_ENDPOINT, json=records, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def main():
    print("Fetching daily token...")
    token = get_token()
    print("Token received.")

    print("Fetching URA private residential transactions...")
    all_projects = fetch_all(token)
    print(f"Total project entries returned: {len(all_projects)}")

    records = filter_and_enrich(all_projects)
    landed = [r for r in records if r["category"] == "landed"]
    condo = [r for r in records if r["category"] == "condo"]
    print(f"District 13 transactions: {len(records)} "
          f"({len(condo)} condo/apartment, {len(landed)} landed)")

    if DRY_RUN:
        print("\nDRY RUN, nothing sent. Sample records:")
        for r in records[:5]:
            print(f"  {r['contract_date']}  {(r['project'] or r['street']):<30} "
                  f"{r['property_type']:<20} ${r['price']:,.0f}  {r['psf']:.0f} psf")
        print(f"\n{len(records)} records would be sent to WordPress. "
              "Set DRY_RUN to false in the workflow file once this looks right.")
        return

    total_inserted = 0
    total_skipped = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        result = send_batch(batch)
        total_inserted += result["inserted"]
        total_skipped += result["skipped_duplicates"]
        print(f"  Batch {i // BATCH_SIZE + 1}: inserted {result['inserted']}, "
              f"skipped {result['skipped_duplicates']}")

    print(f"\nDone. New records inserted: {total_inserted}. "
          f"Already known: {total_skipped}.")


if __name__ == "__main__":
    main()
