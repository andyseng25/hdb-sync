"""
Automated sync script for URA private residential transactions, District 13
only (postal sectors 34-37: Macpherson, Braddell, Potong Pasir, Bidadari,
Joo Seng, parts of Bartley). Runs on GitHub's servers on a schedule, same
pattern as sync_hdb_data.py.

Sends to the standalone "URA District 13 Private Property" plugin, not the
HDB plugin. Different endpoint, different secret, no shared code between
the two on the WordPress side.

v5 note: the first live import stored 2,553 of 2,636 records, 83 skipped
as "already known" by the dedup key, on the very first run, when the
table started empty. That means those 83 weren't stale re-sends, the
dedup key found them identical to something else in the very same pull.
71 of the 83 are landed, only 12 are condo, a 22x rate difference, too
lopsided to be random. report_collisions() below prints exactly which
records share a hash so we can see with our own eyes whether these are
genuinely different transactions being wrongly merged (a real gap in
what free URA data can disambiguate for standalone landed houses) or the
same transaction legitimately appearing twice in URA's own feed (the
dedup key working correctly). No fix is applied yet, this version only
adds visibility. Endpoint history for anyone picking this up fresh:
eservice.ura.gov.sg is the correct host, www.ura.gov.sg's old API path
is dead (migrated to a general Isomer-built site), and both auth calls
need browser-style headers, see earlier commit history for why.

v6 note: diagnostic-only change, one print statement added to main() to
dump a raw project entry and inspect its actual field names before
touching enrich(). See the ongoing thread on why street looks wrong for
named-estate landed transactions (Sennett Estate, MacPherson Garden
Estate, Braddell Heights Estate) specifically. enrich() currently reads
street from project_entry, not from the individual txn, on the theory
that named estates span multiple real streets under one project and the
per-transaction street lives somewhere in txn instead. Not fixed yet,
need to see the real JSON shape first.

Reads secrets from environment variables, set as GitHub Actions secrets.
Never hardcode keys in this file.
"""

import os
import json
import hashlib
import requests
from collections import defaultdict

TOKEN_URL = "https://eservice.ura.gov.sg/uraDataService/insertNewToken/v1"
DATA_URL = "https://eservice.ura.gov.sg/uraDataService/invokeUraDS/v1"
SITE_URL = "https://andyseng.me"
IMPORT_ENDPOINT = f"{SITE_URL}/wp-json/ura/v1/import"

URA_ACCESS_KEY = os.environ["URA_ACCESS_KEY"]
URA_WP_IMPORT_KEY = os.environ["URA_WP_IMPORT_KEY"]

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"

TARGET_DISTRICT = "13"
MAX_BATCHES = 6
BATCH_SIZE = 300
SQM_TO_SQFT = 10.7639

LANDED_KEYWORDS = ("terrace", "semi-detached", "detached", "bungalow")

SALE_TYPE_LABELS = {
    "1": "New Sale",
    "2": "Sub Sale",
    "3": "Resale",
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-SG,en;q=0.9",
}


def _debug_body(response, label):
    print(f"{label} status code: {response.status_code}")
    body = response.text or "(empty body)"
    print(f"{label} response body (first 500 chars):\n{body[:500]}")


def get_token():
    headers = {**BROWSER_HEADERS, "AccessKey": URA_ACCESS_KEY}
    response = requests.get(TOKEN_URL, headers=headers, timeout=30)
    if response.status_code != 200:
        _debug_body(response, "Token request")
        raise RuntimeError(f"Token request returned HTTP {response.status_code}, see body above.")
    try:
        data = response.json()
    except ValueError:
        _debug_body(response, "Token request")
        raise RuntimeError("Token endpoint returned HTTP 200 but the body wasn't JSON.")
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

    for r in records:
        if r["district"] != TARGET_DISTRICT:
            raise RuntimeError(
                f"Filter mismatch: kept a record for district {r['district']}. "
                "Stopping rather than risk importing the wrong data."
            )

    return records


def report_collisions(records):
    """
    Groups records by dedup hash and prints any group with more than one
    record. Purely diagnostic, changes nothing about what gets sent.
    """
    groups = defaultdict(list)
    for r in records:
        groups[r["txn_hash"]].append(r)

    collisions = {h: rs for h, rs in groups.items() if len(rs) > 1}
    if not collisions:
        print("\nNo hash collisions in this pull. Every record has a unique key.")
        return

    dropped = sum(len(rs) - 1 for rs in collisions.values())
    landed_dropped = sum(len(rs) - 1 for rs in collisions.values() if rs[0]["category"] == "landed")
    condo_dropped = dropped - landed_dropped
    print(f"\n{len(collisions)} hash groups share a key "
          f"({dropped} records would be treated as duplicates: "
          f"{landed_dropped} landed, {condo_dropped} condo).")
    print("First 8 groups, so we can see if these look like the same sale twice "
          "or two different sales that happen to share every field we can see:")
    for h, rs in list(collisions.items())[:8]:
        print(f"\n  Hash {h[:12]}... ({len(rs)} records):")
        for r in rs:
            print(f"    {r['category']:<7} {r['street']:<28} {r['area_sqm']:>7.1f} sqm  "
                  f"${r['price']:>13,.0f}  {r['contract_date']}  {r['type_of_sale']}")


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

    print("\nAll project entries with street containing 'ALJUNIED', District 13 transactions only:")
    aljunied_entries = [
        p for p in all_projects
        if 'ALJUNIED' in p.get('street', '').upper()
        and any(t.get('district') == TARGET_DISTRICT for t in p.get('transaction', []))
    ]
    print(f"{len(aljunied_entries)} distinct project entries found.")
    for p in aljunied_entries:
        d13_txns = [t for t in p.get('transaction', []) if t.get('district') == TARGET_DISTRICT]
        print(f"\n  project={p.get('project')!r}  street={p.get('street')!r}  "
              f"{len(d13_txns)} D13 transaction(s) in this entry:")
        for t in d13_txns:
            print(f"    {t.get('contractDate')}  ${t.get('price')}  {t.get('area')} sqm  {t.get('typeOfSale')}")

    records = filter_and_enrich(all_projects)
    landed = [r for r in records if r["category"] == "landed"]
    condo = [r for r in records if r["category"] == "condo"]
    print(f"\nDistrict 13 transactions: {len(records)} "
          f"({len(condo)} condo/apartment, {len(landed)} landed)")

    report_collisions(records)

    if DRY_RUN:
        print("\nDRY RUN, nothing sent.")
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
