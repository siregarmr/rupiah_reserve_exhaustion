#!/usr/bin/env python3
# update_trade_data.py
# Fetches Indonesian export/import data from BPS API.
# API limit: maximum 20 HS codes per request. Chunk 99 codes into groups of 20.

import os
import csv
import sys
import re
import datetime
import requests
import time

BPS_BASE_URL = "https://webapi.bps.go.id/v1/api/dataexim"
BPS_API_KEY = os.environ.get("BPS_API_KEY")

# All two‑digit HS codes from 01 to 99
ALL_HS_CODES = [f"{i:02d}" for i in range(1, 100)]
CHUNK_SIZE = 20   # API maximum is 20, safe to use 20

HEADERS = ["year", "monthly_exports_usd", "monthly_imports_usd"]


def extract_month_number(bulan_str: str) -> int:
    """Extract month number from string like '[02] Februari'."""
    match = re.search(r'\[(\d{2})\]', bulan_str)
    if match:
        return int(match.group(1))
    month_map = {
        "januari": 1, "februari": 2, "maret": 3, "april": 4,
        "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
        "september": 9, "oktober": 10, "november": 11, "desember": 12
    }
    return month_map.get(bulan_str.strip().lower(), 0)


def fetch_monthly_totals_for_chunk(api_key: str, trade_type: str, year: int, hs_codes: list):
    """
    Fetch monthly totals for a chunk of HS codes (≤20).
    Returns dict {month: total_value} for this chunk.
    """
    sumber = "1" if trade_type == "export" else "2"
    hs_str = ";".join(hs_codes)
    url = (f"{BPS_BASE_URL}/sumber/{sumber}/kodehs/{hs_str}/jenishs/1"
           f"/tahun/{year}/periode/1/key/{api_key}")
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "OK":
            print(f"API error for chunk {hs_str[:10]}...: {data.get('status')}", file=sys.stderr)
            if "message" in data:
                print(f"  Message: {data['message']}", file=sys.stderr)
            return {}
        monthly = {}
        for item in data.get("data", []):
            bulan_str = item.get("bulan", "")
            month = extract_month_number(bulan_str)
            if month == 0:
                continue
            try:
                value = float(item.get("value", 0))
            except (ValueError, TypeError):
                continue
            monthly[month] = monthly.get(month, 0) + value
        return monthly
    except Exception as e:
        print(f"Error fetching chunk {hs_str[:20]}...: {e}", file=sys.stderr)
        return {}


def fetch_yearly_monthly_totals(api_key: str, trade_type: str, year: int):
    """Iterate over HS code chunks and combine results."""
    combined = {}
    for i in range(0, len(ALL_HS_CODES), CHUNK_SIZE):
        chunk = ALL_HS_CODES[i:i+CHUNK_SIZE]
        print(f"  Fetching {trade_type} for HS codes {chunk[0]}..{chunk[-1]} ({len(chunk)} codes)")
        chunk_data = fetch_monthly_totals_for_chunk(api_key, trade_type, year, chunk)
        for month, value in chunk_data.items():
            combined[month] = combined.get(month, 0) + value
        time.sleep(0.2)   # be polite to the API
    return combined


def main():
    if not BPS_API_KEY:
        print("ERROR: BPS_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    now = datetime.datetime.now()
    year = now.year
    current_month = now.month

    print(f"Fetching trade data for {year} (chunked by {CHUNK_SIZE} HS codes per request)...")
    exports = fetch_yearly_monthly_totals(BPS_API_KEY, "export", year)
    imports = fetch_yearly_monthly_totals(BPS_API_KEY, "import", year)

    if not exports or not imports:
        print("ERROR: Failed to retrieve export or import data.", file=sys.stderr)
        sys.exit(1)

    # Determine months with data in both exports and imports, up to current month
    common_months = set(exports.keys()) & set(imports.keys())
    months = sorted([m for m in common_months if m <= current_month])

    if not months:
        print(f"No data for months 1-{current_month}.", file=sys.stderr)
        sys.exit(1)

    total_exports = sum(exports[m] for m in months)
    total_imports = sum(imports[m] for m in months)
    avg_exports = total_exports / len(months)
    avg_imports = total_imports / len(months)

    with open("bps_trade_latest.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerow([year, avg_exports, avg_imports])

    print(f"\n✅ Successfully wrote bps_trade_latest.csv")
    print(f"   Year: {year}, Months included: {months}")
    print(f"   Monthly avg exports: ${avg_exports:,.2f} USD")
    print(f"   Monthly avg imports: ${avg_imports:,.2f} USD")


if __name__ == "__main__":
    main()
