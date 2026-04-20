import os
import csv
import datetime
import requests
import time

BPS_BASE_URL = "https://webapi.bps.go.id/v1/api"
BPS_API_KEY = os.environ.get("BPS_API_KEY")

def fetch_total_trade_for_year(api_key, trade_type, year):
    sumber = "1" if trade_type == "export" else "2"
    total = 0.0
    hs_codes = [f"{i:02d}" for i in range(1, 100)]
    for hs in hs_codes:
        params = {
            "sumber": sumber,
            "periode": "1",
            "kodehs": hs,
            "jenishs": "1",
            "tahun": str(year),
            "key": api_key
        }
        try:
            resp = requests.get(f"{BPS_BASE_URL}/dataexim",
                                params=params, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data.get("status") != "OK":
                continue
            items = data.get("data", [])
            for item in items:
                total += float(item.get("value", 0))
            time.sleep(0.05)
        except Exception:
            continue
    return total if total > 0 else None

if __name__ == "__main__":
    year = datetime.datetime.now().year
    exports = fetch_total_trade_for_year(BPS_API_KEY, "export", year)
    imports = fetch_total_trade_for_year(BPS_API_KEY, "import", year)
    if exports and imports:
        with open("bps_trade_latest.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["year", "monthly_exports_usd", "monthly_imports_usd"]
            )
            writer.writerow([year, exports / 12.0, imports / 12.0])
        print(f"Updated bps_trade_latest.csv for {year}")
    else:
        print("Failed to fetch BPS data")
        exit(1)
