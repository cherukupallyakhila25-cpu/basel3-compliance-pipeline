"""
Fetch FDIC Call Report data for every US bank for one quarter, compute
industry averages, and store them in PostgreSQL (table: fdic_benchmarks).
"""
import argparse
import logging
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from config.settings import FDIC_API_BASE, FDIC_REPORT_DATE, RAW_DATA_DIR, SQLALCHEMY_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("fetch_fdic")

# FDIC field code -> our metric name. FDIC reports these as percentages.
FIELD_MAP = {
    "RBC1RWAJ": "CAR",   # Tier 1 risk-based capital ratio (comparable to our CAR)
    "NCLNLSR": "NPL",    # Noncurrent loans to total loans (comparable to our NPL)
}
PAGE_SIZE = 10_000       # max rows per API call; about 4,500 banks fit in one page


def fetch_financials(report_date: str) -> pd.DataFrame:
    """Call /financials page by page until every bank for that quarter is fetched."""
    url = f"{FDIC_API_BASE}/financials"
    fields = ",".join(["CERT", "REPDTE", "ASSET", *FIELD_MAP.keys()])
    rows, offset = [], 0

    while True:
        params = {
            "filters": f"REPDTE:{report_date}",   # only this quarter-end
            "fields": fields,                     # only the columns we need
            "limit": PAGE_SIZE,
            "offset": offset,
            "format": "json",
        }
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()                   # stop loudly on HTTP errors
        payload = resp.json()

        # Each record is wrapped like {"data": {...}}; unwrap it
        batch = [item["data"] for item in payload.get("data", [])]
        rows.extend(batch)
        total = payload.get("meta", {}).get("total", 0)
        log.info("Fetched %s / %s records", len(rows), total)

        offset += PAGE_SIZE
        if not batch or offset >= total:
            break

    return pd.DataFrame(rows)


def compute_benchmarks(df: pd.DataFrame, report_date: str) -> pd.DataFrame:
    """Turn thousands of bank rows into one industry mean/median per metric."""
    records = []
    for field, metric in FIELD_MAP.items():
        if field not in df.columns:
            log.warning("Field %s not in API response, skipping", field)
            continue
        values = pd.to_numeric(df[field], errors="coerce").dropna() / 100.0   # % -> decimal
        values = values[(values >= 0) & (values <= 1)]   # drop odd outliers (e.g. trust-only banks)
        records.append({
            "report_date": datetime.strptime(report_date, "%Y%m%d").date(),
            "metric_name": metric,
            "industry_mean": round(float(values.mean()), 6),
            "industry_median": round(float(values.median()), 6),
            "bank_count": int(values.count()),
        })
    return pd.DataFrame(records)


def save_to_postgres(bench: pd.DataFrame) -> None:
    """Upsert: insert new rows, or overwrite if this quarter was already loaded."""
    engine = create_engine(SQLALCHEMY_URL)
    upsert = text("""
        INSERT INTO fdic_benchmarks (report_date, metric_name, industry_mean, industry_median, bank_count)
        VALUES (:report_date, :metric_name, :industry_mean, :industry_median, :bank_count)
        ON CONFLICT (report_date, metric_name) DO UPDATE SET
            industry_mean   = EXCLUDED.industry_mean,
            industry_median = EXCLUDED.industry_median,
            bank_count      = EXCLUDED.bank_count,
            loaded_at       = NOW()
    """)
    with engine.begin() as conn:                  # begin() = auto-commit or rollback
        for row in bench.to_dict("records"):
            conn.execute(upsert, row)
    log.info("Saved %s benchmark rows to fdic_benchmarks", len(bench))


def main():
    p = argparse.ArgumentParser(description="Fetch FDIC industry benchmarks")
    p.add_argument("--report-date", default=FDIC_REPORT_DATE, help="Quarter-end, YYYYMMDD")
    p.add_argument("--no-db", action="store_true", help="Only write CSVs, skip Postgres")
    args = p.parse_args()

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    raw = fetch_financials(args.report_date)
    raw.to_csv(RAW_DATA_DIR / f"fdic_financials_{args.report_date}.csv", index=False)

    bench = compute_benchmarks(raw, args.report_date)
    bench.to_csv(RAW_DATA_DIR / f"fdic_benchmarks_{args.report_date}.csv", index=False)
    print(bench.to_string(index=False))

    if not args.no_db:
        save_to_postgres(bench)


if __name__ == "__main__":
    main()
