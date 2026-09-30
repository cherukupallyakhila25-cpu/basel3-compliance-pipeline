"""Smoke test: is PostgreSQL reachable, and did init.sql create everything?"""
from sqlalchemy import create_engine, text

from config.settings import SQLALCHEMY_URL

EXPECTED_TABLES = {
    "risk_weight_config", "hqla_config", "cash_flow_rate_config",
    "regulatory_metrics", "fdic_benchmarks", "audit_log",
}


def main():
    engine = create_engine(SQLALCHEMY_URL)
    with engine.connect() as conn:
        version = conn.execute(text("SELECT version()")).scalar()
        print("Connected:", version.split(",")[0])

        rows = conn.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )).fetchall()
        found = {r[0] for r in rows}
        missing = EXPECTED_TABLES - found
        print("Tables OK" if not missing else f"MISSING tables: {missing}")

        print("\nRisk weights loaded:")
        for asset_class, weight in conn.execute(text(
            "SELECT asset_class, risk_weight FROM risk_weight_config ORDER BY risk_weight"
        )):
            print(f"  {asset_class:<25}{float(weight):>6.0%}")


if __name__ == "__main__":
    main()
