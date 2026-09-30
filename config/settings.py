"""Central configuration: every module imports its settings from here."""
import os
from pathlib import Path

from dotenv import load_dotenv

# Project root = the folder that contains config/ (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parent.parent

# Read KEY=VALUE lines from .env into environment variables
load_dotenv(BASE_DIR / ".env")

# ---------------- PostgreSQL ----------------
POSTGRES_USER = os.getenv("POSTGRES_USER", "basel")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "basel123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "regulatory")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# SQLAlchemy connection string, used by plain Python / pandas code
SQLALCHEMY_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# JDBC connection string, used by PySpark in Part 2
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ---------------- Spark ----------------
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")  # local[*] = use all CPU cores

# ---------------- Folders ----------------
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"

# ---------------- FDIC API ----------------
FDIC_API_BASE = os.getenv("FDIC_API_BASE", "https://api.fdic.gov/banks")
FDIC_REPORT_DATE = os.getenv("FDIC_REPORT_DATE", "20240331")   # YYYYMMDD quarter-end

# ---------------- Regulatory thresholds ----------------
REGULATORY_MINIMUMS = {
    "CAR": 0.06,   # Tier 1 capital must be at least 6% of RWA
    "LCR": 1.00,   # HQLA must cover at least 100% of 30-day net outflows
    "NPL": 0.05,   # internal risk-appetite limit (Basel sets no hard NPL minimum)
}
