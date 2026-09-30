"""
Generate simulated bank balance-sheet data for the Basel III pipeline.

Output CSVs in data/raw/:
  banks.csv, branches.csv, loans.csv, assets.csv, cash_flows.csv, capital.csv
  expected_car.csv  -> "answer key" used to validate the Spark CAR job later
"""
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

from config.settings import RAW_DATA_DIR

fake = Faker("en_US")
Faker.seed(42)      # same seed = same data every run (reproducible tests)
random.seed(42)

# Must match risk_weight_config in sql/init.sql
RISK_WEIGHTS = {
    "CASH": 0.00, "SOVEREIGN_BOND": 0.00, "BANK_EXPOSURE": 0.20,
    "RESIDENTIAL_MORTGAGE": 0.35, "RETAIL": 0.75, "CORPORATE": 1.00,
    "CORPORATE_BOND": 1.00, "COMMERCIAL_REAL_ESTATE": 1.00, "PAST_DUE": 1.50,
}

# Must match hqla_config
HQLA_FACTORS = {"LEVEL_1": 1.00, "LEVEL_2A": 0.85, "LEVEL_2B": 0.50, "NON_HQLA": 0.00}

# Loan class -> (share of portfolio, min amount, max amount)
LOAN_MIX = {
    "RESIDENTIAL_MORTGAGE":   (0.40, 50_000, 800_000),
    "RETAIL":                 (0.35, 1_000, 50_000),
    "CORPORATE":              (0.15, 100_000, 5_000_000),
    "COMMERCIAL_REAL_ESTATE": (0.10, 500_000, 10_000_000),
}

# Cash-flow category -> (direction, stress rate, avg daily amount per $1B of loans)
# Rates must match cash_flow_rate_config
CASH_FLOW_MIX = {
    "RETAIL_STABLE":             ("OUTFLOW", 0.05, 20_000_000),
    "RETAIL_LESS_STABLE":        ("OUTFLOW", 0.10, 10_000_000),
    "OPERATIONAL":               ("OUTFLOW", 0.25, 5_000_000),
    "CORPORATE_NON_OPERATIONAL": ("OUTFLOW", 0.40, 3_000_000),
    "FINANCIAL_INSTITUTION":     ("OUTFLOW", 1.00, 1_000_000),
    "RETAIL_LOAN_REPAYMENT":     ("INFLOW",  0.50, 4_000_000),
}
HISTORY_DAYS = 90   # days of cash-flow history (Spark computes rolling 30-day windows)


def write_csv(path: Path, fieldnames: list, rows) -> int:
    """Stream rows to a CSV one at a time, so memory stays low even for millions of rows."""
    count = 0
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    print(f"  wrote {count:>10,} rows -> {path.name}")
    return count


def split_amount(total: float, parts: int) -> list:
    """Split a total into `parts` random pieces that add up to (about) the total."""
    weights = [random.random() for _ in range(parts)]
    s = sum(weights)
    return [round(total * w / s, 2) for w in weights]


def expected_30d_net_outflow(scale: float) -> float:
    """Expected stressed net outflow over 30 days (used to size each bank's HQLA)."""
    out = sum(avg * rate for d, rate, avg in CASH_FLOW_MIX.values() if d == "OUTFLOW")
    inn = sum(avg * rate for d, rate, avg in CASH_FLOW_MIX.values() if d == "INFLOW")
    inn = min(inn, 0.75 * out)                  # Basel III caps inflows at 75% of outflows
    return (out - inn) * scale * 30


def generate(num_banks: int, branches_per_bank: int, num_loans: int,
             report_date: date, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Generating {num_banks} banks, {num_loans:,} loans, report date {report_date}")

    # ---------- 1. Banks ----------
    banks = []
    for i in range(1, num_banks + 1):
        banks.append({
            "bank_id": f"BANK{i:03d}",
            "bank_name": f"{fake.last_name()} {random.choice(['Bank', 'National Bank', 'Savings Bank', 'Trust'])}",
            "city": fake.city(),
            "state": fake.state_abbr(),
            # hidden "personality" of each bank (not written to CSV):
            "npl_rate": random.uniform(0.01, 0.08),     # share of loans that go bad
            "target_car": random.uniform(0.05, 0.16),   # some banks will fail the 6% test
            "target_lcr": random.uniform(0.85, 1.80),   # some banks will fail the 100% test
        })
    write_csv(out_dir / "banks.csv", ["bank_id", "bank_name", "city", "state"], banks)

    # ---------- 2. Branches ----------
    branches = []
    for b in banks:
        for j in range(1, branches_per_bank + 1):
            city = fake.city()
            branches.append({
                "branch_id": f"{b['bank_id']}-BR{j:02d}",
                "bank_id": b["bank_id"],
                "branch_name": f"{city} Branch",
                "city": city,
            })
    write_csv(out_dir / "branches.csv", ["branch_id", "bank_id", "branch_name", "city"], branches)

    # Running totals per bank, filled while rows are generated
    rwa = {b["bank_id"]: 0.0 for b in banks}          # risk-weighted assets
    loan_total = {b["bank_id"]: 0.0 for b in banks}   # total loan book

    # ---------- 3. Loans (the big file) ----------
    loan_classes = list(LOAN_MIX)
    loan_shares = [LOAN_MIX[c][0] for c in loan_classes]

    def loan_rows():
        for n in range(1, num_loans + 1):
            bank = random.choice(banks)
            bid = bank["bank_id"]
            asset_class = random.choices(loan_classes, weights=loan_shares, k=1)[0]
            _, lo, hi = LOAN_MIX[asset_class]
            amount = round(random.uniform(lo, hi), 2)

            # days past due: 90+ means non-performing (the Basel NPL definition)
            if random.random() < bank["npl_rate"]:
                dpd = random.randint(90, 365)
            elif random.random() < 0.05:
                dpd = random.randint(1, 89)             # late, but still performing
            else:
                dpd = 0
            status = "NON_PERFORMING" if dpd >= 90 else "PERFORMING"

            # Same rule Spark will apply: NPLs get the 150% PAST_DUE weight
            weight = RISK_WEIGHTS["PAST_DUE"] if status == "NON_PERFORMING" else RISK_WEIGHTS[asset_class]
            rwa[bid] += amount * weight
            loan_total[bid] += amount

            yield {
                "loan_id": f"LN{n:09d}",
                "bank_id": bid,
                "branch_id": f"{bid}-BR{random.randint(1, branches_per_bank):02d}",
                "asset_class": asset_class,
                "outstanding_amount": amount,
                "days_past_due": dpd,
                "loan_status": status,
                "origination_date": (report_date - timedelta(days=random.randint(30, 3650))).isoformat(),
            }

    write_csv(out_dir / "loans.csv",
              ["loan_id", "bank_id", "branch_id", "asset_class", "outstanding_amount",
               "days_past_due", "loan_status", "origination_date"],
              loan_rows())

    # ---------- 4. Daily cash flows (for rolling 30-day LCR) ----------
    def cash_flow_rows():
        n = 0
        for b in banks:
            scale = loan_total[b["bank_id"]] / 1_000_000_000     # bank size in $B of loans
            for d in range(HISTORY_DAYS):
                flow_date = report_date - timedelta(days=HISTORY_DAYS - 1 - d)
                for category, (direction, _, avg) in CASH_FLOW_MIX.items():
                    n += 1
                    yield {
                        "flow_id": f"CF{n:09d}",
                        "bank_id": b["bank_id"],
                        "flow_date": flow_date.isoformat(),
                        "direction": direction,
                        "category": category,
                        "amount": round(avg * scale * random.uniform(0.5, 1.5), 2),
                    }

    write_csv(out_dir / "cash_flows.csv",
              ["flow_id", "bank_id", "flow_date", "direction", "category", "amount"],
              cash_flow_rows())

    # ---------- 5. Non-loan assets: liquid securities (HQLA) + interbank ----------
    def asset_rows():
        n = 0
        for b in banks:
            bid = b["bank_id"]
            scale = loan_total[bid] / 1_000_000_000
            hqla_needed = expected_30d_net_outflow(scale) * b["target_lcr"]   # after haircuts

            # (asset_class, hqla_level, share of HQLA). Level 2 = 40%, Level 2B = 10%,
            # within the Basel caps of 40% and 15%.
            buckets = [
                ("CASH", "LEVEL_1", 0.30),
                ("SOVEREIGN_BOND", "LEVEL_1", 0.30),
                ("CORPORATE_BOND", "LEVEL_2A", 0.30),
                ("CORPORATE_BOND", "LEVEL_2B", 0.10),
            ]
            for asset_class, level, share in buckets:
                market_value = hqla_needed * share / HQLA_FACTORS[level]   # gross up for haircut
                for piece in split_amount(market_value, 10):
                    n += 1
                    rwa[bid] += piece * RISK_WEIGHTS[asset_class]
                    yield {"asset_id": f"AS{n:08d}", "bank_id": bid, "asset_class": asset_class,
                           "hqla_level": level, "market_value": piece}

            # Interbank placements: carry credit risk but do NOT count as HQLA
            for piece in split_amount(loan_total[bid] * 0.05, 10):
                n += 1
                rwa[bid] += piece * RISK_WEIGHTS["BANK_EXPOSURE"]
                yield {"asset_id": f"AS{n:08d}", "bank_id": bid, "asset_class": "BANK_EXPOSURE",
                       "hqla_level": "NON_HQLA", "market_value": piece}

    write_csv(out_dir / "assets.csv",
              ["asset_id", "bank_id", "asset_class", "hqla_level", "market_value"],
              asset_rows())

    # ---------- 6. Capital + answer key ----------
    capital_rows, expected_rows = [], []
    for b in banks:
        bid = b["bank_id"]
        tier1 = round(rwa[bid] * b["target_car"], 2)
        cet1 = round(tier1 * 0.85, 2)              # Common Equity Tier 1 (shares + retained earnings)
        at1 = round(tier1 - cet1, 2)               # Additional Tier 1 (e.g. perpetual bonds)
        tier2 = round(rwa[bid] * random.uniform(0.015, 0.03), 2)
        capital_rows.append({"bank_id": bid, "report_date": report_date.isoformat(),
                             "cet1_capital": cet1, "at1_capital": at1, "tier2_capital": tier2})
        expected_rows.append({"bank_id": bid, "tier1_capital": round(cet1 + at1, 2),
                              "rwa": round(rwa[bid], 2),
                              "expected_car": round((cet1 + at1) / rwa[bid], 6)})

    write_csv(out_dir / "capital.csv",
              ["bank_id", "report_date", "cet1_capital", "at1_capital", "tier2_capital"],
              capital_rows)
    write_csv(out_dir / "expected_car.csv",
              ["bank_id", "tier1_capital", "rwa", "expected_car"], expected_rows)

    print("\nExpected CAR (answer key for validation):")
    for r in expected_rows:
        print(f"  {r['bank_id']}: {r['expected_car']:.2%}")


def parse_args():
    p = argparse.ArgumentParser(description="Generate simulated Basel III balance-sheet data")
    p.add_argument("--banks", type=int, default=5)
    p.add_argument("--branches-per-bank", type=int, default=10)
    p.add_argument("--loans", type=int, default=100_000)
    p.add_argument("--report-date", default="2024-03-31")
    p.add_argument("--out-dir", default=str(RAW_DATA_DIR))
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate(
        num_banks=args.banks,
        branches_per_bank=args.branches_per_bank,
        num_loans=args.loans,
        report_date=datetime.strptime(args.report_date, "%Y-%m-%d").date(),
        out_dir=Path(args.out_dir),
    )
