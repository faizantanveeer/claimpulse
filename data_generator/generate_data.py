"""
ClaimPulse synthetic data generator (v2).

Key change from the original version: carriers and providers are REFERENCE
data -- in a real system, the provider/carrier roster changes slowly, not
every day. Claims, carrier_letters, and training_records are EVENT data --
genuinely new things happening constantly.

Treating all 5 tables the same way (fully seeded, regenerated identically
on every run) caused every DAG run to produce byte-for-byte duplicate rows,
which Snowflake's COPY INTO correctly loaded as "new" (since the S3
filenames were unique per run) -- silently multiplying every raw table on
each pipeline run.

Fix: carriers/providers are generated ONCE, with a fixed seed, and persisted.
Every subsequent run reuses the existing IDs instead of regenerating them.
claims/carrier_letters/training_records are generated fresh every run, with
NO fixed seed, referencing the existing (stable) provider/carrier ID pool.
"""

import csv
import os
import random
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

OUTPUT_DIR = Path(os.environ.get("CLAIMPULSE_DATA_DIR", Path(__file__).parent / "output"))
LOAD_DATE = date.today()

N_CARRIERS = 10
N_PROVIDERS = 200
N_CLAIMS = 1500
LETTER_TYPES = ["EOB", "denial", "request_for_records", "payment_confirmation", "status_update"]
CLAIM_STATUSES = ["open", "in_review", "pending_carrier", "closed", "denied"]
SPECIALTIES = ["chiropractic", "physical_therapy", "orthopedics", "ambulance", "pain_management", "neurology"]
STATES = ["MA", "NY", "NJ", "PA", "CT", "FL"]
TRAINING_STATUSES = ["completed", "pending", "not_required"]


def write_csv(table_name: str, rows: list[dict], partition: bool = True):
    if not rows:
        return
    if partition:
        out_dir = OUTPUT_DIR / table_name / f"dt={LOAD_DATE.isoformat()}"
    else:
        out_dir = OUTPUT_DIR / table_name  # reference data: no date partition, one file, overwritten
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{table_name}.csv" if not partition else out_dir / f"{table_name}_batch_001.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {len(rows):>5} rows -> {out_path.relative_to(OUTPUT_DIR.parent)}")


def seed_data_exists() -> bool:
    return (OUTPUT_DIR / "carriers" / "carriers.csv").exists()


def load_existing_ids(table_name: str, id_column: str) -> list[str]:
    path = OUTPUT_DIR / table_name / f"{table_name}.csv"
    with open(path, newline="") as f:
        return [row[id_column] for row in csv.DictReader(f)]


# --------------------------------------------------------------------------
# REFERENCE DATA -- generated once, seeded, persisted (no date partition)
# --------------------------------------------------------------------------

def gen_carriers(rng: random.Random) -> list[dict]:
    fake = Faker()
    fake.seed_instance(42)
    rows = []
    for i in range(N_CARRIERS):
        rows.append({
            "carrier_id": f"CAR-{i+1:03d}",
            "carrier_name": fake.company() + " Insurance",
            "tpa_flag": rng.choice([True, False]),
        })
    return rows


def gen_providers(rng: random.Random) -> list[dict]:
    fake = Faker()
    fake.seed_instance(42)
    rows = []
    for i in range(N_PROVIDERS):
        provider_id = f"PRV-{i+1:04d}"
        specialty = rng.choice(SPECIALTIES)
        if rng.random() < 0.05:  # ~5% missing specialty, by design
            specialty = ""
        rows.append({
            "provider_id": provider_id,
            "provider_name": fake.name(),
            "specialty": specialty,
            "state": rng.choice(STATES),
        })
    # 8 near-duplicate providers (typo'd spacing), same as original design
    for i in range(8):
        src = rng.choice(rows)
        dup = dict(src)
        dup["provider_id"] = f"PRV-DUP-{i+1:02d}"
        dup["provider_name"] = src["provider_name"].replace(" ", "  ", 1)
        rows.append(dup)
    return rows


def generate_seed_data():
    print("Generating seed reference data (carriers, providers) -- one-time, seeded\n")
    seed_rng = random.Random(42)
    carriers = gen_carriers(seed_rng)
    write_csv("carriers", carriers, partition=False)
    providers = gen_providers(seed_rng)
    write_csv("providers", providers, partition=False)
    return carriers, providers


# --------------------------------------------------------------------------
# EVENT DATA -- generated fresh every run, no fixed seed
# --------------------------------------------------------------------------

def gen_claims(rng: random.Random, fake: Faker, carrier_ids, provider_ids) -> list[dict]:
    rows = []
    for i in range(N_CLAIMS):
        date_of_loss = fake.date_between(start_date="-2y", end_date="-30d")
        claim_open_date = date_of_loss + timedelta(days=rng.randint(0, 14))
        provider_id = rng.choice(provider_ids)
        if rng.random() < 0.02:  # ~2% orphaned provider_id, by design
            provider_id = "PRV-9999"
        rows.append({
            "claim_id": f"CLM-{LOAD_DATE.strftime('%Y%m%d')}-{i+1:06d}",  # date-scoped, avoids
            "provider_id": provider_id,                                    # collisions across days
            "carrier_id": rng.choice(carrier_ids),
            "date_of_loss": date_of_loss.isoformat(),
            "claim_open_date": claim_open_date.isoformat(),
            "claim_status": rng.choice(CLAIM_STATUSES),
            "state": rng.choice(STATES),
        })
    return rows


def gen_carrier_letters(rng: random.Random, claims: list[dict]) -> list[dict]:
    rows = []
    seq = 0
    for claim in claims:
        n_letters = rng.choices([0, 1, 2, 3], weights=[10, 50, 30, 10])[0]
        claim_open = date.fromisoformat(claim["claim_open_date"])
        for _ in range(n_letters):
            seq += 1
            received_date = claim_open + timedelta(days=rng.randint(1, 400))
            response_due_date = received_date + timedelta(days=rng.choice([14, 21, 30]))
            status = rng.choices(["resolved", "in_progress", "unresolved"], weights=[55, 25, 20])[0]
            classification_status = status if rng.random() > 0.03 else ""
            if rng.random() < 0.01:  # ~1% broken dates, by design
                response_due_date = received_date - timedelta(days=5)
            rows.append({
                "letter_id": f"LTR-{LOAD_DATE.strftime('%Y%m%d')}-{seq:06d}",
                "claim_id": claim["claim_id"],
                "carrier_id": claim["carrier_id"],
                "letter_type": rng.choice(LETTER_TYPES),
                "received_date": received_date.isoformat(),
                "response_due_date": response_due_date.isoformat(),
                "classification_status": classification_status,
            })
    return rows


def gen_training_records(rng: random.Random, fake: Faker, provider_ids, carrier_ids) -> list[dict]:
    rows = []
    seq = 0
    for provider_id in provider_ids:
        for carrier_id in rng.sample(carrier_ids, k=rng.randint(1, min(4, len(carrier_ids)))):
            seq += 1
            assigned_date = fake.date_between(start_date="-1y", end_date="today")
            status = rng.choices(TRAINING_STATUSES, weights=[55, 35, 10])[0]
            completed_date = ""
            if status == "completed":
                completed_date = (assigned_date + timedelta(days=rng.randint(1, 25))).isoformat()
            rows.append({
                "training_id": f"TRN-{LOAD_DATE.strftime('%Y%m%d')}-{seq:06d}",
                "provider_id": provider_id,
                "carrier_id": carrier_id,
                "assigned_date": assigned_date.isoformat(),
                "completed_date": completed_date,
                "status": status,
            })
    return rows


def generate_daily_data(carrier_ids, provider_ids):
    print(f"Generating fresh event data for {LOAD_DATE.isoformat()} -- no fixed seed\n")
    daily_rng = random.Random()   # unseeded -- genuinely random, different every run
    fake = Faker()                # unseeded

    claims = gen_claims(daily_rng, fake, carrier_ids, provider_ids)
    write_csv("claims", claims)

    letters = gen_carrier_letters(daily_rng, claims)
    write_csv("carrier_letters", letters)

    training = gen_training_records(daily_rng, fake, provider_ids, carrier_ids)
    write_csv("training_records", training)

    print(f"\nDone. {len(claims)} claims, {len(letters)} letters, {len(training)} training records.")


def main():
    if not seed_data_exists():
        carriers, providers = generate_seed_data()
        carrier_ids = [c["carrier_id"] for c in carriers]
        provider_ids = [p["provider_id"] for p in providers]
    else:
        print("Seed reference data already exists -- reusing existing carrier/provider IDs\n")
        carrier_ids = load_existing_ids("carriers", "carrier_id")
        provider_ids = load_existing_ids("providers", "provider_id")

    generate_daily_data(carrier_ids, provider_ids)


if __name__ == "__main__":
    main()