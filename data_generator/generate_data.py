"""
ClaimPulse synthetic data generator.

Generates 5 tables matching the ERD in ARCHITECTURE.md:
  carriers, providers, claims, carrier_letters, training_records

Writes CSVs partitioned by load date, mirroring the S3 raw zone layout:
  output/<table>/dt=YYYY-MM-DD/<table>_batch_NNN.csv

Deliberately injects messiness (nulls, duplicates, bad dates, orphaned
foreign keys) so the dbt layer downstream has real things to catch with
tests -- clean synthetic data teaches nothing.
"""

import csv
import os
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

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


def write_csv(table_name: str, rows: list[dict], batch: int = 1):
    if not rows:
        return
    out_dir = OUTPUT_DIR / table_name / f"dt={LOAD_DATE.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{table_name}_batch_{batch:03d}.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {len(rows):>5} rows -> {out_path.relative_to(OUTPUT_DIR.parent)}")


def gen_carriers():
    rows = []
    for i in range(N_CARRIERS):
        rows.append({
            "carrier_id": f"CAR-{i+1:03d}",
            "carrier_name": fake.company() + " Insurance",
            "tpa_flag": random.choice([True, False]),
        })
    return rows


def gen_providers():
    rows = []
    for i in range(N_PROVIDERS):
        provider_id = f"PRV-{i+1:04d}"
        name = fake.name()
        specialty = random.choice(SPECIALTIES)
        # messiness: ~5% missing specialty
        if random.random() < 0.05:
            specialty = ""
        rows.append({
            "provider_id": provider_id,
            "provider_name": name,
            "specialty": specialty,
            "state": random.choice(STATES),
        })
    # messiness: inject a handful of near-duplicate providers (slightly
    # different name spelling, same person) -- realistic MDM problem
    for i in range(8):
        src = random.choice(rows)
        dup = dict(src)
        dup["provider_id"] = f"PRV-DUP-{i+1:02d}"
        dup["provider_name"] = src["provider_name"].replace(" ", "  ", 1)  # double space typo
        rows.append(dup)
    return rows


def gen_claims(carrier_ids, provider_ids):
    rows = []
    for i in range(N_CLAIMS):
        claim_id = f"CLM-{i+1:06d}"
        date_of_loss = fake.date_between(start_date="-2y", end_date="-30d")
        claim_open_date = date_of_loss + timedelta(days=random.randint(0, 14))
        provider_id = random.choice(provider_ids)
        # messiness: ~2% orphaned provider_id (provider that doesn't exist)
        if random.random() < 0.02:
            provider_id = f"PRV-9999"
        rows.append({
            "claim_id": claim_id,
            "provider_id": provider_id,
            "carrier_id": random.choice(carrier_ids),
            "date_of_loss": date_of_loss.isoformat(),
            "claim_open_date": claim_open_date.isoformat(),
            "claim_status": random.choice(CLAIM_STATUSES),
            "state": random.choice(STATES),
        })
    return rows


def gen_carrier_letters(claims):
    rows = []
    letter_seq = 0
    for claim in claims:
        n_letters = random.choices([0, 1, 2, 3], weights=[10, 50, 30, 10])[0]
        claim_open = date.fromisoformat(claim["claim_open_date"])
        for _ in range(n_letters):
            letter_seq += 1
            received_date = claim_open + timedelta(days=random.randint(1, 400))
            due_offset = random.choice([14, 21, 30])
            response_due_date = received_date + timedelta(days=due_offset)
            status = random.choices(
                ["resolved", "in_progress", "unresolved"], weights=[55, 25, 20]
            )[0]
            # messiness: ~3% missing classification_status (unclassified letter)
            classification_status = status if random.random() > 0.03 else ""
            # messiness: ~1% bad data -- received_date after due_date (data entry error)
            if random.random() < 0.01:
                response_due_date = received_date - timedelta(days=5)
            rows.append({
                "letter_id": f"LTR-{letter_seq:06d}",
                "claim_id": claim["claim_id"],
                "carrier_id": claim["carrier_id"],
                "letter_type": random.choice(LETTER_TYPES),
                "received_date": received_date.isoformat(),
                "response_due_date": response_due_date.isoformat(),
                "classification_status": classification_status,
            })
    return rows


def gen_training_records(providers, carrier_ids):
    rows = []
    seq = 0
    for provider in providers:
        # not every provider needs training with every carrier
        for carrier_id in random.sample(carrier_ids, k=random.randint(1, min(4, len(carrier_ids)))):
            seq += 1
            assigned_date = fake.date_between(start_date="-1y", end_date="today")
            status = random.choices(TRAINING_STATUSES, weights=[55, 35, 10])[0]
            completed_date = ""
            if status == "completed":
                completed_date = (assigned_date + timedelta(days=random.randint(1, 25))).isoformat()
            rows.append({
                "training_id": f"TRN-{seq:06d}",
                "provider_id": provider["provider_id"],
                "carrier_id": carrier_id,
                "assigned_date": assigned_date.isoformat(),
                "completed_date": completed_date,
                "status": status,
            })
    return rows


def main():
    print(f"Generating ClaimPulse synthetic data for load date {LOAD_DATE.isoformat()}\n")

    carriers = gen_carriers()
    write_csv("carriers", carriers)

    providers = gen_providers()
    write_csv("providers", providers)

    carrier_ids = [c["carrier_id"] for c in carriers]
    provider_ids = [p["provider_id"] for p in providers]

    claims = gen_claims(carrier_ids, provider_ids)
    write_csv("claims", claims)

    letters = gen_carrier_letters(claims)
    write_csv("carrier_letters", letters)

    training = gen_training_records(providers, carrier_ids)
    write_csv("training_records", training)

    print(f"\nDone. {len(carriers)} carriers, {len(providers)} providers, "
          f"{len(claims)} claims, {len(letters)} letters, {len(training)} training records.")


if __name__ == "__main__":
    main()
