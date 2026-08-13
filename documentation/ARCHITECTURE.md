# ClaimPulse — Architecture

An end-to-end analytics platform that ingests, transforms, and monitors PIP/personal injury insurance claims data — modeled on real carrier-letter and provider-training workflows, built entirely on synthetic data on Snowflake + AWS free tier.

## 1. Business problem

PI/PIP claims stall when carrier response letters aren't processed on time or providers lapse on required training. Both cost law firms and TPAs money — missed statutory deadlines, compliance exposure, manual triage overhead. ClaimPulse ingests claim, carrier-letter, and provider-training data, models it, and surfaces **SLA breaches and training gaps before they become expensive**, with automated alerts instead of a dashboard someone has to remember to check.

## 2. Domain model (ERD)

```mermaid
erDiagram
  CARRIER ||--o{ CARRIER_LETTER : sends
  CLAIM ||--o{ CARRIER_LETTER : generates
  CLAIM }o--|| PROVIDER : treated_by
  PROVIDER ||--o{ TRAINING_RECORD : requires
  CLAIM {
    string claim_id PK
    string provider_id FK
    string carrier_id FK
    date date_of_loss
    date claim_open_date
    string claim_status
    string state
  }
  CARRIER_LETTER {
    string letter_id PK
    string claim_id FK
    string carrier_id FK
    string letter_type
    date received_date
    date response_due_date
    string classification_status
  }
  PROVIDER {
    string provider_id PK
    string provider_name
    string specialty
    string state
  }
  TRAINING_RECORD {
    string training_id PK
    string provider_id FK
    string carrier_id FK
    date assigned_date
    date completed_date
    string status
  }
  CARRIER {
    string carrier_id PK
    string carrier_name
    string tpa_flag
  }
```

**Business rules baked into the model:**
- A `CARRIER_LETTER.response_due_date` that's passed with `classification_status != 'resolved'` = an SLA breach.
- A `TRAINING_RECORD` with `assigned_date` older than 30 days and `status = 'pending'` = a training lapse.
- Both become flags in the marts layer, not just raw columns — the transformation layer's job is to compute these, not the BI tool.

## 3. Pipeline flow

```mermaid
flowchart TD
    A[Generate synthetic data<br/>Python + Faker] --> B[Land raw files in S3<br/>free tier bucket]
    B --> C[Load to Snowflake<br/>Snowpipe auto-ingest to raw]
    C --> D[Transform with dbt<br/>staging -> intermediate -> marts]
    D --> E[Serve and alert<br/>Power BI + Teams/Slack webhook]
```

## 4. S3 layout

Designed to hold today's batch loads and tomorrow's streaming source (e.g. Kinesis/Firehose) without any restructuring:

```
claimspulse-claim-processing/
  raw/
    source=batch/
      table=claims/
        year=2026/month=08/day=13/
          claims_20260813T143201Z_8f3a1c.csv
      table=carrier_letters/...
      table=providers/...
      table=carriers/...
      table=training_records/...
    source=streaming/                    <- reserved, empty until a streaming source exists
      table=claim_events/
        year=/month=/day=/hour=/
  _manifests/
    source=batch/table=claims/year=2026/month=08/day=13/
      manifest_20260813T143201Z.json
  _quarantine/
    table=claims/year=2026/month=08/day=13/
```

**Design decisions and why:**
- **`source=batch/` vs `source=streaming/`** — same table, same downstream Snowpipe target, different arrival pattern. A future streaming source (Kinesis Firehose) lands in its own prefix with its own partition granularity (down to `/hour=`); dbt staging models union both sources, so no rework is needed when streaming is added.
- **Hive-style partitioning** (`year=/month=/day=/`), not a flat date string — this is the convention Snowflake, Athena, and Spark all recognize natively for partition pruning, and it extends cleanly to `/hour=` for streaming.
- **Timestamp + short hash in every filename** (`<table>_<UTC timestamp>_<hash>.csv`) — makes every upload idempotent by construction. Re-running the uploader never overwrites a prior file, so a crashed mid-run script is always safe to re-run, and file-level lineage (exactly when a file landed) is preserved for debugging late claims.
- **`_manifests/`** — one JSON per upload batch recording every file's row count and md5 checksum. This is the fault-tolerance mechanism: if a Snowflake load fails partway, the manifest tells you exactly what should have arrived, so reconciliation doesn't rely on guesswork.
- **`_quarantine/`** — files that fail structural validation (wrong column set) before ever reaching `raw/`, so a malformed batch can't silently corrupt the good data path.
- **Retries**: the uploader uses botocore's adaptive retry mode (exponential backoff + jitter, aware of S3 throttling specifically) rather than a hand-rolled retry loop.
- **Scale**: boto3's `upload_file` automatically switches to multipart upload above 8MB per file — the same call already scales to multi-GB files with no code change, well beyond anything the free tier's 5GB will hold at this project's volume.

## 5. Snowflake account structure

- **RAW database** (`claimpulse_raw.raw_claims`) → loaded via scheduled `COPY INTO`, not Snowpipe (decision: keep a single scheduler — Airflow — rather than running Snowflake's own TASK scheduler alongside it; Snowpipe is a documented future upgrade once event-driven ingestion is needed). Every column is loaded as `STRING`, even dates — RAW does no casting or interpretation, that's staging's job (see section 7).
- **ANALYTICS database** (`claimpulse_analytics`) → three schemas built entirely by dbt: `staging`, `intermediate`, `marts`.
- **Warehouse**: `claimpulse_wh`, XS, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`, `INITIALLY_SUSPENDED = TRUE`.
- **Resource monitor**: hard credit quota of 50 (well under the $400/30-day trial), notifies at 75%, suspends the warehouse outright at 100% — protects against a runaway query or a forgotten idle warehouse draining the trial early.
- **Roles**: `claimpulse_loader` (INSERT/SELECT on RAW only — used by the `COPY INTO` step) and `claimpulse_transformer` (SELECT on RAW, full control of ANALYTICS — used by dbt). Neither runs as `ACCOUNTADMIN`.
- **Stage**: credentials-based external stage pointing at `s3://claimspulse-claim-processing/raw/`, using the AWS access key/secret directly for now. **Documented upgrade path**: replace with a `STORAGE INTEGRATION` (IAM role + trust policy) once a role ARN is available — this avoids ever storing a secret inside Snowflake and is the production-grade pattern, deferred here the same way Snowpipe is.
- **Load semantics**: `COPY INTO` uses `ON_ERROR = 'CONTINUE'` (a malformed row doesn't abort the whole file — consistent with "never silently drop data" from section 7) and relies on Snowflake's built-in 64-day load history to make repeated runs idempotent — a file that already loaded successfully is automatically skipped on the next run, which is what makes the timer-based approach safe to fire on a schedule rather than being tightly coupled to exactly when new files land.

## 6. dbt project structure

```
models/
  staging/
    stg_claims.sql
    stg_carrier_letters.sql
    stg_providers.sql
    stg_training_records.sql
  intermediate/
    int_claims_with_sla_flags.sql
    int_provider_training_status.sql
  marts/
    fct_claims.sql
    fct_carrier_letters.sql
    dim_provider.sql
    dim_carrier.sql
tests/
  (schema tests via YAML: not_null, unique, relationships,
   dbt_utils.accepted_range, dbt_expectations for business rules
   e.g. response_due_date must be after received_date)
macros/
  days_between.sql
  sla_breach_flag.sql
```

## 7. Data quality resolution strategy

Raw data is never touched or corrected in place. Nothing is silently dropped, fixed, or overwritten between RAW and staging — an insurance/compliance-style pipeline needs to be able to prove what was wrong, not just show clean numbers at the end. Each layer has one job:

- **RAW** — exactly what came in from S3. Immutable, never modified.
- **staging** — casts types, standardizes formats, and *flags* problems (e.g. `provider_match_status`, `is_date_anomaly`). Does not decide what a flag means for the business.
- **intermediate** — applies the business logic that decides how a flagged row should behave (e.g. whether an unclassified letter counts as an SLA breach).
- **marts** — the final, decided version served to Power BI and the alerting script. Every row has a clear, defensible state by this point.

**Issue-by-issue resolution:**

| Issue | Caught where | Mechanism | Resolved how | Severity |
|---|---|---|---|---|
| Claim points to a `provider_id` that doesn't exist | `stg_claims` | dbt `relationships` test | Left join keeps the claim, adds `provider_match_status = 'unmatched'`. Still tracked for SLA purposes in `int_claims_with_sla_flags`, plus a `needs_provider_review` flag for a human. Never dropped — a broken provider link doesn't erase a real deadline. | warn |
| Letter missing `classification_status` | `stg_carrier_letters` | `not_null` test | Coalesced to `'unclassified'` (never guessed). Business rule in `int_claims_with_sla_flags`: unclassified is treated as **unresolved** — the conservative assumption. | warn |
| `response_due_date` before `received_date` | `stg_carrier_letters` | `dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B` | Flagged `is_date_anomaly = true`, excluded from SLA breach calculation (can't compute a deadline breach off a broken deadline), but kept and surfaced on a data-quality mart page — never silently dropped. | error |
| Duplicate provider (typo'd name, different ID) | `stg_providers` | No automated test — fuzzy duplicates are a modeling problem, not a null/type problem | Dedup macro: normalize whitespace/casing, group by `(normalized_name, state)`, pick earliest `provider_id` as canonical, emit a `provider_id_map` so future duplicates route to the canonical ID. The one case where staging rewrites a value, because it's resolving identity, not fixing data. | n/a (structural) |

**Severity and the pipeline:** `error`-level test failures block the Airflow DAG from promoting that run — bad dates are a real defect worth stopping for. `warn`-level failures (orphaned provider, unclassified letter) let the pipeline continue but get logged visibly, since a single unmatched provider shouldn't block a whole day's claims from loading.

## 8. Orchestration and CI/CD

```mermaid
flowchart TD
    A[Airflow, Docker, daily schedule] --> B[Generate, load, dbt build]
    B --> C[Marts refreshed, alerts fire]
    D[Pull request opened] --> E[GitHub Actions CI:<br/>dbt build --select state:modified+]
    E --> F[Merge to main<br/>only if tests pass on dev schema]
```
- **Airflow** (local Docker Compose) owns the daily production run: generate → land in S3 → load to Snowflake → `dbt build`. This is what you demo/screenshot — avoids AWS MWAA, which isn't free tier.
- **GitHub Actions** owns CI: on every PR touching `models/`, run `dbt build --select state:modified+` against an isolated dev schema (Slim CI). Merge blocked until tests pass. This is the artifact that shows up as a green check in your GitHub PR history — good for the portfolio, and it's a real cost-control skill (rebuilding only what changed, not the whole DAG).

## 9. Serving layer

- **Power BI**: connects directly to the `marts` schema. Two report pages — claims/SLA overview, provider training compliance.
- **Alerting**: a scheduled step (GitHub Actions cron, or a task inside the Airflow DAG) queries `fct_claims` / `fct_carrier_letters` for new breaches and posts to a Teams or Slack webhook. This is the piece that turns the project from "a dashboard" into "an automated business process" — worth calling out explicitly in your README and LinkedIn post.

## 10. Security and cost guardrails

- No real company data, ever — synthetic generation only, documented as such in the README.
- Snowflake: least-privilege roles (see §5), resource monitor set on the warehouse with a hard credit cap so a runaway query can't burn the trial credit early.
- AWS: dedicated IAM user for this project (not root), scoped to the one S3 bucket, access keys stored as GitHub Actions secrets, never committed.
- Free tier tracking: note the 12-month AWS free tier limits (S3, Lambda if used) and the 30-day/$400 Snowflake trial clock in the README so anyone reviewing your repo sees you designed around real constraints, not infinite budget — that's a realistic engineering signal.

## 11. Repo structure

```
claimpulse/
  README.md
  ARCHITECTURE.md          (this file)
  data_generator/          Python + Faker synthetic data scripts
  airflow/                 DAG + docker-compose.yml
  dbt/                     full dbt project
  .github/workflows/       ci.yml (dbt build on PR)
  powerbi/                 .pbix file + exported screenshots
  alerts/                  SLA breach check script + webhook config
  docs/                    architecture diagrams, dbt docs site
```

## 12. Tech stack summary

| Layer | Tool | Free-tier note |
|---|---|---|
| Data generation | Python, Faker | n/a |
| Storage | AWS S3 | 5GB free, 12 months |
| Warehouse | Snowflake | 30-day trial, $400 credit |
| Transformation | dbt Core | free, open source |
| Orchestration | Airflow (Docker) | self-hosted, no cost |
| CI/CD | GitHub Actions | free for public repos |
| BI | Power BI Desktop | free |
| Alerting | Python + Teams/Slack webhook | free |

## 13. Build order

1. Data generator + domain model validation
2. S3 landing + IAM setup
3. Snowflake account, warehouse, roles, Snowpipe
4. dbt staging layer + first tests
5. dbt intermediate + marts + SLA/training logic
6. Airflow DAG wiring it all together
7. GitHub Actions CI
8. Power BI report
9. Alerting script
10. Docs, README, dbt docs site, demo GIF