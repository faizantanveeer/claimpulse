-- ClaimPulse Snowflake account setup
-- Run as ACCOUNTADMIN once, then switch to the created roles for everything else.
-- Matches ARCHITECTURE.md section 5 (account structure) and section 4 (S3 layout).

-- ===================================================================
-- 1. Warehouse -- XS, aggressive auto-suspend, hard cost cap
-- ===================================================================
CREATE WAREHOUSE IF NOT EXISTS claimpulse_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60          -- seconds idle before suspend; the single biggest
                              -- lever against burning trial credits
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Resource monitor: hard stop if this project ever burns more than
-- 50 credits (well under the $400 trial), so a runaway query or a
-- forgotten warehouse can't drain the trial early.
CREATE RESOURCE MONITOR IF NOT EXISTS claimpulse_monitor
  WITH CREDIT_QUOTA = 50
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE claimpulse_wh SET RESOURCE_MONITOR = claimpulse_monitor;

-- ===================================================================
-- 2. Databases and schemas
-- ===================================================================
CREATE DATABASE IF NOT EXISTS claimpulse_raw;
CREATE SCHEMA IF NOT EXISTS claimpulse_raw.raw_claims;

CREATE DATABASE IF NOT EXISTS claimpulse_analytics;
CREATE SCHEMA IF NOT EXISTS claimpulse_analytics.staging;
CREATE SCHEMA IF NOT EXISTS claimpulse_analytics.intermediate;
CREATE SCHEMA IF NOT EXISTS claimpulse_analytics.marts;
-- dbt will own object creation inside these schemas; we just create the
-- containers here so grants can be scoped to them up front.

-- ===================================================================
-- 3. Least-privilege roles
-- ===================================================================
CREATE ROLE IF NOT EXISTS claimpulse_loader;     -- writes to RAW only
CREATE ROLE IF NOT EXISTS claimpulse_transformer; -- reads RAW, writes ANALYTICS (dbt runs as this)

GRANT USAGE ON WAREHOUSE claimpulse_wh TO ROLE claimpulse_loader;
GRANT USAGE ON WAREHOUSE claimpulse_wh TO ROLE claimpulse_transformer;

GRANT USAGE ON DATABASE claimpulse_raw TO ROLE claimpulse_loader;
GRANT USAGE ON SCHEMA claimpulse_raw.raw_claims TO ROLE claimpulse_loader;
GRANT CREATE TABLE ON SCHEMA claimpulse_raw.raw_claims TO ROLE claimpulse_loader;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA claimpulse_raw.raw_claims TO ROLE claimpulse_loader;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA claimpulse_raw.raw_claims TO ROLE claimpulse_loader;

GRANT USAGE ON DATABASE claimpulse_raw TO ROLE claimpulse_transformer;
GRANT USAGE ON SCHEMA claimpulse_raw.raw_claims TO ROLE claimpulse_transformer;
GRANT SELECT ON ALL TABLES IN SCHEMA claimpulse_raw.raw_claims TO ROLE claimpulse_transformer;
GRANT SELECT ON FUTURE TABLES IN SCHEMA claimpulse_raw.raw_claims TO ROLE claimpulse_transformer;

GRANT USAGE ON DATABASE claimpulse_analytics TO ROLE claimpulse_transformer;
GRANT ALL ON SCHEMA claimpulse_analytics.staging TO ROLE claimpulse_transformer;
GRANT ALL ON SCHEMA claimpulse_analytics.intermediate TO ROLE claimpulse_transformer;
GRANT ALL ON SCHEMA claimpulse_analytics.marts TO ROLE claimpulse_transformer;

-- Assign these roles to yourself (replace with your actual Snowflake username)
-- GRANT ROLE claimpulse_loader TO USER <your_username>;
-- GRANT ROLE claimpulse_transformer TO USER <your_username>;

-- ===================================================================
-- 4. File format -- matches the CSVs generate_data.py produces
-- ===================================================================
USE SCHEMA claimpulse_raw.raw_claims;

CREATE FILE FORMAT IF NOT EXISTS csv_standard
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;

-- ===================================================================
-- 5. External stage -- points at the S3 bucket's raw/ zone
-- Credentials-based for now (documented upgrade: STORAGE INTEGRATION
-- with an IAM role + trust policy, once a role ARN is available instead
-- of long-term access keys -- avoids ever storing a secret in Snowflake).
-- ===================================================================
CREATE STAGE IF NOT EXISTS claimpulse_raw_stage
  URL = 's3://claimspulse-claim-processing/raw/'
  CREDENTIALS = (AWS_KEY_ID = '<your_access_key_id>' AWS_SECRET_KEY = '<your_secret_access_key>')
  FILE_FORMAT = csv_standard;

-- Sanity check once the stage exists: this lists what's actually sitting
-- in the bucket, from Snowflake's side, without loading anything yet.
-- LIST @claimpulse_raw_stage/source=batch/table=claims/;
--
-- carriers/providers are reference data uploaded under a single fixed key
-- (table=carriers/carriers.csv, no year=/month=/day=/ split) instead of a
-- new timestamped file every run -- COPY INTO's load history then skips
-- them once loaded instead of re-ingesting the same 10/208 rows daily.
-- LIST @claimpulse_raw_stage/source=batch/table=carriers/;

-- ===================================================================
-- 6. Raw tables -- one per source table, columns match the CSVs exactly.
-- Everything is loaded as-is; no cleaning happens here (see
-- ARCHITECTURE.md section 7 -- RAW is immutable and untyped-safe).
-- ===================================================================
CREATE TABLE IF NOT EXISTS carriers (
  carrier_id STRING,
  carrier_name STRING,
  tpa_flag STRING,          -- kept as string in RAW; cast to boolean in staging
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _source_file STRING
);

CREATE TABLE IF NOT EXISTS providers (
  provider_id STRING,
  provider_name STRING,
  specialty STRING,
  state STRING,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _source_file STRING
);

CREATE TABLE IF NOT EXISTS claims (
  claim_id STRING,
  provider_id STRING,
  carrier_id STRING,
  date_of_loss STRING,      -- kept as string in RAW; cast to date in staging
  claim_open_date STRING,
  claim_status STRING,
  state STRING,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _source_file STRING
);

CREATE TABLE IF NOT EXISTS carrier_letters (
  letter_id STRING,
  claim_id STRING,
  carrier_id STRING,
  letter_type STRING,
  received_date STRING,
  response_due_date STRING,
  classification_status STRING,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _source_file STRING
);

CREATE TABLE IF NOT EXISTS training_records (
  training_id STRING,
  provider_id STRING,
  carrier_id STRING,
  assigned_date STRING,
  completed_date STRING,
  status STRING,
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _source_file STRING
);