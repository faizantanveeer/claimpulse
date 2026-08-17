"""
ClaimPulse Snowflake loader.

Runs COPY INTO for each raw table, pulling from the S3 stage set up in
snowflake_setup.sql. Intended to be called on a schedule (for now: run it
manually or via cron; later: an Airflow task calls this on the daily DAG).

Fault tolerance note: Snowflake's COPY INTO tracks a 64-day load history
per table automatically. Re-running this script never double-loads a file
that already succeeded -- COPY INTO silently skips it. This means the
"timer" approach is safe even if it fires more often than new data
arrives, or if a previous run partially failed and gets retried.
"""

import logging
import os
import sys

import snowflake.connector

from dotenv import load_dotenv
load_dotenv() 

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("claimpulse-loader")

SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = "claimpulse_loader"
SNOWFLAKE_WAREHOUSE = "claimpulse_wh"
SNOWFLAKE_DATABASE = "claimpulse_raw"
SNOWFLAKE_SCHEMA = "raw_claims"
STAGE_NAME = "claimpulse_raw_stage"

TABLES = ["carriers", "providers", "claims", "carrier_letters", "training_records"]

# Explicit column order matching each CSV exactly, since a transformation-based
# COPY INTO (needed to populate _loaded_at / _source_file) addresses source
# columns positionally ($1, $2, ...) rather than by name.
TABLE_COLUMNS = {
    "carriers": ["carrier_id", "carrier_name", "tpa_flag"],
    "providers": ["provider_id", "provider_name", "specialty", "state"],
    "claims": ["claim_id", "provider_id", "carrier_id", "date_of_loss",
               "claim_open_date", "claim_status", "state"],
    "carrier_letters": ["letter_id", "claim_id", "carrier_id", "letter_type",
                        "received_date", "response_due_date", "classification_status"],
    "training_records": ["training_id", "provider_id", "carrier_id",
                          "assigned_date", "completed_date", "status"],
}


def get_connection():
    required = [SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD]
    if not all(required):
        log.error("Missing Snowflake credentials. Set SNOWFLAKE_ACCOUNT, "
                   "SNOWFLAKE_USER, SNOWFLAKE_PASSWORD as environment variables.")
        sys.exit(1)
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def copy_into_table(cursor, table_name: str):
    stage_path = f"@{STAGE_NAME}/source=batch/table={table_name}/"
    source_columns = TABLE_COLUMNS[table_name]
    target_columns = source_columns + ["_loaded_at", "_source_file"]

    # Positional $1, $2, ... refs matching source_columns order exactly,
    # plus CURRENT_TIMESTAMP() and METADATA$FILENAME for the two audit
    # columns COPY INTO would otherwise silently leave NULL -- MATCH_BY_
    # COLUMN_NAME only maps columns present in the file, and does NOT fall
    # back to a column's DEFAULT for anything missing; it inserts NULL.
    positional_refs = ", ".join(f"${i+1}" for i in range(len(source_columns)))

    query = f"""
        COPY INTO {table_name} ({", ".join(target_columns)})
        FROM (
            SELECT {positional_refs}, CURRENT_TIMESTAMP(), METADATA$FILENAME
            FROM {stage_path} (FILE_FORMAT => 'csv_standard')
        )
        ON_ERROR = 'CONTINUE'
    """
    log.info(f"Loading {table_name} from {stage_path}")
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [col[0] for col in cursor.description]

    total_rows_loaded = 0
    total_rows_errored = 0
    for row in results:
        row_dict = dict(zip(columns, row))
        total_rows_loaded += row_dict.get("rows_loaded", 0) or 0
        total_rows_errored += row_dict.get("errors_seen", 0) or 0
        log.info(f"  {row_dict.get('file')}: status={row_dict.get('status')}, "
                 f"rows_loaded={row_dict.get('rows_loaded')}, "
                 f"errors={row_dict.get('errors_seen')}")

    log.info(f"  {table_name} totals: {total_rows_loaded} rows loaded, "
             f"{total_rows_errored} rows errored")
    return total_rows_loaded, total_rows_errored


def main():
    conn = get_connection()
    cursor = conn.cursor()
    grand_total_loaded = 0
    grand_total_errored = 0

    try:
        for table_name in TABLES:
            loaded, errored = copy_into_table(cursor, table_name)
            grand_total_loaded += loaded
            grand_total_errored += errored
    finally:
        cursor.close()
        conn.close()

    log.info(f"Load complete: {grand_total_loaded} rows loaded across "
             f"{len(TABLES)} tables, {grand_total_errored} rows errored total")

    if grand_total_errored > 0:
        log.warning("Some rows errored during load -- check COPY_HISTORY "
                     "in Snowflake for details before assuming the load is clean.")


if __name__ == "__main__":
    main()