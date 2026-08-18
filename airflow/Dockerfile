# Extends the official Airflow image with everything ClaimPulse's tasks need:
# boto3 (S3 uploader), snowflake-connector-python (loader), python-dotenv
# (.env support), faker (data generator), and dbt-snowflake (transformations).
FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
