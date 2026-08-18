from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id = "claimpulse_daily_pipeline",
    default_args = default_args,
    schedule = "@daily",
    start_date = datetime(2026, 1, 1),
    catchup = False,
) as dag:
    
    task_1_generate_data = BashOperator(
        task_id = "generate_data_using_python", 
        bash_command = "python  /opt/airflow/data_generator/generate_data.py"
    )
    
    task_2_upload_data_to_s3 = BashOperator(
        task_id = "upload_data_to_s3", 
        bash_command = "python /opt/airflow/scripts/upload_to_s3.py"
    )
    
    task_3_upload_data_to_snowflake = BashOperator(
        task_id = "upload_data_to_snowflake", 
        bash_command = "python /opt/airflow/scripts/upload_to_snowflake.py"
    )

    task_4_dbt_run = BashOperator(
        task_id="dbt_run", 
        bash_command="dbt run --project-dir /opt/airflow/dbt/claimpulse --profiles-dir /opt/airflow/dbt/claimpulse",
    )

    task_5_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/dbt/claimpulse --profiles-dir /opt/airflow/dbt/claimpulse",
        retries=0,   # overrides default_args -- a failed test won't pass on retry, it's a real data issue
    )
        
    

    task_1_generate_data >> task_2_upload_data_to_s3 >> task_3_upload_data_to_snowflake >> task_4_dbt_run >> task_5_dbt_test