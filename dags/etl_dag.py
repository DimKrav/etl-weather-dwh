from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    'etl_dag',
    default_args=default_args,
    schedule_interval='15 * * * *', 
    catchup=False,  
) as dag:
    run_extraction_script = BashOperator(
        task_id='run_extraction_script',
        bash_command='python /opt/airflow/src/extract_raw_weather_data.py'
    )

    run_transform_script = BashOperator(
        task_id='run_transform_script',
        bash_command='python /opt/airflow/src/transform_raw_to_staging.py'
    )

    run_load_script = BashOperator(
        task_id='run_load_script',
        bash_command='python /opt/airflow/src/load_staging_to_serving.py'
    )

    # Define task dependencies
    run_extraction_script >> run_transform_script >> run_load_script
