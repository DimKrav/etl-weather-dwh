from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.extract_raw_weather_data import extract_weather_data
from src.transform_raw_to_staging import transform_raw_to_staging
from src.load_staging_to_serving import load_staging_to_serving

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup' : False
}

with DAG(
    'etl_dag',
    default_args=default_args,
    schedule_interval='15 * * * *', 
    catchup=False,  
) as dag:
    run_extraction_script = PythonOperator(
        task_id='run_extraction_script',
        python_callable=extract_weather_data
    )

    run_transform_script = PythonOperator(
        task_id='run_transform_script',
        python_callable=transform_raw_to_staging
    )

    run_load_script = PythonOperator(
        task_id='run_load_script',
        python_callable=load_staging_to_serving
    )
    
    # Define task dependencies
    run_extraction_script >> run_transform_script >> run_load_script
