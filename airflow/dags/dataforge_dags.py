from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='dataforge_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='python /opt/airflow/scripts/producer.py'
    )
    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='python /opt/airflow/scripts/consumer.py'
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dbt_dataforge && dbt run'
    )

    run_producer >> run_consumer >> run_dbt
    