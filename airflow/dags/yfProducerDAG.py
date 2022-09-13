from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id = "yf_producer",
    default_args=default_args,
    start_date = datetime(2022,8,16,0),
    schedule_interval = None,
) as dag:
    task1 = BashOperator(
        task_id = 'execute_yf_producer',
        bash_command = "python /opt/airflow/dags/producer/yfProducer.py"
        )

    task1