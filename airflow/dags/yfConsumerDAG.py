from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id = "yf_consumer",
    default_args=default_args,
    start_date = datetime(2022,8,16,0),
    schedule_interval = None,
) as dag:
    task1 = BashOperator(
        task_id = 'execute_yf_consumer',
        bash_command = "spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --driver-memory 16G --master spark://spark:7077 /opt/airflow/dags/Batch_layer/yfConsumer.py"
        )

    task1