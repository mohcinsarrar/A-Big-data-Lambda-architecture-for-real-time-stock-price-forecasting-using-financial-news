from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id = "BatchPipeline",
    default_args=default_args,
    start_date = datetime(2022,8,16,0),
    schedule_interval = '@daily',
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id = 'execute_batch_pipeline',
        bash_command = "spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.2.0,com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.4 --driver-memory 16G --master spark://spark:7077 /opt/airflow/dags/Batch_layer/batchPipeline.py"
        )

    task1