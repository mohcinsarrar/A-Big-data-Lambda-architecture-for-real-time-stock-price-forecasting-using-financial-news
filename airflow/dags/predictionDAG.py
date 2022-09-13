from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id = "Prediction",
    default_args=default_args,
    start_date = datetime(2022,8,16,0),
    schedule_interval = '*/10 * * * *',
    #schedule_interval = None,
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id = 'execute_prediction',
        bash_command = "spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.2.0 --driver-memory 16G --master spark://spark:7077 /opt/airflow/dags/Serving_layer/prediction.py"
        )

    task1