from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'livydag',
    default_args=default_args,
    description='Livy DAG',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)


def response_check(response):
    return response.text != 'invalid response'


# airflow test livydag livy_batch 2020-06-06
t2 = SimpleHttpOperator(
    dag=dag,
    task_id="livy_batch",
    method='POST',
    endpoint='/batches',
    http_conn_id="livy",
    headers={"Content-Type": "application/json"},
    data='{"className": "org.apache.spark.examples.SparkPi", "file": "/home/livy/spark-2.4.6-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.6.jar"}',
    log_response=True,
    response_check=response_check
)

# airflow test livydag livy_batch_2 2020-06-06
t3 = LivyOperator(
    dag=dag,
    task_id="livy_batch_2",
    file="/home/livy/spark-2.4.6-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.6.jar",
    class_name="org.apache.spark.examples.SparkPi",
    polling_interval=1
)