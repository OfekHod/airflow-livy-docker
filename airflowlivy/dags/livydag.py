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

livy_batch = LivyOperator(
    dag=dag,
    task_id="livy_batch",
    file="/opt/jars/spark-examples_2.11-2.4.6.jar",
    class_name="org.apache.spark.examples.SparkPi",
    polling_interval=1
)