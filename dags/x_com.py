from airflow.sdk.bases import xcom
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.timezone import utc

dag = DAG(
    dag_id="xcom_example_dag",
    start_date=datetime(2026, 5, 31),
    schedule=CronDataIntervalTimetable(cron ="*/2 * * * *", timezone=utc),
    )

def product_page_callable(**context):
    ti = context["ti"]  
    ti.xcom_push(key="output_path", value="/airflow/output_files/folder1/test.csv")
    print("Pushed Xcom with key 'output_path' and value '/airflow/output_files/folder1/test.csv'")

product_page = PythonOperator(
    task_id="product_page",
    dag=dag,
    python_callable= product_page_callable
)

def read_raw_callable(**context):
    ti = context["ti"]  
    output_path = ti.xcom_pull(key="output_path", task_ids="product_page")
    print(f"Pulled Xcom with key 'output_path' from task 'product_page': {output_path}")

read_raw = PythonOperator(
    task_id="read_raw_data",
    dag=dag,
    python_callable= read_raw_callable
)

product_page >> read_raw
