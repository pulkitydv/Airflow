from airflow.sdk import DAG
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id="consumer_dag",
    start_date=datetime(2025,6,2),
    schedule="@hourly",
    catchup=False
)

wait_for_producer = ExternalTaskSensor(
    task_id="wait_for_producer",
    external_dag_id="producer_dag",
    external_task_id="generate_file",
    mode="reschedule",
    timeout=300,
    poke_interval=30,
    dag=dag
)

consumer_task = BashOperator(
    task_id="consumer_task",
    bash_command="echo Consumer started",
    dag=dag
)

wait_for_producer >> consumer_task