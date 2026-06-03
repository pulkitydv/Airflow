from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id="producer_dag",
    start_date=datetime(2026,6,2),
    schedule="@hourly",
    catchup=False
)

task1 = BashOperator(
    task_id="generate_file",
    dag=dag,
    bash_command="echo Producer DAG completed"
)