from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag_names = ["dag_1", "dag_2", "dag_3"]

for name in dag_names:

    dag = DAG(
        dag_id=name,
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        catchup=False
    )

    BashOperator(
        task_id="start_task",
        bash_command="echo Hello",
        dag=dag
    )

    globals()[name] = dag