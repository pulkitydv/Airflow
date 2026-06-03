from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(dag_id="my_first_dag")

def print_context(**kwargs):
    print(kwargs)
    print("Job is done")

copy_file = BashOperator(dag = dag, task_id="copy_file", bash_command="echo copying file")

task2 = PythonOperator(dag = dag, task_id="task2", python_callable=print_context)

copy_file >> task2