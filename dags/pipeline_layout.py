from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from datetime import datetime

dag = DAG(
    dag_id="pipeline_layout",
    start_date=datetime(2026, 5, 31),
    schedule="0/2 * * * *",
)

def decide_flow(**context):
    checkout_amount = 50
    if checkout_amount < 100:
        return "checkout"
    else:
        return "merged"

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag, trigger_rule="none_failed")

product_page = BashOperator(task_id="product_page", dag=dag, bash_command="echo 'Processing product page data'")
checkout_page = BashOperator(task_id="checkout_page", dag=dag, bash_command="echo 'Processing checkout page data'")
user_login_page = BashOperator(task_id="user_login_page", dag=dag, bash_command="echo 'Processing user login page data'")

read_raw = BranchPythonOperator(
    task_id="read_raw_data",
    dag=dag,
    python_callable= decide_flow
)

checkout = BashOperator(task_id="checkout", dag=dag, bash_command="echo 'This is checkout task'")
merged = BashOperator(task_id="merged", dag=dag, bash_command="echo 'This is merged task'")

alarming_situation = BashOperator(task_id="alarming_situation", dag=dag, bash_command="echo 'Alarm! Checkout amount is less than 100'")

notify = BashOperator(task_id="notify", dag=dag, bash_command="echo 'This is notify task.'")

start >> [product_page, checkout_page, user_login_page]
[product_page, checkout_page, user_login_page] >> read_raw
read_raw >> [checkout, merged]
checkout >> alarming_situation
merged >> notify
[alarming_situation, notify] >> end
