from airflow.sdk import DAG, dag, task
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# dag = DAG(
#     dag_id="taskflow_api_dag",
#     start_date=datetime(2026, 5, 31),
#     schedule="@daily",
#     catchup=False,
#     )

# def extract(**context):
#     ti = context["ti"]
#     ti = ti.xcom_push(key="output_path", value='path/to/data.csv')
#     print("Pushed Xcom with key 'output_path' and value 'path/to/data.csv'")

# def transform(**context):
#     ti = context["ti"]
#     extracted_data = ti.xcom_pull(key="output_path", task_ids="extract")

#     input_path = extracted_data["output_path"]
#     print(f"Transforming data from: {input_path}")

# extract_task = PythonOperator(
#     task_id="extract",
#     python_callable=extract,
#     dag=dag 
#     )

# transform_task = PythonOperator(
#     task_id="transform",
#     python_callable=transform,
#     dag=dag 
#     ) 

# extract_task >> transform_task


#taskflow api code example

@dag(
    dag_id="taskflow_api_dag",
    start_date=datetime(2026, 5, 31),
    schedule="@daily",
    catchup=False,
    )
def taskflow_api_dag():
    @task(multiple_outputs=True)
    def extract():
        return {"output_path": "path/to/data.csv", "status": "success", "location": "asia"}
    
    @task
    def transform(extracted_data):
        input_path = extracted_data["output_path"]
        status = extracted_data["status"]
        location = extracted_data["location"]
        print(f"Transforming data from: {input_path}")

    def load():
        print("Loading transformed data...")

    load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    transform(extract()) >> load


taskflow_api_dag()