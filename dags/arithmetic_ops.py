from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- Task callables ---
def start_number(**context):
    value = 100
    context["ti"].xcom_push(key="current_value", value=value)
    print(f"Starting Number is {value}")

def add_fifty(**context):
    current_value = context["ti"].xcom_pull(
        key="current_value", task_ids="start_number"
    )
    new_value = current_value + 50
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"add 50: {current_value} + 50 = {new_value}")

def multiply_two(**context):
    current_value = context["ti"].xcom_pull(
        key="current_value", task_ids="add_fifty"
    )
    new_value = current_value * 2
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"multiply by 2: {current_value} * 2 = {new_value}")

def divide_10(**context):
    current_value = context["ti"].xcom_pull(
        key="current_value", task_ids="multiply_two"
    )
    new_value = current_value / 10
    context["ti"].xcom_push(key="current_value", value=new_value)
    print(f"divide by 10: {current_value} / 10 = {new_value}")

# --- Define DAG & tasks ---
with DAG(
    dag_id="arithmetic_operation",
    start_date=datetime(2025, 9, 19),
    schedule=None,   # Airflow >=2.7
    catchup=False,
) as dag:

    start_number_task = PythonOperator(
        task_id="start_number",
        python_callable=start_number,
    )

    add_fifty_task = PythonOperator(
        task_id="add_fifty",
        python_callable=add_fifty,
    )

    multiply_two_task = PythonOperator(
        task_id="multiply_two",
        python_callable=multiply_two,
    )

    divide_10_task = PythonOperator(
        task_id="divide_10",
        python_callable=divide_10,
    )

    start_number_task >> add_fifty_task >> multiply_two_task >> divide_10_task