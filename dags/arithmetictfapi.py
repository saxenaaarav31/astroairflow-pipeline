'''
Task flow api allows us to use decorators instead of operators
such as pythonOperator
'''

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id = 'arithmetic_operator_tfapi'
) as dag:
    
    # Task 1: to start with a number (say 100)
    @task
    def start_number():
        initial_value = 100
        print(f"starting_number: {initial_value}")
        return initial_value
    
    # Task 3: add 50 to the number
    @task
    def add_fifty(number):
        new_value=number + 50
        print(f"Add fifty : {number} + 50 = {new_value}")
        return new_value
    # Task 4: multiply the number by 2
    @task
    def multiply_two(number):
        new_value = number * 2
        print(f"multiply by 2: {number} * 2 = {new_value}")
        return new_value
        
    # Task 5: divide the number by 10
    @task
    def divide_10(number):
        new_value = number / 10
        print(f"divide by 10: {number} / 10 = {new_value}")
        return new_value
        
        
    # dependecies
    
    start_value = start_number()
    second_value = add_fifty(start_value)
    third_value = multiply_two(second_value)
    fourth_value = divide_10(third_value)