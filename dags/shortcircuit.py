from airflow.decorators import task, dag
from airflow.models.baseoperator import chain
from datetime import datetime

def my_evaluation(value):
    return value

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
)
def shortcircuit_dag():
    
    @task
    def start():
        return True
    
    @task.short_circuit
    def my_evaluation(value):
        return value
    
    @task
    def end():
        print('bye')
        
    my_evaluation(start()) >> end()
    
shortcircuit_dag()