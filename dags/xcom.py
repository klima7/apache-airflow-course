from airflow.sdk import dag, task, Context


@dag
def xcom_dag():
    
    @task
    def t1():
        return {
            "a": 1,
            "b": 2,
        }
        
    
    @task
    def t2(data):
        print(data)
    
    data = t1()
    t2(data)
    
xcom_dag()