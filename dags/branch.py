from airflow.sdk import dag, task

@dag
def branch():
    
    @task
    def a():
        return 1
    
    @task.branch
    def b(val):
        if val == 1:
            return "equal_1"
        else:
            return "different_than_1"
    
    @task
    def equal_1(val):
        print(f"equal_1: {val}")
    
    @task
    def different_than_1(val):
        print(f"different_than_1: {val}")
    
    val = a()
    b(val) >> [equal_1(val), different_than_1(val)]

branch()