from airflow.sdk import dag, task, task_group

@dag
def group():
    
    @task
    def a():
        print("a")
        return 42
        
    @task_group(default_args={
        "retries": 2
    })
    def my_group():
        
        @task
        def b():
            print("b")
            
        @task
        def c():
            print("c")
            
        b() >> c()
        
    a() >> my_group()


group()
