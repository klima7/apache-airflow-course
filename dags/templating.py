from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator


def get_number():
    return 42

def add_one(num):
    return int(num) + 1

@dag(
    params={"num": 4},
    user_defined_macros={"get_number": get_number},
    user_defined_filters={"add_one": add_one},
    default_args={"author": "Łukasz Klimkiewicz"},
    owner_links={
        "data-team": "https://company.com/data-team",
        "slack": "https://company.slack.com/archives/data-team",
        "docs": "https://docs.company.com/dags/my-dag"
    }
)
def templating_dag():
    
    def _extract_data(a, current_dag_run_date, templates_dict):
        print(a, current_dag_run_date)
        print(templates_dict['new_variable'])

    PythonOperator(
        task_id="extract_task",
        python_callable=_extract_data,
        op_args=["{{ds}}"],
        op_kwargs={
            "current_dag_run_date": "{{ds}}"
        },
        templates_dict={
            "new_variable": "Ala ma kota {{get_number()}} {{6 | add_one}}",
        },
    )
    
templating_dag()
