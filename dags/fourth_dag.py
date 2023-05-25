import json
import pendulum
from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator
from textwrap import dedent

@dag(
    'fourth_dag',
    schedule=None,
    start_date=pendulum.datetime(2023,5,24)
)
def hello():

    @task()
    def get_name(name: str):
        BashOperator(
            task_id='bash_task',
            bash_command='echo "iniciando dag" && date'
        ).execute(context={})
        return name
    
    @task()
    def welcome(name: str):
        BashOperator(
            task_id='welcome',
            bash_command=f'echo "Bem vindo {name}!!"',
        ).execute(context={})

    get_name_task = get_name('Jeferson')
    welcome(get_name_task)
hello()
