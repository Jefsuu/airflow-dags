import pendulum
from airflow.decorators import task_group, task 
from airflow import DAG

@task
def task_start():
    return '[task_start]'
@task
def task_1(value: int) -> str:
    return f'[task1 {value} ]'

@task
def task_2(value: str) -> str:
    return f'[ task2 {value} ]'

@task
def task_3(value: str) -> None:
    print(f'[ task3 {value} ]')

@task
def task_end() -> None:
    print(f'[ task_end ]')

@task_group
def task_group_function(value: int) -> None:
    task_3(task_2(task_1(value)))

with DAG(
    dag_id='sixteenth_dag',
    start_date=pendulum.datetime(2023,5,29),
    schedule=None
) as dag:
    start_task = task_start()
    end_task = task_end()

    for i in range(5):
        current_task_group = task_group_function(i)
        start_task >> current_task_group >> end_task