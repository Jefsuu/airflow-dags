import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

@task.branch()
def should_run(**kwargs) -> str:
    print(
        f"------------- exec dttm = {kwargs['execution_date']} and minute = {kwargs['execution_date'].minute}"
    )

    if kwargs['execution_date'].minute % 2 == 0:
        return "empty_task_1"
    else:
        return "empty_task_2"
@task()
def empty_task_1():
    EmptyOperator(task_id='empty_task_1')

@task()
def empty_task_2():
    EmptyOperator(task_id='empty_task_2')

with DAG(
    dag_id='eighth_dag',
    schedule=None,
    start_date=pendulum.datetime(2023,5,27)
) as dag:
    cond = should_run()

    # @task()
    # def empty_task_1():
    # empty_task_1 = EmptyOperator(task_id='empty_task_1')
    
    # @task()
    # def empty_task_2():
    # empty_task_2 = EmptyOperator(task_id='empty_task_2')

    empty_task_1 = empty_task_1()
    empty_task_2 = empty_task_2()
    
    cond >> [empty_task_1, empty_task_2]
