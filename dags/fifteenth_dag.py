import pendulum
from airflow.models.dag import DAG 
from airflow.operators.bash import BashOperator 
from airflow.operators.empty import EmptyOperator 
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='fifteenth_dag',
    start_date=pendulum.datetime(2023,5,29),
    schedule=None
) as dag:
    start = EmptyOperator(task_id='start')

    with TaskGroup('section_1', tooltip='Tasks for section_1') as section_1:
        task_1 = EmptyOperator(task_id='task_1')
        task_2 = BashOperator(task_id='task_2', bash_command='echo 1')
        task_3 = EmptyOperator(task_id='task_3')

        task_1 >> [task_2, task_3]

    
    with TaskGroup('section_2', tooltip='Tasks for section_2') as section_2:
        task_1 = EmptyOperator(task_id='task_1')

        with TaskGroup('innert_section_2', tooltip='Tasks for inner_section2') as inner_section2:
            task_2 = BashOperator(task_id='task_2', bash_command='echo 1')
            task_3 = EmptyOperator(task_id='task_3')
            task_4 = EmptyOperator(task_id='task_4')

            [task_2, task_3] >> task_4
    
    end = EmptyOperator(task_id='end')

    start >> section_1 >> section_2 >> end

