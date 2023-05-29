import pendulum
from airflow import DAG 
from airflow.example_dags.subdags.subdag import subdag 
from airflow.operators.empty import EmptyOperator 
from airflow.operators.subdag import SubDagOperator

with DAG(
    dag_id='seventeenth_dag',
    default_args={'retries': 2},
    start_date=pendulum.datetime(2023,5,29),
    schedule=None,
) as dag:
    start = EmptyOperator(task_id='start')

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag('seventeenth_dag', 'section-1', dag.default_args),

    )

    some_other_task = EmptyOperator(
        task_id='some-other-task',

    )

    section_2 = SubDagOperator(
        task_id='section-2',
        subdag=subdag('seventeenth_dag', 'section-2', dag.default_args)
    )

    end = EmptyOperator(task_id='end')

    start >> section_1 >> some_other_task >> section_2 >> end