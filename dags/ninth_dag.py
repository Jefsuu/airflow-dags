import pendulum
from airflow import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator

dag1 = DAG(
    dag_id='ninth_dag_1',
    start_date=pendulum.datetime(2023,5,27),
    schedule=None
)

empty_task_11 = EmptyOperator(task_id='date_in_range', dag=dag1)
empty_task_21 = EmptyOperator(task_id='date_outside_range', dag=dag1)

cond1 = BranchDateTimeOperator(
    task_id='datetime_branch',
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=pendulum.datetime(2023,5,27,15,0,0),
    target_lower=pendulum.datetime(2023,5,27,14,0,0),
    dag=dag1,
)

cond1 >> [empty_task_11, empty_task_21]

dag2 = DAG(
    dag_id='ninth_dag_2',
    start_date=pendulum.datetime(2023,5,27),
    schedule=None
)

empty_task_12 = EmptyOperator(task_id="date_in_range", dag=dag2)
empty_task_22 = EmptyOperator(task_id="date_outside_range", dag=dag2)

cond2 = BranchDateTimeOperator(
    task_id="datetime_branch",
    follow_task_ids_if_true=["date_in_range"],
    follow_task_ids_if_false=["date_outside_range"],
    target_upper=pendulum.time(0, 0, 0),
    target_lower=pendulum.time(15, 0, 0),
    dag=dag2,
)

cond2 >> [empty_task_12, empty_task_22]

dag3 = DAG(
    dag_id='ninth_dag_3',
    start_date=pendulum.datetime(2023,5,27),
    schedule=None
)

empty_task_13 = EmptyOperator(task_id="date_in_range", dag=dag3)
empty_task_23 = EmptyOperator(task_id="date_outside_range", dag=dag3)

cond3 = BranchDateTimeOperator(
    task_id='datetime_branch',
    use_task_execution_date=True,
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=pendulum.datetime(2023, 5, 27, 16, 0, 0),
    target_lower=pendulum.datetime(2023, 5, 27, 18, 0, 0),
    dag=dag3,
)

cond3 >> [empty_task_13, empty_task_23]