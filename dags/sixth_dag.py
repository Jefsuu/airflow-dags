import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay

with DAG(
    dag_id='sixth_dag',
    start_date=pendulum.datetime(2023,5,25),
    schedule=None,
) as dag:

    empty_task_1 = EmptyOperator(task_id="branch_true")
    empty_task_2 = EmptyOperator(task_id="branch_false")
    empty_task_3 = EmptyOperator(task_id="branch_branch_weekend")
    empty_task_4 = EmptyOperator(task_id="branch_mid_week")

    branch = BranchDayOfWeekOperator(
        task_id = 'make_choice',
        follow_task_ids_if_false='branch_false',
        follow_task_ids_if_true='branch_true',
        week_day="THURSDAY",
    )

    branch_weekend = BranchDayOfWeekOperator(
        task_id='make_weekend_choice',
        follow_task_ids_if_false='branch_mid_week',
        follow_task_ids_if_true='branch_weekend',
        week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},
    )

    branch >> [empty_task_1, empty_task_2]

    empty_task_2 >> branch_weekend >> [empty_task_3, empty_task_4]