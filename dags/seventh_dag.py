import random
import pendulum
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG

# @dag(
#     dag_id='seventh_dag',
#     schedule=None,
#     start_date=pendulum.datetime(2023,5,25),
#     default_args={'email_on_sucess': open('/home/jefsu/airflow/files/email.txt', 'r').readlines()}
# )
with DAG(
    dag_id='seventh_dag',
    schedule=None,
    start_date=pendulum.datetime(2023,5,25),
    default_args={'email_on_sucess': open('/home/jefsu/airflow/files/email.txt', 'r').readlines()}
) as dag:
# def seventh_dag():
    run_this_first = EmptyOperator(task_id="run_this_first")
    options = ["branch_a", "branch_b", "branch_c", "branch_d"]

    @task.branch(task_id="branching")
    def random_choice(choices: list[str]) -> str:
        return random.choice(choices)

    random_choice_instance = random_choice(choices=options)

    run_this_first >> random_choice_instance

    join = EmptyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    for option in options:
        t = EmptyOperator(task_id=option)
        
        empty_follow = EmptyOperator(task_id="follow_" + option)

        random_choice_instance >> Label(option) >> t >> empty_follow >> join