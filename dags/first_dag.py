from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past": False,
    "email": open('/home/jefsu/airflow/files/email.txt', 'r').readlines(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'trigger_rule': 'all_success'
}

with DAG(
    "tutorial",
    default_args=default_args,
    schedule=None
) as dag:
    t1 = BashOperator(
        'print_date',
        bash_command='date'
    )

    t2 = BashOperator(
        'sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3
    )

    t1 >> t2