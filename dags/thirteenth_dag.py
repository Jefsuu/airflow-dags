import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException 
from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator 
from airflow.utils.context import  Context
from airflow.utils.trigger_rule import TriggerRule 

class EmptySkipOperator(BaseOperator):
    ui_color = "#e8b7e4"

    def execute(self, context: Context):
        raise AirflowSkipException
    
def create_test_pipeline(suffix, trigger_rule):
    skip_operator = EmptySkipOperator(task_id=f'skip_operator_{suffix}')
    always_true = EmptySkipOperator(task_id=f'always_true_{suffix}')
    join = EmptyOperator(task_id=trigger_rule, trigger_rule=trigger_rule)
    final = EmptyOperator(task_id=f'final_{suffix}')

    skip_operator >> join
    always_true >> join
    join >> final

with DAG(
    dag_id='thirteenth_dag',
    start_date=pendulum.datetime(2023,5,28),
    schedule=None
) as dag:
    create_test_pipeline('1', TriggerRule.ALL_SUCCESS)
    create_test_pipeline('2', TriggerRule.ONE_SUCCESS)