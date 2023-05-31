import pendulum
from airflow import DAG
from airflow.decorators import task
import logging

logger = logging.getLogger(__name__)

with DAG(
    dag_id='eighteenth_dag',
    start_date=pendulum.datetime(2023,5,30),
    schedule=None
) as dag:
    
    @task
    def add_one(x: int) -> int:
        return x + 1
    
    @task
    def sum_it(values):
        total = sum(values)
        logger.info(f'total was {total}')

    added_values = add_one.expand(x=[1,2,3])
    sum_it(added_values)
