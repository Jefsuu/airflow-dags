import logging
import pendulum
from airflow import DAG
from airflow.decorators import task

logger = logging.getLogger(__name__)

@task
def generate_value():
    return "Bring me a coca"

@task
def print_value(value, ts=None):
    logger.info("The knights of Ni say: %s (at %s)", value, ts)

with DAG(
    dag_id='eleventh_dag',
    start_date=pendulum.datetime(2023,5,27),
    schedule=None
) as dag:
    print_value(generate_value())


