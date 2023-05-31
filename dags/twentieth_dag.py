from airflow.datasets import Dataset
#from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

example_dataset = Dataset('/home/jefsu/airflow/files/email.txt')

with DAG(
    dag_id='twentieth_dag',
    start_date=pendulum.datetime(2023,5,30),
    schedule=None,
) as dag:
    producer = BashOperator(
        task_id='producer',
        outlets=[example_dataset],
        bash_command='sleep 5'
    )
    x = example_dataset
    consumer = BashOperator(
        task_id='consumer',
        bash_command='sleep 5'
    )

    producer >> consumer