from datetime import datetime, timedeltaa
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator


def task_hello_world():
    return "Hello, world!"


def task_send_email(**kwargs):
    task_instance = kwargs['ti']
    message = task_instance.xcom_pull(task_ids='task_hello_world')
    subject = "Resultado da tarefa 'task_hello_world'"
    to = "seu-email@exemplo.com"
    return EmailOperator(
        task_id='task_send_email',
        to=to,
        subject=subject,
        html_content=message
    ).execute(context=kwargs)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email': ['seu-email@exemplo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('exemplo_dag',
         default_args=default_args,
         schedule_interval=timedelta(days=1)) as dag:

    task1 = PythonOperator(
        task_id='task_hello_world',
        python_callable=task_hello_world
    )

    task2 = PythonOperator(
        task_id='task_send_email',
        python_callable=task_send_email,
        provide_context=True
    )

    task1 >> task2
