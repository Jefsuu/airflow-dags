import logging
import shutil
import sys
import tempfile
import time
import pendulum
from pprint import pprint
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator 

logger = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable
BASE_DIR = tempfile.gettempdir()

def x():
    pass

with DAG(
    dag_id='twelfth_dag',
    schedule=None,
    start_date=pendulum.datetime(2023,5,28),

) as dag:
    @task(task_id='print_the_context')
    def print_context(ds=None, **kwargs):
        pprint(kwargs)
        logger.info(ds)

        return 'Whatever you return gets printed in the logs'
    
    run_this = print_context()

    @task(task_id='log_sql_query', templates_dict={'query': 'sql/sample.sql'}, templates_exts=['.sql'])
    def log_sql(**kwargs):
        logger.info('Python task decorator query: %s', str(kwargs['templates_dict']['query']))

    log_the_sql = log_sql()

    for i in range(5):
        @task(task_id=f'sleep_for_{i}')
        def my_sleeping_function(random_base):
            time.sleep(random_base)
        
        sleeping_task = my_sleeping_function(random_base=float(i)/10)

        run_this >> log_the_sql >> sleeping_task

    if not shutil.which('virtualenv'):
        logger.warning('The virtualenv_python example task requires virtualenv, please install it')
    else:
        @task.virtualenv(
            task_id='virtualenv_python', requirements=['colorama==0.4.0'], system_site_packages=False
        )
        def callable_virtualenv():
            from time import sleep
            from colorama import Back, Fore, Style

            logger.info(Fore.RED + 'some red text')
            logger.info(Back.GREEN + 'and with a green background')
            logger.info(Style.DIM + 'and in dim text')
            logger.info(Style.RESET_ALL)

            for _ in range(4):
                print(Style.DIM + 'Please wait...', flush=True)
                sleep(1)
            logger.info('Finished')

            virtualenv_task = callable_virtualenv()

            sleeping_task >> virtualenv_task

        @task.external_python(task_id='external_python', python=PATH_TO_PYTHON_BINARY)
        def callable_external_python():
            import sys
            from time import sleep

            logger.info(f'Running task via {sys.executable}')
            logger.info('Sleeping')
            for _ in range(4):
                print('Please wait...', flush=True)
                sleep(1)
            logger.info('Finished')

        
        external_python_task = callable_external_python()

        external_classic = ExternalPythonOperator(
            task_id='external_python_classic',
            python=PATH_TO_PYTHON_BINARY,
            python_callable=x
        )

        virtual_classic = PythonVirtualenvOperator(
            task_id='virtualenv_classic',
            requirements='colorama==0.4.0',
            python_callable=x
        )

        run_this >> external_classic >> external_python_task >> virtual_classic
