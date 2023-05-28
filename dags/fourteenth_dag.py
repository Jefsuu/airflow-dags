import pendulum
import logging
from pathlib import Path
from airflow import DAG
from airflow.decorators import task 
from airflow.models.dagrun import DagRun 
from airflow.models.param import Param 
from airflow.models.taskinstance import TaskInstance 
from airflow.utils.trigger_rule import TriggerRule 

logger = logging.getLogger(__name__)


with DAG(
    dag_id='fourteenth_dag',
    start_date=pendulum.datetime(2023,5,28),
    schedule=None,
    params={
        'names':Param(
            ['linda', 'Martha', 'Thomas'],
            type='array',
            title='names to greet'
        ),
        'english':Param(True, type='boolean', title='English'),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },
) as dag:
    @task(task_id='get_names')
    def get_names(**kwargs) -> list[str]:
        ti: TaskInstance = kwargs['ti']
        dag_run: DagRun = ti.dag_run
        if 'names' not in dag_run.conf:
            logger.warning('no names given, was no ui used to trigger?')
            return []
        return dag_run.conf['names']
    
    @task.branch(task_id='select_languages')
    def select_languages(**kwargs) -> list[str]:
        ti: TaskInstance = kwargs['ti']
        dag_run: DagRun = ti.dag_run
        selectd_languages = []
        for lang in ['english', 'german', 'french']:
            if lang in dag_run.conf and dag_run.conf[lang]:
                selectd_languages.append(f'generate_{lang}_greeting')

        return selectd_languages

    @task(task_id='generate_english_greeting')
    def generate_english_greeting(name: str) -> str:
        return f'hello {name}!'
             
    @task(task_id="generate_german_greeting")
    def generate_german_greeting(name: str) -> str:
        return f"Sehr geehrter Herr/Frau {name}."

    @task(task_id="generate_french_greeting")
    def generate_french_greeting(name: str) -> str:
        return f"Bonjour {name}!"

    @task(task_id='print_greetings', trigger_rule=TriggerRule.ALL_DONE)
    def print_greetings(greetings1, greetings2, greetings3):
        for g in greetings1 if greetings1 else []:
            logger.info(g)
        for g in greetings2 if greetings2 else []:
            logger.info(g)
        for g in greetings3 if greetings3 else []:
            logger.info(g)
        if not greetings1 and not greetings2 and not greetings3:
            logger.info('sad, nobody to greet')
        

    lang_select = select_languages()
    names = get_names()
    english_greetings = generate_english_greeting.expand(name=names)
    german_greetings = generate_german_greeting.expand(name=names)
    french_greetings = generate_french_greeting.expand(name=names)
    lang_select >> [english_greetings, german_greetings, french_greetings]
    results_print = print_greetings(english_greetings, german_greetings, french_greetings)
