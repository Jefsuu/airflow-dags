import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

with DAG(
    dag_id='nineteenth_dag_parent',
    start_date=pendulum.datetime(2023,5,30),
    schedule=None
) as parent_dag:
    parent_task = ExternalTaskMarker(
        task_id='parent_task',
        external_dag_id='nineteenth_dag_child',
        external_task_id='child_task1',
    )

with DAG(
    dag_id='nineteenth_dag_child',
    start_date=pendulum.datetime(2023,5,30),
    schedule=None,
) as child_dag:
    child_task1 = ExternalTaskSensor(
        task_id='child_task1',
        external_dag_id=parent_dag.dag_id,
        external_task_id=parent_task.task_id,
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
    )

    child_task2 = ExternalTaskSensor(
        task_id="child_task2",
        external_dag_id=parent_dag.dag_id,
        external_task_group_id="parent_dag_task_group_id",
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
    )

    child_task3 = EmptyOperator(task_id='child_task3')
    child_task1 >> child_task2 >> child_task3