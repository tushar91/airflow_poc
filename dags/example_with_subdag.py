# -*- coding: utf-8 -*-
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.celery_executor import CeleryExecutor
from datetime import datetime, timedelta
from dags.library_files.library_example import example_python_task
from dags.subdags.subdag_example import subdag_example

args = {
    'owner': 'Tushar',
    'start_date': datetime(2021, 4, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

DAG_NAME = 'example_with_subdag'

dag = airflow.DAG(
    dag_id=DAG_NAME,
    schedule_interval="*/7 * * * *",
    default_args=args,
    max_active_runs=1
)

start = PythonOperator(
    task_id="start",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

repeated_1 = SubDagOperator(
    executor=CeleryExecutor(),
    subdag=subdag_example(
        parent_dag_name=DAG_NAME,
        child_dag_name='repeated_1',
        repeat=4,
        args=args
    ),
    task_id='repeated_1',
    default_args=args,
    dag=dag,
)

do_something_else = PythonOperator(
    task_id="do_something_else",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

repeated_2 = SubDagOperator(
    executor=CeleryExecutor(),
    subdag=subdag_example(
        parent_dag_name=DAG_NAME,
        child_dag_name='repeated_2',
        repeat=3,
        args=args
    ),
    task_id='repeated_2',
    default_args=args,
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

start >> repeated_1 >> do_something_else >> repeated_2 >> end
