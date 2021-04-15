# -*- coding: utf-8 -*-
import airflow
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dags.library_files.library_example import example_python_task

args = {
    'owner': 'Tushar',
    'start_date': datetime(2021, 4, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

DAG_NAME = 'example_dag'

dag = airflow.DAG(
    dag_id=DAG_NAME,
    schedule_interval="*/4 * * * *",
    default_args=args,
    max_active_runs=1
)

task_1 = PythonOperator(
    task_id="task_1",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

task_2 = PythonOperator(
    task_id="task_2",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

task_3 = PythonOperator(
    task_id="task_3",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

runafter_1_2_3 = PythonOperator(
    task_id="runafter_1_2_3",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

run_seperate = PythonOperator(
    task_id="run_seperate",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

run_last = PythonOperator(
    task_id="run_last",
    python_callable=example_python_task,
    default_args=args,
    dag=dag
)

[task_1, task_2, task_3] >> runafter_1_2_3 >> run_last
run_seperate >> run_last
