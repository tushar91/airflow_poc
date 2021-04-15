# -*- coding: utf-8 -*-
from airflow.models import DAG
from dags.library_files.library_example import example_python_task
from airflow.operators.python_operator import PythonOperator


def subdag_example(parent_dag_name, child_dag_name, repeat, args):
    dag = DAG(
        f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@daily",
    )
    for i in range(repeat):
        section_task = PythonOperator(
            task_id=f"section_task_{i}",
            python_callable=example_python_task,
            default_args=args,
            dag=dag
        )
    return dag
