import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id="test_dag",
        start_date=datetime.datetime(2024, 6, 15),
        schedule="@daily",
):
    EmptyOperator(task_id="task") >> EmptyOperator(task_id="task_1")