from airflow import DAG
import logging

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


def print_start_date(**context):
    logging.info(str(context['execution_date']))


DEFAULT_ARGS = {
    'owner': 'vivichv9',
    'start_date': days_ago(1),
    'poke_interval': 600,
}

with DAG(
        'print_start_date',
        default_args=DEFAULT_ARGS,
        schedule_interval='@daily',
        max_active_runs=1,
        tags=['de-train'],
) as dag:
    empty_print_date = EmptyOperator(task_id='empty_print_date', dag=dag)

    bash_print_date = BashOperator(task_id='bash_print_date', bash_command='echo {{ ds }}')

    python_print_date = PythonOperator(task_id='python_print_date', python_callable=print_start_date,
                                       provide_context=True, dag=dag)

    empty_print_date >> [bash_print_date, python_print_date]