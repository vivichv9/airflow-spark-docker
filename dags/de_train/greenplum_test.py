import logging
from datetime import datetime
from dateutil import parser
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    'owner': 'vivichv9',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 100,
}


def get_data(date: str, **context):
    pg_hook = PostgresHook(postgres_conn_id='greenplum_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT heading FROM public.articles '
                   'WHERE id = {ds}'.format(ds=datetime.weekday(parser.parse(date))))
    result = cursor.fetchall()
    logging.info(result)
    cursor.close()


with DAG(
        dag_id='greenplum_test',
        default_args=DEFAULT_ARGS,
        schedule_interval='1 1 * * MON,TUE,WED,THU,FRI,SAT',
        tags=['de_train'],
        max_active_runs=2,
) as dag:
    start_operator = EmptyOperator(task_id='StartOperator', dag=dag)

    get_data_operator = PythonOperator(task_id='GetDataOperator', python_callable=get_data, op_args=['{{ ds }}'],
                                       dag=dag)

    stop_operator = EmptyOperator(task_id='StopOperator', dag=dag)

    start_operator >> get_data_operator >> stop_operator
