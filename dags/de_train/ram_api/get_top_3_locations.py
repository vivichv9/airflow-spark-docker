import logging

import psycopg2
from airflow import DAG

from operators.RamTopLocationOperator import RamTopLocationsOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'vivichv9',
    'start_date': days_ago(1),
    'poke_interval': 100,
}

table_creation = ("CREATE TABLE IF NOT EXISTS k_efimenko_ram_location("
                  "id int PRIMARY KEY,"
                  "name text,"
                  "type text,"
                  "dimension text,"
                  "resident_cnt int)")

insertion = ("INSERT INTO k_efimenko_ram_location (id, name, type, dimension, resident_cnt) "
             "VALUES ({id}, '{name}', '{type}', '{dimension}', {resident_cnt})")


def insert_to_db(**context):
    hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    locations = list(context['ti'].xcom_pull(task_ids='top3', key='return_value'))
    conn = hook.get_conn()

    try:
        cursor = conn.cursor()
        for location in locations:
            try:
                cursor.execute(
                    insertion.format(id=location[0], name=location[1], type=location[2], dimension=location[3],
                                     resident_cnt=location[4]))
            except psycopg2.IntegrityError as pe:
                logging.error(pe)
                conn.rollback()
            else:
                conn.commit()
        cursor.close()

    except Exception as e:
        logging.error(e)

    conn.close()


with DAG('get_top_3_locations', default_args=DEFAULT_ARGS, schedule_interval='@daily', tags=['de-train']) as dag:
    create_table = SQLExecuteQueryOperator(task_id='create_table', conn_id='conn_greenplum_write', sql=table_creation,
                                           dag=dag)

    top3_operator = RamTopLocationsOperator(task_id='top3', locations_count=3, dag=dag)

    insert = PythonOperator(task_id="insert_to_db", dag=dag, python_callable=insert_to_db)

    create_table >> top3_operator >> insert
