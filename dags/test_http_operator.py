import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from bs4 import BeautifulSoup


def check_response(**context):
    html_code = context['ti'].xcom_pull(task_ids='get_op', key='return_value')
    soup = BeautifulSoup(str(html_code), 'lxml')
    return soup.find('div', class_='mainpagediv')


dag = DAG(
    dag_id='test_http_operator',
    start_date=datetime.datetime(2024, 6, 16),
    schedule_interval='@daily',
)

task_get_op = HttpOperator(
    task_id="get_op",
    method="GET",
    http_conn_id="parse_train",
    dag=dag
)

check_response_op = PythonOperator(task_id='check_response',
                                   python_callable=check_response, )

task_get_op >> check_response_op
