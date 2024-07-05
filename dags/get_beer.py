from airflow import DAG
import datetime
import json

from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
import psycopg2


def parse_response(beer_list, **context):
    beers = json.loads(beer_list)
    ti = context['ti']

    conn = psycopg2.connect(dbname="beer_db", user="beer", password="beer", host="172.19.0.2")
    cursor = conn.cursor()

    for i, el in enumerate(beers):
        ti.xcom_push(key=f'beer_{i}', value=el)
        brewery_name = el['name']
        brewery_type = el['brewery_type']
        brewery_address = el['street']
        brewery_city = el['city']
        brewery_state = el['state']
        brewery_latitude = el['latitude']
        brewery_longitude = el['longitude']
        brewery_website = el['website_url']

        cursor.execute(
            f"INSERT INTO h_brewery(brewery_name, brewery_type, source_id) VALUES('{brewery_name}', '{brewery_type}'"
            f", NULL) RETURNING brewery_sk")

        brewery_sk = cursor.fetchone()[0]

        cursor.execute(
            f"INSERT INTO s_brewery_address(brewery_sk, website_url, street, latitude, longitude, source_id) "
            f"VALUES({brewery_sk}, '{brewery_website}', '{brewery_address}', '{brewery_latitude}',"
            f"'{brewery_longitude}', NULL)")

        cursor.execute(f"INSERT INTO h_state(source_id) VALUES(NULL) RETURNING state_sk")

        state_sk = cursor.fetchone()[0]
        cursor.execute(f"INSERT INTO s_state(state_sk, state_name, source_id) VALUES({state_sk}, '{brewery_state}', NULL)")

        cursor.execute(f"INSERT INTO l_brewery_state(brewery_sk, state_sk, source_id) VALUES({brewery_sk}, {state_sk}, NULL)")

    conn.commit()
    print("data inserted")


def check_response(response):
    if response.status_code == 200:
        return True

    return False


dag = DAG(dag_id='get_beer',
          start_date=datetime.datetime(2024, 6, 16),
          schedule_interval='@daily')

get_response = HttpOperator(task_id='get_response',
                            method='GET',
                            http_conn_id='beer_conn',
                            response_check=check_response,
                            dag=dag)

parse_response = PythonOperator(task_id='parse_response',
                                python_callable=parse_response,
                                dag=dag,
                                op_kwargs={
                                    'beer_list': '{{ ti.xcom_pull(task_ids="get_response", key="return_value") }}'})

get_response >> parse_response
