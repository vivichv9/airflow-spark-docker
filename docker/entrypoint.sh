#!/bin/bash

# DB create
sleep 10
airflow db init
sleep 10

# User create
airflow users create \
          --username admin \
          --firstname admin \
          --lastname admin \
          --role Admin \
          --email admin@example.org \
          -p 12345

# start scheduler and webserver
airflow scheduler & airflow webserver