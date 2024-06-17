FROM python:3.8

ENV AIRFLOW_HOME=/usr/local/airflow

# set Airflow version
ARG AIRFLOW_VERSION=2.9.1

ARG AIRFLOW_WEBSERVER_PORT=8081
ARG AIRFLOW_WEBSERVER_URL=http://localhost:8081
ARG DOCKER_DB_SERVICE=airflow_postgres
ARG AIRFLOW_DB_PORT=5432
ARG AIRFLOW_DB=airflow
ARG AIRFLOW_DB_USER=airflow
ARG AIRFLOW_DB_PASSWORD=airflow

# Dags and plugins folder
ENV AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/dags
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/usr/local/airflow/plugins
ENV AIRFLOW__CORE__FERNET_KEY=eFzwxNhUp6Nc7DXJfDD1CJtM5usv44dCmYL_1CBQMWg=

# Change metadata db and executor
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@${DOCKER_DB_SERVICE}:${AIRFLOW_DB_PORT}/${AIRFLOW_DB}"

# Disable code examples
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Web server config
ENV AIRFLOW__WEBSERVER__BASE_URL=${AIRFLOW_WEBSERVER_URL}
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=${AIRFLOW_WEBSERVER_PORT}
ENV AIRFLOW__WEBSERVER__RBAC=True

COPY requirements-airflow.txt requirements.txt
RUN pip3 install -r requirements.txt

# Copy to container init script
RUN mkdir /project
COPY entrypoint.sh /project/scripts/
RUN chmod +x /project/scripts/entrypoint.sh

# Exec init script
ENTRYPOINT ["/project/scripts/entrypoint.sh"]
