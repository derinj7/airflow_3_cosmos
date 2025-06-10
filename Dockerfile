FROM astrocrpublic.azurecr.io/runtime:3.0-2

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.9.4 && deactivate

WORKDIR /usr/local/airflow/dbt/snowflake_demo/
RUN source /usr/local/airflow/dbt_venv/bin/activate && dbt deps && deactivate
WORKDIR /usr/local/airflow

# set a connection to the airflow metadata db to use for testing
ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres