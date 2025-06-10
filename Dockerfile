FROM astrocrpublic.azurecr.io/runtime:3.0-2

# set a connection to the airflow metadata db to use for testing
ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres