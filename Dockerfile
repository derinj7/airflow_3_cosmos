FROM astrocrpublic.azurecr.io/runtime:3.0-2

# Install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.9.4

# Copy dbt project files
COPY dbt/snowflake_demo /usr/local/airflow/dbt/snowflake_demo

# Set working directory for dbt operations
WORKDIR /usr/local/airflow/dbt/snowflake_demo

# Create profiles.yml and generate manifest in one step
RUN mkdir -p ~/.dbt && \
    echo 'snowflake_demo:' > ~/.dbt/profiles.yml && \
    echo '  target: dev' >> ~/.dbt/profiles.yml && \
    echo '  outputs:' >> ~/.dbt/profiles.yml && \
    echo '    dev:' >> ~/.dbt/profiles.yml && \
    echo '      type: snowflake' >> ~/.dbt/profiles.yml && \
    echo '      account: dummy' >> ~/.dbt/profiles.yml && \
    echo '      user: dummy' >> ~/.dbt/profiles.yml && \
    echo '      password: dummy' >> ~/.dbt/profiles.yml && \
    echo '      role: dummy' >> ~/.dbt/profiles.yml && \
    echo '      snowflake_demo: dummy' >> ~/.dbt/profiles.yml && \
    echo '      database: dummy' >> ~/.dbt/profiles.yml && \
    echo '      schema: dummy' >> ~/.dbt/profiles.yml && \
    echo '      threads: 4' >> ~/.dbt/profiles.yml && \
    source /usr/local/airflow/dbt_venv/bin/activate && \
    dbt deps && \
    dbt parse

# Verify manifest was created
RUN ls -la target/manifest.json

# Return to airflow working directory
WORKDIR /usr/local/airflow

# Set a connection to the airflow metadata db to use for testing
ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres