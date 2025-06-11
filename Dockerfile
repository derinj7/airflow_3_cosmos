FROM astrocrpublic.azurecr.io/runtime:3.0-3

# Install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.9.4

# Copy dbt project files to match DAG expectations
COPY dbt/warehouse /usr/local/airflow/dbt/snowflake_demo

# Set working directory for dbt operations
WORKDIR /usr/local/airflow/dbt/snowflake_demo

# Create dummy profiles.yml and generate manifest.json
RUN mkdir -p ~/.dbt && \
    cat > ~/.dbt/profiles.yml << 'EOF' && \
warehouse:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: dummy
      user: dummy
      password: dummy
      role: dummy
      warehouse: dummy
      database: dummy
      schema: dummy
      threads: 4
EOF
    source /usr/local/airflow/dbt_venv/bin/activate && \
    dbt deps && \
    dbt parse

# Verify manifest was created
RUN ls -la target/manifest.json

# Return to airflow working directory
WORKDIR /usr/local/airflow

ENV AIRFLOW_CONN_AIRFLOW_METADATA_DB=postgresql+psycopg2://postgres:postgres@postgres:5432/postgres