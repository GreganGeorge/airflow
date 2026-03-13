FROM quay.io/astronomer/astro-runtime:12.9.0

USER root
# Create a venv for dbt to keep it isolated from Airflow/Spark
RUN python -m venv dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake
USER astro