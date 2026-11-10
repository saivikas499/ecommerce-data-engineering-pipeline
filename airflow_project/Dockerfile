FROM apache/airflow:3.4.0-python3.13

USER airflow

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-core==1.12.6" \
    "dbt-snowflake==1.12.5"
