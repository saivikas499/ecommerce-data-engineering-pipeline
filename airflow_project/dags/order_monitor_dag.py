
import pendulum

from datetime import timedelta
import os
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification


default_args = {
    "owner": "airflow",
    "email": [os.environ["ALERT_EMAIL"]],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

email_alert = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email=os.environ["ALERT_EMAIL"],
    to=os.environ["ALERT_EMAIL"],
    subject="[ALERT] E-commerce Order Monitoring Failed",
    html_content="""
    <h3>E-commerce Order Monitoring Alert</h3>
    <p>A task in the order monitoring pipeline has failed.</p>
    <p><b>DAG:</b> {{ dag.dag_id }}</p>
    <p><b>Task:</b> {{ ti.task_id }}</p>
    <p><b>Execution Date:</b> {{ ts }}</p>
    <p><b>Error:</b> {{ exception }}</p>
    <p><b>Task Logs:</b> <a href="{{ ti.log_url }}">View Logs</a></p>
    """,
)

with DAG(
    dag_id="order_monitoring_dag",
    default_args=default_args,
    description="Monitor e-commerce orders and send alerts when delayed orders are found",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ecommerce", "order-monitoring"],
) as dag:

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
        "/home/airflow/dbt-venv/bin/dbt run "
        "--project-dir /opt/airflow/dbt_ecommerce "
        "--profiles-dir /opt/airflow/config/dbt"
        ),
    )

    check_orders = BashOperator(
        task_id="check_orders",
        bash_command=(
        "/home/airflow/dbt-venv/bin/python "
        "/opt/airflow/dags/utils/check_delayed_orders.py"
        ),
        on_failure_callback=[email_alert],
    )

    dbt_run >> check_orders
