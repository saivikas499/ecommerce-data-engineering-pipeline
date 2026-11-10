
import os
from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator


with DAG(
    dag_id="smtp_delivery_test",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["smtp", "testing"],
) as dag:

    send_test_email = EmailOperator(
        task_id="send_test_email",
        conn_id="smtp_default",
        to=os.environ["ALERT_EMAIL"],
        from_email=os.environ["ALERT_EMAIL"],
        subject="Airflow SMTP Delivery Test",
        html_content="""
            <h2>SMTP Test Successful</h2>
            <p>Airflow successfully sent this test email.</p>
        """,
    )
