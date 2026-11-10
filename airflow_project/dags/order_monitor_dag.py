
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
    
email_alert = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email=os.environ["ALERT_EMAIL"],
    to=os.environ["ALERT_EMAIL"],
    subject="[FAILED] E-Commerce Order Monitoring | {{ ti.task_id }}",
    html_content="""
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #f4f6f8; padding: 20px;">

        <div style="max-width: 650px; margin: auto; background-color: #ffffff;
                    border: 1px solid #dddddd; border-radius: 8px; overflow: hidden;">

            <div style="background-color: #b42318; color: white; padding: 18px 24px;">
                <h2 style="margin: 0;">Pipeline Failure Alert</h2>
                <p style="margin: 6px 0 0;">E-Commerce Data Engineering Pipeline</p>
            </div>

            <div style="padding: 24px;">

                <h3 style="color: #b42318;">Task Execution Failed</h3>

                <p>
                    Hello,<br><br>
                    An Airflow task has failed during the execution of the
                    E-Commerce Order Monitoring pipeline.
                </p>

                <h3>Execution Details</h3>

                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 10px; border: 1px solid #dddddd;"><b>DAG</b></td>
                        <td style="padding: 10px; border: 1px solid #dddddd;">{{ dag.dag_id }}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #dddddd;"><b>Task</b></td>
                        <td style="padding: 10px; border: 1px solid #dddddd;">{{ ti.task_id }}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #dddddd;"><b>Status</b></td>
                        <td style="padding: 10px; border: 1px solid #dddddd; color: #b42318;">
                            <b>FAILED</b>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #dddddd;"><b>Execution Time (UTC)</b></td>
                        <td style="padding: 10px; border: 1px solid #dddddd;">{{ ts }}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #dddddd;"><b>Attempt</b></td>
                        <td style="padding: 10px; border: 1px solid #dddddd;">{{ ti.try_number }}</td>
                    </tr>
                </table>

                <h3>Error Details</h3>

                <div style="background-color: #f8f8f8; border-left: 4px solid #b42318;
                            padding: 12px; word-break: break-word;">
                    {{ exception }}
                </div>

                <div style="margin-top: 24px;">
                    <a href="{{ ti.log_url }}"
                       style="background-color: #2563eb; color: white;
                              padding: 12px 20px; text-decoration: none;
                              border-radius: 5px; display: inline-block;">
                        View Airflow Task Logs
                    </a>
                </div>

                <p style="margin-top: 24px;">
                    Please investigate the task logs to identify the cause
                    of the failure.
                </p>

            </div>

            <div style="background-color: #f4f6f8; padding: 14px 24px;
                        color: #666666; font-size: 12px;">
                Automated notification from the E-Commerce Data Engineering Pipeline.
            </div>

        </div>
    </body>
    </html>
    """,
)
,
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
