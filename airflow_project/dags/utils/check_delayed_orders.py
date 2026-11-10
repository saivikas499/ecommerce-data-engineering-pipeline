
import os
import sys
import snowflake.connector


def load_snowflake_config():
    return {
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
    }


def main():
    print("Running Snowflake Delayed Orders Check...")
    print("Python path:", sys.executable)
    print("Snowflake connector version:", snowflake.connector.__version__)

    snowflake_config = load_snowflake_config()

    conn = None
    cursor = None

    try:
        conn = snowflake.connector.connect(
            **snowflake_config,
            login_timeout=20,
            client_session_keep_alive=False,
            ocsp_fail_open=True,
        )

        print("Successfully connected to Snowflake.")

        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*)
            FROM ECOMMERCE_DB.ANALYTICS.ORDER_STATUS
            WHERE FINAL_STATUS = 'Delayed'
        """)

        result = cursor.fetchone()
        delayed_count = result[0]

        print(f"Query result: {delayed_count} delayed orders found.")

        if delayed_count > 0:
            raise Exception(
                f"Found {delayed_count} delayed orders in Snowflake."
            )

    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
