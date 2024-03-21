from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from datetime import datetime, timedelta
import json
import pg8000

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 14),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "init_datawarehouse",
    default_args=default_args,
    description="Initialize Data Warehouse Schema and Tables",
    schedule_interval=None,
    start_date=datetime(2024, 2, 14),
)

# Fetch the database configuration from Airflow's Variables
database_config_str = Variable.get("reply_db", default_var=None)
if database_config_str is None:
    raise ValueError("DATABASE_CONFIG Airflow variable is not set")

database_config = json.loads(database_config_str)


def create_schema_and_tables(**kwargs):
    # SQL commands to create schema and tables
    sql_commands = [
        "DROP SCHEMA IF EXISTS staging CASCADE ;",
        "DROP SCHEMA IF EXISTS landing CASCADE ;",
        "CREATE SCHEMA IF NOT EXISTS landing;",
        """
        CREATE TABLE landing.customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100)
        )
        """,
        """
        CREATE TABLE landing.orders (
            order_id SERIAL PRIMARY KEY,
            customer_id INT,
            order_date DATE,
            order_amount DECIMAL(10,2)
        )
        """,
        """
        INSERT INTO landing.customers (first_name, last_name, email) VALUES
            ('John', 'Doe', 'john.doe@example.com'),
            ('Jane', 'Doe', 'jane.doe@example.com')
        """,
        """
        INSERT INTO landing.orders (customer_id, order_date, order_amount) VALUES
            (1, '2023-01-01', 100.00),
            (2, '2023-01-02', 150.00)
        """
    ]
    # Establish a connection to PostgreSQL
    conn = pg8000.connect(**database_config)
    cursor = conn.cursor()

    try:
        for sql_command in sql_commands:
            cursor.execute(sql_command)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


create_schema_and_tables_task = PythonOperator(
    task_id="create_schema_and_tables",
    python_callable=create_schema_and_tables,
    dag=dag,
)
