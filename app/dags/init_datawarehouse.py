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
database_config_str = Variable.get("DATABASE_CONFIG", default_var=None)
if database_config_str is None:
    raise ValueError("DATABASE_CONFIG Airflow variable is not set")

database_config = json.loads(database_config_str)


def create_schema_and_tables(**kwargs):
    # SQL commands to create schema and tables
    sql_commands = [
        "CREATE SCHEMA IF NOT EXISTS curated;",
        """
        CREATE TABLE IF NOT EXISTS curated.transactions (
            id UUID PRIMARY KEY,
            type VARCHAR(255) NOT NULL,
            amount NUMERIC(10, 2) NOT NULL,
            status VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            date DATE NOT NULL,
            wallet_id UUID NOT NULL,
            siret VARCHAR(14) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS curated.naf_details (
            siret VARCHAR(14) NOT NULL,
            naf_code VARCHAR(10) NOT NULL,
            creation_date DATE,
            address TEXT,
            postal_code VARCHAR(10),
            city VARCHAR(255),
            establishment VARCHAR(255),
            brand VARCHAR(255),
            stored_date DATE NOT NULL,
            PRIMARY KEY (siret, stored_date)
        );
        """,
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
