import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.bash_operator import (
    BashOperator,
)  # Change to DbtRunOperator if available
from airflow.models import Variable
import json

from handler.transaction_handler import *
from handler.read_set_handler import *

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 14),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "transaction_handler",
    default_args=default_args,
    description="A DAG to process transactions",
    schedule_interval=timedelta(days=1),
)

# Fetch the database configuration from Airflow's Variables
database_config_str = Variable.get("DATABASE_CONFIG", default_var=None)
if database_config_str is None:
    raise ValueError("DATABASE_CONFIG Airflow variable is not set")

database_config = json.loads(database_config_str)

# Fetch API configurations from Airflow's Variables
api_consumer_key = Variable.get("API_KEY")
api_consumer_secret = Variable.get("API_SECRET")

# Fetch MinIO configurations from Airflow's Variables
minio_endpoint_url = Variable.get("MINIO_ENDPOINT_URL")
minio_access_key = Variable.get("MINIO_ACCESS_KEY")
minio_secret_key = Variable.get("MINIO_SECRET_KEY")
source_bucket_name = Variable.get("SOURCE_BUCKET_NAME")


def process_transactions(date, **kwargs):
    handler = TransactionHandler(
        minio_endpoint_url=minio_endpoint_url,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
        source_bucket_name=source_bucket_name,
        database_config=database_config,
    )

    # Attempt to read transactions for the given date
    df_day = handler.read_transactions([date])

    # Check if the DataFrame is empty
    if df_day.empty:
        logging.warning(f"No transactions found for {date}. Skipping further processing.")
        return 'end_task_no_transactions'

    # If DataFrame is not empty, proceed with upsert and any other processing
    handler.upsert_data_to_rds(df_day)
    return 'process_siret'


def process_siret(date, **kwargs):
    handler = SiretSetHandler(
        database_config=database_config,
        api_consumer_key=api_consumer_key,
        api_consumer_secret=api_consumer_secret,
    )
    handler.add_missing_sirets_for_date(date)


add_transactions = BranchPythonOperator(
    task_id="process_transactions",
    python_callable=process_transactions,
    provide_context=True,
    op_kwargs={
        "date": '{{ dag_run.conf["date"] if dag_run and dag_run.conf.get("date") else execution_date.strftime("%Y-%m-%d") }}'
    },
    dag=dag,
)

add_sirets = PythonOperator(
    task_id="process_siret",
    python_callable=process_siret,
    op_kwargs={
        "date": '{{ dag_run.conf["date"] if dag_run and dag_run.conf.get("date") else execution_date.strftime("%Y-%m-%d") }}'
    },
    dag=dag,
)
run_spending_by_naf_code = BashOperator(
    task_id="spending_by_naf_code",
    bash_command="""dbt run --project-dir /opt/airflow/dags/dbt_data_pipeline --models spending_by_naf_code --vars 'target_date: "{{ dag_run.conf['date'] if dag_run and dag_run.conf.get('date') else execution_date.strftime('%Y-%m-%d') }}"'
    """,
    dag=dag,
)

run_spending_by_naf_code_test = BashOperator(
    task_id="spending_by_naf_code_test",
    bash_command="""dbt test --project-dir /opt/airflow/dags/dbt_data_pipeline --models spending_by_naf_code
    """,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_task_no_transactions',
    dag=dag
)

add_transactions >> [add_sirets,end_task]
add_sirets >> run_spending_by_naf_code >> run_spending_by_naf_code_test
