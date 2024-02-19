import logging

import boto3
import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta
import pg8000


class TransactionHandler:
    schema = "curated"
    table_name = "transactions"
    base_path = "transactions"
    primary_keys = ["id"]
    expected_columns = [
        "id",
        "type",
        "amount",
        "status",
        "created_at",
        "date",
        "wallet_id",
        "siret",
    ]
    insert_mode = "upsert"
    dtype = {
        "id": "VARCHAR(36)",
        "type": "VARCHAR(255)",
        "amount": "FLOAT",
        "status": "VARCHAR(255)",
        "created_at": "TIMESTAMP",
        "wallet_id": "VARCHAR(36)",
        "siret": "VARCHAR(14)",
    }

    def __init__(
        self,
        minio_endpoint_url,
        minio_access_key,
        minio_secret_key,
        source_bucket_name,
        database_config,
    ):

        self.minio_endpoint_url = minio_endpoint_url
        self.source_bucket_name = source_bucket_name
        self.database_config = database_config

        # Configure AWS Data Wrangler's S3 endpoint URL to MinIO
        wr.config.s3_endpoint_url = self.minio_endpoint_url

        # Create a boto3 session
        self.session = boto3.Session(
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name="us-east-1",  # The region name is arbitrary here but required by boto3
        )

    def get_dates(self, year, month=None, day=None):
        """Generate list of dates for the given year and month or a specific day."""
        if day and month:
            # Specific day mode
            return [f"{year}-{month:02d}-{day:02d}"]
        elif month:
            # Month mode, generate all days in the month
            num_days = (datetime(year, month + 1, 1) - timedelta(days=1)).day
            return [f"{year}-{month:02d}-{day:02d}" for day in range(1, num_days + 1)]
        else:
            raise ValueError(
                "Must specify either both year and month or year, month, and day."
            )

    def read_transactions(self, dates):
        """Read transactions for the given list of dates."""
        dfs = []
        for date in dates:
            object_key = f"{self.base_path}/{date}.json"
            try:
                df = wr.s3.read_json(
                    path=f"s3://{self.source_bucket_name}/{object_key}",
                    boto3_session=self.session,
                )
                dfs.append(df)
            except Exception as e:
                logging.error(f"Error reading {object_key}: {e}")
                #todo handel this error
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def upsert_data_to_rds(self, df):
        """
        Upsert data into an RDS table.

        df: DataFrame containing the data to upsert.
        table_name: The name of the table to upsert data into.
        schema: The schema of the table.
        primary_keys: A list of columns that uniquely identify a row (used for upsert logic).
        """
        # Ensure the 'created_at' column is datetime type for extracting year, month, day
        if not pd.api.types.is_datetime64_any_dtype(df["created_at"]):
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

        df["date"] = df["created_at"].dt.strftime("%Y-%m-%d")

        # Upsert the DataFrame into the RDS table

        # Establish a connection to PostgreSQL
        conn = pg8000.connect(**self.database_config)

        wr.postgresql.to_sql(
            df=df[self.expected_columns],
            con=conn,
            schema=self.schema,
            table=self.table_name,
            mode=self.insert_mode,
            index=False,
            dtype=self.dtype,
            upsert_conflict_columns=self.primary_keys,
        )

    def store_partitioned_df(self, df, dist_bucket_name, base_path):
        """
        Storing the DataFrame partitioned by year, month, and day in Parquet format, Upsert Mode.

        :param df: DataFrame to store
        :param dist_bucket_name: Name of the bucket where the DataFrame should be stored
        :param base_path: Base path within the bucket for storing the data
        """
        if not pd.api.types.is_datetime64_any_dtype(df["created_at"]):
            df["created_at"] = pd.to_datetime(df["created_at"])

        # Extract year, month, and day into separate columns
        df["year"] = df["created_at"].dt.year
        df["month"] = df["created_at"].dt.month
        df["date"] = df["created_at"].dt.date

        # The S3 path where the DataFrame will be stored
        s3_path = f"s3://{dist_bucket_name}/{base_path}"

        # The DataFrame as Parquet, partitioned by year, month, day
        wr.config.s3_endpoint_url = self.minio_endpoint_url
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            partition_cols=["year", "month", "date"],
            index=False,
            mode="overwrite_partitions",  # Overwrite the partitions for upsert mode.
            boto3_session=self.session,
        )
