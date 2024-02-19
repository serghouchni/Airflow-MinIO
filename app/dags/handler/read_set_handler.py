import logging

import awswrangler as wr
import pg8000

from handler.naf_handler import SireneAPIFetcher


class SiretSetHandler:
    schema = "curated"
    tr_table = "transactions"
    naf_table = "naf_details"

    def __init__(self, database_config, api_consumer_key, api_consumer_secret):
        self.database_config = database_config
        self.api_consumer_key = api_consumer_key
        self.api_consumer_secret = api_consumer_secret

    def add_missing_sirets_for_date(self, target_date, chunksize=30):
        """
        Retrieves SIRETs present in transactions missing in naf_details.

        :param target_date: The date for which to retrieve SIRETs (format: 'YYYY-MM-DD').
        :param chunksize: The number of rows to include in each chunk (default: 30 as is the max call/min for the API).
        :return: A list of unique SIRETs not present in naf_details for the given date.
        """
        conn = pg8000.connect(**self.database_config)

        sql_query = f"""
        SELECT DISTINCT t.siret
        FROM {self.schema}.{self.tr_table} t
        LEFT JOIN {self.schema}.{self.naf_table} d ON t.siret = d.siret
        WHERE t.date = '{target_date}'
        AND d.siret IS NULL;
        """
        sirene_fetcher = SireneAPIFetcher(
            api_consumer_key=self.api_consumer_key,
            api_consumer_secret=self.api_consumer_secret,
            database_config=self.database_config,
        )

        for chunk_df in wr.postgresql.read_sql_query(
            sql=sql_query, con=conn, chunksize=chunksize
        ):

            sirets = chunk_df["siret"].tolist()
            df = sirene_fetcher.fetch_details_sequentially(sirets)
            if df.empty:
                logging.info("No new Siret to add")
            else:
                sirene_fetcher.upsert_data_to_rds(df)
