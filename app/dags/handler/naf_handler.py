import base64
from handler.api_handler import ApiClient
import awswrangler as wr
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pg8000
import re  # Import the regular expressions module


class SireneAPIFetcher:
    expected_columns = [
        "siret",
        "naf_code",
        "creation_date",
        "address",
        "postal_code",
        "city",
        "establishment",
        "brand",
        "stored_date",
    ]
    dtype = {
        "siret": "VARCHAR(14)",
        "naf_code": "VARCHAR(10)",
        "creation_date": "DATE",
        "address": "TEXT",
        "postal_code": "VARCHAR(10)",
        "city": "VARCHAR(255)",
        "establishment": "VARCHAR(255)",
        "brand": "VARCHAR(255)",
        "stored_date": "DATE",
    }
    base_url = "https://api.insee.fr"
    primary_keys = ["siret", "stored_date"]
    schema = "curated"
    table_name = "naf_details"
    insert_mode = "upsert"

    def __init__(
        self,
        api_consumer_key,
        api_consumer_secret,
        database_config,
        region_name="us-east-1",
    ):
        self.api_consumer_key = api_consumer_key
        self.api_consumer_secret = api_consumer_secret
        self.region_name = region_name
        self.database_config = database_config

        self.api_client = ApiClient(self.base_url)

        self.token = self.get_access_token()

    def get_access_token(self):
        """Fetches an access token for the SIRENE API."""
        auth = base64.b64encode(
            f"{self.api_consumer_key}:{self.api_consumer_secret}".encode()
        ).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = "grant_type=client_credentials"

        response = self.api_client.execute_request(
            "POST", "/token", data=data, headers=headers
        )
        return response["access_token"]

    def fetch_details(self, siret):
        """Fetches details for a given SIRET number."""
        headers = {"Authorization": f"Bearer {self.token}"}

        response = self.api_client.execute_request(
            "GET", f"/entreprises/sirene/V3/siret/{siret}", headers=headers
        )
        if "etablissement" in response:
            data = response["etablissement"]
            naf_code = data["periodesEtablissement"][0]["activitePrincipaleEtablissement"]

            # Check if the naf_code matches the expected format
            if not re.match(r'^[0-9]{2}\.[0-9]{2}[A-Z]$', naf_code):
                raise ValueError(f"The NAF code '{naf_code}' does not match the expected format.")

            # If the code is valid, continue processing
            details = {
                "siret": siret,
                "naf_code": naf_code,
                "creation_date": data["dateCreationEtablissement"],
                "address": f"{data['adresseEtablissement']['numeroVoieEtablissement']} {data['adresseEtablissement']['typeVoieEtablissement']} {data['adresseEtablissement']['libelleVoieEtablissement']}",
                "postal_code": data["adresseEtablissement"]["codePostalEtablissement"],
                "city": data["adresseEtablissement"]["libelleCommuneEtablissement"],
                "establishment": data["periodesEtablissement"][0].get(
                    "enseigne1Etablissement", ""
                ),
                "brand": data["uniteLegale"].get("denominationUniteLegale", ""),
            }
            return details

    def fetch_details_sequentially(self, sirets):
        """Fetches details for a batch of SIRET numbers sequentially."""
        batch_details = []

        for siret in sirets:
            try:
                details = self.fetch_details(siret)
                if details:
                    batch_details.append(details)
            except Exception as e:
                logging.error(f"Error fetching details for SIRET {siret}: {e}")
                self.handle_exception(e, siret)

        # Convert the list of dictionaries to a DataFrame
        if batch_details:
            return pd.DataFrame(batch_details)
        else:
            return pd.DataFrame()

    def fetch_details_concurrently(self, sirets, max_workers=10):
        """Fetches details for a batch of SIRET numbers using ApiClient with multithreading."""
        # max_workers to improve
        batch_details = []
        # fetch details concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all fetch_details tasks and return futures
            future_to_siret = {
                executor.submit(self.fetch_details, siret): siret for siret in sirets
            }
            for future in as_completed(future_to_siret):
                siret = future_to_siret[future]
                try:
                    details = future.result()
                    if details:
                        batch_details.append(details)
                except Exception as e:
                    logging.error(f"Error fetching details for SIRET {siret}: {e}")
                    self.handle_exception(e, siret)

        # Convert the list of dictionaries to a DataFrame
        if batch_details:
            return pd.DataFrame(batch_details)
        else:
            return pd.DataFrame()

    def upsert_data_to_rds(self, df):
        """
        Upsert data into an RDS table.

        df: DataFrame containing the data to upsert.
        table_name: The name of the table to upsert data into.
        schema: The schema of the table.
        primary_keys: A list of columns that uniquely identify a row (used for upsert logic).
        """
        current_date = datetime.now().date()
        # Format the current date as a string 'YYYY-MM-DD'
        formatted_date = current_date.strftime("%Y-%m-%d")
        df["stored_date"] = formatted_date
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

    def handle_exception(self, exception: Exception, siret: str):
        """Handle exceptions specific to fetching media insights."""
        # Log the error, perform recovery, etc.
        logging.error(f"Siret: {siret}, Error: {exception}")
        # todo add this siret to a table. none_prcessed_siret
