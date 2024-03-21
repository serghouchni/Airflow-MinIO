# Docker

## Run Docker Compose: 
Execute the following command in your terminal or PowerShell window to start all the services 

`mkdir -p ./minio_data`

`docker-compose up -d`

Check the status of your containers:
`docker-compose ps`

![docker_compose.png](images%2Fdocker_compose.png)

# Access Minio: 
Once all services are up, you can access the Minio webserver by navigating to http://localhost:9090.

# Access Airflow: 
Once all services are up, you can access the Airflow webserver by navigating to http://localhost:8080/
`Dags` : app/dags
`Variables` : app/config
# Access Metabase: 
Once all services are up, you can access the Metabase webserver by navigating to http://localhost:3000/


## Step 1: Set Up Airflow Variables
Airflow variables are used to manage dynamic values across The DAGs. You need to set these variables to integrate with your PostgreSQL and MinIO services.

## 1- Find the IP Address of dw_postgres Container:
The DATABASE_CONFIG variable requires the IP address of your dw_postgres container to establish a connection. Retrieve this IP by inspecting the Docker container network settings:

`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' dw_postgres`

## 2-Generate MinIO Credentials:
Access the MinIO web interface at http://localhost:9090 using the following credentials:

Username: minio_user
Password: 123456789
Navigate to the "Identity" section and create a new user or access key. Note down the MINIO_ACCESS_KEY and MINIO_SECRET_KEY

## 2.1 Transaction Data : 
Create a 'landing-area' bucket and upload your transaction data into it.
![landing-area.png](images%2Flanding-area.png)

### 3-Fill the app/config/airflow_variables.json File AND app/config/profiles.yml (host dw_postgres):
Populate the airflow_variables.json file with the necessary details, 
including the IP address from Step 2 and the MinIO credentials from Step 3. 
Replace <IPAddress> with the actual IP address of dw_postgres, and <AccessKey> and <SecretKey> with the MinIO credentials.
### 4-Import Variables into Airflow
![airflow_var.png](images%2Fairflow_var.png)

### 5-Verify Configuration app/config/profiles.yml and Airflow Variables

# Airflow : 

## 6- Initialize Data Warehouse Dag
The init_datawarehouse DAG is responsible for setting up the schema for curated data within the data warehouse. It performs the initial setup by creating the following tables under the curated schema:

`curated.transactions` : Stores transaction data.
`curated.naf_details` : Contains details related to NAF codes for associated SIRETs.


# 7-Transaction Handler

The transaction_handler DAG orchestrates a sequence of four main operations to process transaction and naf data:

`Data Ingestion`: Retrieves transaction records from the data lake and loads them into the data warehouse's curated.transactions table.
`NAF Code Enrichment`: Identifies new SIRETs in the transaction data that require NAF codes. It then fetches these codes from the INSEE API to only collect the delta of new information, ensuring that the data warehouse is incrementally updated without duplicating existing entries.
`DBT Run for Incremental Marts`: Utilizes DBT to execute models that calculate and store the total spent by date and NAF code, creating incremental marts.
`DBT Test for Data Quality and Integrity`: Conducts a series of DBT tests on the resulting data models to ensure their quality and integrity.

You can trigger the DAG for a specific date using the DAG configuration: `{"date": "2023-10-03"}`.

![dags.png](images%2Fdags.png)
![transaction_handler.png](images%2Ftransaction_handler.png)
![transaction_handler2.png](images%2Ftransaction_handler2.png)

# Metabase:
http://localhost:3000/
![metabase.png](images%2Fmetabase.png)


UPDATE core_user SET password_hash = '$2y$10$Jx/Y3M4Kv2IggyElBj98n.JZLSGv.wy5IXlW1yFAcgfJjZHgyRTNS' WHERE id = 1;



