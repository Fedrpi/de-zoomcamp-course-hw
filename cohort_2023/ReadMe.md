# DE-ZOOMCAMP-2023

## Week 6: Kafka Streaming

### Homework
Homework 6 located [here](kafka/)
## Week 5: Spark
### Homework:
Jupyter notebook with Spark homework 5 located [here](spark/nb/spark_hw.ipynb)
## Week 4: DBT, LookerStudio, Metabase
### How to dbt:
1. Install dbt with command ```pip install dbt-<adapter>``` for BigQuery I used ```pip install dbt-bigquery```
2. Configure your project by instruction with command ```dbt init```
3. All models, macros, seeds located in specific folder. See config in file ```/dbt/de-zoomcamp/dbt_project.yml```
4. To apply models run commands:
   1.  ```dbt seed``` 
   2.  ```dbt deps```
   3.  ```dbt run```

### Homework
1. SQL queries with homework 4 located [here](sql/dbt.sql)
2. [Looker datastudio dashboard](https://lookerstudio.google.com/s/r0bK19NvLCw)
3. [Metabase dashboard](http://34.65.65.223:3000/dashboard/3-fhv-rides)
4. [Dbt-docs](http://34.65.65.223:8080)
5. [Dbt-elementary-report](http://34.65.65.223:8081/#/report/dashboard)
## Week 3: DWH with BigQuery
### Homework 3: SQL queries
SQL queries file with homework 3 located [here](sql/bq.sql)
## Week 2: Workflow orchestration with Prefect and GCP
### How to Prefect:
1. Create cloud workspace using this [docs](https://docs.prefect.io/ui/cloud-quickstart/)
2. Install prefect with command ```pip install -U prefect```
3. Login into cloud account with command ```prefect cloud login```
4. Command ```prefect deployment build -n "flows_to_gcs" flows/cloud/etl_web_to_gcs.py:flows_to_gcs -a --output ./flows/deployments/deploy_flows_to_gcs.yaml``` will deploy flow to load data to datalake
5. Command ```prefect deployment build -n "flows_to_bq" flows/cloud/etl_gcs_to_bq.py:flows_gcs_to_bq -a --output ./flows/deployments/deploy_flows_gcs_to_bq.yaml``` will deploy flow to load data from data lake to DWH
6. Command ```prefect block register -m prefect_gcp``` will install extra blocks to config GCP
7. Config GCP Creds, Bucket etc with Prefect UI
8. Run default prefect agent with command ```prefect agent start --work-queue "default"```
9.  Run flows using CLI or UI
### Homework 2: Prefect flows
Prefect flow scripts located [here](flows)
## Week 1: Ingest data to local Postgress
### How to:
1. Command ```docker-compose up``` will create two containers
   - Postgres db on host localhost:5432
   - pgAdmin to manage postgres on host localhost:80
2. Command ```docker build -t ingest_data .``` will use Dockerfile to create container with scripts that allows download and ingest data
3. Command ```docker run -it ingest_data``` will execute bash in container.
   
  To download file use next command
   ```
   python get_data.py <url to download file> \
                      <target path to save file> \
                      <file name prefix> \
                      --file-ext=<allow values 0 of 1
                                  0 for parquet extention
                                  1 for csv extention if csv is packet .gz if will be unpack>
   ```

   To ingest data to postgres use next command

   ```
   python ingest_data.py <path to folder with csv data to ingest> \
                         <db user> \
                         <db password> \
                         <db host> \
                         <db post> \
                         <db name> \
                         <db table name> \
                         <file name prefix>
   ```
### Homework 1: SQL queries
SQL queries file with homework 1 located [here](sql/ny_taxi.sql)
