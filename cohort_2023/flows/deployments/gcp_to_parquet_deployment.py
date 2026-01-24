import sys
sys.path.append('../cloud')
from gcp_csv_to_parquet import *
import config
from prefect.deployments import Deployment
from prefect.filesystems import GCS
storage = GCS.load("flows-storage-dev")

deployment = Deployment.build_from_flow(
    flow=convert_loop_csv_to_parquet_gcp,
    name="csv_to_parquet_gcp",
    version='1.0.0',
    storage=storage,
    tags=['raw'],
    parameters={
        'months':[1,2,3,4,5,6,7,8,9,10,11,12],
        'years':[2020, 2021],
        'service_name':'fhv',
        'raw_path':'data/raw/{service_name}/',
        'raw_name_template':'{service_name}_tripdata_{year}-{month:02}.csv.gz',
        'parquet_path':'data/parquet/{service_name}/',
        'parquet_name_template':'{service_name}_tripdata_{year}-{month:02}.parquet'
    }
)

if __name__ == "__main__":
    deployment.apply()