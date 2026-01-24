import sys
sys.path.append('..')
from cloud.raw_csv_to_gcs import *
from prefect.deployments import Deployment
from prefect.filesystems import GCS
storage = GCS.load("flows-storage-dev")

deployment = Deployment.build_from_flow(
    flow=raw_csvs_to_gcs,
    # flow_name='raw_csvs_to_gcs',
    name="raw_csvs_to_gcs",
    version='1.0.0',
    storage=storage,
    tags=['raw'],
    parameters={
        'months':[1,2,3,4,5,6,7,8,9,10,11,12],
        'years':[2020, 2021],
        'service_name':'fhv',
        'data_path':'data/raw/{service_name}',
        'file_name_template':'{service_name}_tripdata_{year}-{month:02}.csv.gz',
        'url_tamplate':'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service_name}/{dataset_file}'
    }
)

if __name__ == "__main__":
    deployment.apply()