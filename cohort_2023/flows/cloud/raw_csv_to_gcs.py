import pandas as pd
import re
from pathlib import Path
import urllib.request
from prefect import flow, task
from prefect.filesystems import GCS
from prefect_gcp.cloud_storage import GcsBucket



@task(log_prints=True)
def write_local(url_path: str, 
                dataset_file: str, 
                data_path:str) -> Path:
    """Write DataFrame out locally as parquet file

    :param str url_path: path to file in web
    :param str dataset_file: name of file with data
    :param str data_path: path to save file
    :return Path: path downloaded file
    """
    print(f'Create path {data_path}')
    Path(f"{data_path}").mkdir(parents=True, exist_ok=True)
    print(f'Download data by url {url_path} to {data_path}/{dataset_file}')
    urllib.request.urlretrieve(f"{url_path}", f"{data_path}/{dataset_file}")
    return f"{data_path}/{dataset_file}"


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS
    :param Path path: path to file for upload
    """
    print('Load data to GCS bucket')
    gcs_block = GcsBucket.load("dtc-zoomcamp")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def raw_csv_to_gcs(dataset_url: str, dataset_file:str, data_path: str) -> None:
    """Task orchestrator
    :param str dataset_url: path to file in web
    :param str dataset_file: name of file with data
    :param str data_path: path to save file
    """
    path = write_local(dataset_url, dataset_file, data_path)
    write_gcs(path)


@flow(log_prints=True)
def raw_csvs_to_gcs(years: list[int], 
                    months: list[int],
                    service_name: str,
                    file_name_template: str,
                    url_tamplate:str,
                    data_path:str):
    """Orchestrate loop
    :param list[int] years: years for load data
    :param list[int] months: months for load data
    :param str service_name: name of ny taxi service
    :param str file_name_template: use years and month to calc file name
    :param str url_tamplate: use years and month to calc url
    :param str data_path: path to save data
    """
    data_path = data_path.format(service_name=service_name)
    for year in years:
        print(f'Load year: {year}')
        for month in months:
            dataset_file = file_name_template.format(service_name=service_name, 
                                                     year=year, 
                                                     month=month)
            dataset_url = url_tamplate.format(service_name=service_name,
                                              dataset_file=dataset_file)
            raw_csv_to_gcs(dataset_url, dataset_file, data_path)


if __name__ == "__main__":
    months = list(range(1,13))
    years = [2020, 2021]
    service_name = 'fhv'
    data_path = 'data/raw/{service_name}'
    file_name_template = '{service_name}_tripdata_{year}-{month:02}.csv.gz'
    url_tamplate = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service_name}/{dataset_file}'
    raw_csvs_to_gcs(years, months, service_name, file_name_template, url_tamplate, data_path)
