import pandas as pd
import re
import urllib.request
from config import *
from pathlib import Path
from prefect import flow, task
from prefect.filesystems import GCS
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True)
def get_gcs_object(obj_path:str, obj_name:str) -> str:
    """Load raw file from gcs bucket

    :param str obj_path: path to file in bucket
    :param str obj_name: file name in bucket
    :return str: path to file in local folder
    """
    gcs_block = GcsBucket.load("dtc-zoomcamp")
    print(f'Load {obj_name} from {obj_path}')
    Path(f"{obj_path}").mkdir(parents=True, exist_ok=True)    
    gcs_block.download_object_to_path(from_path=f'{obj_path}/{obj_name}', 
                                      to_path=f'{obj_path}/{obj_name}')
    return f'{obj_path}/{obj_name}'

@task(log_prints=True)
def clean_data(raw_file_path: str, column_types: dict) -> pd.DataFrame:
    """Clean raw data: 1. Rename columns
                       2. Set types for columns

    :param str raw_file_path: path to file in local folder
    :param dict column_types: map of column name and types
    :return pd.DataFrame: clean data
    """
    print('Start clean data')
    df = pd.read_csv(raw_file_path, low_memory=False, encoding='ISO-8859-1')
    df.columns = list(column_types.keys())
    df = df.astype(column_types)
    print('Finish clean data')
    return df

@task(log_prints=True)
def write_parquet_gcs(gcp_path:str, df:pd.DataFrame, file_name:str, parquet_schema) -> None:
    """Save clean data in parquet format and push it to gcs bucket

    :param str gcp_path: path for parquet data
    :param pd.DataFrame df: clean data
    :param str file_name: result file name
    :param parquet_schema: parquet file custom schema
    """
    Path(f"{gcp_path}").mkdir(parents=True, exist_ok=True) 
    df.to_parquet(f'{gcp_path}/{file_name}', schema=parquet_schema)
    gcs_block = GcsBucket.load("dtc-zoomcamp")
    print(f'Load data to {gcp_path}/{file_name}')
    gcs_block.upload_from_path(from_path=f'{gcp_path}/{file_name}', 
                               to_path=f'{gcp_path}/{file_name}')
    return

@flow()
def convert_csv_to_parquet_gcp(raw_path:str, 
                               raw_name:str, 
                               parquet_path:str, 
                               parquet_name:str,
                               column_types:dict,
                               parquet_schema) -> None:
    """Orchestrate etl tasks

    :param str raw_path: path to raw file in bucket
    :param str raw_name: raw file name in bucket
    :param str parquet_path: target path in bucket
    :param str parquet_name: target file name
    :param dict column_types: column names and types mapping
    :param parquet_schema: parquet file custom schema
    """
    raw_file_path = get_gcs_object(raw_path, raw_name)
    df = clean_data(raw_file_path, column_types)
    write_parquet_gcs(parquet_path, df, parquet_name, parquet_schema)
    return 

@flow(log_prints=True)
def convert_loop_csv_to_parquet_gcp(months:list,
                                    years:list[int],
                                    service_name:str,
                                    raw_path:str, 
                                    raw_name_template:str, 
                                    parquet_path:str, 
                                    parquet_name_template:str) -> None:
    """Orchestrate loop

    :param list months: list of month to create file names
    :param int year: year to create file name
    :param str service_name: name of ny taxi service
    :param str raw_path: path to raw file in bucket
    :param str raw_name: raw file name in bucket
    :param str parquet_path: target path in bucket
    :param str parquet_name: target file name
    """
    column_types = service_columns.get(service_name).get('column_types')
    parquet_schema = globals()[service_columns.get(service_name).get('parquet_schema')]
    raw_path = raw_path.format(service_name=service_name)
    parquet_path = parquet_path.format(service_name=service_name)
    for year in years:
        for month in months:
            print('Year:', year, ' Month:', month)
            raw_name = raw_name_template.format(service_name=service_name,
                                                year=year, 
                                                month=month)
            parquet_name = parquet_name_template.format(service_name=service_name,
                                                        year=year, 
                                                        month=month)
            convert_csv_to_parquet_gcp(raw_path, 
                                       raw_name_template, 
                                       parquet_path, 
                                       parquet_name_template,
                                       column_types,
                                       parquet_schema)
    return

if __name__ == "__main__":
    # months = list(range(1,13))
    months = [1]
    years = [2019]
    service_name = 'yellow'
    raw_path = 'data/raw/{service_name}/'
    raw_name_template = '{service_name}_tripdata_{year}-{month:02}.csv.gz'
    parquet_path = 'data/parquet/{service_name}/'
    parquet_name_template = '{service_name}_tripdata_{year}-{month:02}.parquet'
    convert_loop_csv_to_parquet_gcp(months,
                                    years,
                                    service_name,
                                    raw_path, 
                                    raw_name_template, 
                                    parquet_path, 
                                    parquet_name_template)
