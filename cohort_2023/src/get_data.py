import pandas as pd
import typer
import urllib.request
import sys
import gzip
import shutil
import os
from loguru import logger
from pathlib import Path
from datetime import datetime

app = typer.Typer()

def unpack_data(gz_file:str):
  """Unpack gz file data

  :param str gz_file: path to gz packed file
  """
  target_file = '.'.join(gz_file.split('.')[0:-1])
  logger.debug(f'unpack file {gz_file}')
  logger.debug(f'read file {gz_file}')
  with gzip.open(gz_file, 'rb') as f_in:
      with open(target_file, 'wb') as f_out:
          logger.debug(f'save data to {target_file}')
          shutil.copyfileobj(f_in, f_out)
  logger.debug('remove source file')
  os.remove(gz_file)

@app.command()
def get_data(file_url:str, 
             target_path:str, 
             file_name:str, 
             file_ext:int=0):
    """download ny_taxi data in parquet format and save it in csv

    :param str file_url: url to data file
    :param str target_path: path to save csv
    :param str file_name: name of csv file
    :param int file_ext: extension of file 0 - parquet
                                           1 - csv
    """
    logger.debug(f'crate data dir {target_path} if not exist')
    Path(target_path).mkdir(parents=True, exist_ok=True)
    logger.debug('start get data')
    logger.debug(f'read data from url {file_url}')
    if file_ext == 0:
      df = pd.read_parquet(file_url)
      logger.debug(f'save data to {target_path}')
      df.to_csv(target_path+'/'+file_name+'_'+str(datetime.now().date())+'.csv', index=False)
      logger.debug('pipeline finished')
    elif file_ext == 1:
      # urllib.request.urlretrieve(file_url, target_path+'/'+file_name+'_'+str(datetime.now().date())+'.csv')
      urllib.request.urlretrieve(file_url, target_path+'/'+file_url.split('/')[-1])
      if file_url.split('.')[-1] == 'gz':
         unpack_data(target_path+'/'+file_url.split('/')[-1])
    else:
      logger.error('Unsupported file extension')
      sys.exit(-1)

if __name__ == '__main__':
    app()
