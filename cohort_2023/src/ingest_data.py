import pandas as pd
import typer
import re
from sqlalchemy import create_engine
from time import time
from loguru import logger
from os import listdir
from os.path import isfile, join

app = typer.Typer()

def camel_to_snake(name:str):
    """Function to convert camel case columns to snake_case

    :param str name: camel case string
    :return str: snake case string
    """
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

@app.command()
def ingest_data(path:str,
                user:str,
                password:str,
                host:str,
                port:int,
                db:str,
                table:str,
                file_name_prefix:str):
    """Function to ingest data from csv files to postgres

    :param str path: folder with files
    :param str user: db user
    :param str password: db password
    :param str host: db host
    :param int port: db port
    :param str db: db name
    :param str table: db table name
    :param str file_name_prefix: ingest file name prefix
    """
    
    logger.debug('load data start')

    connection_str = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    logger.debug(f'create connection engine to {connection_str}')

    engine = create_engine(connection_str)

    onlyfiles = [f for f in listdir(path) if isfile(join(path, f)) if f.startswith(file_name_prefix)]

    logger.debug(f'list of files to load data from {onlyfiles}')

    for file in onlyfiles:
        logger.debug(f'load data from {file}')
        df_iter = pd.read_csv(path+'/'+file, iterator=True, chunksize=100000)
        df = next(df_iter)
        df.columns = [camel_to_snake(col) for col in df.columns]

        if file_name_prefix == 'taxi_data':
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')

        df.to_sql(name=table, con=engine, if_exists='append')
        logger.debug('load first chunk')

        while True: 

            try:
                t_start = time()

                df = next(df_iter)
                df.columns = [camel_to_snake(col) for col in df.columns]
                if file_name_prefix == 'taxi_data':
                    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(name=table, con=engine, if_exists='append')

                t_end = time()

                logger.debug('load another chunk, took %.3f second' % (t_end - t_start))

            except StopIteration:
                logger.debug("finished load data into the postgres database")
                break

if __name__ == '__main__':
    app()
