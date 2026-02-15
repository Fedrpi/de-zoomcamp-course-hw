"""
NYC Taxi Data ETL Pipeline
Загружает данные NYC Taxi за 2019-2020 годы, конвертирует в Parquet и загружает в DuckDB.
Поддерживает параллельную обработку для ускорения загрузки.
"""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple

import duckdb
import requests
from tqdm import tqdm

# Базовый URL для загрузки данных NYC Taxi
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# Количество параллельных потоков для загрузки
MAX_WORKERS = 4


def download_and_convert_file(
    taxi_type: str, year: int, month: int
) -> Tuple[bool, str]:
    """
    Загружает и конвертирует один файл данных такси.

    Args:
        taxi_type: Тип такси ('yellow' или 'green')
        year: Год данных
        month: Месяц данных

    Returns:
        Tuple[bool, str]: (успешность операции, сообщение)
    """
    # Создаем директорию для данных
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    parquet_filepath = data_dir / parquet_filename

    # Пропускаем уже загруженные файлы
    if parquet_filepath.exists():
        return True, f"Skipped {parquet_filename} (already exists)"

    csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    csv_gz_filepath = data_dir / csv_gz_filename

    try:
        # ШАГ 1: Загрузка CSV.gz файла с прогресс-баром
        response = requests.get(
            f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True
        )
        response.raise_for_status()

        # Получаем размер файла для прогресс-бара
        total_size = int(response.headers.get("content-length", 0))

        # Загружаем файл с отображением прогресса
        with open(csv_gz_filepath, "wb") as f:
            with tqdm(
                total=total_size,
                unit="iB",
                unit_scale=True,
                desc=f"Downloading {csv_gz_filename}",
                leave=False,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    pbar.update(size)

        # ШАГ 2: Конвертация CSV.gz в Parquet с помощью DuckDB
        con = duckdb.connect()
        con.execute(
            f"""
            COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
            TO '{parquet_filepath}' (FORMAT PARQUET)
        """
        )
        con.close()

        # ШАГ 3: Удаление CSV.gz файла для экономии места
        csv_gz_filepath.unlink()

        return True, f"Completed {parquet_filename}"

    except Exception as e:
        return False, f"Error processing {parquet_filename}: {str(e)}"


def download_and_convert_files(taxi_type: str):
    """
    Загружает и конвертирует все файлы для указанного типа такси.
    Использует параллельную обработку для ускорения.

    Args:
        taxi_type: Тип такси fhv
    """
    print(f"\n{'='*60}")
    print(f"Обработка данных {taxi_type.upper()} taxi")
    print(f"{'='*60}")

    # Создаем список всех задач (год, месяц)
    tasks = [(year, month) for year in [2019] for month in range(1, 13)]

    # Используем ThreadPoolExecutor для параллельной загрузки
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Отправляем все задачи на выполнение
        future_to_task = {
            executor.submit(download_and_convert_file, taxi_type, year, month): (
                year,
                month,
            )
            for year, month in tasks
        }

        # Отслеживаем прогресс выполнения всех задач
        with tqdm(total=len(tasks), desc=f"Overall progress ({taxi_type})") as pbar:
            for future in as_completed(future_to_task):
                success, message = future.result()
                if not success:
                    tqdm.write(f"❌ {message}")
                else:
                    tqdm.write(f"✓ {message}")
                pbar.update(1)


if __name__ == "__main__":
    # for taxi_type in ["yellow", "green"]:
    for taxi_type in ["fhv"]:
        download_and_convert_files(taxi_type)

    con = duckdb.connect("taxi_rides_ny.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    for taxi_type in ["fhv"]:
        con.execute(f"""
            CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
            SELECT * FROM read_parquet('data/{taxi_type}/*.parquet', union_by_name=true)
        """)

    con.close()
