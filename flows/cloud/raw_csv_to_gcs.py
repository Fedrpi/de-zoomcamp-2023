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
    """Write DataFrame out locally as parquet file"""
    print(f'Create path {data_path}')
    Path(f"{data_path}").mkdir(parents=True, exist_ok=True)
    print(f'Download data by url {url_path} to {data_path}/{dataset_file}')
    urllib.request.urlretrieve(f"{url_path}", f"{data_path}/{dataset_file}")
    return f"{data_path}/{dataset_file}"


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    # gcs_block = GcsBucket.load("dtc-zoomcamp")
    print('Load data to GCS bucket')
    gcs_block = GcsBucket.load("dtc-zoomcamp")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def raw_csv_to_gcs(dataset_url: str, dataset_file:str, data_path: str) -> None:
    """The main ETL function"""
    path = write_local(dataset_url, dataset_file, data_path)
    # write_gcs(path)


@flow(log_prints=True)
def raw_csvs_to_gcs(years: list[int], 
                    months: list[int],
                    file_name_template: str,
                    url_tamplate:str,
                    data_path:str):
    for year in years:
        print(f'Load year: {year}')
        for month in months:
            dataset_file = file_name_template.format(year=year, month=month)
            dataset_url = url_tamplate.format(dataset_file=dataset_file)
            raw_csv_to_gcs(dataset_url, dataset_file, data_path)


if __name__ == "__main__":
    months = list(range(1,13))
    years = [2019, 2020, 2021]
    data_path = 'data/raw/yellow'
    file_name_template = 'yellow_tripdata_{year}-{month:02}.csv.gz'
    url_tamplate = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}'
                    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
    raw_csvs_to_gcs(years, months, file_name_template, url_tamplate, data_path)
