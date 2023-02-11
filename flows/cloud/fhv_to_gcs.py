import pandas as pd
import re
from pathlib import Path
import urllib.request
from prefect import flow, task
from prefect.filesystems import GCS
from prefect_gcp.cloud_storage import GcsBucket



@task(log_prints=True)
def write_local(url_path: str, dataset_file: str, data_path:str) -> Path:
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
def fhv_to_gcs(year: int, month: int, data_path: str) -> None:
    """The main ETL function"""
 
    dataset_file = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"
    path = write_local(dataset_url, dataset_file, data_path)
    write_gcs(path)


@flow()
def fhvs_to_gcs(year: int = 2019, 
                months: list[int] = [1, 2],
                data_path:str = 'data/raw'):
    for month in months:
        fhv_to_gcs(year, month, data_path)


if __name__ == "__main__":
    months = list(range(1,13))
    year = 2019
    data_path = 'data/raw/fhv'
    fhvs_to_gcs(year, months, data_path)
