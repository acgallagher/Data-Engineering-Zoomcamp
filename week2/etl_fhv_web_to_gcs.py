from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import dask
from prefect_dask.task_runners import DaskTaskRunner


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe."""
    df = pd.read_csv(dataset_url)
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out as a parquet file"""
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@flow(
    task_runner=DaskTaskRunner(
        cluster_kwargs={"n_workers": 1, "resources": {"memory": 20e9}}
    )
)
def etl_web_to_gcs(months: list[int], years: list[int]) -> None:
    """Parent ETL function across file names"""
    for year in years:
        for month in months:
            with dask.annotate(resources={"memory": 20e9}):
                dataset_file = f"fhv_tripdata_{year}-{month:02}"
                dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

                df = fetch.submit(dataset_url)
                path = write_local.submit(df, dataset_file)
                write_gcs.submit(path)


if __name__ == "__main__":
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    years = [2019]
    etl_web_to_gcs(months, years)
