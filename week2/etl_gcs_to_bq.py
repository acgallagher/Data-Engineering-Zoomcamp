from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(gcs_path)


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning examples"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcs-desktop")

    df.to_gbq(
        destination_table="de_zoomcamp.rides",
        project_id="arctic-study-375823",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(
    months: list[int] = [1, 2],
    years: list[int] = [2021],
    colors: list[str] = ["yellow"],
):
    """Main ETL flow to load data into Big Query."""
    for color in colors:
        for year in years:
            for month in months:
                path = extract_from_gcs(color, year, month)
                df = transform(path)
                write_bq(df)
                print(len(df))


if __name__ == "__main__":
    months = [2, 3]
    years = [2019]
    colors = ["yellow"]
    etl_gcs_to_bq(months, years, colors)
