#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import time
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import timedelta


@task(
    log_prints=True,
    tags=["extract"],
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url: str):
    csv_name = "output.csv.gz"
    os.system(f"wget -O {csv_name} {url}")

    df = pd.read_csv(csv_name)
    df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    return df


@task(log_prints=True)
def transform_data(data):
    data = data[data["passenger_count"] != 0]
    return data


@task(log_prints=True)
def load_data(table_name, data):

    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        data.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        start = time.time()
        data.to_sql(name=table_name, con=engine, if_exists="append")
        end = time.time()
        print("Inserted the dataframe in {} seconds.".format(end - start))


@task(log_prints=True)
def ingest_data(user, password, host, port, db, table_name, url):
    csv_name = "output.csv.gz"

    os.system(f"wget -O {csv_name} {url}")

    df = pd.read_csv(csv_name, nrows=100)

    df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    start = time.time()
    df.to_sql(name=table_name, con=engine, if_exists="append")
    end = time.time()
    print("Inserted the dataframe in {} seconds.".format(end - start))


@flow(name="Ingest data")
def main_flow():
    table_name = "green_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

    raw_data = extract_data(csv_url)
    transformed_data = transform_data(raw_data)
    load_data(table_name, transformed_data)


if __name__ == "__main__":
    main_flow()
