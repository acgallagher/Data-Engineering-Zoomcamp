#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import time
import argparse
import os
from prefect import flow, task

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, url):
    csv_name = "output.csv.gz"

    os.system(f"wget -O {csv_name} {url}")

    df = pd.read_csv(csv_name, nrows=100)

    df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    for chunk in df_iter:
        start = time.time()
        chunk.to_sql(name=table_name, con=engine, if_exists='append')
        end = time.time()
        print("Inserted another chunk in {} seconds.".format(end - start))

@flow(name="Ingest data")
def main_flow():
    user = "postgres"
    password = "admin"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "green_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)

if __name__ == '__main__':
    main_flow()