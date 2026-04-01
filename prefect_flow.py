from prefect import flow, task
import logging

from pipeline.extract import extract_from_csv
from pipeline.transform import clean_data
from pipeline.load import load_to_postgres


@task
def extract_task(path):
    df = extract_from_csv(path)
    print(f"Extracted rows: {len(df)}")
    return df


@task
def transform_task(df):
    df_clean = clean_data(df)
    print(f"Rows after cleaning: {len(df_clean)}")
    return df_clean


@task
def load_task(df):
    load_to_postgres(df, "listings")
    print("Data loaded into PostgreSQL")


@flow
def run_pipeline_flow():
    path = "data/raw/listings.csv"

    df_raw = extract_task(path)
    df_clean = transform_task(df_raw)
    load_task(df_clean)


if __name__ == "__main__":
    run_pipeline_flow()