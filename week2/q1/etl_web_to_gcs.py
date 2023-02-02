from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read taxi data from web into pandas DataFrame.\\
    Pandas `dtypes` are hardcoded to prevent memory overflow when doing `pd.read_csv` on large files.
    """
    print(dataset_url)
    # dtype_dict = {
    #     "VendorID": "int8",
    #     "passenger_count": "int8",
    #     "trip_distance": "float32",
    #     "RatecodeID": "int8",
    #     "store_and_fwd_flag": "object",
    #     "PULocationID": "int16",
    #     "DOLocationID": "int16",
    #     "payment_type": "int8",
    #     "fare_amount": "float32",
    #     "extra": "float32",
    #     "mta_tax": "float32",
    #     "tip_amount": "float32",
    #     "tolls_amount": "float32",
    #     "improvement_surcharge": "float32",
    #     "total_amount": "float32",
    #     "congestion_surcharge": "float32",
    # }
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    for col in df.columns:
        if "pickup_datetime" in col or "dropoff_datetime" in col:
            df[col] = pd.to_datetime(df[col])
    df["passenger_count"].fillna(0, inplace=True)
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"./week2/q1/raw_data/{color}/")
    path.mkdir(parents=True, exist_ok=True)
    filepath = path / f"{dataset_file}.parquet"
    df.to_parquet(filepath, compression="gzip")
    return filepath


@task(retries=2)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-dtc-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(month: int, year: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = transform(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    color = "green"
    month = 1
    year = 2020
    etl_web_to_gcs(month, year, color)
