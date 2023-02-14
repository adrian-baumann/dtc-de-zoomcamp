from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

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
    local_path = Path(f"./week2/q4/raw_data/{color}/")
    local_path.mkdir(parents=True, exist_ok=True)
    filepath = local_path / f"{dataset_file}.parquet"
    gcs_path = Path(f"./raw_data/{color}/{dataset_file}.parquet")
    df.to_parquet(filepath, compression="gzip")
    return filepath, gcs_path


@task(timeout_seconds=120)
def write_gcs(from_path: Path, to_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-dtc-bucket")
    gcs_block.upload_from_path(from_path=from_path, to_path=to_path)
    return


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_transformed = transform(df)
    local_path, gcs_path = write_local(df_transformed, color, dataset_file)
    write_gcs(local_path, gcs_path)


@flow()
def etl_parent_flow_gh(months: list[int] = None, year: int = None, color: str = None):
    if all([months, year, color]):
        for month in months:
            etl_web_to_gcs(year, month, color)
    else:
        print("parameter is missing")


if __name__ == "__main__":
    color = "green"
    months = [11]
    year = 2020
    etl_parent_flow_gh(months, year, color)
