from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from itertools import product


@task(log_prints=True, retries=3)  # cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=3))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    print(f"loading from: {dataset_url}")

    dtype_dict = {
        "VendorID": "object",
        "passenger_count": "Int8",
        "trip_distance": "float32",
        "trip_type": "Int8",
        "payment_type": "Int8",
        "RatecodeID": "object",
        "store_and_fwd_flag": "object",
        "PULocationID": "object",
        "DOLocationID": "object",
        "fare_amount": "float32",
        "extra": "float32",
        "ehail_fee": "float32",
        "mta_tax": "float32",
        "tip_amount": "float32",
        "tolls_amount": "float32",
        "improvement_surcharge": "float32",
        "total_amount": "float32",
        "congestion_surcharge": "float32",
    }

    # fhv files
    if "fhv" in dataset_url:
        dtype_dict = {
            "dispatching_base_num": "object",
            "PUlocationID": "object",
            "DOlocationID": "object",
            "SR_Flag": "Int8",
            "Affiliated_base_number": "object",
        }

    df = pd.read_csv(dataset_url, dtype=dtype_dict)
    return df


@task(log_prints=True)  # cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=3))
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    for col in df.columns:
        if "pickup_datetime" in col.lower() or "dropoff_datetime" in col.lower():
            df[col] = pd.to_datetime(df[col])
        if col == "passenger_count":
            df["passenger_count"].fillna(0, inplace=True)
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)  # cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=3))
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"raw_data/{color}/{dataset_file}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)  # cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=3))
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-dtc-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=(10, 300))
    return


@flow(log_prints=True)
def etl_web_to_local(year: int, month: int, color: str) -> Path:
    """The main ET function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_transformed = transform(df)
    path = write_local(df_transformed, color, dataset_file)
    return path


@flow(log_prints=True)
def etl_local_to_gcs(path: Path) -> None:
    """The main L function"""
    write_gcs(path)


@flow()
def etl_parent_flow_gh(months: list[int] = None, years: list[int] = None, colors: list[str] = None):
    file_not_uploaded = []
    if all([months, years, colors]):
        for year, month, color in product(years, months, colors):
            path = etl_web_to_local(year, month, color)
            try:
                etl_local_to_gcs(path)
            except OSError:
                print(f"Connection Timeout. Try uploading manually.\nFile: {path.name}")
                file_not_uploaded.append(path.name)
    else:
        print("parameter is missing")
    if file_not_uploaded:
        print(f"the following files were not uploaded: {file_not_uploaded}")


if __name__ == "__main__":
    colors = ["yellow"]
    months = [1]
    years = [2019]
    etl_parent_flow_gh(months, years, colors)
