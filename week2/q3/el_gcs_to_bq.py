from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from prefect.orion.schemas.states import Completed, Failed
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_path_from_gcs(month: int, year: int, color: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"raw_data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-dtc-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./week2/q3/")
    return Path(f"./week2/q3/{gcs_path}")


@task(log_prints=True)
def load_parquet_from_gcs(path: Path) -> Path:
    """Download trip data from GCS"""
    df = pd.read_parquet(path)
    print(f"read file: {path.name}")
    return df


@task(log_prints=True)
def write_to_bq(df: pd.DataFrame, destination_table: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcs-dtc-cred")

    df.to_gbq(
        destination_table=destination_table,
        project_id="dtc-de-zoomcamp-376519",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=100_000,
        if_exists="append",
    )


@flow(log_prints=True, retries=2)
def el_gcs_to_bq(path: Path, destination_table: str) -> int:
    """EL flow to load data into Big Query"""
    df = load_parquet_from_gcs(path)
    print(f"row count: {len(df)}")
    write_to_bq(df, destination_table)
    print("wrote file to BigQuery")
    return len(df)


@flow(log_prints=True)
def el_parent_flow(months: list[int] = None, year: int = None, color: str = None):
    """
    Main E-L flow\\
    Could have a connection timeout. Be warned.    
    """
    if all([months, year, color]):
        paths = []
        total_row_cnt = 0
        for month in months:
            paths.append(get_path_from_gcs(month, year, color))
        for path in paths:
            try:
                destination_table = f"rides.{color}_rides"
                total_row_cnt += el_gcs_to_bq(path, destination_table)
            except OSError:
                return Failed(message="Connection Timeout. Try uploading manually.\nFile: {path.name}")
            except:
                return Failed(message="Something failed. Maybe try uploading manually?\nFile: {path.name}")
        return Completed(message=f"Finished successfully. Total number of processed rows: {total_row_cnt}")
    else:
        print("parameter is missing")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    el_parent_flow(months, year, color)
