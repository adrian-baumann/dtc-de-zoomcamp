import re
import os
from pathlib import Path
import pandas as pd
import pyarrow as pa
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta

from time import sleep
import itertools
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile

import gc


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def download(category: str) -> None:
    """
    Download daily mean of the observed air temperatures at 2m height above ground from DWD (German Meteorological Service).
    Loading can take some time as it is intentionally slowed down to decrease load on opendata server.
    """

    dataset_url = f"https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/{category}/"

    page = requests.get(dataset_url)
    soup = BeautifulSoup(page.content, "html.parser")

    path = Path("./data") / category / "download"
    path.mkdir(parents=True, exist_ok=True)

    links = soup.find_all("a", href=re.compile(".pdf$|.txt$|.zip$"))
    print("found {} files for download into {}".format(len(links), str(path)))

    for link in links:
        file_path = path / link["href"]
        mode = "w+b" if "pdf" or "zip" in link["href"] else "w+"
        file_url = dataset_url + link["href"]
        if not file_path.is_file():
            with requests.get(file_url) as response:
                with open(str(file_path), mode) as file:
                    file.write(response.content)
                    sleep(0.5)

        print("download finished")
        # TODO: add error handler for timeout with too many requests


@task(
    log_prints=True,
    retries=1,
)
def unzip(category: str) -> None:
    """
    Unzip all (meta-)data to their respective folders under data:
    - temperature tables \t-> ./data/<type of data>/tables
    - metadata tables \t-> ./data/<type of data>/metadata
    """

    path = Path(f"data/{category}/")
    source_path = path / "download"
    source_path.mkdir(parents=True, exist_ok=True)
    target_data_path = path / "tables"
    target_data_path.mkdir(parents=True, exist_ok=True)
    target_metadata_path = path / "metadata"
    target_metadata_path.mkdir(parents=True, exist_ok=True)

    for file in source_path.glob("*.zip"):
        with ZipFile(file, "r") as zip:
            fname_data_lst = [
                fname for fname in zip.namelist() if "produkt_klima_tag" in fname
            ]
            fname_metadata_lst = [
                fname for fname in zip.namelist() if "Metadaten" in fname
            ]
            for fname in fname_data_lst:
                zip.extract(fname, target_path)
            for fname in fname_metadata_lst:
                zip.extract(fname, target_path)

    files_to_rmv = target_metadata_path.glob("*.html")
    if files_to_rmv:
        for file in files_to_rmv:
            file.unlink()

    print("finished unzipping to folder {}".format(path))


@task(
    log_prints=True,
)
def fetch_dataset(df_name: str) -> (pd.DataFrame, str()):
    """Reads in datasets by creating lists of small dataframes and concatenating them"""
    if df_name == "main":
        dtypes = {
            "STATIONS_ID": "string",
            "QN_3": "UInt8",
            "QN_4": "UInt8",
            " RSK": "Float32",
            "RSKF": "UInt8",
            "SHK_TAG": "UInt8",
            "  NM": "string",
            " TMK": "Float32",
            " UPM": "Float32",
            " TXK": "Float32",
            " TNK": "Float32",
            " TGK": "Float32",
        }
        path = {
            "recent_data": Path("./data/recent_data/download"),
            "historical_data": Path("./data/historical_data/download"),
        }
        usecols = [
            "STATIONS_ID",
            "MESS_DATUM",
            "QN_3",
            "QN_4",
            " RSK",
            "RSKF",
            "SHK_TAG",
            "  NM",
            " TMK",
            " UPM",
            " TXK",
            " TNK",
            " TGK",
        ]

        df_new_lst = [
            pd.read_table(
                file,
                sep=";",
                usecols=usecols,
                dtype_backend="pyarrow",
                dtype=dtypes,
                na_values=None,
            )
            for file in paths["recent_data"].glob("*.txt")
        ]

        df_hist_lst = [
            pd.read_table(
                file,
                sep=";",
                usecols=usecols,
                dtype_backend="pyarrow",
                dtype=dtypes,
                na_values=None,
            )
            for file in paths["historical_data"].glob("*.txt")
        ]
        df = pd.concat(df_new_lst + df_hist_lst).reset_index(drop=True)
        df = df.replace(-999, None).astype(dtypes)

    if df_name == "metadata_geo":
        dtypes = {
            "Stations_id": "string",
            "Stationshoehe": "Float32",
            "Geogr.Breite": "Float32",
            "Geogr.Laenge": "Float32",
            "von_datum": "string",
            "bis_datum": "string",
            "Stationsname": "string",
        }

        df_new_lst = [
            pd.read_table(
                file,
                sep=";",
                encoding="latin1",
                dtype=dtypes,
                dtype_backend="pyarrow",
                na_values=None,
            )
            for file in Path("./data/recent_data/metadata").glob(
                "Metadaten_Geographie*"
            )
        ]

        df_hist_lst = [
            pd.read_table(
                file,
                sep=";",
                encoding="latin1",
                dtype=dtypes,
                dtype_backend="pyarrow",
                na_values=None,
            )
            for file in Path("./data/historical_data/metadata").glob(
                "Metadaten_Geographie*"
            )
        ]

        df = pd.concat(df_new_lst + df_hist_lst).reset_index(drop=True)
        df = df.replace(-999, None).astype(dtypes)

    if df_name == "metadata_operator":
        df_new_lst = [
            pd.read_table(
                file,
                sep=";",
                encoding="latin1",
                dtype_backend="pyarrow",
                na_values=None,
            )
            for file in Path("./data/recent_data/metadata").glob(
                "Metadaten_Stationsname_Betreibername*"
            )
        ]

        df_lst = []
        for df in df_new_lst:
            split_idx = df.loc[df["Stationsname"] == "Betreibername"].index[0]
            df.columns = [
                "stations_id",
                "betreibername",
                "betrieb_von_datum",
                "betrieb_bis_datum",
            ]
            df = df.iloc[split_idx + 1 : -1]
            df_lst.append(df)

        df = pd.concat(df_lst).reset_index(drop=True)

    return df


@task(log_prints=True)
def transform(df: pd.DataFrame, df_name: str()) -> pd.DataFrame:
    """Fix dtype issues"""

    if df_name == "main":
        df["MESS_DATUM"] = pd.to_datetime(
            df["MESS_DATUM"], format="%Y%m%d", errors="coerce", utc=False
        ).dt.tz_localize("Europe/Brussels", ambiguous="NaT", nonexistent="NaT")
    if df_name == "metadata_geo":
        df["Stations_id"] = df["Stations_id"].str.replace(" ", "")
        df["von_datum"] = pd.to_datetime(
            df["von_datum"], format="%Y%m%d", errors="coerce", utc=False
        ).dt.tz_localize("Europe/Brussels", ambiguous="NaT")
        df["bis_datum"] = pd.to_datetime(
            df["bis_datum"], format="%Y%m%d", errors="coerce", utc=False
        ).dt.tz_localize("Europe/Brussels", ambiguous="NaT")
    if df_name == "metadata_operator":
        df["stations_id"] = df["stations_id"].str.replace(" ", "")
        df["betrieb_von_datum"] = pd.to_datetime(
            df["betrieb_von_datum"], format="%Y%m%d", errors="coerce", utc=False
        ).dt.tz_localize("Europe/Brussels", ambiguous="NaT")
        df["betrieb_bis_datum"] = pd.to_datetime(
            df["betrieb_bis_datum"], format="%Y%m%d", errors="coerce", utc=False
        ).dt.tz_localize("Europe/Brussels", ambiguous="NaT")
    df.columns = df.columns.str.replace(" ", "")
    df.columns = [col_name.lower() for col_name in df.columns]
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, df_name: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"./data/parquet/{df_name}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task(timeout_seconds=120)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-dtc-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(log_prints=True, retries=3)
def etl_web_to_local(category: str()) -> Path:
    """The main E function"""

    download(category)
    unzip(category)


@flow(log_prints=True, retries=3)
def etl_transform_write(df_name: str()) -> Path:
    """The main T function"""

    df = fetch_dataset(df_name)
    df = transform(df, df_name)
    path = write_local(df, df_name)

    return path


@flow(log_prints=True)
def etl_local_to_gcs(path: Path) -> None:
    """The main L function"""
    write_gcs(path)


@flow(log_prints=True)
def etl_parent_flow(dataset_categories: list(str), df_names: list(str)) -> None:
    paths = []
    for category in dataset_categories:
        etl_web_to_local(category)
    for df_name in df_names:
        paths.append(etl_transform_write(df_name))
    for path in paths:
        try:
            etl_local_to_gcs(path)
        except OSError:
            print(f"Connection Timeout. Try uploading manually.\nFile: {path.name}")

    else:
        print("parameter is missing")


if __name__ == "__main__":
    dataset_categories["historical", "recent"]
    df_names = ["main", "metadata_geo", "metadata_operator"]
    etl_parent_flow(dataset_categories, df_names)