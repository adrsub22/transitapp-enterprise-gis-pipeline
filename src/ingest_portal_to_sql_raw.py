"""
ingest_portal_to_sql_raw.py

Purpose
-------
Download TransitApp "tapped_trip_view_legs" CSVs from a basic-auth portal,
enrich each file with:
  - source_file / file_date
  - euclidean + manhattan distance (miles)
  - Origin_BG / Dest_BG via spatial join to a Block Group layer
and append new files into SQL Server as: mobility.raw_leg_trips

Public-safe notes
-----------------
- Uses environment variables for credentials and machine-specific paths.
- Schema/table names are illustrative and do not reflect proprietary systems.
"""

from __future__ import annotations

import os
import io
import re
import getpass
from dataclasses import dataclass
from urllib.parse import urljoin

import requests
import pandas as pd
import numpy as np
import geopandas as gpd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError


# -----------------------
# Configuration
# -----------------------

@dataclass
class IngestConfig:
    # Portal
    listing_url: str
    filename_substring: str = "tapped_trip_view_legs"
    file_suffix: str = ".csv"
    only_years: list[int] | None = None  # e.g. [2024, 2025]

    # Spatial
    bg_shp_path: str = ""
    bg_id_field: str = "GEOID"
    dist_crs_epsg: int = 32140  # example: Texas South Central (meters)

    # SQL
    sql_server: str = ""
    sql_database: str = ""
    odbc_driver: str = "ODBC Driver 17 for SQL Server"
    target_table: str = "mobility.raw_leg_trips"  # <--- your choice


def load_config() -> IngestConfig:
    """
    Loads configuration from environment variables with sensible defaults.
    Put these in a .env file locally (do not commit secrets).
    """
    listing_url = os.getenv("TRANSITAPP_LISTING_URL", "").strip()
    if not listing_url:
        raise ValueError("Missing TRANSITAPP_LISTING_URL environment variable.")

    cfg = IngestConfig(
        listing_url=listing_url,
        bg_shp_path=os.getenv("BG_SHP_PATH", "").strip(),
        bg_id_field=os.getenv("BG_ID_FIELD", "GEOID").strip(),
        dist_crs_epsg=int(os.getenv("DIST_CRS_EPSG", "32140")),
        sql_server=os.getenv("SQL_SERVER", "").strip(),
        sql_database=os.getenv("SQL_DATABASE", "").strip(),
        odbc_driver=os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server").strip(),
        target_table=os.getenv("RAW_TABLE_NAME", "mobility.raw_leg_trips").strip(),
    )

    # optional years list: "2024,2025"
    years = os.getenv("ONLY_YEARS", "").strip()
    if years:
        cfg.only_years = [int(y.strip()) for y in years.split(",") if y.strip()]

    # basic validations
    if not cfg.bg_shp_path:
        raise ValueError("Missing BG_SHP_PATH environment variable.")
    if not cfg.sql_server or not cfg.sql_database:
        raise ValueError("Missing SQL_SERVER or SQL_DATABASE environment variable.")

    return cfg


def build_engine(cfg: IngestConfig):
    """
    Build a SQLAlchemy engine for SQL Server using Windows auth.
    """
    engine_string = (
        f"mssql+pyodbc://@{cfg.sql_server}/{cfg.sql_database}"
        f"?driver={cfg.odbc_driver.replace(' ', '+')}"
        "&trusted_connection=yes"
    )
    return create_engine(engine_string, fast_executemany=True)


def get_portal_session() -> requests.Session:
    """
    Create an authenticated session using basic auth.
    Credentials are pulled from env vars; password can be prompted.
    """
    username = os.getenv("TRANSITAPP_USERNAME", "").strip()
    password = os.getenv("TRANSITAPP_PASSWORD", "").strip()

    if not username:
        raise ValueError("Missing TRANSITAPP_USERNAME environment variable.")

    if not password:
        # prompt locally (won't echo)
        password = getpass.getpass("Portal password for stats-replication: ")

    s = requests.Session()
    s.auth = (username, password)
    return s


def list_matching_files(cfg: IngestConfig, session: requests.Session) -> list[str]:
    """
    Scrape the listing page and return file URLs matching substring/suffix.
    """
    resp = session.get(cfg.listing_url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    file_urls: list[str] = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not (href.endswith(cfg.file_suffix) and cfg.filename_substring in href):
            continue

        # optional year filter using YYYY-MM-DD in filename
        if cfg.only_years:
            m = re.search(r"(\d{4})-\d{2}-\d{2}", href)
            if m:
                year = int(m.group(1))
                if year not in cfg.only_years:
                    continue

        file_urls.append(urljoin(cfg.listing_url, href))

    return sorted(set(file_urls))


def get_already_loaded_files(engine, target_table: str) -> set[str]:
    """
    If table exists, pull distinct source_file values to support incremental loads.
    """
    inspector = inspect(engine)
    # SQLAlchemy's has_table doesn't accept schema.table; split it.
    if "." in target_table:
        schema, table = target_table.split(".", 1)
    else:
        schema, table = None, target_table

    if not inspector.has_table(table_name=table, schema=schema):
        return set()

    query = f"SELECT DISTINCT source_file FROM {target_table}"
    existing_df = pd.read_sql(query, engine)
    return set(existing_df["source_file"].dropna().astype(str).tolist())


def load_block_groups(cfg: IngestConfig) -> gpd.GeoDataFrame:
    """
    Load BG polygons once and standardize CRS for spatial join.
    """
    bg = gpd.read_file(cfg.bg_shp_path)

    # normalize to WGS84
    if bg.crs is None:
        bg = bg.set_crs("EPSG:4326")
    else:
        bg = bg.to_crs("EPSG:4326")

    keep = [cfg.bg_id_field, "geometry"]
    bg = bg[keep].copy()
    return bg


def enrich_file_dataframe(df: pd.DataFrame, fname: str, cfg: IngestConfig, bg_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add:
      - source_file, file_date
      - distance metrics (miles)
      - Origin_BG, Dest_BG
    """
    df = df.copy()

    # basic metadata
    df["source_file"] = fname

    # infer file_date from filename pattern: tapped_trip_view_legs_YYYY-MM-DD.csv
    date_str = fname.replace(cfg.filename_substring + "_", "").replace(cfg.file_suffix, "")
    df["file_date"] = pd.to_datetime(date_str, errors="coerce")

    # coordinate cleaning
    coord_cols = ["start_longitude", "start_latitude", "end_longitude", "end_latitude"]
    for c in coord_cols:
        if c not in df.columns:
            raise KeyError(f"Expected column '{c}' not found in CSV {fname}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=coord_cols).reset_index(drop=True)
    if df.empty:
        return df

    # distances in miles (project to meters then convert)
    meters_to_miles = 1 / 1609.344

    orig = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["start_longitude"], df["start_latitude"]),
        crs="EPSG:4326",
    ).to_crs(epsg=cfg.dist_crs_epsg)

    dest = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["end_longitude"], df["end_latitude"]),
        crs="EPSG:4326",
    ).to_crs(epsg=cfg.dist_crs_epsg)

    dx = dest.geometry.x.values - orig.geometry.x.values
    dy = dest.geometry.y.values - orig.geometry.y.values

    df["manhattan_distance_mi"] = (np.abs(dx) + np.abs(dy)) * meters_to_miles
    df["euclidean_distance_mi"] = (np.sqrt(dx**2 + dy**2)) * meters_to_miles

    # spatial joins for origin/destination BG
    orig_pts = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(df["start_longitude"], df["start_latitude"]),
        crs="EPSG:4326",
    )
    orig_join = gpd.sjoin(orig_pts, bg_gdf, how="left", predicate="within").sort_index()
    df["Origin_BG"] = orig_join[cfg.bg_id_field].astype("string").values

    dest_pts = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(df["end_longitude"], df["end_latitude"]),
        crs="EPSG:4326",
    )
    dest_join = gpd.sjoin(dest_pts, bg_gdf, how="left", predicate="within").sort_index()
    df["Dest_BG"] = dest_join[cfg.bg_id_field].astype("string").values

    # normalize string-like columns (avoids pandas -> SQL oddities)
    for col in ["Origin_BG", "Dest_BG", "source_file"]:
        df[col] = df[col].astype("string")

    return df


def write_to_sql(df: pd.DataFrame, engine, target_table: str, chunksize: int = 5000) -> int:
    """
    Append to SQL table. Table is created automatically if it does not exist.
    """
    if df.empty:
        return 0

    try:
        df.to_sql(
            target_table,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            method=None,  # default insert strategy
        )
        return len(df)
    except SQLAlchemyError as e:
        raise RuntimeError(f"SQL write failed for table {target_table}: {e}") from e


def run() -> int:
    cfg = load_config()
    engine = build_engine(cfg)
    session = get_portal_session()

    print(f"[INFO] Listing URL: {cfg.listing_url}")
    file_urls = list_matching_files(cfg, session)
    print(f"[INFO] Found {len(file_urls)} matching CSV files.")

    existing_files = get_already_loaded_files(engine, cfg.target_table)
    print(f"[INFO] Already loaded files: {len(existing_files)}")

    new_urls = []
    for url in file_urls:
        fname = url.split("/")[-1]
        if fname not in existing_files:
            new_urls.append(url)

    print(f"[INFO] New files to ingest: {len(new_urls)}")
    if not new_urls:
        print("[INFO] No new files found. Nothing to ingest.")
        return 0

    print("[INFO] Loading block groups once...")
    bg_gdf = load_block_groups(cfg)
    print(f"[INFO] Block groups loaded: {len(bg_gdf)}")

    total_inserted = 0

    for url in new_urls:
        fname = url.split("/")[-1]
        print(f"\n[INFO] Processing: {fname}")

        r = session.get(url, timeout=120)
        r.raise_for_status()

        try:
            df = pd.read_csv(io.StringIO(r.text))
        except Exception as e:
            print(f"[WARN] Skipping {fname} (CSV parse failed): {e}")
            continue

        df2 = enrich_file_dataframe(df, fname, cfg, bg_gdf)
        if df2.empty:
            print("[WARN] No valid rows after cleaning/enrichment. Skipping.")
            continue

        inserted = write_to_sql(df2, engine, cfg.target_table)
        total_inserted += inserted
        print(f"[INFO] Inserted rows: {inserted:,} (running total: {total_inserted:,})")

    print(f"\n[INFO] Done. Total rows inserted this run: {total_inserted:,}")
    return total_inserted


if __name__ == "__main__":
    run()
