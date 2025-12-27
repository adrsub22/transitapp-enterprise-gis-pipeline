"""
transform_incremental_clean.py

Purpose
-------
Module 2 of the pipeline:
  1) Incrementally fetch new rows from mobility.raw_leg_trips
  2) Clean/standardize fields and compute a deterministic row hash
  3) Upsert into mobility.LegTrips_Clean (insert-only on row_hash)
  4) Execute mobility.usp_Refresh_Rolling31_Aggregates to rebuild rollups
     and materialized ArcGIS tables.

Public-safe notes
-----------------
- Schema/table names are illustrative.
- All credentials/paths come from environment variables.
"""

from __future__ import annotations

import os
import json
import math
import hashlib
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyodbc


# -----------------------------
# Configuration
# -----------------------------

@dataclass
class TransformConfig:
    # State file (watermark)
    state_path: Path

    # SQL connection
    sql_server: str
    sql_database: str
    odbc_driver: str = "ODBC Driver 17 for SQL Server"

    # Tables (neutralized)
    source_table: str = "mobility.raw_leg_trips"
    clean_table: str = "mobility.LegTrips_Clean"

    # Rolling window parameters
    rolling_days: int = 34
    overlap_days: int = 5

    # Stored procedure
    refresh_proc: str = "mobility.usp_Refresh_Rolling31_Aggregates"
    proc_days_back: int = 31
    proc_region_prefix: str | None = None  # e.g., "48029" or None


def load_config() -> TransformConfig:
    state_path = Path(os.getenv("STATE_PATH", "state/state_mobility_incremental.json"))

    sql_server = os.getenv("SQL_SERVER", "").strip()
    sql_database = os.getenv("SQL_DATABASE", "").strip()
    odbc_driver = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server").strip()

    if not sql_server or not sql_database:
        raise ValueError("Missing SQL_SERVER or SQL_DATABASE environment variable.")

    cfg = TransformConfig(
        state_path=state_path,
        sql_server=sql_server,
        sql_database=sql_database,
        odbc_driver=odbc_driver,
        source_table=os.getenv("RAW_TABLE_NAME", "mobility.raw_leg_trips").strip(),
        clean_table=os.getenv("CLEAN_TABLE_NAME", "mobility.LegTrips_Clean").strip(),
        rolling_days=int(os.getenv("ROLLING_DAYS", "34")),
        overlap_days=int(os.getenv("OVERLAP_DAYS", "5")),
        refresh_proc=os.getenv("REFRESH_PROC", "mobility.usp_Refresh_Rolling31_Aggregates").strip(),
        proc_days_back=int(os.getenv("PROC_DAYS_BACK", "31")),
        proc_region_prefix=(os.getenv("PROC_REGION_PREFIX", "").strip() or None),
    )
    return cfg


# -----------------------------
# State helpers
# -----------------------------

def load_state(state_path: Path) -> dict[str, Any]:
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {"last_file_dt": None, "last_run_utc": None}


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


# -----------------------------
# SQL helpers
# -----------------------------

def connect_sql(cfg: TransformConfig) -> pyodbc.Connection:
    """
    Connect to SQL Server using Windows auth.
    """
    conn_str = (
        f"DRIVER={{{cfg.odbc_driver}}};"
        f"SERVER={cfg.sql_server};"
        f"DATABASE={cfg.sql_database};"
        "Trusted_Connection=yes;"
    )
    cn = pyodbc.connect(conn_str)
    cn.autocommit = False
    return cn


def _sql_safe(v: Any) -> Any:
    """
    Convert pandas/numpy values into SQL-friendly Python types.
    """
    if v is None:
        return None

    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None

    if pd.isna(v):
        return None

    if isinstance(v, (np.integer,)):
        return int(v)

    if isinstance(v, (np.floating,)):
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            return None
        return fv

    if isinstance(v, pd.Timestamp):
        py_dt = v.to_pydatetime()
        # round down to milliseconds for datetime2(3)
        return py_dt.replace(microsecond=(py_dt.microsecond // 1000) * 1000)

    if isinstance(v, pd.Timedelta):
        return v.total_seconds()

    return v


# -----------------------------
# Incremental extract
# -----------------------------

def compute_since(last_file_dt: str | None, rolling_days: int, overlap_days: int) -> dt.datetime:
    """
    Determine the incremental watermark time.
    - If no previous state: go back rolling_days from today
    - Else: subtract overlap_days from last watermark to reprocess recent files safely
    """
    if last_file_dt is None:
        return dt.datetime.combine(dt.date.today() - dt.timedelta(days=rolling_days), dt.time.min)

    watermark = dt.datetime.fromisoformat(last_file_dt)
    return watermark - dt.timedelta(days=overlap_days)


def fetch_new_raw(cn: pyodbc.Connection, cfg: TransformConfig, since: dt.datetime) -> pd.DataFrame:
    """
    Pull raw rows where file_date >= since.
    """
    sql = f"""
    SELECT
        user_trip_id,
        start_time,
        end_time,
        start_longitude, start_latitude,
        end_longitude, end_latitude,
        service_name,
        route_short_name,
        mode,
        start_stop_name,
        end_stop_name,
        source_file,
        file_date AS file_date_raw,
        manhattan_distance_mi,
        euclidean_distance_mi,
        Origin_BG,
        Dest_BG
    FROM {cfg.source_table}
    WHERE file_date >= ?
    """
    return pd.read_sql(sql, cn, params=[since])


# -----------------------------
# Transform / hashing
# -----------------------------

def sha256_rowhash(row: dict[str, Any]) -> str:
    """
    Deterministic hash used for insert-only upsert into clean table.
    This lets us safely reprocess overlapping windows without duplicates.
    """
    parts = [
        str(row.get("user_trip_id") or ""),
        str(row.get("start_time") or ""),
        str(row.get("end_time") or ""),
        str(row.get("start_longitude") or ""),
        str(row.get("start_latitude") or ""),
        str(row.get("end_longitude") or ""),
        str(row.get("end_latitude") or ""),
        str(row.get("service_name") or ""),
        str(row.get("route_short_name") or ""),
        str(row.get("mode") or ""),
        str(row.get("start_stop_name") or ""),
        str(row.get("end_stop_name") or ""),
        str(row.get("source_file") or ""),
        str(row.get("file_date_raw") or ""),
        str(row.get("Origin_BG") or ""),
        str(row.get("Dest_BG") or ""),
    ]
    s = "|".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(s).hexdigest()


def clean_transform(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize types/columns and produce a clean, consistent output frame.
    """
    if raw.empty:
        return raw

    df = raw.copy()

    # Dates/times
    df["trip_date"] = pd.to_datetime(df["file_date_raw"], errors="coerce").dt.date
    df["start_time_utc"] = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    df["end_time_utc"] = pd.to_datetime(df["end_time"], errors="coerce", utc=True)

    # numeric fields
    for col in [
        "start_longitude", "start_latitude", "end_longitude", "end_latitude",
        "manhattan_distance_mi", "euclidean_distance_mi"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # IDs as strings (important: user_trip_id behaves like a string)
    df["user_trip_id"] = df["user_trip_id"].astype("string")
    df["Origin_BG"] = df["Origin_BG"].astype("string")
    df["Dest_BG"] = df["Dest_BG"].astype("string")
    df["source_file"] = df["source_file"].astype("string")

    # row_hash
    df["row_hash_hex"] = df.apply(lambda r: sha256_rowhash(r.to_dict()), axis=1)

    # output schema (aligned with clean table expectations)
    out = pd.DataFrame({
        "row_hash_hex": df["row_hash_hex"],
        "user_trip_id": df["user_trip_id"],

        "trip_date": df["trip_date"],
        "start_time_utc": df["start_time_utc"].dt.tz_convert(None),
        "end_time_utc": df["end_time_utc"].dt.tz_convert(None),

        "start_longitude": df["start_longitude"],
        "start_latitude": df["start_latitude"],
        "end_longitude": df["end_longitude"],
        "end_latitude": df["end_latitude"],

        "service_name": df["service_name"],
        "route_short_name": df["route_short_name"],
        "mode": df["mode"],
        "start_stop_name": df["start_stop_name"],
        "end_stop_name": df["end_stop_name"],

        "source_file": df["source_file"],
        "file_date_raw": df["file_date_raw"],

        "manhattan_distance_mi": df["manhattan_distance_mi"],
        "euclidean_distance_mi": df["euclidean_distance_mi"],

        "Origin_BG": df["Origin_BG"],
        "Dest_BG": df["Dest_BG"],
    })

    # remove rows without trip_date
    out = out[~out["trip_date"].isna()].copy()

    # replace inf with NaN then allow NULL handling later
    out = out.replace([np.inf, -np.inf], np.nan)

    return out


# -----------------------------
# Load: upsert into clean table
# -----------------------------

def upsert_legtrips_clean(cn: pyodbc.Connection, cfg: TransformConfig, clean_df: pd.DataFrame) -> int:
    """
    Insert-only upsert:
      - stage rows into #stg_legtrips
      - MERGE into mobility.LegTrips_Clean where row_hash not already present

    Returns count staged (approx count processed).
    """
    if clean_df.empty:
        return 0

    cur = cn.cursor()
    cur.fast_executemany = True

    # We store row_hash in SQL as varbinary(32).
    # The python dataframe contains row_hash_hex (64 hex chars).
    cur.execute("""
    IF OBJECT_ID('tempdb..#stg_legtrips') IS NOT NULL DROP TABLE #stg_legtrips;

    CREATE TABLE #stg_legtrips (
        row_hash_hex nvarchar(64) NOT NULL,
        user_trip_id nvarchar(100) NOT NULL,

        trip_date date NOT NULL,
        start_time_utc datetime2(3) NULL,
        end_time_utc datetime2(3) NULL,

        start_longitude float NULL,
        start_latitude float NULL,
        end_longitude float NULL,
        end_latitude float NULL,

        service_name nvarchar(200) NULL,
        route_short_name nvarchar(50) NULL,
        mode nvarchar(50) NULL,
        start_stop_name nvarchar(200) NULL,
        end_stop_name nvarchar(200) NULL,

        source_file nvarchar(300) NULL,
        file_date_raw nvarchar(50) NULL,

        manhattan_distance_mi float NULL,
        euclidean_distance_mi float NULL,

        Origin_BG varchar(12) NULL,
        Dest_BG varchar(12) NULL
    );
    """)

    cols = [
        "row_hash_hex", "user_trip_id",
        "trip_date", "start_time_utc", "end_time_utc",
        "start_longitude", "start_latitude", "end_longitude", "end_latitude",
        "service_name", "route_short_name", "mode", "start_stop_name", "end_stop_name",
        "source_file", "file_date_raw",
        "manhattan_distance_mi", "euclidean_distance_mi",
        "Origin_BG", "Dest_BG"
    ]

    insert_sql = """
        INSERT INTO #stg_legtrips (
            row_hash_hex, user_trip_id,
            trip_date, start_time_utc, end_time_utc,
            start_longitude, start_latitude, end_longitude, end_latitude,
            service_name, route_short_name, mode, start_stop_name, end_stop_name,
            source_file, file_date_raw,
            manhattan_distance_mi, euclidean_distance_mi,
            Origin_BG, Dest_BG
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    CHUNK_SIZE = 50000
    staged = 0
    batch: list[tuple[Any, ...]] = []

    for r in clean_df[cols].itertuples(index=False, name=None):
        safe_row = tuple(_sql_safe(v) for v in r)
        batch.append(safe_row)

        if len(batch) >= CHUNK_SIZE:
            cur.executemany(insert_sql, batch)
            staged += len(batch)
            batch = []
            print(f"[INFO] staged into #stg_legtrips: {staged:,}")

    if batch:
        cur.executemany(insert_sql, batch)
        staged += len(batch)
        print(f"[INFO] staged into #stg_legtrips: {staged:,}")

    # MERGE into the clean table
    # IMPORTANT: we convert hex string -> varbinary(32)
    cur.execute(f"""
    MERGE {cfg.clean_table} AS tgt
    USING #stg_legtrips AS src
      ON tgt.row_hash = CONVERT(varbinary(32), src.row_hash_hex, 2)
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        row_hash, user_trip_id, trip_date, start_time_utc, end_time_utc,
        start_longitude, start_latitude, end_longitude, end_latitude,
        service_name, route_short_name, mode, start_stop_name, end_stop_name,
        source_file, file_date_raw, manhattan_distance_mi, euclidean_distance_mi,
        Origin_BG, Dest_BG
      )
      VALUES (
        CONVERT(varbinary(32), src.row_hash_hex, 2),
        src.user_trip_id, src.trip_date, src.start_time_utc, src.end_time_utc,
        src.start_longitude, src.start_latitude, src.end_longitude, src.end_latitude,
        src.service_name, src.route_short_name, src.mode, src.start_stop_name, src.end_stop_name,
        src.source_file, src.file_date_raw, src.manhattan_distance_mi, src.euclidean_distance_mi,
        src.Origin_BG, src.Dest_BG
      );
    """)

    return staged


# -----------------------------
# Stored procedure refresh
# -----------------------------

def refresh_rolling_aggregates(cn: pyodbc.Connection, cfg: TransformConfig) -> None:
    """
    Rebuild rolling aggregates + ArcGIS materialized tables.
    """
    cur = cn.cursor()
    cur.execute("SELECT DB_NAME();")
    print("[INFO] Connected DB:", cur.fetchone()[0])

    # Parameterized proc call (matches our public SQL version)
    if cfg.proc_region_prefix is None:
        cur.execute(f"EXEC {cfg.refresh_proc} @days_back=?, @region_prefix=NULL;", cfg.proc_days_back)
    else:
        cur.execute(f"EXEC {cfg.refresh_proc} @days_back=?, @region_prefix=?;", cfg.proc_days_back, cfg.proc_region_prefix)

    print("[INFO] Rolling aggregates refreshed.")


# -----------------------------
# Main
# -----------------------------

def run() -> None:
    cfg = load_config()
    state = load_state(cfg.state_path)

    last_file_dt = state.get("last_file_dt")
    print("[INFO] last_file_dt in state:", last_file_dt)

    since = compute_since(last_file_dt, cfg.rolling_days, cfg.overlap_days)
    print("[INFO] incremental since:", since)

    cn: pyodbc.Connection | None = None
    try:
        cn = connect_sql(cfg)

        raw = fetch_new_raw(cn, cfg, since)
        print("[INFO] Raw rows fetched:", len(raw))

        clean = clean_transform(raw)
        print("[INFO] Clean rows after transform:", len(clean))

        staged = upsert_legtrips_clean(cn, cfg, clean)
        print("[INFO] Staged rows merged (approx):", staged)

        refresh_rolling_aggregates(cn, cfg)

        cn.commit()

        # update watermark only if we fetched rows
        if not raw.empty:
            max_fdt = pd.to_datetime(raw["file_date_raw"], errors="coerce").max()
            if pd.notna(max_fdt):
                state["last_file_dt"] = max_fdt.to_pydatetime().isoformat(sep=" ")

        state["last_run_utc"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        save_state(cfg.state_path, state)
        print("[INFO] State saved:", state)

    except Exception:
        if cn is not None:
            cn.rollback()
        raise
    finally:
        if cn is not None:
            cn.close()


if __name__ == "__main__":
    run()
