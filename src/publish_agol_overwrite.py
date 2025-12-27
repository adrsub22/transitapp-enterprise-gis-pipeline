"""
publish_agol_overwrite.py

Purpose
-------
Module 3 of the pipeline:
  1) Copy materialized ArcGIS tables from Enterprise SDE -> Local FGDB staging
  2) Build local feature classes (lines + points) from XY fields
  3) Overwrite existing Hosted Feature Layers in ArcGIS Online via ArcGIS Pro publishing

Why this exists
---------------
ArcGIS Pro/ArcPy can be finicky with views/query layers when running tools like GetCount,
XYTableToPoint, XYToLine, and publishing/overwrite flows. Materializing tables in SQL
and then staging them locally before creating feature classes makes this automation
more reliable and repeatable.

Notes
-----
- This module is intended to run in an ArcGIS Pro Python environment (arcpy available).
- Use DRY_RUN=1 to test everything except the final overwrite upload step.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

# ArcPy is only available in the ArcGIS Pro python environment
import arcpy  # type: ignore


# -----------------------------
# Configuration
# -----------------------------

@dataclass
class PublishConfig:
    # ArcGIS Pro project + map used for publishing
    aprx_path: str
    map_name: str

    # Local FGDB workspace
    gdb_path: str
    scratch_dir: str

    # Enterprise connection (.sde)
    sde_conn_path: str

    # Materialized tables in SDE
    sde_flows_table: str
    sde_tp_table: str
    sde_walk_table: str

    # Local staging tables in FGDB
    tb_flows_local: str
    tb_tp_local: str
    tb_walk_local: str

    # Local output feature classes in FGDB
    fc_flows: str
    fc_tp: str
    fc_walk_egress: str

    # Hosted service names (must already exist in AGOL for overwrite)
    agol_flow_service_name: str
    agol_tp_service_name: str
    agol_walk_egress_service_name: str

    # Runtime
    dry_run: bool = False


def load_config() -> PublishConfig:
    """
    Read configuration from environment variables.
    """
    aprx_path = os.getenv("APRX_PATH", "").strip()
    map_name = os.getenv("MAP_NAME", "Publish").strip()
    gdb_path = os.getenv("GDB_PATH", "").strip()
    scratch_dir = os.getenv("SCRATCH_DIR", "scratch").strip()
    sde_conn_path = os.getenv("SDE_CONN", "").strip()

    if not aprx_path or not os.path.exists(aprx_path):
        raise FileNotFoundError("APRX_PATH is missing or does not exist.")
    if not gdb_path:
        raise ValueError("GDB_PATH is missing.")
    if not sde_conn_path or not os.path.exists(sde_conn_path):
        raise FileNotFoundError("SDE_CONN is missing or does not exist.")

    # materialized SDE tables
    sde_flows_table = os.getenv("SDE_FLOWS_TABLE", "mobility.ArcGIS_Flow_BG_BG_Daily_Route_Rolling31").strip()
    sde_tp_table = os.getenv("SDE_TP_TABLE", "mobility.ArcGIS_Transfer_Hotspots_Daily_Route_Rolling31").strip()
    sde_walk_table = os.getenv("SDE_WALK_TABLE", "mobility.ArcGIS_Walk_Egress_Daily_Rolling31").strip()

    # local FGDB outputs
    fc_flows = os.path.join(gdb_path, os.getenv("FC_FLOWS", "BG_Flows_ByDay_Route"))
    fc_tp = os.path.join(gdb_path, os.getenv("FC_TP", "Transfer_Hotspots_ByDay_Route"))
    fc_walk_egress = os.path.join(gdb_path, os.getenv("FC_WALK_EGRESS", "Walk_Egress_ByDay"))

    # local staging tables
    tb_flows_local = os.path.join(gdb_path, os.getenv("TB_FLOWS_LOCAL", "_stg_Flows_R31"))
    tb_tp_local = os.path.join(gdb_path, os.getenv("TB_TP_LOCAL", "_stg_TP_R31"))
    tb_walk_local = os.path.join(gdb_path, os.getenv("TB_WALK_LOCAL", "_stg_WalkEgress_R31"))

    # AGOL services
    agol_flow_service_name = os.getenv("AGOL_FLOW_SERVICE_NAME", "").strip()
    agol_tp_service_name = os.getenv("AGOL_TP_SERVICE_NAME", "").strip()
    agol_walk_egress_service_name = os.getenv("AGOL_WALK_EGRESS_SERVICE_NAME", "").strip()

    if not (agol_flow_service_name and agol_tp_service_name and agol_walk_egress_service_name):
        raise ValueError("AGOL_*_SERVICE_NAME variables must be set (existing hosted layers required for overwrite).")

    dry_run = os.getenv("DRY_RUN", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

    os.makedirs(scratch_dir, exist_ok=True)

    return PublishConfig(
        aprx_path=aprx_path,
        map_name=map_name,
        gdb_path=gdb_path,
        scratch_dir=scratch_dir,
        sde_conn_path=sde_conn_path,
        sde_flows_table=sde_flows_table,
        sde_tp_table=sde_tp_table,
        sde_walk_table=sde_walk_table,
        tb_flows_local=tb_flows_local,
        tb_tp_local=tb_tp_local,
        tb_walk_local=tb_walk_local,
        fc_flows=fc_flows,
        fc_tp=fc_tp,
        fc_walk_egress=fc_walk_egress,
        agol_flow_service_name=agol_flow_service_name,
        agol_tp_service_name=agol_tp_service_name,
        agol_walk_egress_service_name=agol_walk_egress_service_name,
        dry_run=dry_run,
    )


# -----------------------------
# ArcGIS helpers
# -----------------------------

SR_WGS84 = arcpy.SpatialReference(4326)
arcpy.env.overwriteOutput = True


def sde_path(cfg: PublishConfig, table_name: str) -> str:
    """
    Join SDE connection file path to the table name.
    In ArcGIS, the .sde file behaves like a workspace root.
    """
    return os.path.join(cfg.sde_conn_path, table_name)


def copy_sde_table_to_fgdb(sde_table: str, fgdb_table: str) -> str:
    """
    Copy SDE table into local FGDB as a stable staging table.
    """
    if arcpy.Exists(fgdb_table):
        arcpy.management.Delete(fgdb_table)
    arcpy.management.CopyRows(sde_table, fgdb_table)
    return fgdb_table


def rebuild_flows_fc(cfg: PublishConfig) -> str:
    """
    Build a line feature class representing O-D flows using mean start/end coords.
    """
    sde_src = sde_path(cfg, cfg.sde_flows_table)
    local_tbl = copy_sde_table_to_fgdb(sde_src, cfg.tb_flows_local)

    if arcpy.Exists(cfg.fc_flows):
        arcpy.management.Delete(cfg.fc_flows)

    # ATTRIBUTES retains fields (including oid) from table in output lines
    arcpy.management.XYToLine(
        in_table=local_tbl,
        out_featureclass=cfg.fc_flows,
        startx_field="mean_start_lon",
        starty_field="mean_start_lat",
        endx_field="mean_end_lon",
        endy_field="mean_end_lat",
        line_type="GEODESIC",
        attributes="ATTRIBUTES",
        spatial_reference=SR_WGS84,
    )
    arcpy.management.DefineProjection(cfg.fc_flows, SR_WGS84)
    return cfg.fc_flows


def rebuild_tp_fc(cfg: PublishConfig) -> str:
    """
    Build a point feature class for transfer hotspots using mean lon/lat.
    """
    sde_src = sde_path(cfg, cfg.sde_tp_table)
    local_tbl = copy_sde_table_to_fgdb(sde_src, cfg.tb_tp_local)

    if arcpy.Exists(cfg.fc_tp):
        arcpy.management.Delete(cfg.fc_tp)

    arcpy.management.XYTableToPoint(
        in_table=local_tbl,
        out_feature_class=cfg.fc_tp,
        x_field="mean_lon",
        y_field="mean_lat",
        coordinate_system=SR_WGS84,
    )
    arcpy.management.DefineProjection(cfg.fc_tp, SR_WGS84)
    return cfg.fc_tp


def rebuild_walk_egress_fc(cfg: PublishConfig) -> str:
    """
    Build a line feature class for egress walk legs using mean start/end coords.
    """
    sde_src = sde_path(cfg, cfg.sde_walk_table)
    local_tbl = copy_sde_table_to_fgdb(sde_src, cfg.tb_walk_local)

    if arcpy.Exists(cfg.fc_walk_egress):
        arcpy.management.Delete(cfg.fc_walk_egress)

    arcpy.management.XYToLine(
        in_table=local_tbl,
        out_featureclass=cfg.fc_walk_egress,
        startx_field="mean_start_lon",
        starty_field="mean_start_lat",
        endx_field="mean_end_lon",
        endy_field="mean_end_lat",
        line_type="GEODESIC",
        attributes="ATTRIBUTES",
        spatial_reference=SR_WGS84,
    )
    arcpy.management.DefineProjection(cfg.fc_walk_egress, SR_WGS84)
    return cfg.fc_walk_egress


# -----------------------------
# Publishing helper: overwrite hosted layer
# -----------------------------

def overwrite_hosted_layer(cfg: PublishConfig, local_fc: str, service_name: str) -> None:
    """
    Overwrite an existing hosted feature layer service in AGOL.

    Requirements:
    - You must already be signed into ArcGIS Pro with an account that has permission
      to overwrite the target hosted service.
    - The hosted service must already exist (this is overwrite, not create).
    """
    if cfg.dry_run:
        print(f"[DRY_RUN] Would overwrite hosted layer: {service_name} using {local_fc}")
        return

    if not arcpy.Exists(local_fc):
        raise FileNotFoundError(f"Local feature class not found: {local_fc}")

    aprx = arcpy.mp.ArcGISProject(cfg.aprx_path)
    maps = aprx.listMaps(cfg.map_name)
    if not maps:
        raise ValueError(f"Map '{cfg.map_name}' not found in APRX: {cfg.aprx_path}")
    mp = maps[0]

    # Remove any existing layers to keep the map clean/repeatable
    for lyr in mp.listLayers():
        try:
            mp.removeLayer(lyr)
        except Exception:
            pass

    mp.addDataFromPath(local_fc)
    aprx.save()

    sddraft_path = os.path.join(cfg.scratch_dir, f"{service_name}.sddraft")
    sd_path = os.path.join(cfg.scratch_dir, f"{service_name}.sd")

    for p in (sddraft_path, sd_path):
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass

    # Create a sharing draft configured to overwrite the existing hosted service
    sharing_draft = mp.getWebLayerSharingDraft(
        server_type="HOSTING_SERVER",
        service_type="FEATURE",
        service_name=service_name,
    )
    sharing_draft.overwriteExistingService = True
    sharing_draft.summary = "Auto-updated Rolling 31 Day mobility analytics (materialized tables -> hosted layers)."
    sharing_draft.tags = "GIS, Automation, ETL, Rolling Window"
    sharing_draft.exportToSDDraft(sddraft_path)

    arcpy.StageService_server(sddraft_path, sd_path)
    arcpy.UploadServiceDefinition_server(sd_path, "My Hosted Services")

    print(f"[INFO] Overwrote hosted layer: {service_name}")
    time.sleep(2)


# -----------------------------
# Orchestrator
# -----------------------------

def run() -> None:
    cfg = load_config()
    print("[INFO] Starting publish module (Module 3).")
    print("[INFO] DRY_RUN:", cfg.dry_run)

    # Build local feature classes
    flows_fc = rebuild_flows_fc(cfg)
    tp_fc = rebuild_tp_fc(cfg)
    walk_fc = rebuild_walk_egress_fc(cfg)

    # Overwrite hosted layers
    overwrite_hosted_layer(cfg, flows_fc, cfg.agol_flow_service_name)
    overwrite_hosted_layer(cfg, tp_fc, cfg.agol_tp_service_name)
    overwrite_hosted_layer(cfg, walk_fc, cfg.agol_walk_egress_service_name)

    print("[INFO] Module 3 finished successfully.")


if __name__ == "__main__":
    run()

