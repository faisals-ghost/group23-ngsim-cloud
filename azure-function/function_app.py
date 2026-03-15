"""
NGSIM US-101 Data Preprocessing — Azure Function
Group 23 - Member 2 (Faisal Akbar)
SOFE4630U Cloud Computing - Phase 1

HTTP Trigger: processes raw NGSIM CSV from Blob Storage,
outputs preprocessed 5-second windows back to Blob Storage.
"""

import azure.functions as func
import logging
import json
import os
import numpy as np
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ── Configuration ──
LOCATION = "us-101"
WINDOW_SIZE = 50          # 5 seconds at 10Hz
WINDOW_STEP = 10          # 1-second slide step
CONTAINER = "ngsim-data"
RAW_BLOB = "raw/ngsim_us101.csv"
OUTPUT_BLOB = "processed/preprocessed_windows.json"


def get_blob_service():
    conn_str = os.environ.get("AzureWebJobsStorage", "")
    return BlobServiceClient.from_connection_string(conn_str)


def load_data():
    """Load NGSIM CSV from Azure Blob Storage."""
    blob_service = get_blob_service()
    blob_client = blob_service.get_blob_client(container=CONTAINER, blob=RAW_BLOB)
    logging.info(f"Downloading {CONTAINER}/{RAW_BLOB}...")
    data = blob_client.download_blob().readall()
    df = pd.read_csv(BytesIO(data))
    logging.info(f"Loaded {len(df):,} rows")
    return df


def filter_and_clean(df):
    """Filter for US-101 and clean data."""
    df = df[df["Location"] == LOCATION].copy()
    logging.info(f"US-101 rows: {len(df):,}")

    drop_cols = ["O_Zone", "D_Zone", "Int_ID", "Section_ID", "Direction",
                 "Movement", "Global_X", "Global_Y", "Location"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    before = len(df)
    df = df.drop_duplicates(subset=["Vehicle_ID", "Frame_ID"])
    if before != len(df):
        logging.info(f"Removed {before - len(df):,} duplicates")

    df["has_preceding"] = df["Preceding"] != 0
    df["has_following"] = df["Following"] != 0
    df = df.sort_values(["Vehicle_ID", "Frame_ID"]).reset_index(drop=True)

    logging.info(f"Unique vehicles: {df['Vehicle_ID'].nunique():,}")
    return df


def compute_derived_metrics(df):
    """Compute relative velocity, TTC, lane change flags."""
    df = df.sort_values(["Vehicle_ID", "Frame_ID"]).reset_index(drop=True)

    vel_lookup = df.set_index(["Vehicle_ID", "Frame_ID"])["v_Vel"]
    lookup_keys = list(zip(df["Preceding"], df["Frame_ID"]))
    prec_vel = pd.Series(
        [vel_lookup.get((pid, fid), np.nan) if pid != 0 else np.nan
         for pid, fid in lookup_keys],
        index=df.index
    )

    df["v_rel"] = df["v_Vel"] - prec_vel
    df["TTC"] = np.where(
        (df["v_rel"] > 0) & (df["Space_Headway"] > 0),
        df["Space_Headway"] / df["v_rel"],
        np.nan
    )

    df["prev_lane"] = df.groupby("Vehicle_ID")["Lane_ID"].shift(1)
    df["lane_change"] = (df["Lane_ID"] != df["prev_lane"]) & df["prev_lane"].notna()

    logging.info(f"Derived metrics computed. Lane changes: {df['lane_change'].sum():,}")
    return df


def create_sliding_windows(df):
    """Create sliding 5-second windows for each vehicle."""
    windows = []
    vehicle_ids = df["Vehicle_ID"].unique()

    for vid in vehicle_ids:
        vdf = df[df["Vehicle_ID"] == vid].sort_values("Frame_ID").reset_index(drop=True)
        if len(vdf) < WINDOW_SIZE:
            continue

        for start in range(0, len(vdf) - WINDOW_SIZE + 1, WINDOW_STEP):
            window = vdf.iloc[start:start + WINDOW_SIZE]
            ego_start = window.iloc[0]
            ego_end = window.iloc[-1]

            preceding_ids = set(window[window["has_preceding"]]["Preceding"].unique().astype(int)) - {0}
            following_ids = set(window[window["has_following"]]["Following"].unique().astype(int)) - {0}
            surrounding = list(preceding_ids | following_ids)

            speed_mean = float(window["v_Vel"].mean())
            speed_var = float(window["v_Vel"].var())
            acc_mean = float(window["v_Acc"].mean())
            acc_var = float(window["v_Acc"].var())
            acc_sign_changes = int((np.diff(np.sign(window["v_Acc"].values)) != 0).sum())

            avg_headway = float(window["Space_Headway"].replace(0, np.nan).mean()) if window["has_preceding"].any() else None
            avg_time_headway = float(window["Time_Headway"].replace(0, np.nan).mean()) if window["has_preceding"].any() else None
            min_ttc = float(window["TTC"].min()) if window["TTC"].notna().any() else None

            lane_changes_in_window = int(window["lane_change"].sum())
            lanes_used = window["Lane_ID"].unique()

            win_record = {
                "ego_vehicle_id": int(vid),
                "start_frame": int(ego_start["Frame_ID"]),
                "end_frame": int(ego_end["Frame_ID"]),
                "start_time_ms": int(ego_start["Global_Time"]),
                "end_time_ms": int(ego_end["Global_Time"]),
                "duration_frames": WINDOW_SIZE,
                "lane_id": int(ego_start["Lane_ID"]),
                "surrounding_vehicle_ids": [int(x) for x in surrounding],
                "metrics": {
                    "speed_mean": round(speed_mean, 3),
                    "speed_variance": round(speed_var, 3),
                    "acceleration_mean": round(acc_mean, 3),
                    "acceleration_variance": round(acc_var, 3),
                    "acceleration_sign_changes": acc_sign_changes,
                    "avg_space_headway": round(avg_headway, 3) if avg_headway else None,
                    "avg_time_headway": round(avg_time_headway, 3) if avg_time_headway else None,
                    "min_TTC": round(min_ttc, 3) if min_ttc else None,
                    "lane_changes": lane_changes_in_window,
                    "same_lane_throughout": len(lanes_used) == 1,
                    "lanes_used": [int(x) for x in lanes_used],
                },
                "scenario_label": None
            }
            windows.append(win_record)

    logging.info(f"Created {len(windows):,} windows from {len(vehicle_ids):,} vehicles")
    return windows


def save_output(windows):
    """Save preprocessed windows to Blob Storage."""
    blob_service = get_blob_service()
    blob_client = blob_service.get_blob_client(container=CONTAINER, blob=OUTPUT_BLOB)
    body = json.dumps(windows, indent=2)
    blob_client.upload_blob(body, overwrite=True)
    logging.info(f"Saved {len(windows):,} windows to {CONTAINER}/{OUTPUT_BLOB}")


@app.route(route="preprocess")
def preprocess(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger — run the full preprocessing pipeline."""
    logging.info("NGSIM Preprocessing Pipeline started")

    try:
        # Step 1: Load
        df = load_data()

        # Step 2: Filter & Clean
        df = filter_and_clean(df)

        # Step 3: Compute Derived Metrics
        df = compute_derived_metrics(df)

        # Step 4: Segment into Windows
        windows = create_sliding_windows(df)

        # Step 5: Save
        save_output(windows)

        summary = {
            "status": "success",
            "total_windows": len(windows),
            "unique_vehicles": len(set(w["ego_vehicle_id"] for w in windows)),
            "window_size_frames": WINDOW_SIZE,
            "window_step_frames": WINDOW_STEP,
            "location": LOCATION,
            "output_blob": f"{CONTAINER}/{OUTPUT_BLOB}"
        }

        return func.HttpResponse(
            json.dumps(summary, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route(route="status")
def status(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    return func.HttpResponse(
        json.dumps({"status": "running", "service": "NGSIM Preprocessing", "group": 23}),
        mimetype="application/json"
    )
