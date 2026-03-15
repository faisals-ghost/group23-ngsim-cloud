"""
NGSIM US-101 Data Preprocessing Module
Group 23 - Member 2 (Faisal Akbar)
SOFE4630U Cloud Computing - Phase 1

This module:
1. Loads raw NGSIM CSV (from S3 or local)
2. Filters for US-101 roadway
3. Cleans and validates data
4. Computes derived metrics (relative velocity, TTC, lane change flags, accel variance)
5. Segments into sliding 5-second windows (50 frames at 10Hz)
6. Outputs structured JSON scenario samples to S3 or local
"""

import json
import os
import sys
import time
import numpy as np
import pandas as pd

# ── Configuration ──
LOCATION = "us-101"
WINDOW_SIZE = 50          # 5 seconds at 10Hz
WINDOW_STEP = 10          # 1-second slide step (overlapping windows)
OUTPUT_DIR = "output"

# Azure Blob Storage config (used when running on Azure VM)
AZURE_CONN_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_CONTAINER = os.environ.get("AZURE_CONTAINER", "ngsim-data")
AZURE_RAW_BLOB = os.environ.get("AZURE_RAW_BLOB", "raw/ngsim_us101.csv")
AZURE_OUTPUT_PREFIX = os.environ.get("AZURE_OUTPUT_PREFIX", "processed/")
USE_CLOUD = os.environ.get("USE_CLOUD", "false").lower() == "true"


# ── Azure Blob Helpers ──
def load_from_azure(container, blob_name):
    """Load CSV from Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STRING)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    print(f"[Ingestion] Downloading from Azure Blob: {container}/{blob_name} ...")
    data = blob_client.download_blob().readall()
    from io import BytesIO
    df = pd.read_csv(BytesIO(data))
    print(f"[Ingestion] Loaded {len(df):,} rows from Azure Blob Storage")
    return df


def save_to_azure(data, container, blob_name):
    """Save JSON data to Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STRING)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    body = json.dumps(data, indent=2)
    blob_client.upload_blob(body, overwrite=True, content_settings={"content_type": "application/json"})
    print(f"[Output] Saved to Azure Blob: {container}/{blob_name}")


# ── Step 1: Load Data ──
def load_data(filepath=None):
    """Load NGSIM data from local file or S3."""
    if USE_CLOUD:
        df = load_from_azure(AZURE_CONTAINER, AZURE_RAW_BLOB)
    else:
        filepath = filepath or "ngsim_us101.csv"
        print(f"[Ingestion] Loading local file: {filepath}")
        df = pd.read_csv(filepath)
        print(f"[Ingestion] Loaded {len(df):,} rows")
    return df


# ── Step 2: Filter & Clean ──
def filter_and_clean(df):
    """Filter for US-101 and clean the data."""
    print(f"[Preprocessing] Filtering for location: {LOCATION}")
    df = df[df["Location"] == LOCATION].copy()
    print(f"[Preprocessing] US-101 rows: {len(df):,}")

    # Drop columns not needed for scenario extraction
    drop_cols = ["O_Zone", "D_Zone", "Int_ID", "Section_ID", "Direction",
                 "Movement", "Global_X", "Global_Y", "Location"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # Remove duplicates (dataset has some)
    before = len(df)
    df = df.drop_duplicates(subset=["Vehicle_ID", "Frame_ID"])
    after = len(df)
    if before != after:
        print(f"[Preprocessing] Removed {before - after:,} duplicate rows")

    # Remove rows where Preceding or Following is 0 (no vehicle ahead/behind)
    # Keep them but flag — 0 means no preceding/following vehicle
    df["has_preceding"] = df["Preceding"] != 0
    df["has_following"] = df["Following"] != 0

    # Sort by Vehicle_ID and Frame_ID for time-series processing
    df = df.sort_values(["Vehicle_ID", "Frame_ID"]).reset_index(drop=True)

    print(f"[Preprocessing] Unique vehicles: {df['Vehicle_ID'].nunique():,}")
    print(f"[Preprocessing] Frame range: {df['Frame_ID'].min()} - {df['Frame_ID'].max()}")
    print(f"[Preprocessing] Lane IDs: {sorted(df['Lane_ID'].unique())}")

    return df


# ── Step 3: Compute Derived Metrics ──
def compute_derived_metrics(df):
    """Compute relative velocity, TTC, lane change flags, and acceleration stats."""
    print("[Preprocessing] Computing derived metrics...")

    # Build a lookup: (Vehicle_ID, Frame_ID) -> row for fast preceding vehicle lookups
    df = df.set_index(["Vehicle_ID", "Frame_ID"])

    # Pre-compute preceding vehicle velocity for relative velocity calculation
    # We need to look up the preceding vehicle's velocity at the same frame
    preceding_vel = {}
    for (vid, fid), row in df.iterrows():
        preceding_vel[(vid, fid)] = row["v_Vel"]

    records = []
    for (vid, fid), row in df.iterrows():
        rec = row.to_dict()
        rec["Vehicle_ID"] = vid
        rec["Frame_ID"] = fid

        # Relative velocity (ego - lead)
        prec_id = int(row["Preceding"])
        if prec_id != 0 and (prec_id, fid) in preceding_vel:
            prec_v = preceding_vel[(prec_id, fid)]
            rec["v_rel"] = row["v_Vel"] - prec_v
            # Time-to-collision
            if rec["v_rel"] > 0 and row["Space_Headway"] > 0:
                rec["TTC"] = row["Space_Headway"] / rec["v_rel"]
            else:
                rec["TTC"] = None  # not closing in or no headway
        else:
            rec["v_rel"] = None
            rec["TTC"] = None

        records.append(rec)

    df = pd.DataFrame(records)

    # Lane change detection: compare Lane_ID with previous frame for same vehicle
    df = df.sort_values(["Vehicle_ID", "Frame_ID"])
    df["prev_lane"] = df.groupby("Vehicle_ID")["Lane_ID"].shift(1)
    df["lane_change"] = (df["Lane_ID"] != df["prev_lane"]) & df["prev_lane"].notna()

    print(f"[Preprocessing] Derived metrics computed for {len(df):,} rows")
    print(f"[Preprocessing] Lane changes detected: {df['lane_change'].sum():,}")

    return df


# ── Optimized version using vectorized operations ──
def compute_derived_metrics_fast(df):
    """Compute derived metrics using vectorized pandas operations (much faster)."""
    print("[Preprocessing] Computing derived metrics (vectorized)...")

    df = df.sort_values(["Vehicle_ID", "Frame_ID"]).reset_index(drop=True)

    # Create lookup for velocity by (Vehicle_ID, Frame_ID)
    vel_lookup = df.set_index(["Vehicle_ID", "Frame_ID"])["v_Vel"]

    # For each row, look up the preceding vehicle's velocity at the same frame
    lookup_keys = list(zip(df["Preceding"], df["Frame_ID"]))
    prec_vel = pd.Series(
        [vel_lookup.get((pid, fid), np.nan) if pid != 0 else np.nan
         for pid, fid in lookup_keys],
        index=df.index
    )

    # Relative velocity
    df["v_rel"] = df["v_Vel"] - prec_vel

    # Time-to-collision (only when closing in: v_rel > 0)
    df["TTC"] = np.where(
        (df["v_rel"] > 0) & (df["Space_Headway"] > 0),
        df["Space_Headway"] / df["v_rel"],
        np.nan
    )

    # Lane change detection
    df["prev_lane"] = df.groupby("Vehicle_ID")["Lane_ID"].shift(1)
    df["lane_change"] = (df["Lane_ID"] != df["prev_lane"]) & df["prev_lane"].notna()

    print(f"[Preprocessing] Derived metrics computed for {len(df):,} rows")
    print(f"[Preprocessing] Lane changes detected: {df['lane_change'].sum():,}")
    print(f"[Preprocessing] Rows with valid TTC: {df['TTC'].notna().sum():,}")

    return df


# ── Step 4: Segment into 5-Second Windows ──
def create_sliding_windows(df):
    """Create sliding 5-second windows for each vehicle trajectory."""
    print(f"[Segmentation] Creating {WINDOW_SIZE}-frame sliding windows (step={WINDOW_STEP})...")

    windows = []
    vehicle_ids = df["Vehicle_ID"].unique()

    for i, vid in enumerate(vehicle_ids):
        if (i + 1) % 500 == 0:
            print(f"  Processing vehicle {i + 1}/{len(vehicle_ids)}...")

        vdf = df[df["Vehicle_ID"] == vid].sort_values("Frame_ID").reset_index(drop=True)

        if len(vdf) < WINDOW_SIZE:
            continue  # not enough frames for a 5-second window

        for start in range(0, len(vdf) - WINDOW_SIZE + 1, WINDOW_STEP):
            window = vdf.iloc[start:start + WINDOW_SIZE]

            # Get ego vehicle info
            ego_start = window.iloc[0]
            ego_end = window.iloc[-1]

            # Surrounding vehicles in this window
            preceding_ids = set(window[window["has_preceding"]]["Preceding"].unique().astype(int)) - {0}
            following_ids = set(window[window["has_following"]]["Following"].unique().astype(int)) - {0}
            surrounding = list(preceding_ids | following_ids)

            # Compute window-level aggregated metrics
            speed_mean = float(window["v_Vel"].mean())
            speed_var = float(window["v_Vel"].var())
            acc_mean = float(window["v_Acc"].mean())
            acc_var = float(window["v_Acc"].var())
            acc_sign_changes = int((np.diff(np.sign(window["v_Acc"].values)) != 0).sum())

            avg_headway = float(window["Space_Headway"].replace(0, np.nan).mean()) if window["has_preceding"].any() else None
            avg_time_headway = float(window["Time_Headway"].replace(0, np.nan).mean()) if window["has_preceding"].any() else None
            min_ttc = float(window["TTC"].min()) if window["TTC"].notna().any() else None

            lane_changes_in_window = int(window["lane_change"].sum())

            # Determine lane consistency
            lanes_used = window["Lane_ID"].unique()
            same_lane = len(lanes_used) == 1

            # Build window record
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
                    "same_lane_throughout": same_lane,
                    "lanes_used": [int(x) for x in lanes_used],
                },
                "scenario_label": None  # filled by scenario detector (Member 3)
            }

            windows.append(win_record)

    print(f"[Segmentation] Created {len(windows):,} windows from {len(vehicle_ids):,} vehicles")
    return windows


# ── Step 5: Save Output ──
def save_output(windows, output_dir=None):
    """Save windowed data as JSON."""
    if USE_CLOUD:
        save_to_azure(windows, AZURE_CONTAINER, f"{AZURE_OUTPUT_PREFIX}preprocessed_windows.json")
    else:
        output_dir = output_dir or OUTPUT_DIR
        os.makedirs(output_dir, exist_ok=True)
        outpath = os.path.join(output_dir, "preprocessed_windows.json")
        with open(outpath, "w") as f:
            json.dump(windows, f, indent=2)
        print(f"[Output] Saved {len(windows):,} windows to {outpath}")

        # Also save a summary
        summary = {
            "total_windows": len(windows),
            "unique_ego_vehicles": len(set(w["ego_vehicle_id"] for w in windows)),
            "window_size_frames": WINDOW_SIZE,
            "window_step_frames": WINDOW_STEP,
            "location": LOCATION,
        }
        summary_path = os.path.join(output_dir, "preprocessing_summary.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"[Output] Summary saved to {summary_path}")


# ── Main Pipeline ──
def main(filepath=None):
    """Run the full preprocessing pipeline."""
    start = time.time()
    print("=" * 60)
    print("NGSIM US-101 Preprocessing Pipeline — Group 23")
    print("=" * 60)

    # Step 1: Load
    df = load_data(filepath)

    # Step 2: Filter & Clean
    df = filter_and_clean(df)

    # Step 3: Compute Derived Metrics
    df = compute_derived_metrics_fast(df)

    # Step 4: Segment into Windows
    windows = create_sliding_windows(df)

    # Step 5: Save
    save_output(windows)

    elapsed = time.time() - start
    print(f"\n[Done] Pipeline completed in {elapsed:.1f} seconds")
    print(f"[Done] {len(windows):,} scenario windows ready for detection")

    return windows


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else None
    main(filepath)
