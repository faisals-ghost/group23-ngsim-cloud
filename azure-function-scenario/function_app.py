"""
NGSIM US-101 Scenario Detection — Azure Function
Group 23 - Member 3 (Sanzir Anarbaev)
SOFE4630U Cloud Computing - Phase 1

HTTP Trigger: reads preprocessed windows from Blob Storage,
applies rule-based scenario detection, outputs labeled windows.
"""

import azure.functions as func
import logging
import json
import os
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ── Configuration ──
CONTAINER = "ngsim-data"
INPUT_BLOB = "processed/preprocessed_windows.json"
OUTPUT_BLOB = "processed/labeled_windows.json"

# ── Detection Thresholds ──
# Car-Following
CF_MAX_SPEED_VARIANCE = 50.0       # Low speed variance = steady following
CF_MAX_HEADWAY = 200.0             # Reasonable following distance (feet)

# Lane-Change
# Simply detected by lane_changes > 0

# Stop-and-Go
SG_MIN_ACC_VARIANCE = 10.0         # High acceleration variance
SG_MIN_ACC_SIGN_CHANGES = 10       # Frequent accel/decel switches
SG_MAX_SPEED_MEAN = 30.0           # Low average speed (ft/s) ~20 mph


def get_blob_service():
    conn_str = os.environ.get("AzureWebJobsStorage", "")
    return BlobServiceClient.from_connection_string(conn_str)


def load_windows():
    """Load preprocessed windows from Blob Storage."""
    blob_service = get_blob_service()
    blob_client = blob_service.get_blob_client(container=CONTAINER, blob=INPUT_BLOB)
    logging.info(f"Loading preprocessed windows from {CONTAINER}/{INPUT_BLOB}...")
    data = blob_client.download_blob().readall()
    windows = json.loads(data)
    logging.info(f"Loaded {len(windows):,} windows")
    return windows


def detect_scenario(window):
    """
    Apply rule-based scenario detection to a single window.
    Returns the scenario label and confidence reason.
    
    Priority: Lane-Change > Stop-and-Go > Car-Following > unclassified
    """
    metrics = window["metrics"]
    
    speed_mean = metrics["speed_mean"]
    speed_var = metrics["speed_variance"]
    acc_var = metrics["acceleration_variance"]
    acc_sign_changes = metrics["acceleration_sign_changes"]
    lane_changes = metrics["lane_changes"]
    same_lane = metrics["same_lane_throughout"]
    avg_headway = metrics["avg_space_headway"]
    min_ttc = metrics["min_TTC"]

    # ── Scenario 2: Lane-Change Detection ──
    # If the ego vehicle changed lanes during the window
    if lane_changes > 0 and not same_lane:
        return "lane-change", {
            "reason": "Lane ID changed during window",
            "lane_changes": lane_changes,
            "lanes_used": metrics["lanes_used"]
        }

    # ── Scenario 3: Stop-and-Go Detection ──
    # High acceleration variance + frequent sign changes + low speed
    if (acc_var >= SG_MIN_ACC_VARIANCE and 
        acc_sign_changes >= SG_MIN_ACC_SIGN_CHANGES and 
        speed_mean <= SG_MAX_SPEED_MEAN):
        return "stop-and-go", {
            "reason": "High acceleration variance with frequent speed oscillations at low speed",
            "acceleration_variance": acc_var,
            "acceleration_sign_changes": acc_sign_changes,
            "speed_mean": speed_mean
        }

    # ── Scenario 1: Car-Following Detection ──
    # Same lane throughout, has a preceding vehicle, relatively steady speed
    if (same_lane and 
        avg_headway is not None and 
        avg_headway > 0 and 
        avg_headway <= CF_MAX_HEADWAY and 
        speed_var <= CF_MAX_SPEED_VARIANCE):
        return "car-following", {
            "reason": "Steady following in same lane with preceding vehicle",
            "avg_space_headway": avg_headway,
            "speed_variance": speed_var,
            "min_TTC": min_ttc
        }

    # ── Unclassified ──
    return "unclassified", {
        "reason": "Does not match any defined scenario criteria"
    }


def run_detection(windows):
    """Run scenario detection on all windows."""
    logging.info("Running scenario detection...")
    
    counts = {"car-following": 0, "lane-change": 0, "stop-and-go": 0, "unclassified": 0}
    
    for window in windows:
        label, details = detect_scenario(window)
        window["scenario_label"] = label
        window["detection_details"] = details
        counts[label] += 1
    
    logging.info(f"Detection complete:")
    for label, count in counts.items():
        pct = (count / len(windows) * 100) if windows else 0
        logging.info(f"  {label}: {count:,} ({pct:.1f}%)")
    
    return windows, counts


def save_output(windows):
    """Save labeled windows to Blob Storage."""
    blob_service = get_blob_service()
    blob_client = blob_service.get_blob_client(container=CONTAINER, blob=OUTPUT_BLOB)
    body = json.dumps(windows, indent=2)
    blob_client.upload_blob(body, overwrite=True)
    logging.info(f"Saved {len(windows):,} labeled windows to {CONTAINER}/{OUTPUT_BLOB}")


@app.route(route="detect")
def detect(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger — run scenario detection on preprocessed windows."""
    logging.info("Scenario Detection Pipeline started")

    try:
        # Step 1: Load preprocessed windows
        windows = load_windows()

        # Step 2: Run detection
        labeled_windows, counts = run_detection(windows)

        # Step 3: Save labeled output
        save_output(labeled_windows)

        # Build summary with example from each scenario
        examples = {}
        for w in labeled_windows:
            label = w["scenario_label"]
            if label not in examples:
                examples[label] = {
                    "ego_vehicle_id": w["ego_vehicle_id"],
                    "start_frame": w["start_frame"],
                    "end_frame": w["end_frame"],
                    "metrics_summary": {
                        "speed_mean": w["metrics"]["speed_mean"],
                        "acceleration_variance": w["metrics"]["acceleration_variance"],
                        "lane_changes": w["metrics"]["lane_changes"]
                    },
                    "detection_details": w["detection_details"]
                }

        summary = {
            "status": "success",
            "total_windows": len(labeled_windows),
            "scenario_counts": counts,
            "scenario_percentages": {
                k: round(v / len(labeled_windows) * 100, 1) 
                for k, v in counts.items()
            },
            "examples": examples,
            "output_blob": f"{CONTAINER}/{OUTPUT_BLOB}"
        }

        return func.HttpResponse(
            json.dumps(summary, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Detection failed: {str(e)}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route(route="status")
def status(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint."""
    return func.HttpResponse(
        json.dumps({
            "status": "running", 
            "service": "NGSIM Scenario Detection", 
            "group": 23,
            "scenarios": ["car-following", "lane-change", "stop-and-go"]
        }),
        mimetype="application/json"
    )
