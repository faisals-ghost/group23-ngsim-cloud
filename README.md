# Group 23 — NGSIM Cloud Processing (SOFE4630U)

Cloud-based scenario extraction and processing from the NGSIM US-101 dataset.

## Team
- Member 1: Muhammad Idrees — Azure Setup
- Member 2: Faisal Akbar — Data Preprocessing
- Member 3: Sanzir Anarbaev — Scenario Detection
- Member 4: Rishab Singh — Output + Documentation

## Architecture (Phase 1 — Monolithic)
- **Storage:** Azure Blob Storage
- **Compute:** Azure VM (Ubuntu 22.04)
- **Language:** Python 3.11

## Setup
```bash
pip install -r requirements.txt
python preprocessing.py ngsim_us101.csv
```

## Cloud Deployment
See `DEPLOYMENT_GUIDE.pdf` for step-by-step Azure instructions.
