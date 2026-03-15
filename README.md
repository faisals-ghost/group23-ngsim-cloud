# Group 23 — NGSIM Cloud Processing (SOFE4630U)

Cloud-based scenario extraction and processing from the NGSIM US-101 dataset.

## Team
- Member 1: Muhammad Idrees — Azure Setup
- Member 2: Faisal Akbar — Data Preprocessing
- Member 3: Sanzir Anarbaev — Scenario Detection
- Member 4: Rishab Singh — Output + Documentation

## Architecture (Phase 1)
- **Cloud:** Microsoft Azure (Azure for Students)
- **Storage:** Azure Blob Storage (`group23ngsimdata` / `ngsim-data` container)
- **Compute:** Azure Functions (Consumption Plan, Python 3.11)
- **Region:** Canada Central
- **Language:** Python 3.11

## Project Structure
```
├── preprocessing.py                    # Local preprocessing script
├── azure-function/                     # Member 2: Preprocessing Azure Function
│   ├── function_app.py                 #   HTTP trigger → /api/preprocess
│   ├── host.json
│   └── requirements.txt
├── azure-function-scenario/            # Member 3: Scenario Detection Azure Function
│   ├── function_app.py                 #   HTTP trigger → /api/detect
│   ├── host.json
│   └── requirements.txt
├── DEPLOYMENT_GUIDE.md                 # Step-by-step Azure setup
└── DEPLOYMENT_GUIDE.pdf
```

## Live Endpoints
| Service | URL |
|---------|-----|
| Preprocessing Status | https://group23-ngsim-preprocess.azurewebsites.net/api/status |
| Run Preprocessing | https://group23-ngsim-preprocess.azurewebsites.net/api/preprocess |
| Detection Status | https://group23-ngsim-scenario.azurewebsites.net/api/status |
| Run Detection | https://group23-ngsim-scenario.azurewebsites.net/api/detect |

## Pipeline Flow
1. Raw NGSIM CSV → Azure Blob Storage (`raw/ngsim_us101.csv`)
2. `/api/preprocess` → cleans data, computes metrics, creates 5-sec windows → `processed/preprocessed_windows.json`
3. `/api/detect` → labels each window (car-following, lane-change, stop-and-go) → `processed/labeled_windows.json`

## Cloud Deployment
See `DEPLOYMENT_GUIDE.pdf` for step-by-step Azure instructions.
