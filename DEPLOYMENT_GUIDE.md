# Group 23 — Azure Deployment Guide
## NGSIM US-101 Preprocessing (Phase 1)

---

## Overview
- **Blob Storage** = stores your files (raw CSV + processed output)
- **Azure VM** = runs your Python preprocessing script
- **Flow:** CSV in Blob → VM reads it → processes → writes results back to Blob

---

## Step 1: Log into Azure Portal
1. Go to https://portal.azure.com
2. Sign in with your group's Azure account
3. You should see resource group: `group23-ngsim-function_group` (Canada Central)

---

## Step 2: Create a Storage Account
1. In Azure Portal, search **"Storage accounts"** → click **+ Create**
2. Settings:
   - **Resource group:** `group23-ngsim-function_group`
   - **Storage account name:** `group23ngsimstorage` (lowercase, no dashes)
   - **Region:** Canada Central
   - **Performance:** Standard
   - **Redundancy:** LRS (cheapest)
3. Click **Review + Create** → **Create**
4. Once created, go to the storage account

---

## Step 3: Create a Blob Container
1. In your storage account, click **Containers** (left sidebar under "Data storage")
2. Click **+ Container**
   - **Name:** `ngsim-data`
   - **Public access level:** Private
3. Click **Create**

---

## Step 4: Upload the NGSIM CSV
1. Click into the `ngsim-data` container
2. Click **Upload**
3. Browse and select `ngsim_us101.csv` (163MB file)
4. Click **Upload** — wait for it to finish

---

## Step 5: Get the Connection String
1. Go back to your **Storage Account**
2. Left sidebar → **Access keys** (under "Security + networking")
3. Click **Show** next to key1's Connection string
4. **Copy it** — you'll need this to run the script

---

## Step 6: Create an Azure VM
1. Search **"Virtual machines"** → click **+ Create** → **Azure Virtual Machine**
2. Settings:
   - **Resource group:** `group23-ngsim-function_group`
   - **VM name:** `group23-ngsim-vm`
   - **Region:** Canada Central
   - **Image:** Ubuntu 22.04 LTS
   - **Size:** Standard_B2s (2 vCPUs, 4GB RAM — enough for this)
   - **Authentication:** SSH public key (or password, your choice)
   - **Username:** `azureuser`
3. Click **Review + Create** → **Create**
4. Download the SSH key if you chose SSH authentication

---

## Step 7: SSH into the VM
```bash
# If using SSH key:
ssh -i ~/path/to/key.pem azureuser@<VM_PUBLIC_IP>

# If using password:
ssh azureuser@<VM_PUBLIC_IP>
```
Find the public IP in the VM's Overview page in Azure Portal.

---

## Step 8: Set Up the VM
Run these commands on the VM:

```bash
# Update and install Python
sudo apt update && sudo apt install -y python3 python3-pip python3-venv git

# Clone the repo
git clone https://github.com/faisals-ghost/group23-ngsim-cloud.git
cd group23-ngsim-cloud

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install pandas numpy azure-storage-blob
```

---

## Step 9: Run the Preprocessing Script
```bash
# Set environment variables
export USE_CLOUD=true
export AZURE_STORAGE_CONNECTION_STRING="<paste your connection string here>"
export AZURE_CONTAINER="ngsim-data"
export AZURE_RAW_BLOB="ngsim_us101.csv"
export AZURE_OUTPUT_PREFIX="processed/"

# Run it
python3 preprocessing.py
```

Expected output:
```
[Ingestion] Downloading from Azure Blob: ngsim-data/ngsim_us101.csv ...
[Ingestion] Loaded 1,340,990 rows from Azure Blob Storage
[Preprocessing] Filtering for location: us-101
[Preprocessing] US-101 rows: 722,017
...
[Segmentation] Created 48,718 windows from 2,847 vehicles
[Output] Saved to Azure Blob: ngsim-data/processed/preprocessed_windows.json
[Done] Pipeline completed in ~25 seconds
```

---

## Step 10: Verify Output
1. Go back to Azure Portal → Storage Account → Containers → `ngsim-data`
2. You should see a `processed/` folder with `preprocessed_windows.json`
3. Download it to verify — it should have ~48,718 scenario windows

---

## Done! ✅
Your preprocessing is complete. The output JSON is in Blob Storage ready for Member 3 (Sanzir) to run scenario detection on.

---

## Troubleshooting
- **Can't SSH?** Make sure port 22 is open in the VM's Network Security Group
- **pip install fails?** Try `pip install --upgrade pip` first
- **Script runs slow?** The B2s VM should handle it in under 60 seconds
- **Connection string error?** Make sure you copied the FULL string starting with `DefaultEndpointsProtocol=`
