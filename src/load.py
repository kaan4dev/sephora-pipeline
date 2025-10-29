import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.io_datalake import upload_to_datalake

PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "sephora")

latest_item = sorted(os.listdir(PROCESSED_DIR))[-1]
local_path = os.path.join(PROCESSED_DIR, latest_item)

if os.path.isdir(local_path):
    parquet_files = [f for f in os.listdir(local_path) if f.endswith(".parquet")]
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {local_path}")
    local_path = os.path.join(local_path, parquet_files[0])

remote_path = f"processed/sephora/{os.path.basename(local_path)}"

upload_to_datalake(local_path, remote_path)
