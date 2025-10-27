import os
import pandas as pd
import logging 
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",  
    handlers=[
        logging.StreamHandler() 
    ]
)

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("sephora-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

os.chdir(CURRENT_DIR)
logging.info(f"Working Directory is Set To: {CURRENT_DIR}")

RAW_DIR = os.path.join("data/raw/sephora")
OUT_DIR = os.path.join(CURRENT_DIR, "data/raw_parquet/sephora")
os.makedirs(OUT_DIR, exist_ok=True)

files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
if not files:
    raise FileNotFoundError(f"No CSV file found in: {RAW_DIR}")

dataframes = {}
for file in files:
    path = os.path.join(RAW_DIR, file)
    name = file.replace(".csv", "")
    try:
        df = pd.read_csv(path, encoding="utf-8")
        dataframes[name] = df
        logging.info(f"Loaded {file} -> {df.shape[0]} rows, {df.shape[1]} columns")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin1")
        dataframes[name] = df
        logging.info(f"Loaded {file} (latin1) -> {df.shape[0]} rows, {df.shape[1]} columns")
    except Exception as e:
        logging.error(f"Error reading {file}: {e}")
        continue

for name, df in dataframes.items():
    logging.info(f"Preview of {name}:\n{df.head(3)}")

for name, df in dataframes.items():
    out_path = os.path.join(OUT_DIR, f"raw_{name}.parquet")
    try:
        df.to_parquet(out_path, index=False)
        logging.info(f"Saved to {out_path}")
    except Exception as e:
        logging.error(f"Error saving {name} to parquet: {e}")

# azure section
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
file_system_name = os.getenv("AZURE_FILE_SYSTEM")

if account_name and account_key and file_system_name:
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)

        for name in dataframes.keys():
            local_path = os.path.join(OUT_DIR, f"raw_{name}.parquet")
            remote_path = f"raw/ecar/raw_{name}.parquet"
            file_client = file_system_client.get_file_client(remote_path)

            with open(local_path, "rb") as f:
                file_client.upload_data(f, overwrite=True)

            logging.info(f"Uploaded {local_path} to Azure Data Lake as {remote_path}")

    except Exception as e:
        logging.error(f"Azure upload failed: {e}")
else:
    logging.warning("Azure credentials not found in .env; skipping upload.")