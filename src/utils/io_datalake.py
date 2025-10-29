import os
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()


def _require_env(*keys: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    raise ValueError(f"Missing required environment variable(s): {', '.join(keys)}")


def upload_to_datalake(local_path: str, remote_path: str):
    account_name = _require_env("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = _require_env("AZURE_STORAGE_ACCOUNT_KEY")
    file_system = _require_env("AZURE_FILESYSTEM_NAME", "AZURE_FILE_SYSTEM")

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    file_client = file_system_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

    print(f"Uploaded {local_path} -> {remote_path} (Azure Data Lake)")
