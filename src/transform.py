import os
import logging
import json
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
import great_expectations as ge
from great_expectations.dataset import PandasDataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when, round as spark_round
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
while not CURRENT_DIR.endswith("sephora-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

os.chdir(CURRENT_DIR)
logging.info(f"Working directory set to: {CURRENT_DIR}")

RAW_DIR = os.path.join(CURRENT_DIR, "data/raw_parquet/sephora")
PROCESSED_DIR = os.path.join(CURRENT_DIR, "data/processed/sephora")
os.makedirs(PROCESSED_DIR, exist_ok=True)


def transform_sephora_data():
    spark = SparkSession.builder.appName("Sephora_Transform_Pipeline").getOrCreate()

    latest_file = sorted(os.listdir(RAW_DIR))[-1]
    raw_path = os.path.join(RAW_DIR, latest_file)
    logging.info(f"Reading data from: {raw_path}")

    df = spark.read.parquet(raw_path)
    df.printSchema()

    # rename columns
    rename_map = {
        "love": "liked",
        "price": "currentPrice",
        "value_price": "originalPrice"
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # drop unnecessary columns
    for col_to_drop in ["limited_edition", "limited_time_offer"]:
        if col_to_drop in df.columns:
            df = df.drop(col_to_drop)

    # convert numeric columns
    for c in ["currentPrice", "originalPrice", "rating", "number_of_reviews", "liked"]:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("double"))

    # drop nulls in important columns
    df = df.dropna(subset=["brand", "category", "currentPrice", "rating"])

    # clean text columns
    df = df.withColumn("brand", lower(trim(col("brand"))))
    df = df.withColumn("category", lower(trim(col("category"))))
    df = df.withColumn("name", trim(col("name")))

    # parse size column
    df = df.withColumn("size_value", regexp_replace(col("size"), "[^0-9.]", "").cast("double"))
    df = df.withColumn("size_unit",
                       lower(when(col("size").rlike("(?i)ml"), "ml")
                             .when(col("size").rlike("(?i)oz"), "oz")
                             .otherwise(None)))

    # convert binary to boolean
    for bcol in ["online_only", "exclusive"]:
        if bcol in df.columns:
            df = df.withColumn(bcol, col(bcol).cast("boolean"))

    # feature engineering
    df = df.withColumn(
        "discount_percentage",
        spark_round(((col("originalPrice") - col("currentPrice")) / col("originalPrice")) * 100, 2)
    )
    df = df.withColumn(
        "review_density",
        spark_round(col("number_of_reviews") / col("liked"), 3)
    )

    # collect manageable sample to pandas for Great Expectations validation
    sample_df = df.limit(5000).toPandas()
    ge_df = PandasDataset(sample_df)
    ge_df.expect_column_values_to_not_be_null("brand")
    ge_df.expect_column_values_to_be_between("rating", min_value=0, max_value=5)
    ge_df.expect_column_values_to_be_between("currentPrice", min_value=0)
    ge_df.expect_column_values_to_be_between("originalPrice", min_value=0)
    ge_df.expect_column_values_to_be_in_type_list("discount_percentage", ["float64", "float32"])
    ge_df.expect_column_values_to_not_be_null("category")

    validation = ge_df.validate()
    if not validation.success:
        logging.error("Data validation failed")
        logging.error(json.dumps(validation.to_json_dict(), indent=2))
        raise ValueError("Quality checks failed.")
    else:
        logging.info("All Great Expectations checks passed successfully.")

    out_path = os.path.join(PROCESSED_DIR, f"processed_{latest_file}")
    df.write.mode("overwrite").parquet(out_path)
    logging.info(f"Saved processed data: {out_path}")

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    file_system_name = os.getenv("AZURE_FILESYSTEM_NAME")

    if account_name and account_key and file_system_name:
        try:
            service_client = DataLakeServiceClient(
                account_url=f"https://{account_name}.dfs.core.windows.net",
                credential=account_key,
            )
            file_system_client = service_client.get_file_system_client(file_system=file_system_name)

            remote_path = f"processed/sephora/{os.path.basename(out_path)}"
            file_client = file_system_client.get_file_client(remote_path)
            with open(out_path + "/_SUCCESS", "rb") as f:
                file_client.upload_data(f, overwrite=True)
            logging.info(f"Uploaded marker file for {remote_path}")
        except Exception as e:
            logging.error(f"Azure upload failed: {e}")
    else:
        logging.warning("Azure credentials not found; skipping upload.")


if __name__ == "__main__":
    transform_sephora_data()
