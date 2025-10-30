import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, lower, trim

# set logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# set working dir
CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("sephora-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)

os.chdir(CURRENT_DIR)
logging.info(f"working dir set to: {CURRENT_DIR}")

# set paths
PROCESSED_DIR = os.path.join(CURRENT_DIR, "data/processed/sephora")
MODELED_DIR = os.path.join(CURRENT_DIR, "data/modeled/sephora")
os.makedirs(MODELED_DIR, exist_ok=True)

def model_sephora_data():
    # init spark
    spark = SparkSession.builder.appName("sephora_model_pipeline").getOrCreate()

    # read latest processed file
    latest_file = sorted(os.listdir(PROCESSED_DIR))[-1]
    processed_path = os.path.join(PROCESSED_DIR, latest_file)
    logging.info(f"reading data from: {processed_path}")
    df = spark.read.parquet(processed_path)

    # dim brand
    dim_brand = (
        df.select(lower(trim(col("brand"))).alias("brand_name"))
        .dropDuplicates()
        .withColumn("brand_id", monotonically_increasing_id())
    )
    dim_brand.write.mode("overwrite").parquet(os.path.join(MODELED_DIR, "dim_brand"))
    logging.info("dim_brand done")

    # dim category
    dim_category = (
        df.select(lower(trim(col("category"))).alias("category_name"))
        .dropDuplicates()
        .withColumn("category_id", monotonically_increasing_id())
    )
    dim_category.write.mode("overwrite").parquet(os.path.join(MODELED_DIR, "dim_category"))
    logging.info("dim_category done")

    # join dims
    df_joined = (
        df.join(dim_brand, lower(trim(df["brand"])) == dim_brand["brand_name"], "left")
          .join(dim_category, lower(trim(df["category"])) == dim_category["category_name"], "left")
    )

    # dim product
    dim_product = df_joined.select(
        col("id").alias("product_id"),
        trim(col("name")).alias("product_name"),
        col("size_value"),
        col("size_unit"),
        col("currentPrice"),
        col("originalPrice"),
        col("discount_percentage"),
        col("brand_id"),
        col("category_id")
    ).dropDuplicates(["product_id"])
    dim_product.write.mode("overwrite").parquet(os.path.join(MODELED_DIR, "dim_product"))
    logging.info("dim_product done")

    # fact reviews
    fact_reviews = df_joined.select(
        col("id").alias("product_id"),
        col("rating"),
        col("number_of_reviews"),
        col("liked"),
        col("review_density")
    )
    fact_reviews.write.mode("overwrite").parquet(os.path.join(MODELED_DIR, "fact_reviews"))
    logging.info("fact_reviews done")

    # close
    spark.stop()
    logging.info("modeling complete")

if __name__ == "__main__":
    model_sephora_data()
