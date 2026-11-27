
import os
import sys
from pathlib import Path

# ---------------------------------------------------------
# 1. Configure Spark Python interpreter
# ---------------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


# ---------------------------------------------------------
# 2. Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_PARQUET_PATH = (
    PROJECT_ROOT / "data" / "output" / "silver" / "fraud-results"
)

DELTA_SILVER_PATH = (
    PROJECT_ROOT / "data" / "output" / "silver-delta" / "fraud-results"
)


# ---------------------------------------------------------
# 3. Create Spark session with Delta Lake
# ---------------------------------------------------------

def create_spark_session():

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("FraudDetection-Day10-DeltaSilver")
        .master("local[2]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")

        # Enable Delta Lake extensions
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------
# 4. Read existing Silver Parquet data
# ---------------------------------------------------------

def read_silver_data(spark):

    if not SILVER_PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"Silver Parquet directory not found: "
            f"{SILVER_PARQUET_PATH}"
        )

    print("\nReading existing Silver Parquet data...")

    df = spark.read.parquet(str(SILVER_PARQUET_PATH))

    record_count = df.count()

    print(f"Silver records read: {record_count}")

    return df


# ---------------------------------------------------------
# 5. Write Delta table
# ---------------------------------------------------------

def write_delta_table(df):

    print("\nCreating Delta Silver table...")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(DELTA_SILVER_PATH))
    )

    print(f"Delta table created at: {DELTA_SILVER_PATH}")


# ---------------------------------------------------------
# 6. Read Delta table
# ---------------------------------------------------------

def read_delta_table(spark):

    print("\nReading Delta Silver table...")

    delta_df = (
        spark.read
        .format("delta")
        .load(str(DELTA_SILVER_PATH))
    )

    delta_count = delta_df.count()

    print(f"Delta records read: {delta_count}")

    print("\nDelta table schema:")

    delta_df.printSchema()

    print("\nSample Delta records:")

    delta_df.show(10, truncate=False)

    return delta_df


# ---------------------------------------------------------
# 7. Inspect transaction log
# ---------------------------------------------------------

def inspect_delta_log():

    log_path = DELTA_SILVER_PATH / "_delta_log"

    print("\nInspecting Delta transaction log...")

    if not log_path.exists():
        raise FileNotFoundError(
            f"Delta transaction log not found: {log_path}"
        )

    log_files = sorted(log_path.glob("*.json"))

    for file in log_files:
        print(f"Transaction log: {file.name}")

    if not log_files:
        raise RuntimeError(
            "No JSON transaction log found."
        )

    print(f"Transaction log directory: {log_path}")


# ---------------------------------------------------------
# 8. Main execution
# ---------------------------------------------------------

def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n========== DAY 10: DELTA LAKE ==========")

        # Read Parquet
        silver_df = read_silver_data(spark)

        # Create Delta table
        write_delta_table(silver_df)

        # Read Delta table
        delta_df = read_delta_table(spark)

        # Verify record counts
        original_count = silver_df.count()
        delta_count = delta_df.count()

        print("\n========== VALIDATION ==========")

        print(f"Original Silver count: {original_count}")
        print(f"Delta Silver count   : {delta_count}")

        if original_count != delta_count:
            raise RuntimeError(
                "Record count mismatch between Parquet and Delta!"
            )

        print("\nSUCCESS: Delta record count verified.")

        # Inspect transaction log
        inspect_delta_log()

        print("\n========== DELTA IMPLEMENTATION COMPLETE ==========")

    finally:

        spark.stop()

        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
