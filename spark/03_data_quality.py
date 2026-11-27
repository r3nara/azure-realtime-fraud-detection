
import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, to_timestamp


def main():

    project_root = Path(__file__).resolve().parent.parent
    data_folder = project_root / "data" / "fraud-results"

    json_files = sorted(data_folder.glob("*.json"))

    if not json_files:
        raise FileNotFoundError("No fraud-result JSON files found")

    spark = (
        SparkSession.builder
        .appName("FraudDetection-DataQuality")
        .master("local[2]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        file_paths = [str(path.resolve()) for path in json_files]

        df = (
            spark.read
            .option("multiLine", True)
            .option("inferSchema", True)
            .json(file_paths)
        )

        print("\n========== ORIGINAL DATA ==========\n")
        df.show(5, truncate=False)

        print("\n========== DATA QUALITY SUMMARY ==========\n")

        total_records = df.count()

        quality_summary = df.select(
            count("*").alias("total_records"),

            count(
                when(
                    col("transaction_id").isNull() |
                    (col("transaction_id") == ""),
                    True
                )
            ).alias("missing_transaction_id"),

            count(
                when(
                    col("customer_id").isNull() |
                    (col("customer_id") == ""),
                    True
                )
            ).alias("missing_customer_id"),

            count(
                when(
                    col("risk_score").isNull() |
                    (col("risk_score") < 0) |
                    (col("risk_score") > 100),
                    True
                )
            ).alias("invalid_risk_score"),

            count(
                when(
                    ~col("risk_level").isin("LOW", "MEDIUM", "HIGH") |
                    col("risk_level").isNull(),
                    True
                )
            ).alias("invalid_risk_level"),

            count(
                when(
                    to_timestamp("processed_at").isNull(),
                    True
                )
            ).alias("invalid_timestamp")
        )

        quality_summary.show(truncate=False)

        print("\n========== DUPLICATE TRANSACTION IDS ==========\n")

        duplicates = (
            df.groupBy("transaction_id")
            .agg(count("*").alias("record_count"))
            .filter(col("record_count") > 1)
        )

        duplicates.show(truncate=False)

        print("\nTotal duplicate transaction IDs:",
              duplicates.count())

        print("\n========== RECORD COUNT ==========\n")
        print("Total records:", total_records)

    finally:
        spark.stop()
        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
