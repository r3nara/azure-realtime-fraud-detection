
import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, size, desc


def main():

    project_root = Path(__file__).resolve().parent.parent
    data_folder = project_root / "data" / "fraud-results"

    # Discover JSON files using Python, not Hadoop globbing
    json_files = sorted(data_folder.glob("*.json"))

    print(f"\nJSON files discovered: {len(json_files)}")

    if not json_files:
        raise FileNotFoundError(
            f"No JSON files found in: {data_folder}"
        )

    spark = (
        SparkSession.builder
        .appName("FraudDetection-MultiJSON")
        .master("local[2]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n========== READING FRAUD JSON FILES ==========\n")

        file_paths = [str(path.resolve()) for path in json_files]

        fraud_df = (
            spark.read
            .option("multiLine", True)
            .option("inferSchema", True)
            .json(file_paths)
        )

        print("\n========== SCHEMA ==========\n")
        fraud_df.printSchema()

        print("\n========== SAMPLE RECORDS ==========\n")
        fraud_df.show(10, truncate=False)

        print("\n========== TOTAL RECORDS ==========\n")
        print("Total fraud records:", fraud_df.count())

        print("\n========== RISK LEVEL DISTRIBUTION ==========\n")

        fraud_df.groupBy("risk_level") \
            .agg(count("*").alias("transaction_count")) \
            .orderBy(desc("transaction_count")) \
            .show()

        print("\n========== AVERAGE RISK SCORE ==========\n")

        fraud_df.agg(
            avg("risk_score").alias("average_risk_score")
        ).show()

        print("\n========== TRANSACTIONS WITH TRIGGERED RULES ==========\n")

        fraud_df.filter(
            size(col("triggered_rules")) > 0
        ).show(truncate=False)

    finally:
        spark.stop()
        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
