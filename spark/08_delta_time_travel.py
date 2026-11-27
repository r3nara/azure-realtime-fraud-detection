
import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DELTA_PATH = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "silver-delta"
    / "fraud-results"
)


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("FraudDetection-Day10-TimeTravel")
        .master("local[2]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
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


def read_version(spark, version):

    return (
        spark.read
        .format("delta")
        .option("versionAsOf", version)
        .load(str(DELTA_PATH))
    )


def main():

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n========== DELTA TIME TRAVEL ==========")

        if not DeltaTable.isDeltaTable(spark, str(DELTA_PATH)):
            raise RuntimeError("The specified path is not a Delta table.")

        delta_table = DeltaTable.forPath(
            spark,
            str(DELTA_PATH)
        )

        print("\nComplete transaction history:")

        delta_table.history().select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        # Read version 0
        version_0_df = read_version(spark, 0)

        # Read version 1
        version_1_df = read_version(spark, 1)

        version_0_count = version_0_df.count()
        version_1_count = version_1_df.count()

        print("\n========== VERSION COMPARISON ==========")

        print(f"Version 0 records: {version_0_count}")
        print(f"Version 1 records: {version_1_count}")

        print("\nRecords added between versions:")

        print(
            f"Difference: {version_1_count - version_0_count}"
        )

        print("\nSample records from version 0:")

        version_0_df.select(
            "transaction_id",
            "customer_id",
            "risk_score"
        ).show(5, truncate=False)

        print("\nSample appended records from version 1:")

        (
            version_1_df
            .filter(
                version_1_df.transaction_id.startswith("TXN-APPEND-")
            )
            .select(
                "transaction_id",
                "customer_id",
                "risk_score"
            )
            .show(5, truncate=False)
        )

        if version_0_count != 117:
            raise RuntimeError(
                f"Expected 117 records in version 0, got {version_0_count}"
            )

        if version_1_count != 167:
            raise RuntimeError(
                f"Expected 167 records in version 1, got {version_1_count}"
            )

        print("\nSUCCESS: Delta time travel verified.")

    finally:

        spark.stop()
        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
