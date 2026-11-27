
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
    TimestampType
)

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
        .appName("FraudDetection-Day10-DeltaAppend")
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


def create_new_transactions(spark):

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("risk_level", StringType(), True),
        StructField("risk_score", LongType(), True),
        StructField("transaction_id", StringType(), True),
        StructField(
            "triggered_rules",
            ArrayType(StringType()),
            True
        ),
        StructField("processed_at", TimestampType(), True)
    ])

    now = datetime.now(timezone.utc)

    records = []

    for i in range(1, 51):

        records.append((
            f"CUST-APPEND-{i:03d}",
            "LOW",
            10,
            f"TXN-APPEND-{i:03d}",
            [],
            now
        ))

    return spark.createDataFrame(records, schema=schema)


def main():

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n========== DELTA APPEND EXERCISE ==========")

        delta_table = DeltaTable.forPath(
            spark,
            str(DELTA_PATH)
        )

        before_count = (
            spark.read
            .format("delta")
            .load(str(DELTA_PATH))
            .count()
        )

        print(f"Records before append: {before_count}")

        new_df = create_new_transactions(spark)

        print(f"New records to append: {new_df.count()}")

        (
            new_df.write
            .format("delta")
            .mode("append")
            .save(str(DELTA_PATH))
        )

        after_count = (
            spark.read
            .format("delta")
            .load(str(DELTA_PATH))
            .count()
        )

        print(f"Records after append: {after_count}")

        print("\nDelta transaction history:")

        delta_table.history().select(
            "version",
            "timestamp",
            "operation",
            "operationMetrics"
        ).show(truncate=False)

        if after_count != before_count + 50:
            raise RuntimeError("Append count verification failed!")

        print("\nSUCCESS: 50 records appended.")

    finally:

        spark.stop()
        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
