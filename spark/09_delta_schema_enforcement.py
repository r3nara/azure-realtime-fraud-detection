
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
    TimestampType,
    DoubleType
)

from pyspark.errors import AnalysisException
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


PROJECT_ROOT = Path(__file__).resolve().parent.parent

MAIN_DELTA_PATH = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "silver-delta"
    / "fraud-results"
)

LAB_DELTA_PATH = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "delta-schema-lab"
    / "fraud-results"
)


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("FraudDetection-Delta-Schema-Lab")
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


def create_incoming_record(spark):

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("risk_score", LongType(), False),
        StructField("risk_level", StringType(), False),
        StructField(
            "triggered_rules",
            ArrayType(StringType()),
            False
        ),
        StructField("processed_at", TimestampType(), False),
        StructField("fraud_probability", DoubleType(), False)
    ])

    record = [(
        "TXN-SCHEMA-001",
        "CUST-SCHEMA-001",
        75,
        "HIGH",
        ["F01_HIGH_VALUE"],
        datetime.now(timezone.utc),
        0.87
    )]

    return spark.createDataFrame(record, schema)


def create_schema_lab(spark):

    print("\nCreating isolated schema laboratory...")

    source_df = (
        spark.read
        .format("delta")
        .load(str(MAIN_DELTA_PATH))
    )

    source_count = source_df.count()

    (
        source_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(LAB_DELTA_PATH))
    )

    print(f"Schema lab initialized with {source_count} records.")

    return source_count


def test_schema_enforcement(spark):

    print("\n========== TEST 1: SCHEMA ENFORCEMENT ==========")

    incoming_df = create_incoming_record(spark)

    try:

        (
            incoming_df.write
            .format("delta")
            .mode("append")
            .save(str(LAB_DELTA_PATH))
        )

        raise RuntimeError(
            "Unexpected result: Delta accepted an incompatible schema."
        )

    except AnalysisException as error:

        print("\nEXPECTED RESULT: Write rejected.")

        print("Delta Lake detected a schema mismatch.")

        print("Error details:")
        print(str(error)[:1200])

    current_count = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
        .count()
    )

    print(f"\nRecords after rejected write: {current_count}")

    return current_count


def test_schema_evolution(spark):

    print("\n========== TEST 2: SCHEMA EVOLUTION ==========")

    incoming_df = create_incoming_record(spark)

    (
        incoming_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .save(str(LAB_DELTA_PATH))
    )

    print("\nNew record successfully appended with schema evolution.")

    result_df = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
    )

    final_count = result_df.count()

    print(f"\nRecords after schema evolution: {final_count}")

    print("\nUpdated Delta schema:")

    result_df.printSchema()

    print("\nNew fraud probability column:")

    result_df.select(
        "transaction_id",
        "customer_id",
        "fraud_probability"
    ).filter(
        result_df.transaction_id == "TXN-SCHEMA-001"
    ).show(truncate=False)

    null_count = result_df.filter(
        result_df.fraud_probability.isNull()
    ).count()

    print(f"Existing records with NULL fraud_probability: {null_count}")

    print("\nDelta transaction history:")

    DeltaTable.forPath(
        spark,
        str(LAB_DELTA_PATH)
    ).history().select(
        "version",
        "operation",
        "operationParameters"
    ).show(truncate=False)

    return final_count, null_count


def main():

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:

        print("\n========== DELTA SCHEMA ENFORCEMENT LAB ==========")

        if not MAIN_DELTA_PATH.exists():
            raise FileNotFoundError(
                f"Main Delta table not found: {MAIN_DELTA_PATH}"
            )

        initial_count = create_schema_lab(spark)

        # Expected: 167
        count_after_rejection = test_schema_enforcement(spark)

        if count_after_rejection != initial_count:
            raise RuntimeError(
                "Record count changed after rejected write."
            )

        # Expected: 168 records, 167 null values
        final_count, null_count = test_schema_evolution(spark)

        if final_count != initial_count + 1:
            raise RuntimeError(
                "Schema evolution record count verification failed."
            )

        if null_count != initial_count:
            raise RuntimeError(
                "Unexpected NULL count in fraud_probability."
            )

        print("\n========== FINAL VALIDATION ==========")

        print("Schema enforcement: PASSED")
        print("Schema evolution: PASSED")
        print(f"Initial records: {initial_count}")
        print(f"Final records: {final_count}")
        print(f"Existing rows with NULL new column: {null_count}")

        print("\nSUCCESS: Both schema tests passed.")

    finally:

        spark.stop()

        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
