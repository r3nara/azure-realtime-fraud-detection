
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# ---------------------------------------------------------
# 1. Configure Spark Python interpreter
# ---------------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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


# ---------------------------------------------------------
# 2. Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_DELTA_PATH = (
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
    / "delta-dml-lab"
    / "fraud-results"
)


# ---------------------------------------------------------
# 3. Create Spark session
# ---------------------------------------------------------

def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("FraudDetection-Delta-DML-Merge")
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


# ---------------------------------------------------------
# 4. Define transaction schema
# ---------------------------------------------------------

def get_transaction_schema():

    return StructType([
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


# ---------------------------------------------------------
# 5. Initialize isolated Delta laboratory
# ---------------------------------------------------------

def initialize_lab(spark):

    print("\n========== INITIALIZE DELTA LAB ==========")

    if not SOURCE_DELTA_PATH.exists():
        raise FileNotFoundError(
            f"Source Delta table not found: {SOURCE_DELTA_PATH}"
        )

    source_df = (
        spark.read
        .format("delta")
        .load(str(SOURCE_DELTA_PATH))
    )

    source_count = source_df.count()

    (
        source_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(LAB_DELTA_PATH))
    )

    print(f"Source records: {source_count}")
    print(f"Laboratory records: {source_count}")

    return source_count


# ---------------------------------------------------------
# 6. UPDATE operation
# ---------------------------------------------------------

def update_transaction(spark):

    print("\n========== EXERCISE 1: UPDATE ==========")

    delta_table = DeltaTable.forPath(
        spark,
        str(LAB_DELTA_PATH)
    )

    # Find the original transaction
    before = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
        .filter(
            F.col("transaction_id") == "TXN-APPEND-001"
        )
    )

    print("\nBefore UPDATE:")
    before.show(truncate=False)

    if before.count() != 1:
        raise RuntimeError(
            "Expected exactly one TXN-APPEND-001 record."
        )

    # Update risk score and risk level
    delta_table.update(
        condition="transaction_id = 'TXN-APPEND-001'",
        set={
            "risk_score": F.lit(85),
            "risk_level": F.lit("HIGH")
        }
    )

    after = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
        .filter(
            F.col("transaction_id") == "TXN-APPEND-001"
        )
    )

    print("\nAfter UPDATE:")
    after.show(truncate=False)

    updated = after.first()

    if updated["risk_score"] != 85:
        raise RuntimeError("UPDATE validation failed.")

    if updated["risk_level"] != "HIGH":
        raise RuntimeError("Risk level update failed.")

    print("SUCCESS: Transaction updated.")


# ---------------------------------------------------------
# 7. DELETE operation
# ---------------------------------------------------------

def delete_transaction(spark):

    print("\n========== EXERCISE 2: DELETE ==========")

    delta_table = DeltaTable.forPath(
        spark,
        str(LAB_DELTA_PATH)
    )

    # Create a temporary test transaction
    test_record = [(
        "CUST-DELETE-001",
        "LOW",
        5,
        "DELETE-TEST-001",
        [],
        datetime.now(timezone.utc)
    )]

    test_df = spark.createDataFrame(
        test_record,
        schema=get_transaction_schema()
    )

    (
        test_df.write
        .format("delta")
        .mode("append")
        .save(str(LAB_DELTA_PATH))
    )

    print("Test transaction inserted.")

    before_delete = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
        .count()
    )

    print(f"Records before DELETE: {before_delete}")

    # Delete the test transaction
    delta_table.delete(
        condition="transaction_id = 'DELETE-TEST-001'"
    )

    after_delete_df = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
    )

    after_delete = after_delete_df.count()

    remaining = after_delete_df.filter(
        F.col("transaction_id") == "DELETE-TEST-001"
    ).count()

    print(f"Records after DELETE: {after_delete}")
    print(f"Remaining test records: {remaining}")

    if remaining != 0:
        raise RuntimeError("DELETE validation failed.")

    print("SUCCESS: Test transaction deleted.")


# ---------------------------------------------------------
# 8. Create MERGE source data
# ---------------------------------------------------------

def create_merge_source(spark):

    records = [
        (
            "CUST-APPEND-002",
            "HIGH",
            70,
            "TXN-APPEND-002",
            ["F01_HIGH_VALUE"],
            datetime.now(timezone.utc)
        ),
        (
            "CUST-MERGE-NEW",
            "MEDIUM",
            40,
            "TXN-MERGE-NEW-001",
            ["F02_HIGH_VELOCITY"],
            datetime.now(timezone.utc)
        )
    ]

    return spark.createDataFrame(
        records,
        schema=get_transaction_schema()
    )


# ---------------------------------------------------------
# 9. MERGE operation
# ---------------------------------------------------------

def merge_transactions(spark):

    print("\n========== EXERCISE 3: MERGE ==========")

    delta_table = DeltaTable.forPath(
        spark,
        str(LAB_DELTA_PATH)
    )

    source_df = create_merge_source(spark)

    print("\nIncoming MERGE records:")

    source_df.show(truncate=False)

    # Match using transaction_id
    (
        delta_table.alias("target")
        .merge(
            source_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("\nMERGE operation completed.")

    result_df = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
    )

    print("\nUpdated existing transaction:")

    result_df.filter(
        F.col("transaction_id") == "TXN-APPEND-002"
    ).show(truncate=False)

    print("\nNewly inserted transaction:")

    result_df.filter(
        F.col("transaction_id") == "TXN-MERGE-NEW-001"
    ).show(truncate=False)

    updated_record = result_df.filter(
        F.col("transaction_id") == "TXN-APPEND-002"
    ).first()

    inserted_record = result_df.filter(
        F.col("transaction_id") == "TXN-MERGE-NEW-001"
    ).first()

    if updated_record is None:
        raise RuntimeError("MERGE update record missing.")

    if updated_record["risk_score"] != 70:
        raise RuntimeError("MERGE update validation failed.")

    if updated_record["risk_level"] != "HIGH":
        raise RuntimeError("MERGE risk level validation failed.")

    if inserted_record is None:
        raise RuntimeError("MERGE insert validation failed.")

    print("SUCCESS: MERGE update and insert verified.")

    return result_df


# ---------------------------------------------------------
# 10. Verify final table and transaction history
# ---------------------------------------------------------

def verify_results(spark, initial_count):

    print("\n========== FINAL VALIDATION ==========")

    delta_table = DeltaTable.forPath(
        spark,
        str(LAB_DELTA_PATH)
    )

    final_df = (
        spark.read
        .format("delta")
        .load(str(LAB_DELTA_PATH))
    )

    final_count = final_df.count()

    print(f"Initial records: {initial_count}")
    print(f"Final records: {final_count}")

    print("\nDelta transaction history:")

    delta_table.history().select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    ).show(truncate=False)

    if final_count != initial_count + 1:
        raise RuntimeError(
            "Final record count verification failed."
        )

    print("\nSUCCESS: UPDATE, DELETE and MERGE verified.")


# ---------------------------------------------------------
# 11. Main execution
# ---------------------------------------------------------

def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n========== DELTA DML LAB ==========")

        initial_count = initialize_lab(spark)

        update_transaction(spark)

        delete_transaction(spark)

        merge_transactions(spark)

        verify_results(spark, initial_count)

        print("\n========== DELTA DML LAB COMPLETED ==========")

    finally:

        spark.stop()

        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
