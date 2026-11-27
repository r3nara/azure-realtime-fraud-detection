
import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_INPUT = PROJECT_ROOT / "data" / "bronze-raw"

BRONZE_DELTA_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "bronze-delta"
    / "transactions"
)


# ---------------------------------------------------------
# 1. Create Spark session
# ---------------------------------------------------------

def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("FraudDetection-Incremental-Bronze")
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
# 2. Define original transaction schema
# ---------------------------------------------------------

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("status", StringType(), True)
])


# ---------------------------------------------------------
# 3. Read archived JSON files
# ---------------------------------------------------------

def read_bronze_archive(spark):

    print("\nReading Bronze archive files...")

    files = sorted(BRONZE_INPUT.glob("*.json"))

    if not files:
        raise FileNotFoundError(
            f"No JSON files found in {BRONZE_INPUT}"
        )

    print(f"Files discovered: {len(files)}")

    raw_df = (
        spark.read
        .option("multiLine", True)
        .json([str(file) for file in files])
    )

    return raw_df


# ---------------------------------------------------------
# 4. Parse original transaction payload
# ---------------------------------------------------------

def parse_transaction_payload(raw_df):

    print("\nParsing original transaction JSON...")

    parsed_df = raw_df.withColumn(
        "transaction",
        F.from_json(
            F.col("raw_event"),
            TRANSACTION_SCHEMA
        )
    )

    parse_failures = parsed_df.filter(
        F.col("transaction").isNull()
    ).count()

    print(f"Unparseable transaction payloads: {parse_failures}")

    result_df = parsed_df.select(
        F.col("transaction.transaction_id").alias(
            "transaction_id"
        ),
        F.col("transaction.customer_id").alias(
            "customer_id"
        ),
        F.col("transaction.amount").alias("amount"),
        F.col("transaction.currency").alias("currency"),
        F.col("transaction.merchant").alias("merchant"),
        F.col("transaction.transaction_type").alias(
            "transaction_type"
        ),
        F.col("transaction.location").alias("location"),
        F.to_timestamp(
            F.col("transaction.timestamp")
        ).alias("transaction_timestamp"),
        F.col("transaction.payment_method").alias(
            "payment_method"
        ),
        F.col("transaction.device_id").alias("device_id"),
        F.col("transaction.status").alias("status"),

        F.col("source"),
        F.col("namespace"),
        F.col("event_hub"),
        F.col("consumer_group"),
        F.col("partition_id"),
        F.col("sequence_number"),
        F.col("offset"),

        F.to_timestamp(
            F.col("enqueued_time")
        ).alias("enqueued_time"),

        F.to_timestamp(
            F.col("archived_at")
        ).alias("archived_at"),

        F.col("raw_event")
    )

    return result_df


# ---------------------------------------------------------
# 5. Validate Bronze records
# ---------------------------------------------------------

def validate_bronze(df):

    print("\nValidating Bronze records...")

    validation_conditions = F.array(
        F.when(
            F.col("transaction_id").isNull() |
            (F.trim(F.col("transaction_id")) == ""),
            F.lit("DQ01_MISSING_TRANSACTION_ID")
        ),

        F.when(
            F.col("customer_id").isNull() |
            (F.trim(F.col("customer_id")) == ""),
            F.lit("DQ02_MISSING_CUSTOMER_ID")
        ),

        F.when(
            F.col("amount").isNull() |
            (F.col("amount") < 0),
            F.lit("DQ03_INVALID_AMOUNT")
        ),

        F.when(
            F.col("partition_id").isNull() |
            F.col("sequence_number").isNull(),
            F.lit("DQ04_MISSING_EVENT_METADATA")
        ),

        F.when(
            F.col("transaction_timestamp").isNull(),
            F.lit("DQ05_INVALID_TRANSACTION_TIMESTAMP")
        ),

        F.when(
            F.col("raw_event").isNull(),
            F.lit("DQ06_MISSING_RAW_EVENT")
        )
    )

    validation_df = df.withColumn(
        "validation_errors",
        F.filter(
            validation_conditions,
            lambda reason: reason.isNotNull()
        )
    )

    valid_df = validation_df.filter(
        F.size("validation_errors") == 0
    )

    quarantine_df = validation_df.filter(
        F.size("validation_errors") > 0
    )

    return valid_df, quarantine_df


# ---------------------------------------------------------
# 6. Incremental Delta ingestion using MERGE
# ---------------------------------------------------------

def merge_into_bronze_delta(valid_df):

    print("\n========== INCREMENTAL DELTA MERGE ==========")

    # Remove validation-only column.
    source_df = valid_df.drop("validation_errors")

    # One source row per Event Hubs event identity.
    source_df = source_df.dropDuplicates([
        "partition_id",
        "sequence_number"
    ])

    source_count = source_df.count()

    print(f"Unique source events: {source_count}")

    if source_count == 0:
        print("No valid source records to merge.")
        return

    # -----------------------------------------------------
    # First execution: create the Delta table.
    # Later executions: merge new events.
    # -----------------------------------------------------

    if not DeltaTable.isDeltaTable(
        sparkSession=source_df.sparkSession,
        identifier=str(BRONZE_DELTA_OUTPUT)
    ):

        print("Delta table does not exist. Creating initial table.")

        (
            source_df.write
            .format("delta")
            .mode("errorifexists")
            .save(str(BRONZE_DELTA_OUTPUT))
        )

        print("Initial Bronze Delta table created.")

    else:

        delta_table = DeltaTable.forPath(
            source_df.sparkSession,
            str(BRONZE_DELTA_OUTPUT)
        )

        before_count = delta_table.toDF().count()

        print(f"Records before merge: {before_count}")

        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                """
                target.partition_id = source.partition_id
                AND target.sequence_number = source.sequence_number
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        after_count = delta_table.toDF().count()

        print(f"Records after merge: {after_count}")
        print(f"New records inserted: {after_count - before_count}")

        if after_count < before_count:
            raise RuntimeError(
                "Unexpected reduction in Bronze table record count."
            )

    print("Delta MERGE completed.")


# ---------------------------------------------------------
# 7. Main execution
# ---------------------------------------------------------

def main():

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n========== INCREMENTAL BRONZE PIPELINE ==========")

        raw_df = read_bronze_archive(spark)

        bronze_df = parse_transaction_payload(raw_df)

        valid_df, quarantine_df = validate_bronze(bronze_df)

        input_count = bronze_df.count()
        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()

        print("\n========== DATA QUALITY SUMMARY ==========")

        print(f"Input records: {input_count}")
        print(f"Valid records: {valid_count}")
        print(f"Quarantined records: {quarantine_count}")

        if input_count != valid_count + quarantine_count:
            raise RuntimeError(
                "Data quality reconciliation failed."
            )

        if valid_count > 0:
            merge_into_bronze_delta(valid_df)

        print("\n========== FINAL BRONZE VALIDATION ==========")

        if DeltaTable.isDeltaTable(
            sparkSession=spark,
            identifier=str(BRONZE_DELTA_OUTPUT)
        ):

            final_df = (
                spark.read
                .format("delta")
                .load(str(BRONZE_DELTA_OUTPUT))
            )

            final_count = final_df.count()

            print(f"Final Bronze Delta count: {final_count}")

            final_df.select(
                "transaction_id",
                "partition_id",
                "sequence_number",
                "amount"
            ).show(truncate=False)

        else:
            raise RuntimeError(
                "Bronze Delta table was not created."
            )

        print("\nSUCCESS: Incremental Bronze pipeline completed.")

    finally:

        spark.stop()

        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
