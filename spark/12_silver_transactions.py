
from pathlib import Path
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# ---------------------------------------------------------
# PATHS
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_DELTA_PATH = (
    PROJECT_ROOT / "data" / "output" / "bronze-delta" / "transactions"
)

SILVER_DELTA_PATH = (
    PROJECT_ROOT / "data" / "output" / "silver" / "transactions"
)

QUARANTINE_DELTA_PATH = (
    PROJECT_ROOT
    / "data" / "output"
    / "quarantine" / "silver" / "transactions"
)


# ---------------------------------------------------------
# BUSINESS RULES
# Keep these aligned with transaction_schema.py
# ---------------------------------------------------------

VALID_TRANSACTION_TYPES = ["ONLINE", "POS", "ATM"]

VALID_PAYMENT_METHODS = [
    "CREDIT_CARD",
    "DEBIT_CARD",
    "UPI",
]

VALID_STATUSES = ["SUCCESS", "FAILED"]


# ---------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------

def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("Bronze-To-Silver-Transactions")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


# ---------------------------------------------------------
# READ BRONZE
# ---------------------------------------------------------

def read_bronze(spark):

    path = str(BRONZE_DELTA_PATH)

    if not DeltaTable.isDeltaTable(spark, path):
        raise RuntimeError(
            f"Bronze Delta table does not exist: {path}"
        )

    print("\nReading Bronze Delta transactions...")

    df = spark.read.format("delta").load(path)

    total = df.count()

    print(f"Bronze records read: {total}")

    return df


# ---------------------------------------------------------
# STANDARDISE TRANSACTION FIELDS
# ---------------------------------------------------------

def standardise_transactions(df):

    print("\nStandardising transaction fields...")

    string_columns = [
        "transaction_id",
        "customer_id",
        "currency",
        "merchant",
        "transaction_type",
        "location",
        "payment_method",
        "device_id",
        "status",
        "source",
        "namespace",
        "event_hub",
        "consumer_group",
        "partition_id",
        "offset",
    ]

    for column in string_columns:
        df = df.withColumn(
            column,
            F.trim(F.col(column))
        )

    df = (
        df
        .withColumn("currency", F.upper("currency"))
        .withColumn("transaction_type", F.upper("transaction_type"))
        .withColumn("payment_method", F.upper("payment_method"))
        .withColumn("status", F.upper("status"))
        .withColumn(
            "merchant",
            F.regexp_replace(F.col("merchant"), r"\s+", " ")
        )
        .withColumn(
            "location",
            F.regexp_replace(F.col("location"), r"\s+", " ")
        )
    )

    return df


# ---------------------------------------------------------
# DATA QUALITY VALIDATION
# ---------------------------------------------------------

def validate_transactions(df):

    print("\nApplying Silver data quality rules...")

    validation_conditions = [

        F.when(
            F.col("transaction_id").isNull()
            | (F.col("transaction_id") == ""),
            F.lit("S01_MISSING_TRANSACTION_ID")
        ),

        F.when(
            F.col("customer_id").isNull()
            | (F.col("customer_id") == ""),
            F.lit("S02_MISSING_CUSTOMER_ID")
        ),

        F.when(
            F.col("amount").isNull()
            | (F.col("amount") <= 0),
            F.lit("S03_INVALID_AMOUNT")
        ),

        F.when(
            F.col("currency").isNull()
            | (~F.col("currency").rlike("^[A-Z]{3}$")),
            F.lit("S04_INVALID_CURRENCY")
        ),

        F.when(
            F.col("merchant").isNull()
            | (F.col("merchant") == ""),
            F.lit("S05_MISSING_MERCHANT")
        ),

        F.when(
            F.col("transaction_type").isNull()
            | (~F.col("transaction_type").isin(
                VALID_TRANSACTION_TYPES
            )),
            F.lit("S06_INVALID_TRANSACTION_TYPE")
        ),

        F.when(
            F.col("transaction_timestamp").isNull(),
            F.lit("S07_INVALID_TRANSACTION_TIMESTAMP")
        ),

        F.when(
            F.col("payment_method").isNull()
            | (~F.col("payment_method").isin(
                VALID_PAYMENT_METHODS
            )),
            F.lit("S08_INVALID_PAYMENT_METHOD")
        ),

        F.when(
            F.col("status").isNull()
            | (~F.col("status").isin(VALID_STATUSES)),
            F.lit("S09_INVALID_STATUS")
        ),

        F.when(
            F.col("location").isNull()
            | (F.col("location") == ""),
            F.lit("S10_MISSING_LOCATION")
        ),

        F.when(
            F.col("device_id").isNull()
            | (F.col("device_id") == ""),
            F.lit("S11_MISSING_DEVICE_ID")
        ),

        F.when(
            F.col("partition_id").isNull()
            | F.col("sequence_number").isNull(),
            F.lit("S12_MISSING_EVENT_HUB_METADATA")
        ),

        F.when(
            F.col("raw_event").isNull()
            | (F.col("raw_event") == ""),
            F.lit("S13_MISSING_RAW_EVENT")
        ),
    ]

    validated_df = df.withColumn(
        "validation_errors",
        F.filter(
            F.array(*validation_conditions),
            lambda reason: reason.isNotNull()
        )
    )

    valid_df = validated_df.filter(
        F.size("validation_errors") == 0
    )

    invalid_df = validated_df.filter(
        F.size("validation_errors") > 0
    )

    return valid_df, invalid_df


# ---------------------------------------------------------
# DUPLICATE TRANSACTION DETECTION
# ---------------------------------------------------------

def handle_duplicates(valid_df):

    print("\nChecking duplicate transaction IDs...")

    # A transaction ID is the business-level identity.
    # Retain the latest archived copy.
    # Event Hubs metadata provides deterministic tie-breakers.

    window_spec = (
        Window
        .partitionBy("transaction_id")
        .orderBy(
            F.col("archived_at").desc_nulls_last(),
            F.col("enqueued_time").desc_nulls_last(),
            F.col("partition_id").desc_nulls_last(),
            F.col("sequence_number").desc_nulls_last(),
        )
    )

    ranked_df = valid_df.withColumn(
        "_duplicate_rank",
        F.row_number().over(window_spec)
    )

    winners_df = (
        ranked_df
        .filter(F.col("_duplicate_rank") == 1)
        .drop("_duplicate_rank")
    )

    duplicate_df = (
        ranked_df
        .filter(F.col("_duplicate_rank") > 1)
        .drop("_duplicate_rank")
        .withColumn(
            "validation_errors",
            F.array(
                F.lit("S14_DUPLICATE_TRANSACTION_ID")
            )
        )
    )

    return winners_df, duplicate_df


# ---------------------------------------------------------
# QUARANTINE DELTA MERGE
# ---------------------------------------------------------

def write_quarantine(quarantine_df, spark):

    count = quarantine_df.count()

    print(f"\nRecords to quarantine: {count}")

    if count == 0:
        print("No records to quarantine.")
        return

    quarantine_df = (
        quarantine_df
        .withColumn(
            "quarantine_record_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("source"), F.lit("")),
                    F.coalesce(F.col("namespace"), F.lit("")),
                    F.coalesce(F.col("event_hub"), F.lit("")),
                    F.coalesce(F.col("partition_id"), F.lit("")),
                    F.coalesce(
                        F.col("sequence_number").cast("string"),
                        F.lit("")
                    ),
                    F.coalesce(F.col("transaction_id"), F.lit("")),
                    F.coalesce(F.col("raw_event"), F.lit("")),
                ),
                256
            )
        )
        .withColumn(
            "quarantine_layer",
            F.lit("SILVER")
        )
        .withColumn(
            "quarantined_at",
            F.current_timestamp()
        )
    )

    path = str(QUARANTINE_DELTA_PATH)

    if not DeltaTable.isDeltaTable(spark, path):

        (
            quarantine_df.write
            .format("delta")
            .mode("errorifexists")
            .save(path)
        )

        print("Created Silver quarantine Delta table.")

    else:

        delta_table = DeltaTable.forPath(spark, path)

        (
            delta_table.alias("target")
            .merge(
                quarantine_df.alias("source"),
                """
                target.quarantine_record_id =
                source.quarantine_record_id
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Merged records into Silver quarantine table.")

    final_count = (
        spark.read.format("delta")
        .load(path)
        .count()
    )

    print(f"Total quarantine records: {final_count}")


# ---------------------------------------------------------
# SILVER DELTA MERGE
# ---------------------------------------------------------

def merge_into_silver(valid_df, spark):

    source_df = (
        valid_df
        .withColumn(
            "silver_processed_at",
            F.current_timestamp()
        )
    )

    source_count = source_df.count()

    print(f"\nUnique valid records for Silver: {source_count}")

    path = str(SILVER_DELTA_PATH)

    if not DeltaTable.isDeltaTable(spark, path):

        (
            source_df.write
            .format("delta")
            .mode("errorifexists")
            .save(path)
        )

        print("Created Silver transactions Delta table.")

    else:

        delta_table = DeltaTable.forPath(spark, path)

        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                "target.transaction_id = source.transaction_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Merged transactions into Silver Delta table.")

    silver_df = (
        spark.read.format("delta")
        .load(path)
    )

    final_count = silver_df.count()

    print(f"Total Silver records: {final_count}")

    return silver_df


# ---------------------------------------------------------
# RECONCILIATION AND SUMMARY
# ---------------------------------------------------------

def print_summary(
    bronze_count,
    invalid_count,
    duplicate_count,
    silver_source_count,
    silver_final_count,
    quarantine_final_count,
):

    print("\n" + "=" * 65)
    print("SILVER PIPELINE SUMMARY")
    print("=" * 65)

    print(f"Bronze records read          : {bronze_count}")
    print(f"Invalid records              : {invalid_count}")
    print(f"Duplicate records            : {duplicate_count}")
    print(f"Unique valid source records  : {silver_source_count}")
    print(f"Final Silver records         : {silver_final_count}")
    print(f"Total Quarantine records     : {quarantine_final_count}")

    print("=" * 65)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():

    spark = create_spark_session()

    try:

        # Step 1: Read Bronze
        bronze_df = read_bronze(spark)
        bronze_count = bronze_df.count()

        # Step 2: Standardise
        standardised_df = standardise_transactions(
            bronze_df
        )

        # Step 3: Validate
        valid_df, invalid_df = validate_transactions(
            standardised_df
        )

        invalid_count = invalid_df.count()

        # Step 4: Deduplicate valid records
        unique_valid_df, duplicate_df = handle_duplicates(
            valid_df
        )

        duplicate_count = duplicate_df.count()

        silver_source_count = unique_valid_df.count()

        # Step 5: Combine all rejected records
        quarantine_df = invalid_df.unionByName(
            duplicate_df
        )

        # Step 6: Persist quarantine
        write_quarantine(quarantine_df, spark)

        # Step 7: Merge valid records into Silver
        silver_df = merge_into_silver(
            unique_valid_df,
            spark
        )

        silver_final_count = silver_df.count()

        quarantine_path = str(QUARANTINE_DELTA_PATH)

        if DeltaTable.isDeltaTable(spark, quarantine_path):
            quarantine_final_count = (
                spark.read.format("delta")
                .load(quarantine_path)
                .count()
            )
        else:
            quarantine_final_count = 0

        # Step 8: Summary
        print_summary(
            bronze_count=bronze_count,
            invalid_count=invalid_count,
            duplicate_count=duplicate_count,
            silver_source_count=silver_source_count,
            silver_final_count=silver_final_count,
            quarantine_final_count=quarantine_final_count,
        )

        print("\nSilver transaction sample:")
        silver_df.orderBy("transaction_id").show(
            10,
            truncate=False
        )

        print("\nSilver pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
