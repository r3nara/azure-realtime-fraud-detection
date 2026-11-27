
import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    when,
    array,
    filter as array_filter,
    size,
    to_timestamp,
    trim,
    lit,
    count,
    row_number,
    desc
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName("FraudDetection-DataQuality-Deduplication")
        .master("local[2]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_test_records(spark):

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("processed_at", StringType(), True),
        StructField("risk_level", StringType(), True),
        StructField("risk_score", LongType(), True),
        StructField("transaction_id", StringType(), True),
        StructField(
            "triggered_rules",
            ArrayType(StringType()),
            True
        )
    ])

    test_data = [

        # One valid record
        (
            "CUST-TEST-001",
            "2026-11-27T10:00:00+00:00",
            "LOW",
            15,
            "TEST-VALID-001",
            ["F04_REPEATED_FAILURES"]
        ),

        # Missing customer ID
        (
            None,
            "2026-11-27T10:00:00+00:00",
            "LOW",
            10,
            "TEST-MISSING-CUSTOMER",
            []
        ),

        # Invalid risk score
        (
            "CUST-TEST-003",
            "2026-11-27T10:00:00+00:00",
            "HIGH",
            120,
            "TEST-INVALID-SCORE",
            []
        ),

        # Invalid timestamp
        (
            "CUST-TEST-004",
            "not-a-valid-date",
            "MEDIUM",
            35,
            "TEST-INVALID-TIMESTAMP",
            []
        ),

        # Invalid risk level
        (
            "CUST-TEST-005",
            "2026-11-27T10:00:00+00:00",
            "UNKNOWN",
            20,
            "TEST-INVALID-LEVEL",
            []
        ),

        # Missing triggered rules
        (
            "CUST-TEST-006",
            "2026-11-27T10:00:00+00:00",
            "LOW",
            10,
            "TEST-MISSING-RULES",
            None
        ),

        # Older version of a duplicate transaction
        (
            "CUST-TEST-007",
            "2026-11-27T09:00:00+00:00",
            "LOW",
            20,
            "TEST-DUPLICATE-001",
            []
        ),

        # Latest version of the same transaction
        (
            "CUST-TEST-007",
            "2026-11-27T11:00:00+00:00",
            "MEDIUM",
            45,
            "TEST-DUPLICATE-001",
            ["F02_HIGH_VELOCITY"]
        )
    ]

    return spark.createDataFrame(test_data, schema)


def validate_records(df):

    # Normalize important string fields
    df = (
        df.withColumn(
            "transaction_id",
            trim(col("transaction_id"))
        )
        .withColumn(
            "customer_id",
            trim(col("customer_id"))
        )
        .withColumn(
            "risk_level",
            trim(col("risk_level"))
        )
    )

    # Convert timestamp string to Spark timestamp
    df = df.withColumn(
        "processed_at_timestamp",
        to_timestamp(col("processed_at"))
    )

    # Apply all validation rules
    df = df.withColumn(
        "validation_errors",
        array(
            when(
                col("transaction_id").isNull() |
                (col("transaction_id") == ""),
                lit("DQ01_MISSING_TRANSACTION_ID")
            ),

            when(
                col("customer_id").isNull() |
                (col("customer_id") == ""),
                lit("DQ02_MISSING_CUSTOMER_ID")
            ),

            when(
                col("risk_score").isNull() |
                (col("risk_score") < 0) |
                (col("risk_score") > 100),
                lit("DQ03_INVALID_RISK_SCORE")
            ),

            when(
                col("risk_level").isNull() |
                (~col("risk_level").isin("LOW", "MEDIUM", "HIGH")),
                lit("DQ04_INVALID_RISK_LEVEL")
            ),

            when(
                col("processed_at_timestamp").isNull(),
                lit("DQ05_INVALID_TIMESTAMP")
            ),

            when(
                col("triggered_rules").isNull(),
                lit("DQ06_MISSING_TRIGGERED_RULES")
            )
        )
    )

    # Keep only actual validation errors
    df = df.withColumn(
        "validation_errors",
        array_filter(
            col("validation_errors"),
            lambda error: error.isNotNull()
        )
    )

    return df


def deduplicate_records(valid_candidates):

    # Rank records within each transaction ID.
    # Latest processed_at receives row number 1.
    window_spec = (
        Window
        .partitionBy("transaction_id")
        .orderBy(desc("processed_at_timestamp"))
    )

    ranked_df = valid_candidates.withColumn(
        "duplicate_rank",
        row_number().over(window_spec)
    )

    # Keep the latest record
    silver_candidates = (
        ranked_df
        .filter(col("duplicate_rank") == 1)
        .drop("duplicate_rank", "validation_errors", "processed_at")
        .withColumnRenamed(
            "processed_at_timestamp",
            "processed_at"
        )
    )

    # Older records are quarantined
    duplicate_quarantine = (
        ranked_df
        .filter(col("duplicate_rank") > 1)
        .withColumn(
            "validation_errors",
            array(lit("DQ07_OLDER_DUPLICATE"))
        )
        .drop("duplicate_rank")
    )

    return silver_candidates, duplicate_quarantine


def main():

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:

        project_root = Path(__file__).resolve().parent.parent
        data_folder = project_root / "data" / "fraud-results"

        json_files = sorted(data_folder.glob("*.json"))

        if not json_files:
            raise FileNotFoundError(
                f"No JSON files found in {data_folder}"
            )

        file_paths = [
            str(path.resolve())
            for path in json_files
        ]

        print("\n========== READING SOURCE DATA ==========\n")

        source_df = (
            spark.read
            .option("multiLine", True)
            .option("inferSchema", True)
            .json(file_paths)
        )

        print("Original records:", source_df.count())

        test_df = create_test_records(spark)

        combined_df = source_df.unionByName(test_df)

        print("Combined records:", combined_df.count())

        print("\n========== VALIDATING RECORDS ==========\n")

        validated_df = validate_records(combined_df)

        # First quarantine: records failing data-quality checks
        invalid_quarantine = validated_df.filter(
            size(col("validation_errors")) > 0
        )

        # Only valid records proceed to deduplication
        valid_candidates = validated_df.filter(
            size(col("validation_errors")) == 0
        )

        print(
            "Records failing validation:",
            invalid_quarantine.count()
        )

        print("\n========== DEDUPLICATING RECORDS ==========\n")

        silver_df, duplicate_quarantine = deduplicate_records(
            valid_candidates
        )

        # Combine all rejected records
        quarantine_df = invalid_quarantine.unionByName(
            duplicate_quarantine
        )

        print("\n========== LATEST DUPLICATE RECORD ==========\n")

        silver_df.filter(
            col("transaction_id") == "TEST-DUPLICATE-001"
        ).show(truncate=False)

        print("\n========== OLDER DUPLICATE QUARANTINED ==========\n")

        quarantine_df.filter(
            col("transaction_id") == "TEST-DUPLICATE-001"
        ).select(
            "transaction_id",
            "processed_at",
            "risk_score",
            "validation_errors"
        ).show(truncate=False)

        print("\n========== FINAL PIPELINE SUMMARY ==========\n")

        total_input = combined_df.count()
        total_valid = silver_df.count()
        total_quarantined = quarantine_df.count()

        print("Total input records:", total_input)
        print("Valid Silver records:", total_valid)
        print("Quarantined records:", total_quarantined)

        print("\n========== QUARANTINE REASONS ==========\n")

        quarantine_df.selectExpr(
            "explode(validation_errors) AS rejection_reason"
        ).groupBy("rejection_reason") \
            .agg(count("*").alias("rejected_count")) \
            .orderBy("rejection_reason") \
            .show(truncate=False)

        print("\n========== FINAL SILVER SCHEMA ==========\n")
        silver_df.printSchema()

    finally:
        spark.stop()
        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
