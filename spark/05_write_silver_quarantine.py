
import os
import sys
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# 1. Configure Python interpreter for Spark workers
# ---------------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, row_number


# ---------------------------------------------------------
# 2. Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FOLDER = PROJECT_ROOT / "data" / "fraud-results"

OUTPUT_FOLDER = PROJECT_ROOT / "data" / "output"

SILVER_OUTPUT = OUTPUT_FOLDER / "silver" / "fraud-results"

QUARANTINE_OUTPUT = (
    OUTPUT_FOLDER / "quarantine" / "data-quality" / "fraud-results"
)


# ---------------------------------------------------------
# 3. Controlled test records
# ---------------------------------------------------------

def create_test_records():

    return [
        {
            "transaction_id": "TEST-VALID-001",
            "customer_id": "CUST-TEST-001",
            "risk_score": 20,
            "risk_level": "LOW",
            "triggered_rules": [],
            "processed_at": "2026-11-27T08:00:00+00:00"
        },
        {
            "transaction_id": "TEST-MISSING-CUSTOMER",
            "customer_id": None,
            "risk_score": 10,
            "risk_level": "LOW",
            "triggered_rules": [],
            "processed_at": "2026-11-27T08:01:00+00:00"
        },
        {
            "transaction_id": "TEST-INVALID-SCORE",
            "customer_id": "CUST-TEST-002",
            "risk_score": 120,
            "risk_level": "HIGH",
            "triggered_rules": ["F01_HIGH_VALUE"],
            "processed_at": "2026-11-27T08:02:00+00:00"
        },
        {
            "transaction_id": "TEST-INVALID-TIMESTAMP",
            "customer_id": "CUST-TEST-003",
            "risk_score": 10,
            "risk_level": "LOW",
            "triggered_rules": [],
            "processed_at": "invalid-timestamp"
        },
        {
            "transaction_id": "TEST-INVALID-LEVEL",
            "customer_id": "CUST-TEST-004",
            "risk_score": 50,
            "risk_level": "UNKNOWN",
            "triggered_rules": [],
            "processed_at": "2026-11-27T08:04:00+00:00"
        },
        {
            "transaction_id": "TEST-MISSING-RULES",
            "customer_id": "CUST-TEST-005",
            "risk_score": 10,
            "risk_level": "LOW",
            "triggered_rules": None,
            "processed_at": "2026-11-27T08:05:00+00:00"
        },
        {
            "transaction_id": "TEST-DUPLICATE-001",
            "customer_id": "CUST-TEST-007",
            "risk_score": 20,
            "risk_level": "LOW",
            "triggered_rules": [],
            "processed_at": "2026-11-27T09:00:00+00:00"
        },
        {
            "transaction_id": "TEST-DUPLICATE-001",
            "customer_id": "CUST-TEST-007",
            "risk_score": 45,
            "risk_level": "MEDIUM",
            "triggered_rules": ["F02_HIGH_VELOCITY"],
            "processed_at": "2026-11-27T11:00:00+00:00"
        }
    ]


# ---------------------------------------------------------
# 4. Read source JSON files
# ---------------------------------------------------------

def read_source_data(spark):

    json_files = sorted(INPUT_FOLDER.glob("*.json"))

    if not json_files:
        raise FileNotFoundError(
            f"No JSON files found in {INPUT_FOLDER}"
        )

    file_paths = [str(file.resolve()) for file in json_files]

    print(f"\nSource JSON files found: {len(file_paths)}")

    source_df = (
        spark.read
        .option("multiLine", True)
        .option("inferSchema", True)
        .json(file_paths)
    )

    return source_df


# ---------------------------------------------------------
# 5. Validate records
# ---------------------------------------------------------

def validate_records(df):

    df = (
        df
        .withColumn(
            "transaction_id",
            F.trim(F.col("transaction_id"))
        )
        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id"))
        )
        .withColumn(
            "risk_level",
            F.upper(F.trim(F.col("risk_level")))
        )
        .withColumn(
            "processed_at_timestamp",
            F.to_timestamp(F.col("processed_at"))
        )
    )

    validation_errors = F.array(
        F.when(
            F.col("transaction_id").isNull()
            | (F.col("transaction_id") == ""),
            F.lit("DQ01_MISSING_TRANSACTION_ID")
        ),

        F.when(
            F.col("customer_id").isNull()
            | (F.col("customer_id") == ""),
            F.lit("DQ02_MISSING_CUSTOMER_ID")
        ),

        F.when(
            F.col("risk_score").isNull()
            | (F.col("risk_score") < 0)
            | (F.col("risk_score") > 100),
            F.lit("DQ03_INVALID_RISK_SCORE")
        ),

        F.when(
            F.col("risk_level").isNull()
            | (~F.col("risk_level").isin(
                "LOW", "MEDIUM", "HIGH"
            )),
            F.lit("DQ04_INVALID_RISK_LEVEL")
        ),

        F.when(
            F.col("processed_at_timestamp").isNull(),
            F.lit("DQ05_INVALID_TIMESTAMP")
        ),

        F.when(
            F.col("triggered_rules").isNull(),
            F.lit("DQ06_MISSING_TRIGGERED_RULES")
        )
    )

    df = df.withColumn(
        "validation_errors",
        F.filter(
            validation_errors,
            lambda error: error.isNotNull()
        )
    )

    valid_candidates = df.filter(
        F.size("validation_errors") == 0
    )

    invalid_quarantine = df.filter(
        F.size("validation_errors") > 0
    )

    return valid_candidates, invalid_quarantine


# ---------------------------------------------------------
# 6. Deduplicate records
# ---------------------------------------------------------

def deduplicate_records(valid_candidates):

    window_spec = (
        Window
        .partitionBy("transaction_id")
        .orderBy(desc("processed_at_timestamp"))
    )

    ranked_df = valid_candidates.withColumn(
        "duplicate_rank",
        row_number().over(window_spec)
    )

    silver_df = (
        ranked_df
        .filter(F.col("duplicate_rank") == 1)
        .drop(
            "duplicate_rank",
            "validation_errors",
            "processed_at"
        )
        .withColumnRenamed(
            "processed_at_timestamp",
            "processed_at"
        )
    )

    duplicate_quarantine = (
        ranked_df
        .filter(F.col("duplicate_rank") > 1)
        .withColumn(
            "validation_errors",
            F.array(
                F.lit("DQ07_OLDER_DUPLICATE")
            )
        )
        .drop("duplicate_rank")
    )

    return silver_df, duplicate_quarantine


# ---------------------------------------------------------
# 7. Write Silver and quarantine as Parquet
# ---------------------------------------------------------

def write_parquet_outputs(silver_df, quarantine_df):

    print("\nWriting Silver Parquet output...")

    (
        silver_df
        .write
        .mode("overwrite")
        .parquet(str(SILVER_OUTPUT))
    )

    print(f"Silver output written to: {SILVER_OUTPUT}")

    print("\nWriting quarantine Parquet output...")

    (
        quarantine_df
        .write
        .mode("overwrite")
        .parquet(str(QUARANTINE_OUTPUT))
    )

    print(
        f"Quarantine output written to: {QUARANTINE_OUTPUT}"
    )


# ---------------------------------------------------------
# 8. Main execution
# ---------------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("FraudDetection-Day09-Parquet")
        .master("local[2]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\n========== DAY 9: PARQUET OUTPUT ==========")

        # Read original records
        source_df = read_source_data(spark)

        # Create test records
        test_df = spark.createDataFrame(
            create_test_records()
        )

        # Combine original and test data
        combined_df = source_df.unionByName(
            test_df,
            allowMissingColumns=True
        )

        total_input = combined_df.count()

        print(f"\nTotal input records: {total_input}")

        # Validate
        valid_candidates, invalid_quarantine = (
            validate_records(combined_df)
        )

        # Deduplicate
        silver_df, duplicate_quarantine = (
            deduplicate_records(valid_candidates)
        )

        # Combine both quarantine categories
        quarantine_df = invalid_quarantine.unionByName(
            duplicate_quarantine,
            allowMissingColumns=True
        )

        # Materialize and display counts
        silver_count = silver_df.count()
        quarantine_count = quarantine_df.count()

        print("\n========== OUTPUT SUMMARY ==========")

        print(f"Total input records : {total_input}")
        print(f"Silver records      : {silver_count}")
        print(f"Quarantined records : {quarantine_count}")

        print("\nSilver sample:")

        silver_df.show(10, truncate=False)

        print("\nQuarantine reasons:")

        (
            quarantine_df
            .select(F.explode("validation_errors").alias("reason"))
            .groupBy("reason")
            .count()
            .orderBy("reason")
            .show(truncate=False)
        )

        # Write outputs
        write_parquet_outputs(
            silver_df,
            quarantine_df
        )

        print("\n========== PARQUET WRITE COMPLETED ==========")

    finally:

        spark.stop()

        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
