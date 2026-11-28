
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

TRANSACTION_SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "silver"
    / "transactions"
)

FRAUD_SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "output"
    / "silver-delta"
    / "fraud-results"
)

GOLD_ROOT = PROJECT_ROOT / "data" / "output" / "gold"


# =========================================================
# GOLD OUTPUT TABLES
# =========================================================

GOLD_TABLES = {
    "customer_transaction_summary":
        GOLD_ROOT / "customer_transaction_summary",

    "merchant_transaction_summary":
        GOLD_ROOT / "merchant_transaction_summary",

    "daily_transaction_metrics":
        GOLD_ROOT / "daily_transaction_metrics",

    "customer_fraud_risk_summary":
        GOLD_ROOT / "customer_fraud_risk_summary",

    "daily_fraud_metrics":
        GOLD_ROOT / "daily_fraud_metrics",

    "fraud_rule_distribution":
        GOLD_ROOT / "fraud_rule_distribution",
}


# =========================================================
# SPARK SESSION
# =========================================================

def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("Gold-Fraud-Analytics")
        .master("local[2]")
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

    spark = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


# =========================================================
# READ SILVER TABLES
# =========================================================

def read_silver_tables(spark):

    print("\nReading Silver Delta tables...")

    if not TRANSACTION_SILVER_PATH.exists():
        raise FileNotFoundError(
            f"Transaction Silver path not found: "
            f"{TRANSACTION_SILVER_PATH}"
        )

    if not FRAUD_SILVER_PATH.exists():
        raise FileNotFoundError(
            f"Fraud Silver path not found: "
            f"{FRAUD_SILVER_PATH}"
        )

    transactions = (
        spark.read
        .format("delta")
        .load(str(TRANSACTION_SILVER_PATH))
    )

    fraud_results = (
        spark.read
        .format("delta")
        .load(str(FRAUD_SILVER_PATH))
    )

    transaction_count = transactions.count()
    fraud_count = fraud_results.count()

    print(f"Transaction Silver records: {transaction_count}")
    print(f"Fraud Silver records: {fraud_count}")

    return transactions, fraud_results


# =========================================================
# CUSTOMER TRANSACTION SUMMARY
# =========================================================

def build_customer_transaction_summary(transactions):

    print("\nBuilding customer transaction summary...")

    result = (
        transactions
        .groupBy("customer_id")
        .agg(
            F.countDistinct("transaction_id")
            .alias("transaction_count"),

            F.sum("amount")
            .alias("total_transaction_amount"),

            F.avg("amount")
            .alias("average_transaction_amount"),

            F.max("amount")
            .alias("maximum_transaction_amount"),

            F.sum(
                F.when(F.col("status") == "SUCCESS", 1)
                .otherwise(0)
            ).alias("successful_transactions"),

            F.sum(
                F.when(F.col("status") == "FAILED", 1)
                .otherwise(0)
            ).alias("failed_transactions"),

            F.max("transaction_timestamp")
            .alias("last_transaction_at"),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )

    return result


# =========================================================
# MERCHANT TRANSACTION SUMMARY
# =========================================================

def build_merchant_transaction_summary(transactions):

    print("\nBuilding merchant transaction summary...")

    result = (
        transactions
        .groupBy("merchant")
        .agg(
            F.countDistinct("transaction_id")
            .alias("transaction_count"),

            F.sum("amount")
            .alias("total_transaction_amount"),

            F.avg("amount")
            .alias("average_transaction_amount"),

            F.max("amount")
            .alias("maximum_transaction_amount"),

            F.sum(
                F.when(F.col("status") == "SUCCESS", 1)
                .otherwise(0)
            ).alias("successful_transactions"),

            F.sum(
                F.when(F.col("status") == "FAILED", 1)
                .otherwise(0)
            ).alias("failed_transactions"),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )

    return result


# =========================================================
# DAILY TRANSACTION METRICS
# =========================================================

def build_daily_transaction_metrics(transactions):

    print("\nBuilding daily transaction metrics...")

    result = (
        transactions
        .withColumn(
            "transaction_date",
            F.to_date("transaction_timestamp")
        )
        .groupBy("transaction_date", "currency")
        .agg(
            F.countDistinct("transaction_id")
            .alias("transaction_count"),

            F.sum("amount")
            .alias("total_transaction_amount"),

            F.avg("amount")
            .alias("average_transaction_amount"),

            F.sum(
                F.when(F.col("status") == "SUCCESS", 1)
                .otherwise(0)
            ).alias("successful_transactions"),

            F.sum(
                F.when(F.col("status") == "FAILED", 1)
                .otherwise(0)
            ).alias("failed_transactions"),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )

    return result


# =========================================================
# CUSTOMER FRAUD RISK SUMMARY
# =========================================================

def build_customer_fraud_risk_summary(fraud_results):

    print("\nBuilding customer fraud risk summary...")

    result = (
        fraud_results
        .groupBy("customer_id")
        .agg(
            F.countDistinct("transaction_id")
            .alias("scored_transaction_count"),

            F.avg("risk_score")
            .alias("average_risk_score"),

            F.max("risk_score")
            .alias("maximum_risk_score"),

            F.sum(
                F.when(F.col("risk_level") == "LOW", 1)
                .otherwise(0)
            ).alias("low_risk_count"),

            F.sum(
                F.when(F.col("risk_level") == "MEDIUM", 1)
                .otherwise(0)
            ).alias("medium_risk_count"),

            F.sum(
                F.when(F.col("risk_level") == "HIGH", 1)
                .otherwise(0)
            ).alias("high_risk_count"),

            F.sum(
                F.when(
                    F.col("risk_level").isin("MEDIUM", "HIGH"),
                    1
                ).otherwise(0)
            ).alias("flagged_transaction_count"),

            F.max("processed_at")
            .alias("last_scored_at"),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )

    return result


# =========================================================
# DAILY FRAUD METRICS
# =========================================================

def build_daily_fraud_metrics(fraud_results):

    print("\nBuilding daily fraud metrics...")

    daily = fraud_results.withColumn(
        "scoring_date",
        F.to_date("processed_at")
    )

    result = (
        daily
        .groupBy("scoring_date")
        .agg(
            F.countDistinct("transaction_id")
            .alias("scored_transaction_count"),

            F.avg("risk_score")
            .alias("average_risk_score"),

            F.max("risk_score")
            .alias("maximum_risk_score"),

            F.sum(
                F.when(F.col("risk_level") == "LOW", 1)
                .otherwise(0)
            ).alias("low_risk_count"),

            F.sum(
                F.when(F.col("risk_level") == "MEDIUM", 1)
                .otherwise(0)
            ).alias("medium_risk_count"),

            F.sum(
                F.when(F.col("risk_level") == "HIGH", 1)
                .otherwise(0)
            ).alias("high_risk_count"),

            F.sum(
                F.when(
                    F.col("risk_level").isin("MEDIUM", "HIGH"),
                    1
                ).otherwise(0)
            ).alias("flagged_transaction_count"),
        )
        .withColumn(
            "flagged_transaction_rate",
            F.when(
                F.col("scored_transaction_count") > 0,
                F.col("flagged_transaction_count")
                / F.col("scored_transaction_count")
            ).otherwise(0.0)
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
    )

    return result


# =========================================================
# FRAUD RULE DISTRIBUTION
# =========================================================

def build_fraud_rule_distribution(fraud_results):

    print("\nBuilding fraud rule distribution...")

    exploded = (
        fraud_results
        .select(
            "transaction_id",
            F.explode("triggered_rules").alias("fraud_rule")
        )
        .filter(F.col("fraud_rule").isNotNull())
    )

    result = (
        exploded
        .groupBy("fraud_rule")
        .agg(
            F.countDistinct("transaction_id")
            .alias("triggered_transaction_count")
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp()
        )
        .orderBy(F.desc("triggered_transaction_count"))
    )

    return result


# =========================================================
# WRITE GOLD DELTA TABLE
# FULL REFRESH
# =========================================================

def write_gold_table(df, table_name):

    output_path = GOLD_TABLES[table_name]

    print(f"\nWriting Gold table: {table_name}")

    record_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )

    print(f"Records written: {record_count}")
    print(f"Output path: {output_path}")

    return record_count


# =========================================================
# MAIN PIPELINE
# =========================================================

def main():

    spark = create_spark_session()

    try:

        # Step 1: Read Silver
        transactions, fraud_results = read_silver_tables(
            spark
        )

        # Step 2: Build transaction analytics
        customer_transactions = (
            build_customer_transaction_summary(
                transactions
            )
        )

        merchant_transactions = (
            build_merchant_transaction_summary(
                transactions
            )
        )

        daily_transactions = (
            build_daily_transaction_metrics(
                transactions
            )
        )

        # Step 3: Build fraud analytics
        customer_fraud = (
            build_customer_fraud_risk_summary(
                fraud_results
            )
        )

        daily_fraud = (
            build_daily_fraud_metrics(
                fraud_results
            )
        )

        fraud_rules = (
            build_fraud_rule_distribution(
                fraud_results
            )
        )

        # Step 4: Write Gold Delta tables
        gold_datasets = {
            "customer_transaction_summary":
                customer_transactions,

            "merchant_transaction_summary":
                merchant_transactions,

            "daily_transaction_metrics":
                daily_transactions,

            "customer_fraud_risk_summary":
                customer_fraud,

            "daily_fraud_metrics":
                daily_fraud,

            "fraud_rule_distribution":
                fraud_rules,
        }

        print("\n" + "=" * 65)
        print("WRITING GOLD ANALYTICS TABLES")
        print("=" * 65)

        summary = {}

        for table_name, dataframe in gold_datasets.items():

            summary[table_name] = write_gold_table(
                dataframe,
                table_name
            )

        # Step 5: Print final summary
        print("\n" + "=" * 65)
        print("GOLD PIPELINE SUMMARY")
        print("=" * 65)

        for table_name, count in summary.items():
            print(f"{table_name:<38}: {count}")

        print("=" * 65)

        # Step 6: Display sample data
        for table_name, dataframe in gold_datasets.items():

            print(f"\nSample: {table_name}")

            dataframe.show(
                10,
                truncate=False
            )

        print("\nGold analytics pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
