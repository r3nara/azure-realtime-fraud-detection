
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id


def show_partition_distribution(df, title):
    print(f"\n--- {title} ---")

    partitioned = df.withColumn(
        "partition_id",
        spark_partition_id(),
    )

    partitioned.show()

    print("Partition count:", df.rdd.getNumPartitions())

    distribution = (
        partitioned
        .groupBy("partition_id")
        .count()
        .orderBy("partition_id")
    )

    distribution.show()


def main():

    print("Python executable:", sys.executable)

    spark = (
        SparkSession.builder
        .appName("FraudDetection-Day09")
        .master("local[2]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        transactions = [
            ("TXN001", "C001", 1500.0, "Chennai"),
            ("TXN002", "C002", 75000.0, "Mumbai"),
            ("TXN003", "C001", 2500.0, "Chennai"),
            ("TXN004", "C003", 90000.0, "Delhi"),
            ("TXN005", "C002", 1200.0, "Mumbai"),
        ]

        columns = [
            "transaction_id",
            "customer_id",
            "amount",
            "location",
        ]

        df = spark.createDataFrame(transactions, columns)

        print("\n--- Original Transactions ---")
        df.show()

        print("\n--- High Value Transactions ---")
        df.filter(col("amount") > 50000).show()

        show_partition_distribution(
            df,
            "Original DataFrame",
        )

        repartitioned_df = df.repartition(4)

        show_partition_distribution(
            repartitioned_df,
            "After repartition(4)",
        )

        coalesced_df = repartitioned_df.coalesce(2)

        show_partition_distribution(
            coalesced_df,
            "After coalesce(2)",
        )

        print("\n--- Total Transaction Count ---")
        print(df.count())

        print("\n--- Repartition Execution Plan ---")
        repartitioned_df.explain()

        print("\n--- Coalesce Execution Plan ---")
        coalesced_df.explain()

    finally:

        spark.stop()
        print("\nSparkSession stopped successfully.")


if __name__ == "__main__":
    main()
