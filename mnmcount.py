import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
print("✅ mnmcount.py started")

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession.
    spark = (
        SparkSession.builder
        .appName("PythonMnMCount")
        .getOrCreate()
    )
    # Optional: reduce log noise
    spark.sparkContext.setLogLevel("WARN")
    print("✅ mnmcount.py started")

    # Get the M&M data set filename from the command-line arguments
    mnm_file = sys.argv[1]

    # Read the file into a Spark DataFrame using CSV format
    mnm_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file)
        .cache()
    )

    # Aggregate counts for all states and colors
    count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(_sum("Count").alias("TotalCount"))
        .orderBy("TotalCount", ascending=False)
    )

    # Show aggregations for all states and colors
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % count_mnm_df.count())

    # Aggregate counts for California only (CA)
    ca_count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(_sum("Count").alias("TotalCount"))
        .orderBy("TotalCount", ascending=False)
    )

    # Show top 10 aggregations for California
    ca_count_mnm_df.show(n=10, truncate=False)

    # Stop the SparkSession
    input("Press Enter to stop Spark and exit...")

    spark.stop()
