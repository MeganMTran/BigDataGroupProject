import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, length

LABELS = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}

def load_agnews(spark, path):
    df = spark.read.parquet(path).select("text", "label")
    name = when(col("label") == 0, "World") \
          .when(col("label") == 1, "Sports") \
          .when(col("label") == 2, "Business") \
          .otherwise("Sci/Tech")
    df = df.withColumn("label_name", name)
    print(f"AG News rows: {df.count()}")
    df.groupBy("label", "label_name").count().orderBy("label").show()
    return df

def load_stream_text(spark, source, file_path, bootstrap, topic):
    if source == "file":
        raw = spark.read.text(file_path).withColumnRenamed("value", "text")
    else:
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", bootstrap)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .load()
               .selectExpr("CAST(value AS STRING) AS text"))
    print(f"Streamed messages before cleaning: {raw.count()}")

    cleaned = (raw.withColumn("text", trim(col("text")))
                  .filter(col("text").isNotNull() & (length(col("text")) > 0))
                  .dropDuplicates(["text"]))
    print(f"Streamed messages after removing empty and duplicates: {cleaned.count()}")
    return cleaned

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--agnews-path", required=True)
    ap.add_argument("--source", choices=["file", "kafka"], default="file")
    ap.add_argument("--file-path", default=None, help="messages file (source=file)")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="topic1")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("GP1_Part2_Load").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ag = load_agnews(spark, args.agnews_path)
    ag.show(5, truncate=80)

    stream_df = load_stream_text(spark, args.source, args.file_path, args.bootstrap, args.topic)
    stream_df.show(5, truncate=80)

if __name__ == "__main__":
    main()
