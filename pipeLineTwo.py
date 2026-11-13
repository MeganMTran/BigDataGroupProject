import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel


def run_batch(spark, model, file_path):
    df = spark.read.text(file_path).withColumnRenamed("value", "text")
    df = df.filter(col("text") != "").dropDuplicates(["text"])
    print(f"Total unique messages for batch prediction: {df.count()}")

    predictions = model.transform(df)
    predictions.groupBy("prediction").count().show()
    predictions.select("text", "prediction", "probability").show(20, truncate=False)

    # Save output for manual labeling requirement
    predictions.select("text", "prediction", "probability").write.mode("overwrite").json("/tmp/batch_predictions")
    print("Batch predictions saved to /tmp/batch_predictions")


def run_stream(spark, model, bootstrap):
    stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap) \
        .option("subscribe", "topic1").load()

    text_stream = stream_df.selectExpr("CAST(value AS STRING) as text")
    predictions = model.transform(text_stream)
    counts = predictions.groupBy("prediction").count()

    query = counts.writeStream.outputMode("complete").format("console") \
        .option("checkpointLocation", "/tmp/spark-checkpoints-ml").start()
    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=["batch", "stream"])
    parser.add_argument("--model-path", default="/tmp/mllib_model")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--file-path", default="messages.txt", help="Path to backup file for batch mode")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("MLlib_Predict").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Loading model from {args.model_path}...")
    model = PipelineModel.load(args.model_path)

    if args.mode == "batch":
        run_batch(spark, model, args.file_path)
    else:
        run_stream(spark, model, args.bootstrap)


if __name__ == "__main__":
    main()