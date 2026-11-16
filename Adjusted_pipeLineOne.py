import argparse
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, struct, to_json, when
from pyspark.sql.types import ArrayType, StringType

_nlp = None
KEEP = ["PERSON", "ORG", "GPE", "NORP", "EVENT", "PRODUCT"]

def entities(text):
    global _nlp
    if not text:
        return []
    if _nlp is None:  # load once per worker
        _nlp = spacy.load("en_core_web_sm")
    return [
        e.text.strip() for e in _nlp(text).ents
        if e.label_ in KEEP
        and len(e.text.strip()) > 1
        and not e.text.strip().isnumeric()
    ]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--in-topic", default="topic1")
    parser.add_argument("--out-topic", default="topic2")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--model-path", default=None)
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("NER_Stream")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    ner_udf = udf(entities, ArrayType(StringType()))

    df = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", args.bootstrap)
          .option("subscribe", args.in_topic)
          .load())

    msgs = df.selectExpr("CAST(value AS STRING) as text")

    entity_counts = (msgs.select(explode(ner_udf(col("text"))).alias("entity"))
                     .groupBy("entity").count())
    kafka_output = entity_counts.select(
        to_json(struct(col("entity"), col("count"))).alias("value"))

    (kafka_output.writeStream.outputMode("update").format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("topic", args.out_topic)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start())

import argparse
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, struct, to_json, when
from pyspark.sql.types import ArrayType, StringType

_nlp = None
KEEP = ["PERSON", "ORG", "GPE", "NORP", "EVENT", "PRODUCT"]

def entities(text):
    global _nlp
    if not text:
        return []
    if _nlp is None:  # load once per worker
        _nlp = spacy.load("en_core_web_sm")
    return [
        e.text.strip() for e in _nlp(text).ents
        if e.label_ in KEEP
        and len(e.text.strip()) > 1
        and not e.text.strip().isnumeric()
    ]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--in-topic", default="topic1")
    parser.add_argument("--out-topic", default="topic2")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--trigger", default="30 seconds")
    parser.add_argument("--model-path", default=None)
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("NER_Stream")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    ner_udf = udf(entities, ArrayType(StringType()))

    df = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", args.bootstrap)
          .option("subscribe", args.in_topic)
          .load())

    msgs = df.selectExpr("CAST(value AS STRING) as text")

  #Query 1
    entity_counts = (msgs.select(explode(ner_udf(col("text"))).alias("entity"))
                     .groupBy("entity").count())
    kafka_output = entity_counts.select(
        to_json(struct(col("entity"), col("count"))).alias("value"))

    (kafka_output.writeStream.outputMode("update").format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("topic", args.out_topic)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start())

    #Query 2
    if args.model_path:
        from pyspark.ml import PipelineModel
        model = PipelineModel.load(args.model_path)
        pred = model.transform(msgs)
        name = (when(col("prediction") == 0, "World")
                .when(col("prediction") == 1, "Sports")
                .when(col("prediction") == 2, "Business")
                .otherwise("Sci/Tech"))
        class_counts = (pred.withColumn("predicted_class", name)
                        .groupBy("predicted_class").count())
        (class_counts.writeStream.outputMode("complete").format("console")
            .option("truncate", False)
            .option("checkpointLocation", args.checkpoint + "_classes")
            .trigger(processingTime=args.trigger)
            .start())

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
