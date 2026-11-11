import argparse
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, struct, to_json
from pyspark.sql.types import ArrayType, StringType

_nlp = None
KEEP = ["PERSON", "ORG", "GPE", "NORP", "EVENT", "PRODUCT"]

def entities(text):
    global _nlp
    if _nlp is None:    # once per worker
        _nlp = spacy.load("en_core_web_sm")
        return [
            e.text for e in _nlp(text).ents
            if e.label_ in KEEP     #correct label type
            and e.text != ""        #nonempty
            and len(e.text) > 1     #longer than 1 char
            and not e.text.lower().isnumeric() #is not a number
        ]
    return []

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("NER_Stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ner_udf = udf(entities, ArrayType(StringType()))

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap) \
        .option("subscribe", "topic1").load()

    msgs = df.selectExpr("CAST(value AS STRING) as text")
    entity_counts = msgs.select(explode(ner_udf(col("text"))).alias("entity")).groupBy("entity").count()
    kafka_output = entity_counts.select(to_json(struct(col("entity"), col("count"))).alias("value"))

    query = kafka_output.writeStream.outputMode("update").format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap) \
        .option("topic", "topic2") \
        .option("checkpointLocation", "/tmp/spark-checkpoints-ner") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()