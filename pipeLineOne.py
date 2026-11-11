import spacy
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, struct, to_json
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession.builder \
    .appName("NER_Stream") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic1") \
    .load()

_nlp = None
KEEP = ["PERSON", "ORG", "GPE", "NORP", "EVENT", "PRODUCT"]

msgs = df.selectExpr("CAST(value AS STRING) as text")

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

ner = udf(entities, ArrayType(StringType()))

entity_counts = msgs.select(explode(ner(col("text"))).alias("entity")) \
    .groupBy("entity") \
    .count()

kafka_output = entity_counts.select(
    to_json(struct(col("entity"), col("count"))).alias("value")
)

# Write to topic2
query = kafka_output.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "topic2") \
    .option("checkpointLocation", "/tmp/spark-checkpoints-ner") \
    .start()

query.awaitTermination()