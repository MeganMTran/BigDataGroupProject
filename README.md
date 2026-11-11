```
docker-compose up -d
```

```
python setup_kafka.py --bootstrap localhost:9092
```

```
python producer.py --bootstrap localhost:9092 --topic topic1 --backup-file messages.txt```
```

```
export PYSPARK_PYTHON=$(which python)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  spark_ner_stream.py --bootstrap localhost:9092
```
to visualize the Data: Navigate to http://localhost:5601 in your browser. Use Kibana to build a bar chart visualization of the top 10 named entities fed by Logstash from topic2.

```
export PYSPARK_PYTHON=$(which python)
    spark-submit spark_ml_train.py --data-path path/to/ag_news.parquet --model-path /tmp/mllib_model
```

```terminal
export PYSPARK_PYTHON=$(which python)
spark-submit spark_ml_predict.py --mode batch --model-path /tmp/mllib_model --file-path messages.txt
```

```commandline
export PYSPARK_PYTHON=$(which python)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  spark_ml_predict.py --mode stream --bootstrap localhost:9092 --model-path /tmp/mllib_model
```

I will fix formatting later
------------
# BigDataGroupProject

## Part 1

Versions: Spark 4.3.0, Kafka 4.4.0, Python version.

Data source: Hacker News API (https://github.com/HackerNews/API)

(For Windows users, run in WSL)

Install the Python packages:
```bash
python -m venv gp1env
source gp1env/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

#### How to run

Use a separate terminal for each step. Activate the virtual environment in the
Python/Spark terminals (`source gp1env/bin/activate`). Replace `<KAFKA_DIR>` with
your Kafka folder.

**1. Start Kafka broker** (first time only: format storage)

```bash
cd <KAFKA_DIR>
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

**2. Create topics** (once)

```bash
cd <KAFKA_DIR>
bin/kafka-topics.sh --create --topic topic1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic2 --bootstrap-server localhost:9092
```

**3. Start producer**

```bash
python Project1_Part1.py --bootstrap localhost:9092 --topic topic1 \
  --poll-seconds 10 --backup-file <PATH_TO_SAVE_MESSAGES>.txt
```

**4. Start Spark job**

```bash
export PYSPARK_PYTHON=$(which python)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  Project1_Part1_spark.py --checkpoint <CHECKPOINT_DIR>
```

**5. Check output**

```bash
cd <KAFKA_DIR>
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning
```

There should be lines like `{"entity":"Google","count":7}` after the first trigger.
Use `--topic topic1` to check the producer.
