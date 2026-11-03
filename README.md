# BigDataGroupProject

##Part 1
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

**1. Start the Kafka broker** (first time only: format storage)

```bash
cd <KAFKA_DIR>
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

**2. Create the topics** (once)

```bash
cd <KAFKA_DIR>
bin/kafka-topics.sh --create --topic topic1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic2 --bootstrap-server localhost:9092
```

**3. Start the producer**

```bash
python Project1_Part1.py --bootstrap localhost:9092 --topic topic1 \
  --poll-seconds 10 --backup-file <PATH_TO_SAVE_MESSAGES>.txt
```

**4. Start the Spark job**

```bash
export PYSPARK_PYTHON=$(which python)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  Project1_Part1_spark.py --checkpoint <CHECKPOINT_DIR>
```

**5. Check the output**

```bash
cd <KAFKA_DIR>
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning
```

You should see lines like `{"entity":"Google","count":7}` after the first trigger.
Use `--topic topic1` to check the producer.
