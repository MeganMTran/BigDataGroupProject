------------
# BigDataGroupProject

## Part 1

Versions: Spark 4.3.0, Kafka 4.4.0, Python version.

Data source: Hacker News API (https://github.com/HackerNews/API)

(For Windows users, run in WSL)

Set up Kafka:
```
docker-compose up -d
```

```
cd <KAFKA_DIR>
source ~/gp1env/bin/activate
python setup_kafka.py --bootstrap localhost:9092
```

```
python producer.py --bootstrap localhost:9092 --topic topic1 --backup-file messages.txt```
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

Check output:

```bash
cd <KAFKA_DIR>
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning
```

There should be lines like `{"entity":"Google","count":7}` after the first trigger.
Use `--topic topic1` to check the producer.

**5. DOWNLOAD AND START ELASTICSEARCH, KIBANA, AND LOGSTASH**
```bash
wget <download link>
```

```bash
cd ~/elasticsearch-9.5.6
bin/elasticsearch
```
Make sure to disable security (add lines to end of file):
```bash
nano ~/elasticsearch-9.5.6/config/elasticsearch.yml
xpack.security.enabled: false
xpack.security.enrollment.enabled: false
```

```bash
cd ~/kibana-9.5.6 && bin/kibana
```

```bash
cd ~/logstash-9.5.6 && bin/logstash -f ~/gp1/topic2.conf
```

to visualize the Data: Navigate to http://localhost:5601 in your browser. Use Kibana to build a bar chart visualization of the top 10 named entities fed by Logstash from topic2.

## Part 2

**1. Start Train Model**

```bash
export PYSPARK_PYTHON=$(which python)
spark-submit train_model.py \
  --agnews-path ~/gp1/data/train-00000-of-00001.parquet \
  --model-path ~/gp1/models/agnews_lr --eval
```

**2. Activate pipeLineTwo**

```bash
spark-submit pipeLineTwo.py \
  --model-path ~/gp1/models/agnews_lr \
  --input-path ~/gp1/messages_clean.txt \
  --output-dir ~/gp1/outputs
```

**3. Compare hand picked labels with model**

```bash
python ~/gp1/compare_labels.py --file ~/gp1/outputs/hand_labels.csv
```

**4. Start Batches**

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  pipeLineOne.py --checkpoint ~/gp1/ckpt_p2 --model-path ~/gp1/models/agnews_lr
```
