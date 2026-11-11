import argparse
from kafka.admin import KafkaAdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    admin_client = KafkaAdminClient(bootstrap_servers=args.bootstrap, client_id='setup_script')

    topics = [
        NewTopic(name="topic1", num_partitions=3, replication_factor=1),
        NewTopic(name="topic2", num_partitions=3, replication_factor=1)
    ]

    existing = admin_client.list_topics()
    new_topics = [t for t in topics if t.name not in existing]

    if new_topics:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"Created topics: {[t.name for t in new_topics]}")
    else:
        print("Topics already exist.")

if __name__ == "__main__":
    main()