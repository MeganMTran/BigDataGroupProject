import argparse, json, os, re, time
import requests
from kafka import KafkaProducer
import html

BASE = "https://hacker-news.firebaseio.com/v0"

def clean(raw):
    text = re.sub(r"<[^>]+>", " ", raw or "")   # remove tags
    text = html.unescape(text)                   # &#x27; -> ', &quot; -> ", &amp; -> &
    return re.sub(r"\s+", " ", text).strip()     # tidy whitespace

def get_json(url):
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="topic1")
    ap.add_argument("--poll-seconds", type=int, default=10)
    ap.add_argument("--backup-file", default=None)  #backup file
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    last_id = get_json(f"{BASE}/maxitem.json")

    while True:
        current = get_json(f"{BASE}/maxitem.json")
        for item_id in range(last_id + 1, current + 1):
            item = get_json(f"{BASE}/item/{item_id}.json")
            if not item or item.get("deleted") or item.get("dead"):
                continue
            text = clean(item.get("title") or item.get("text"))
            if text:
                producer.send(args.topic, text)
                if args.backup_file:
                    with open(args.backup_file, "a", encoding="utf-8") as f:
                        f.write(text.replace("\n", " ") + "\n")
        producer.flush()
        last_id = current
        time.sleep(args.poll_seconds)

if __name__ == "__main__":
    main()