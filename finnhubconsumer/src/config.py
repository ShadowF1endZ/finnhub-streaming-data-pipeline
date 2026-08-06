import os

from dotenv import load_dotenv
load_dotenv()

KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9093")

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_NAME", "market")

KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "finnhub-consumer")

KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest")

AVRO_SCHEMA_PATH = os.environ.get("AVRO_SCHEMA_PATH", "schemas/trades.avsc")

SUMMARY_INTERVAL = float(os.environ.get("SUMMARY_INTERVAL", "5"))
