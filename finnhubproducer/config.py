import os

from dotenv import load_dotenv
load_dotenv()

FINNHUB_API_TOKEN = os.environ["FINNHUB_API_TOKEN"]

KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_NAME", "market")

FINNHUB_SYMBOLS = os.environ.get(
    "FINNHUB_STOCKS_TICKERS",
    "BINANCE:BTCUSDT,BINANCE:ETHUSDT"
).split(",")

AVRO_SCHEMA_PATH = os.environ.get("AVRO_SCHEMA_PATH", "schemas/trades.avsc")
