import io
import json
import logging

import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from kafka import KafkaProducer
import websocket

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("FinnhubProducer")


# --- 1. Nạp Avro schema một lần khi khởi động ---
with open(config.AVRO_SCHEMA_PATH, "r") as f:
    AVRO_SCHEMA = avro.schema.parse(f.read())


def avro_encode(record: dict, schema) -> bytes:

    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(record, encoder)
    return bytes_writer.getvalue()


# --- 2. Khởi tạo Kafka producer ---
# value_serializer=None vì ta tự encode Avro thủ công (đã ra bytes rồi).
producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVER)


# --- 3. Các callback của websocket ---
def on_message(ws, message):
    """
    Được gọi mỗi khi Finnhub gửi dữ liệu.
    Message có dạng: {"data": [{"s":..,"p":..,"t":..,"v":..}, ...], "type":"trade"}
    Ta duyệt từng trade trong mảng 'data', encode và gửi vào Kafka.
    """
    payload = json.loads(message)

    # Finnhub gửi cả ping/ack, chỉ xử lý message loại 'trade'.
    if payload.get("type") != "trade":
        return

    for trade in payload.get("data", []):
        record = {
            "symbol":    trade["s"],
            "price":     float(trade["p"]),
            "timestamp": int(trade["t"]),
            "volume":    float(trade["v"]),
        }
        avro_bytes = avro_encode(record, AVRO_SCHEMA)
        # Dùng symbol làm key -> các trade cùng symbol vào cùng partition,
        # giữ đúng thứ tự thời gian cho từng mã.
        producer.send(
            config.KAFKA_TOPIC,
            key=record["symbol"].encode("utf-8"),
            value=avro_bytes,
        )
    logger.info("Đã gửi %d trade vào Kafka", len(payload.get("data", [])))


def on_error(ws, error):
    logger.error("Lỗi websocket: %s", error)


def on_close(ws, close_status_code, close_msg):
    logger.warning("Websocket đóng: %s %s", close_status_code, close_msg)


def on_open(ws):
    """Khi kết nối mở, gửi lệnh subscribe cho từng symbol."""
    for symbol in config.FINNHUB_SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        logger.info("Đã subscribe: %s", symbol)


def run():
    url = f"wss://ws.finnhub.io?token={config.FINNHUB_API_TOKEN}"
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    # run_forever tự chạy vòng lặp; ping_interval giữ kết nối sống.
    ws.run_forever(ping_interval=10)


if __name__ == "__main__":
    run()
