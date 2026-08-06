import io
import logging
import signal
import time
from collections import defaultdict

import avro.schema
from avro.io import BinaryDecoder, DatumReader
from kafka import KafkaConsumer

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("FinnhubConsumer")


# --- 1. Nạp cùng schema mà producer đã dùng để encode ---
with open(config.AVRO_SCHEMA_PATH, "r") as f:
    AVRO_SCHEMA = avro.schema.parse(f.read())

READER = DatumReader(AVRO_SCHEMA)


def avro_decode(raw: bytes) -> dict:
    """Giải mã Avro binary ngược lại thành dict Python."""
    return READER.read(BinaryDecoder(io.BytesIO(raw)))


# --- 2. Gom thống kê theo symbol ---
stats = defaultdict(lambda: {"count": 0, "volume": 0.0, "last": None,
                             "low": float("inf"), "high": float("-inf")})


def record_trade(trade: dict) -> None:
    s = stats[trade["symbol"]]
    price = trade["price"]
    s["count"] += 1
    s["volume"] += trade["volume"]
    s["last"] = price
    s["low"] = min(s["low"], price)
    s["high"] = max(s["high"], price)


def log_summary(elapsed: float) -> None:
    if not stats:
        logger.info("Chưa nhận được trade nào trong %.0fs qua", elapsed)
        return
    for symbol, s in sorted(stats.items()):
        logger.info(
            "%-18s %4d trade | last %-12.2f low %-12.2f high %-12.2f vol %.4f",
            symbol, s["count"], s["last"], s["low"], s["high"], s["volume"],
        )
    stats.clear()


# --- 3. Dừng êm khi Docker gửi SIGTERM ---
running = True


def stop(signum, frame):
    global running
    running = False
    logger.info("Nhận tín hiệu dừng, đang thoát...")


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def run():
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_SERVER,
        group_id=config.KAFKA_GROUP_ID,
        auto_offset_reset=config.KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        # Trả về sau 1s dù không có message, để vòng lặp còn kiểm tra cờ dừng
        # và kịp in tổng kết đúng chu kỳ.
        consumer_timeout_ms=1000,
    )
    logger.info("Đang nghe topic '%s' tại %s (group=%s)",
                config.KAFKA_TOPIC, config.KAFKA_SERVER, config.KAFKA_GROUP_ID)

    last_summary = time.monotonic()

    while running:
        for message in consumer:
            if not running:
                break
            record_trade(avro_decode(message.value))

        now = time.monotonic()
        if now - last_summary >= config.SUMMARY_INTERVAL:
            log_summary(now - last_summary)
            last_summary = now

    consumer.close()
    logger.info("Đã đóng consumer.")


if __name__ == "__main__":
    run()
