"""Cấu hình cho Spark streaming job — tất cả lấy từ biến môi trường."""

import os

# --- Nguồn: Kafka ---
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9093")

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_NAME", "market")

# earliest = đọc lại từ đầu topic ở lần chạy đầu tiên (khi checkpoint còn trống).
# Sau đó Spark luôn tiếp tục từ offset trong checkpoint, biến này bị bỏ qua.
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "latest")

# Chặn trần số bản ghi mỗi micro-batch để một lần backlog lớn không làm nghẽn job.
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "100000")

# Cùng file .avsc mà producer dùng để encode.
AVRO_SCHEMA_PATH = os.environ.get("AVRO_SCHEMA_PATH", "schemas/trades.avsc")

# --- Cửa sổ thời gian & dữ liệu đến muộn ---
# Độ dài cửa sổ tumbling, tính trên event time (trade.timestamp), không phải giờ xử lý.
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "60"))

# Watermark: chấp nhận trade đến muộn tối đa bấy nhiêu giây thì cửa sổ mới bị đóng.
# Trade muộn hơn ngưỡng này bị Spark bỏ khỏi phép gộp -> ta ghi riêng vào bảng chất lượng.
WATERMARK_SECONDS = int(os.environ.get("WATERMARK_SECONDS", "60"))

# Chu kỳ chạy micro-batch.
TRIGGER_SECONDS = int(os.environ.get("TRIGGER_SECONDS", "20"))

# --- Ngưỡng làm sạch ---
# Trade có event time vượt quá giờ nhận + ngưỡng này là đồng hồ lệch/dữ liệu hỏng.
MAX_FUTURE_SKEW_SECONDS = int(os.environ.get("MAX_FUTURE_SKEW_SECONDS", "60"))

# 2000-01-01T00:00:00Z — mốc chặn dưới, bắt các timestamp 0 / tính nhầm đơn vị giây.
MIN_EVENT_MS = int(os.environ.get("MIN_EVENT_MS", "946684800000"))

# --- Spark ---
SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")

# Nơi Spark lưu offset Kafka + state của aggregation. Mất thư mục này là mất
# tiến độ đọc lẫn các cửa sổ đang mở, nên trong compose nó nằm trên volume.
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/opt/spark-checkpoints")

LOG_LEVEL = os.environ.get("SPARK_LOG_LEVEL", "WARN")

# --- Đích: PostgreSQL ---
PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "finnhub")
PG_USER = os.environ.get("POSTGRES_USER", "finnhub")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "finnhub")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

METRICS_TABLE = "trade_metrics"
# Bảng đệm: mỗi micro-batch ghi đè vào đây rồi MERGE sang bảng chính.
METRICS_STAGING_TABLE = "trade_metrics_staging"
QUALITY_TABLE = "trade_quality_events"
