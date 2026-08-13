"""
Spark Structured Streaming: Kafka (Avro) -> làm sạch -> gộp theo cửa sổ thời gian -> PostgreSQL.

Job chạy 2 query song song trên cùng một topic:
  1. metrics : gộp trade theo cửa sổ event time + watermark, upsert vào bảng trade_metrics.
  2. quality : bản ghi hỏng và bản ghi đến muộn quá watermark, ghi vào trade_quality_events.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQueryListener

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("SparkStreaming")


# --- 1. Đọc Kafka và decode Avro ---------------------------------------------

def read_trades(spark: SparkSession) -> DataFrame:
    """Đọc topic Kafka và giải mã cột value bằng đúng schema producer đã dùng."""
    with open(config.AVRO_SCHEMA_PATH, "r") as f:
        avro_schema_json = f.read()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_SERVER)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
        # Topic để retention 1 giờ; nếu dữ liệu bị xoá trước khi đọc kịp thì
        # cảnh báo rồi đi tiếp, đừng làm chết job.
        .option("failOnDataLoss", "false")
        .load()
    )

    # mode=PERMISSIVE: byte hỏng -> struct null thay vì ném lỗi giết cả query.
    # Producer ghi Avro binary trần (không có magic byte của Schema Registry),
    # nên from_avro dùng thẳng file .avsc là khớp.
    return raw.select(
        F.col("timestamp").alias("ingest_time"),
        F.length(F.col("value")).alias("payload_bytes"),
        from_avro(F.col("value"), avro_schema_json, {"mode": "PERMISSIVE"}).alias("trade"),
    )


# --- 2. Làm sạch và biến đổi --------------------------------------------------

def clean(decoded: DataFrame) -> DataFrame:
    """Chuẩn hoá kiểu dữ liệu, gắn cờ lý do loại bỏ và độ trễ của từng bản ghi."""
    flat = decoded.select(
        F.col("ingest_time"),
        F.col("payload_bytes"),
        F.col("trade").isNull().alias("decode_failed"),
        F.upper(F.trim(F.col("trade.symbol"))).alias("symbol"),
        F.col("trade.price").alias("price"),
        F.col("trade.volume").alias("volume"),
        F.col("trade.timestamp").alias("event_ms"),
    )

    # Finnhub gửi epoch milliseconds -> đổi sang timestamp để Spark dùng làm event time.
    event_time = (F.col("event_ms") / 1000).cast("timestamp")

    reject_reason = (
        F.when(F.col("decode_failed"), F.lit("avro_decode_failed"))
        .when(F.col("symbol").isNull() | (F.length(F.col("symbol")) == 0), F.lit("missing_symbol"))
        .when(F.col("price").isNull() | F.isnan(F.col("price")) | (F.col("price") <= 0), F.lit("invalid_price"))
        .when(F.col("volume").isNull() | F.isnan(F.col("volume")) | (F.col("volume") < 0), F.lit("invalid_volume"))
        .when(F.col("event_ms").isNull() | (F.col("event_ms") < config.MIN_EVENT_MS), F.lit("invalid_timestamp"))
        .when(
            F.col("event_time") > F.col("ingest_time") + F.expr(f"INTERVAL {config.MAX_FUTURE_SKEW_SECONDS} SECONDS"),
            F.lit("timestamp_in_future"),
        )
    )

    return (
        flat.withColumn("event_time", event_time)
        # Trễ = lúc Kafka nhận trừ lúc trade thực sự xảy ra. Đây là thước đo
        # "muộn" duy nhất tính được ở mức từng dòng, dùng cho cả cảnh báo bên dưới.
        .withColumn("delay_ms", (F.col("ingest_time").cast("double") * 1000 - F.col("event_ms")).cast("long"))
        .withColumn("reject_reason", reject_reason)
    )


# --- 3. Gộp theo cửa sổ thời gian ---------------------------------------------

def aggregate(clean_df: DataFrame) -> DataFrame:
    """
    Tumbling window trên event time + watermark.

    Watermark làm 2 việc: cho phép trade đến muộn trong ngưỡng vẫn cập nhật lại
    cửa sổ cũ (nên sink phải upsert), và cho phép Spark dọn state của cửa sổ đã
    quá hạn thay vì giữ mãi trong bộ nhớ.
    """
    valid = clean_df.filter(F.col("reject_reason").isNull())

    return (
        valid.withWatermark("event_time", f"{config.WATERMARK_SECONDS} seconds")
        .groupBy(F.window(F.col("event_time"), f"{config.WINDOW_SECONDS} seconds"), F.col("symbol"))
        .agg(
            F.count(F.lit(1)).alias("trade_count"),
            F.sum("volume").alias("total_volume"),
            F.sum(F.col("price") * F.col("volume")).alias("notional"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.avg("price").alias("avg_price"),
            # min_by/max_by theo event time = giá mở/đóng cửa sổ, không phụ thuộc
            # thứ tự bản ghi đến (first/last trong streaming là không xác định).
            F.expr("min_by(price, event_time)").alias("open_price"),
            F.expr("max_by(price, event_time)").alias("close_price"),
            F.max("delay_ms").alias("max_delay_ms"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("trade_count"),
            F.col("total_volume"),
            # VWAP = giá bình quân gia quyền theo khối lượng.
            F.when(F.col("total_volume") > 0, F.col("notional") / F.col("total_volume")).alias("vwap"),
            F.col("open_price"),
            F.col("close_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("avg_price"),
            F.col("max_delay_ms"),
        )
    )


def quality_events(clean_df: DataFrame) -> DataFrame:
    """
    Bản ghi cần cách ly: hỏng khi làm sạch, hoặc trễ hơn ngưỡng watermark.

    Lưu ý 'late' ở đây đo bằng độ trễ xử lý (giờ Kafka nhận − giờ trade xảy ra),
    nên là cảnh báo chứ không phải kết luận: bản ghi trễ vẫn có thể kịp vào cửa
    sổ nếu watermark — vốn chạy theo event time lớn nhất đã thấy — chưa vượt qua
    cửa sổ đó. Số dòng thực sự bị loại do WatermarkDropListener báo. Giữ lại
    dữ liệu ở đây để còn nạp bù (backfill) khi cần.
    """
    watermark_ms = config.WATERMARK_SECONDS * 1000

    return (
        clean_df.filter(F.col("reject_reason").isNotNull() | (F.col("delay_ms") > watermark_ms))
        .select(
            F.when(F.col("reject_reason").isNotNull(), F.lit("rejected"))
            .otherwise(F.lit("late"))
            .alias("event_type"),
            F.coalesce(F.col("reject_reason"), F.lit("delay_exceeds_watermark")).alias("reason"),
            F.col("symbol"),
            F.col("price"),
            F.col("volume"),
            F.col("event_time"),
            F.col("ingest_time"),
            F.col("delay_ms"),
            F.col("payload_bytes"),
        )
    )


# --- 4. Ghi xuống PostgreSQL ---------------------------------------------------

# Cửa sổ đã ghi vẫn có thể được tính lại khi trade đến muộn, nên phải upsert:
# ghi đè bảng đệm rồi MERGE một phát sang bảng chính -> chạy lại batch cũ vẫn ra
# đúng một dòng cho mỗi (symbol, window_start).
UPSERT_SQL = f"""
INSERT INTO {config.METRICS_TABLE} AS m (
    symbol, window_start, window_end, trade_count, total_volume, vwap,
    open_price, close_price, min_price, max_price, avg_price, max_delay_ms, updated_at
)
SELECT
    symbol, window_start, window_end, trade_count, total_volume, vwap,
    open_price, close_price, min_price, max_price, avg_price, max_delay_ms, now()
FROM {config.METRICS_STAGING_TABLE}
ON CONFLICT (symbol, window_start) DO UPDATE SET
    window_end   = EXCLUDED.window_end,
    trade_count  = EXCLUDED.trade_count,
    total_volume = EXCLUDED.total_volume,
    vwap         = EXCLUDED.vwap,
    open_price   = EXCLUDED.open_price,
    close_price  = EXCLUDED.close_price,
    min_price    = EXCLUDED.min_price,
    max_price    = EXCLUDED.max_price,
    avg_price    = EXCLUDED.avg_price,
    max_delay_ms = EXCLUDED.max_delay_ms,
    updated_at   = now()
"""


def jdbc_write(df: DataFrame, table: str, mode: str) -> None:
    writer = (
        df.write.format("jdbc")
        .option("url", config.JDBC_URL)
        .option("dbtable", table)
        .option("user", config.PG_USER)
        .option("password", config.PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
    )
    if mode == "overwrite":
        # truncate=true: xoá dữ liệu bảng đệm nhưng giữ nguyên schema đã tạo sẵn.
        writer = writer.option("truncate", "true")
    writer.mode(mode).save()


def execute_sql(spark: SparkSession, sql: str) -> None:
    """Chạy một câu SQL trên Postgres qua JDBC driver có sẵn trong JVM của Spark."""
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        config.JDBC_URL, config.PG_USER, config.PG_PASSWORD
    )
    try:
        statement = connection.createStatement()
        try:
            statement.execute(sql)
        finally:
            statement.close()
    finally:
        connection.close()


def upsert_metrics(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()
    try:
        rows = batch_df.count()
        if rows == 0:
            return
        jdbc_write(batch_df, config.METRICS_STAGING_TABLE, mode="overwrite")
        execute_sql(batch_df.sparkSession, UPSERT_SQL)
        logger.info("Batch %s: upsert %d cửa sổ vào %s", batch_id, rows, config.METRICS_TABLE)
    finally:
        batch_df.unpersist()


def write_quality(batch_df: DataFrame, batch_id: int) -> None:
    batch_df.persist()
    try:
        rows = batch_df.count()
        if rows == 0:
            return
        jdbc_write(batch_df, config.QUALITY_TABLE, mode="append")
        logger.warning("Batch %s: %d bản ghi hỏng/đến muộn -> %s", batch_id, rows, config.QUALITY_TABLE)
    finally:
        batch_df.unpersist()


# --- 5. Khởi chạy --------------------------------------------------------------

class WatermarkDropListener(StreamingQueryListener):
    """
    Báo số dòng bị watermark loại khỏi phép gộp — bằng chứng chắc chắn rằng dữ
    liệu đã bị bỏ, khác với nhãn 'late' chỉ ước lượng theo độ trễ xử lý.

    Spark đếm ở tầng state store, tức là sau khi đã gộp cục bộ, nên nhiều trade
    cùng (symbol, cửa sổ) trong một batch chỉ tính là một dòng: con số này nhỏ
    hơn hoặc bằng số bản ghi gốc bị mất.
    """

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        try:
            dropped = sum(op.numRowsDroppedByWatermark for op in event.progress.stateOperators)
        except Exception:  # đổi API giữa các bản Spark thì bỏ qua, không làm chết query
            return
        if dropped:
            logger.warning(
                "Query %s batch %s: %d dòng bị loại vì đến sau watermark",
                event.progress.name, event.progress.batchId, dropped,
            )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("FinnhubTradeMetrics")
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
        # Toàn bộ pipeline (Kafka, cửa sổ, Postgres) tính theo UTC cho khỏi lệch giờ.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.LOG_LEVEL)
    return spark


def run() -> None:
    spark = build_spark()
    spark.streams.addListener(WatermarkDropListener())
    clean_df = clean(read_trades(spark))

    trigger = f"{config.TRIGGER_SECONDS} seconds"

    metrics_query = (
        aggregate(clean_df).writeStream
        # update: mỗi batch chỉ đẩy ra cửa sổ vừa thay đổi (kể cả cửa sổ cũ được
        # trade muộn cập nhật lại), thay vì phát lại toàn bộ như complete mode.
        .outputMode("update")
        .foreachBatch(upsert_metrics)
        .option("checkpointLocation", f"{config.CHECKPOINT_LOCATION}/metrics")
        .trigger(processingTime=trigger)
        .queryName("metrics")
        .start()
    )

    quality_query = (
        quality_events(clean_df).writeStream
        .outputMode("append")
        .foreachBatch(write_quality)
        .option("checkpointLocation", f"{config.CHECKPOINT_LOCATION}/quality")
        .trigger(processingTime=trigger)
        .queryName("quality")
        .start()
    )

    logger.info(
        "Đang đọc '%s' tại %s | cửa sổ %ds, watermark %ds, trigger %ds -> %s",
        config.KAFKA_TOPIC, config.KAFKA_SERVER,
        config.WINDOW_SECONDS, config.WATERMARK_SECONDS, config.TRIGGER_SECONDS,
        config.JDBC_URL,
    )

    # Query nào chết trước thì ném lỗi ở đây, container restart theo policy của compose.
    spark.streams.awaitAnyTermination()
    metrics_query.stop()
    quality_query.stop()


if __name__ == "__main__":
    run()
