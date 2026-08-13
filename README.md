# Finnhub streaming data pipeline

Trade realtime từ Finnhub → Kafka (Avro) → Spark Structured Streaming → PostgreSQL.

```
Finnhub websocket ──► producer ──► Kafka topic 'market' ──► Spark ──► PostgreSQL
                    (Avro encode)      (Avro binary)      (2 query)   (trade_metrics,
                                                                      trade_quality_events)
```

| Thành phần | Thư mục | Việc của nó |
|---|---|---|
| producer | [finnhubproducer/](finnhubproducer/) | Nghe websocket Finnhub, encode Avro, đẩy vào Kafka (key = symbol) |
| spark job | [sparkstreaming/](sparkstreaming/) | Decode Avro, làm sạch, gộp theo cửa sổ thời gian, ghi Postgres |
| schema | [finnhubproducer/src/schemas/trades.avsc](finnhubproducer/src/schemas/trades.avsc) | Dùng chung cho cả encode lẫn decode |
| bảng đích | [postgres/init/01_schema.sql](postgres/init/01_schema.sql) | Tạo sẵn bảng, khoá chính, index |

## Chạy

```bash
echo "FINNHUB_API_TOKEN=<token của bạn>" > .env
docker compose up -d --build
docker compose logs -f spark-streaming
```

- Spark UI: http://localhost:4040 (tab *Structured Streaming*)
- Postgres: `psql -h localhost -p 5434 -U finnhub -d finnhub` (mật khẩu `finnhub`)

```sql
SELECT * FROM trade_metrics_latest;                              -- nến mới nhất mỗi mã
SELECT * FROM trade_metrics ORDER BY window_start DESC LIMIT 20;
SELECT event_type, reason, count(*) FROM trade_quality_events GROUP BY 1, 2;
```

## Spark job làm gì

[sparkstreaming/src/stream.py](sparkstreaming/src/stream.py) chạy 2 streaming query trên cùng topic:

**1. Decode Avro.** `from_avro` với chính file `.avsc` của producer, `mode=PERMISSIVE`
để byte hỏng thành `null` chứ không giết query. Producer ghi Avro binary trần
(không có magic byte của Schema Registry) nên không cần Schema Registry.

**2. Làm sạch.** Mỗi bản ghi được gắn `reject_reason` thay vì bị lọc bỏ im lặng:

| reason | Điều kiện |
|---|---|
| `avro_decode_failed` | không decode được |
| `missing_symbol` | symbol rỗng/null (sau `trim` + `upper`) |
| `invalid_price` | null, NaN hoặc ≤ 0 |
| `invalid_volume` | null, NaN hoặc < 0 |
| `invalid_timestamp` | null hoặc trước `MIN_EVENT_MS` (mặc định 2000-01-01) |
| `timestamp_in_future` | vượt giờ Kafka nhận quá `MAX_FUTURE_SKEW_SECONDS` |

**3. Gộp theo cửa sổ thời gian.** Tumbling window trên *event time* (`trade.timestamp`,
epoch ms), không phải giờ xử lý. Mỗi (symbol, cửa sổ) cho ra: `trade_count`,
`total_volume`, `vwap`, OHLC (`open`/`close` lấy bằng `min_by`/`max_by` theo event
time nên không phụ thuộc thứ tự bản ghi đến), `avg_price`, `max_delay_ms`.

**4. Dữ liệu đến muộn.** Ba lớp xử lý:

- `withWatermark(WATERMARK_SECONDS)` — trade muộn trong ngưỡng vẫn được cộng vào
  cửa sổ cũ; watermark chạy theo event time lớn nhất đã thấy, không theo đồng hồ.
- Sink upsert (`ON CONFLICT (symbol, window_start) DO UPDATE`) — vì một cửa sổ có
  thể bị tính lại nhiều lần, ghi append sẽ sinh dòng trùng. Chạy lại batch cũ cũng
  ra đúng một dòng cho mỗi cửa sổ.
- Bản ghi trễ quá ngưỡng vào bảng `trade_quality_events` (`event_type='late'`) để
  còn nạp bù. Nhãn này dựa trên độ trễ xử lý nên là *cảnh báo*; số dòng thực sự bị
  loại do `WatermarkDropListener` đọc từ metric `numRowsDroppedByWatermark` của
  Spark và log ra.

**5. Ghi Postgres.** Không có JDBC sink cho streaming nên dùng `foreachBatch`:
ghi đè bảng đệm `trade_metrics_staging` rồi `INSERT ... ON CONFLICT` một phát sang
`trade_metrics`. Postgres hợp lý ở đây vì kết quả đã gộp nên khối lượng ghi nhỏ,
lại cần upsert theo khoá và truy vấn SQL/BI ngay trên đó.

### Vì sao chọn thế này

- **Postgres thay vì append vào file/warehouse**: cửa sổ phải cập nhật được sau khi
  đã ghi, nên cần khoá chính + upsert. Nếu sau này dữ liệu lên hàng trăm triệu dòng
  thì đổi sang TimescaleDB/ClickHouse mà không phải sửa logic Spark.
- **Cửa sổ tumbling, không sliding**: mỗi trade thuộc đúng một cửa sổ → state nhỏ,
  bảng kết quả đọc như nến OHLC quen thuộc.
- **Chạy `local[*]`**: đủ cho môi trường học. Lên cluster chỉ đổi `--master` trong
  [sparkstreaming/Dockerfile](sparkstreaming/Dockerfile), code không phải sửa.

## Cấu hình

Chỉnh trong [docker-compose.yml](docker-compose.yml):

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `WINDOW_SECONDS` | 60 | Độ dài cửa sổ |
| `WATERMARK_SECONDS` | 60 | Ngưỡng chấp nhận dữ liệu đến muộn |
| `TRIGGER_SECONDS` | 20 | Chu kỳ micro-batch |
| `KAFKA_STARTING_OFFSETS` | earliest | Chỉ có tác dụng ở lần chạy đầu, sau đó theo checkpoint |
| `MAX_OFFSETS_PER_TRIGGER` | 100000 | Trần bản ghi mỗi batch, tránh nghẽn khi backlog lớn |
| `MAX_FUTURE_SKEW_SECONDS` | 60 | Ngưỡng bắt timestamp tương lai |

Checkpoint nằm trên volume `spark_checkpoints` (offset Kafka + state các cửa sổ đang
mở). Muốn chạy lại từ đầu:

```bash
docker compose down -v          # xoá luôn dữ liệu Kafka và Postgres
```

## Phát triển ở máy local

```bash
uv sync --extra spark    # extra 'spark' chỉ để gợi ý code; trong container PySpark lấy từ image
```

Phiên bản PySpark trong [pyproject.toml](pyproject.toml) phải khớp `SPARK_VERSION`
trong [sparkstreaming/Dockerfile](sparkstreaming/Dockerfile) (hiện tại 3.5.9) — các jar
connector (`spark-sql-kafka`, `spark-avro`, JDBC Postgres) được tải cố định phiên bản
vào image lúc build, không dùng `--packages` lúc chạy.
