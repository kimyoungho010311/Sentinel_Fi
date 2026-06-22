import os
import json
import time
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common import Row


def parse_metric_event(msg):
    try:
        data = json.loads(msg)
    except Exception:
        return {
            "is_error": True,
            "market": "UNKNOWN",
            "latency_ms": 0.0,
            "trade_amount_krw": 0.0,
        }

    market = data.get("code") or data.get("market") or "UNKNOWN"
    trade_price = data.get("trade_price")
    trade_volume = data.get("trade_volume")
    timestamp = data.get("timestamp")

    if trade_price is None or trade_volume is None or timestamp is None:
        return {
            "is_error": True,
            "market": market,
            "latency_ms": 0.0,
            "trade_amount_krw": 0.0,
        }

    try:
        trade_price = float(trade_price)
        trade_volume = float(trade_volume)
        timestamp = int(timestamp)
    except (TypeError, ValueError):
        return {
            "is_error": True,
            "market": market,
            "latency_ms": 0.0,
            "trade_amount_krw": 0.0,
        }

    if trade_price <= 0 or trade_volume < 0 or timestamp <= 0:
        return {
            "is_error": True,
            "market": market,
            "latency_ms": 0.0,
            "trade_amount_krw": 0.0,
        }

    now_ms = time.time() * 1000
    latency_ms = max(0.0, now_ms - timestamp)
    trade_amount_krw = trade_price * trade_volume

    return {
        "is_error": False,
        "market": market,
        "latency_ms": latency_ms,
        "trade_amount_krw": trade_amount_krw,
    }


class FlinkMarketMetricWindow(ProcessWindowFunction):
    def process(self, key, context, elements):
        records = list(elements)

        collected_count = len(records)

        error_count = sum(
            1 for record in records
            if record["is_error"]
        )

        valid_records = [
            record for record in records
            if not record["is_error"]
        ]

        latency_values = [
            record["latency_ms"]
            for record in valid_records
        ]

        average_latency_ms = (
            sum(latency_values) / len(latency_values)
            if latency_values
            else 0.0
        )

        max_latency_ms = (
            max(latency_values)
            if latency_values
            else 0.0
        )

        trade_volume_krw = sum(
            record["trade_amount_krw"]
            for record in valid_records
        )

        error_rate_percentage = (
            (error_count / collected_count) * 100
            if collected_count > 0
            else 0.0
        )

        checked_at = datetime.fromtimestamp(context.window().end / 1000)

        yield Row(
            checked_at,
            key,
            "1s",
            int(collected_count),
            int(error_count),
            float(error_rate_percentage),
            float(average_latency_ms),
            float(max_latency_ms),
            float(trade_volume_krw),
        )


def run_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)
    env.get_checkpoint_config().set_checkpoint_storage_dir("file:///opt/flink/checkpoints")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_servers) \
        .set_topics("ticker") \
        .set_group_id("market_metric_collection_group_v1") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    text_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Market Metric Source"
    )

    parsed_stream = text_stream.map(
        parse_metric_event,
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    market_metric_stream = parsed_stream \
        .key_by(lambda record: record["market"]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(1))) \
        .process(
            FlinkMarketMetricWindow(),
            output_type=Types.ROW([
                Types.SQL_TIMESTAMP(),
                Types.STRING(),
                Types.STRING(),
                Types.LONG(),
                Types.INT(),
                Types.DOUBLE(),
                Types.DOUBLE(),
                Types.DOUBLE(),
                Types.DOUBLE(),
            ])
        )

    jdbc_sink = JdbcSink.sink(
        """
        INSERT INTO flink_market_metric (
            checked_at,
            market,
            time_unit,
            collected_count,
            error_count,
            error_rate_percentage,
            average_latency_ms,
            max_latency_ms,
            trade_volume_krw
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        Types.ROW([
            Types.SQL_TIMESTAMP(),
            Types.STRING(),
            Types.STRING(),
            Types.LONG(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
        ]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url("jdbc:postgresql://sentinel_fi_db:5432/sentinel_fi_db")
            .with_driver_name("org.postgresql.Driver")
            .with_user_name("ssafy")
            .with_password("1q2w3e4r")
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_size(50)
            .with_batch_interval_ms(1000)
            .build()
    )

    market_metric_stream.add_sink(jdbc_sink)

    env.execute("Market Metric Collection Processing Job")


if __name__ == "__main__":
    run_pipeline()