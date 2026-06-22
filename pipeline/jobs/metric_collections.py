import os
import json
import time
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common import Row

class FlinkRealTimeMetricWindow(ProcessAllWindowFunction):
    def process(self, context, elements):
        records = list(elements)

        total_collected_count = len(records)

        total_error_count = sum(
            1 for record in records
            if record['is_error']
        )

        valid_records = [
            record for record in records
            if not record["is_error"]
        ]

        # TODO 1: latency 목록 만들기
        latency_values = [
            record["latency_ms"]
            for record in valid_records
        ]

        # TODO 2: 평균 latency
        average_latency_ms = (
            sum(latency_values) / len(latency_values)
            if latency_values
            else 0.0
        )

        # TODO 3: max latency
        max_latency_ms = max(latency_values) if latency_values else 0.0

        # TODO 4: 고유 마켓 수
        unique_market_count = len({
            record['market']
            for record in valid_records
            if record['market'] is not None
        })

        # TODO 5: 에어율
        error_rate_percentage = (
            (total_error_count / total_collected_count) * 100
            if total_collected_count > 0
            else 0.0
        )

        # TODO 6: 윈도우 종료 시각
        checked_at = datetime.fromtimestamp(
            context.window().end / 1000
        )

        total_trade_volume_krw = sum(
            record['trade_amount_krw']
            for record in valid_records
        )

        yield Row(
            checked_at,
            "1s",
            total_collected_count,
            total_error_count,
            error_rate_percentage,
            average_latency_ms,
            max_latency_ms,
            unique_market_count,
            total_trade_volume_krw,
        )

def parse_metric_event(msg):
    try:
        data = json.loads(msg)
    except Exception:
        return {
            "is_error": True,
            "market": None,
            "latency_ms": 0.0,
            "trade_amount_krw": 0.0,
        }

    market = data.get("code") or data.get("market")
    trade_price = data.get("trade_price")
    trade_volume = data.get("trade_volume")
    timestamp = data.get("timestamp")

    # TODO 1
    # market, trade_price, trade_volume, timestamp 중 하나라도 없으면 error 처리
    if market is None or trade_price is None or trade_volume is None or timestamp is None:
        return {
            "is_error": True,
            "market": market,
            "latency_ms": 0.0,
            "trade_amount_krw": 0.0,
        }

    # TODO 2
    # trade_price, trade_volume, timestamp를 숫자로 변환
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

    # TODO 3
    # trade_price <= 0, trade_volume < 0, timestamp <= 0 이면 error 처리
    if trade_price <= 0 or trade_volume < 0 or timestamp <= 0:
        return {
            "is_error": True,
            "market": market,
            "latency_ms": 0.0,
            "trade_amount_krw": 0.0,
        }

    # TODO 4
    # latency_ms = 현재시간(ms) - timestamp
    now_ms = time.time() * 1000
    latency_ms = max(0.0 , now_ms - timestamp) # 시스템 시간이 이상하거나 timestamp가 미래로 들어오면 음수가 나올 수 있음으로 이런식으로 방어한다고 한다..

    # TODO 5
    # trade_amount_krw = trade_price * trade_volume
    trade_amount_krw = trade_price * trade_volume

    return {
        "is_error": False,
        "market": market,
        "latency_ms": latency_ms,
        "trade_amount_krw": trade_amount_krw,
    }


def run_pipeline():
    # 1. 플링크 실행 환경 시동
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000) # 60초마다 체크포인트
    env.get_checkpoint_config().set_checkpoint_storage_dir("file:///opt/flink/checkpoints")   
    
    # ── [유격 수리] 도커 내부망 카프카 주소를 기본값으로 강제 지정 ────────────────
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # 2. 카프카 소스 설정 (독립된 그룹 지정)
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_servers) \
        .set_topics("ticker") \
        .set_group_id('metric_collection_group_debug_1') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
        
    # 3. 입력 스트림 개설 및 JSON 파싱
    text_stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.no_watermarks(), 
        "Kafka Metric Source"
    )

    # text_stream.print()

    parsed_stream = text_stream.map(
        parse_metric_event,
        output_type=Types.PICKLED_BYTE_ARRAY()
    )        

    metric_stream = parsed_stream.window_all(
        TumblingProcessingTimeWindows.of(Time.seconds(1))
    ).process(
        FlinkRealTimeMetricWindow(),
        output_type=Types.ROW([
            Types.SQL_TIMESTAMP(),
            Types.STRING(),
            Types.LONG(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.INT(),
            Types.DOUBLE(),
        ])
    )

    # metric_stream.print()

    jdbc_sink = JdbcSink.sink(
        """
        INSERT INTO flink_realtime_metric (
            checked_at,
            time_unit,
            total_collected_count,
            total_error_count,
            error_rate_percentage,
            average_latency_ms,
            max_latency_ms,
            unique_market_count,
            total_trade_volume_krw
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        Types.ROW([
            Types.SQL_TIMESTAMP(),
            Types.STRING(),
            Types.LONG(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.INT(),
            Types.DOUBLE(),
        ]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url("jdbc:postgresql://sentinel_fi_db:5432/sentinel_fi_db")
            .with_driver_name("org.postgresql.Driver")
            .with_user_name("ssafy")
            .with_password("1q2w3e4r")
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_size(1)
            .with_batch_interval_ms(10000)
            .build()
    )

    metric_stream.add_sink(jdbc_sink)

    env.execute('Metric Collection Processing Job')

if __name__ == '__main__':
    run_pipeline()