import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration, Time
from pyflink.common.typeinfo import Types
from pyflink.common import Row

# 워터마크 전략
from utils.watermark import get_ticker_watermark_strategy
# 급락 탐지 로직
from utils.metrics import DropDetectorAggregate, WindowMetaFunction
from pyflink.datastream.functions import AggregateFunction

def run_pipeline():
    # 1. Kafka 소스 연결 (ticker 토픽)
    env = StreamExecutionEnvironment.get_execution_environment()
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Kafka 소스 설정
    # 이미 있는 토픽(ticker)를 구독하겠다
    # kafka_source는 Consumer 역할을 한다. 즉, 'ticker' 토픽에서 데이터가 흐를때 거기서 가져오겠다는 의미를 가진다.
    # 기본적으로 source코드를 짤 때는 이미 존재하는 Topic에서 데이터를 읽어온다 라고 생각한다.
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_servers) \
        .set_topics("ticker") \
        .set_group_id('drop_detect_group') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
        
    # 2. JSON 파싱 (문자열 → 딕셔너리)
    text_stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.no_watermarks(), 
        "Kafka Source"
    )
    parsed_stream = text_stream.map(lambda msg: json.loads(msg))
    # parsed_stream.print() # 디버깅용: 데이터가 잘 흘러들어 오는지 도커 taskManager log에서 확인 가능

    # 3. timestamp 기준 Event Time + 워터마크 1초 설정
    timed_stream = parsed_stream.assign_timestamps_and_watermarks(
        get_ticker_watermark_strategy()
    )
    # timed_stream.print()
    # 4. market 코드별로 KeyBy
    #    → 코인마다 따로 윈도우 계산해야 하니까
    keyed_stream = timed_stream.key_by(lambda msg: msg['code'])

    # 5. 슬라이딩 윈도우 (크기 5분, 슬라이드 1분)
    windowed_stream = keyed_stream.window(
        SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))
    )
    # 6. 윈도우 내 첫 가격 / 마지막 가격 추출
    #    → timestamp 가장 작은 것 / 가장 큰 것
    result_stream = windowed_stream.aggregate(
        DropDetectorAggregate(),
        WindowMetaFunction(),
        )

    jdbc_sink_window = JdbcSink.sink(
        'INSERT INTO window_stats (window_start, window_end, start_price, end_price, change_rate, status, market_code) VALUES (?, ?, ?, ?, ?, ?, ?)',
        Types.ROW([
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.STRING(),
            Types.STRING()
        ]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url("jdbc:postgresql://sentinel_fi_db:5432/sentinel_fi_db")
            .with_driver_name("org.postgresql.Driver")
            .with_user_name("ssafy")
            .with_password("1q2w3e4r")
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_size(50) # 50건씩 묶어서 넣기
            .with_batch_interval_ms(1000) # 혹은 1초마다 넣기
            .build()
    )

    window_row_stream = result_stream.map(
    lambda r: Row(
        r['window_start'],
        r['window_end'],
        float(r['start_price']),
        float(r['end_price']),
        float(r['change_rate']),
        r['status'],
        r['code']
    ),
    output_type=Types.ROW([
        Types.SQL_TIMESTAMP(),
        Types.SQL_TIMESTAMP(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
        Types.STRING()
    ])
)
    window_row_stream.add_sink(jdbc_sink_window)
    
    env.execute('Drop Detection Job')

if __name__ == '__main__':
    run_pipeline()