import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy  # 추가됨

def run_flink_test():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # 시스템 환경 변수 'KAFKA_BOOTSTRAP_SERVERS'를 읽어오고, 
    # 값이 없으면 기본값으로 'localhost:9092'를 사용합니다.
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # 1. Kafka 소스 설정
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_servers) \
        .set_topics("sentinel-test") \
        .set_group_id("sentinel_test_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 2. 데이터 스트림 생성 (WatermarkStrategy.for_monotonous_timestamps() 추가)
    # 두 번째 인자로 워터마크 전략을 명시해주어야 합니다.
    stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.for_monotonous_timestamps(), 
        "Kafka Source"
    )

    # 3. 결과 출력
    stream.print()

    env.execute("Flink-Kafka-Connection-Test")

if __name__ == '__main__':
    run_flink_test()