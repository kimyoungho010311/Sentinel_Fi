import os
import sys
import django
import json
import logging
import time
from confluent_kafka import Consumer, KafkaError

# ── Django 초기화 ──────────────────────────────────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from market_data.models import Market, TickerDate, BadTickerDate

# ── 로깅 설정 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Kafka 설정 ─────────────────────────────────────────────
KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC  = "ticker"
GROUP_ID     = "sentinel-fi-consumer"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "latest"
})
consumer.subscribe([KAFKA_TOPIC])

def validate_data(data):
    """UPBIT 데이터 무결성 검사 후 발생한 '모든' 에러 원인 리스트 반환"""
    errors = []

    # 1. 필수 필드 체크 (return하지 않고 리스트에 append)
    required_fields = ['code', 'trade_price', 'timestamp']
    for field in required_fields:
        if data.get(field) is None:
            errors.append(f"Missing Field: {field}")

    # 2. 음수 체크
    if data.get('trade_price', 0) < 0 or data.get('trade_volume', 0) < 0:
        errors.append("Negative Value Error: price or volume is negative")

    # 3. 범주형 데이터 체크
    if data.get('change') not in {'RISE', 'EVEN', 'FALL'}:
        errors.append(f"Invalid Change Type: {data.get('change')}")
        
    if data.get('stream_type') not in {'SNAPSHOT', 'REALTIME'}:
        errors.append(f"Invalid Stream Type: {data.get('stream_type')}")

    # 에러가 없으면 빈 리스트 []가 반환됩니다.
    return errors



def save_to_ticker_date(data):
    """Kafka 메시지 파싱 후 DB 저장"""
    try:
        market = Market.objects.get(market=data["code"])

        TickerDate.objects.create(
            market_code        = market,
            data_type          = data.get("type"),
            opening_price      = data.get("opening_price"),
            high_price         = data.get("high_price"),
            low_price          = data.get("low_price"),
            trade_price        = data.get("trade_price"),
            prev_closing_price = data.get("prev_closing_price"),
            acc_trade_price    = data.get("acc_trade_price"),
            change             = data.get("change"),
            change_price       = data.get("change_price"),
            signed_change_price= data.get("signed_change_price"),
            change_rate        = data.get("change_rate"),
            signed_change_rate = data.get("signed_change_rate"),
            ask_bid            = data.get("ask_bid"),
            trade_volume       = data.get("trade_volume"),
            acc_trade_volume   = data.get("acc_trade_volume"),
            trade_date         = data.get("trade_date"),
            trade_time         = data.get("trade_time"),
            trade_timestamp    = data.get("trade_timestamp"),
            timestamp          = data.get("timestamp"),
            acc_ask_volume     = data.get("acc_ask_volume"),
            acc_bid_volume     = data.get("acc_bid_volume"),
            highest_52_week_price = data.get("highest_52_week_price"),
            highest_52_week_date  = data.get("highest_52_week_date"),
            lowest_52_week_price  = data.get("lowest_52_week_price"),
            lowest_52_week_date   = data.get("lowest_52_week_date"),
            market_state          = data.get("market_state"),
            is_trading_suspended  = data.get("is_trading_suspended"),
            delisting_date        = data.get("delisting_date"),
            market_warning        = data.get("market_warning"),
            acc_trade_price_24h   = data.get("acc_trade_price_24h"),
            acc_trade_volume_24h  = data.get("acc_trade_volume_24h"),
            stream_type           = data.get("stream_type"),
        )
        logger.debug(f"저장 완료: {data.get('code')} | {data.get('trade_price')}")

    except Market.DoesNotExist:
        logger.warning(f"Market 없음: {data.get('code')} - 스킵")
    except Exception as e:
        logger.error(f"저장 실패: {e} | 데이터: {data}")

def save_to_bad_ticker_date(data, error_log):
    BadTickerDate.objects.create(
        raw_data = data,
        error_log = error_log
    )

def run():
    logger.info("Kafka 컨슈머 시작")
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka 에러: {msg.error()}")
                continue

            data = json.loads(msg.value().decode("utf-8"))
            # 지연시간 측정 용도
            # latency = (time.time() * 1000) - data.get('timestamp')
            # print(f"데이터 지연: {latency:.0f}ms")

            errors = validate_data(data)

            if not errors:
                save_to_ticker_date(data)
            else:
                error_message = ' | '.join(errors)
                save_to_bad_ticker_date(data, error_message)

    except KeyboardInterrupt:
        logger.info("컨슈머 종료")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()