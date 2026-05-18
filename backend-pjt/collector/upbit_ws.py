import os
import sys
import django
import asyncio
import json
import logging
from confluent_kafka import Producer

# ── Django 초기화 ──────────────────────────────────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from market_data.models import Market
from asgiref.sync import sync_to_async

# ── 로깅 설정 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── 설정 ───────────────────────────────────────────────────
UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"
KAFKA_BROKER  = "localhost:29092"
KAFKA_TOPIC   = "ticker"


# ── Kafka Producer ─────────────────────────────────────────
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def delivery_report(err, msg):
    if err:
        logger.error(f"Kafka 전송 실패: {err}")


# ── 마켓 코드 조회 ─────────────────────────────────────────
def get_market_codes():
    codes = list(Market.objects.values_list("market", flat=True))
    logger.info(f"구독할 마켓 수: {len(codes)}개")
    return codes


# ── WebSocket 수신 ─────────────────────────────────────────
async def listen():
    import websockets

    codes = await sync_to_async(get_market_codes)()

    subscribe_msg = json.dumps([
        {"ticket": "sentinel-fi"},
        {"type": "ticker", "codes": codes},
        {"format": "DEFAULT"}
    ])

    while True:
        try:
            async with websockets.connect(UPBIT_WS_URL) as ws:
                await ws.send(subscribe_msg)
                logger.info("업비트 WebSocket 연결 완료")

                while True:
                    raw = await ws.recv()
                    data = json.loads(raw)

                    producer.produce(
                        KAFKA_TOPIC,
                        key=data.get("cd", "unknown"),       # 마켓 코드 (KRW-BTC 등)
                        value=json.dumps(data, ensure_ascii=False),
                        callback=delivery_report
                    )
                    producer.poll(0)

                    logger.debug(f"수신: {data.get('cd')} | 현재가: {data.get('tp')}")

        except Exception as e:
            logger.error(f"연결 끊김: {e} | 3초 후 재연결 시도")
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(listen())