import os
import sys
import django
import requests
import logging

# ── Django 초기화 ──────────────────────────────────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

# ── Django 초기화 이후에 ORM import ────────────────────────
from market_data.models import Market

# ── 로깅 설정 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── 업비트 API ─────────────────────────────────────────────
UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all?is_details=true"


def fetch_markets():
    """업비트 마켓 목록 API 호출"""
    response = requests.get(UPBIT_MARKET_URL, headers={"accept": "application/json"})
    response.raise_for_status()
    return response.json()


def parse_market(item):
    """API 응답 한 건을 DB 컬럼에 맞게 파싱"""
    caution = item.get("market_event", {}).get("caution", {})
    return {
        "korean_name":   item["korean_name"],
        "english_name":  item["english_name"],
        "warning":       item.get("market_event", {}).get("warning", False),
        "price_fluc":    caution.get("PRICE_FLUCTUATIONS", False),
        "vol_soar":      caution.get("TRADING_VOLUME_SOARING", False),
        "dep_amt_soar":  caution.get("DEPOSIT_AMOUNT_SOARING", False),
        "global_diff":   caution.get("GLOBAL_PRICE_DIFFERENCES", False),
        "small_acc_conc": caution.get("CONCENTRATION_OF_SMALL_ACCOUNTS", False),
    }


def sync_markets():
    """마켓 목록 동기화 메인 함수"""
    logger.info("마켓 동기화 시작")

    markets = fetch_markets()
    created_count = 0
    updated_count = 0

    for item in markets:
        _, created = Market.objects.update_or_create(
            market=item["market"],
            defaults=parse_market(item)
        )
        if created:
            created_count += 1
        else:
            updated_count += 1

    logger.info(f"동기화 완료 - 신규: {created_count}건 / 업데이트: {updated_count}건")


if __name__ == "__main__":
    sync_markets()