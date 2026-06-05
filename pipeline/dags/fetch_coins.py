from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import django
import sys
import os 

import logging
from utils import LogColor


# DAG 기본 설정
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='sync_upbit_markets',          # DAG 이름
    default_args=default_args,
    schedule_interval='0 1 * * *',        # 매일 새벽 1시
    start_date=datetime(2026, 1, 1),
    catchup=False,                         # 과거 실행 스킵
    tags=['market', 'upbit', 'fetch_coins'],
) as dag:


    def fetch_and_compare(**context):
        logger = logging.getLogger("airflow.task")
        logger.info(LogColor.info("SUCCSEFLLUY RUN fetch_and_compare!!!"))
        
        # ── Django 초기화 ──────────────────────────────────────────

        DJANGO_PROJECT_PATH = '/opt/airflow/backend-pjt'

        if DJANGO_PROJECT_PATH not in sys.path:
            sys.path.append(DJANGO_PROJECT_PATH)

        os.environ["DJANGO_DB_HOST"] = "db"

        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
        django.setup()

        # ── Django 초기화 이후에 ORM import ────────────────────────
        from market_data.models import Market

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

            logger.info(LogColor.info(f"동기화 완료 - 신규: {created_count}건 / 업데이트: {updated_count}건"))

        sync_markets()
        # 1. 업비트 API 호출
        # 2. DB 조회
        # 3. 비교
        # 4. 알림
    sync_task = PythonOperator(
        task_id='fetch_and_compare_markets',
        python_callable=fetch_and_compare,
    )