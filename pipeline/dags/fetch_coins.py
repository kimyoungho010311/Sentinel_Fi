from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, django, sys, os, logging

# DAG 기본 설정
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Django 초기화
DJANGO_PROJECT_PATH = '/opt/airflow/backend-pjt'
if DJANGO_PROJECT_PATH not in sys.path:
    sys.path.append(DJANGO_PROJECT_PATH)
os.environ["DJANGO_DB_HOST"] = "db"
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from market_data.models import Market

UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all?is_details=true"

with DAG(
    dag_id='sync_upbit_markets',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['market', 'upbit'],
) as dag:

    # ── 태스크 1: 업비트 API 호출 ──────────────────────────────────
    def fetch_upbit_markets(**context):
        # **context 에 대한 정보는 노션에 정리
        logger = logging.getLogger("airflow.task")
        logger.info("업비트 API 호출 시작 (전체 마켓 타겟)")

        response = requests.get(UPBIT_MARKET_URL, headers={"accept": "application/json"})
        response.raise_for_status()
        api_data = response.json()

        api_markets = [item["market"] for item in api_data]
        
        context['ti'].xcom_push(key='api_data', value=api_data)       
        context['ti'].xcom_push(key='api_markets', value=api_markets)  

        logger.info(f"수집된 업비트 전체 마켓 수: {len(api_markets)}개")

    # ── 태스크 2: DB 조회 및 비교 ──────────────────────────────────
    def compare_markets(**context):
        logger = logging.getLogger("airflow.task")

        api_markets = set(context['ti'].xcom_pull(
            task_ids='fetch_upbit_markets',
            key='api_markets'
        ))

        existing_markets = set(Market.objects.filter(is_active=True).values_list('market', flat=True))

        new_coins = api_markets - existing_markets    
        del_coins = existing_markets - api_markets    

        logger.info(f"[변경 감지 리포트] 신규 상장 발견: {new_coins}")
        logger.info(f"[변경 감지 리포트] 상장 폐지 감지: {del_coins}")

        context['ti'].xcom_push(key='new_coins', value=list(new_coins))
        context['ti'].xcom_push(key='del_coins', value=list(del_coins))

    # ── 태스크 3: DB 업데이트 ──────────────────────────────────────
    def update_markets(**context):
        logger = logging.getLogger("airflow.task")
        logger.info("전체 마켓 동기화 및 상장폐지 일괄 처리를 시작합니다.")

        api_data = context['ti'].xcom_pull(
            task_ids='fetch_upbit_markets',
            key='api_data'
        )
        
        current_api_markets = [item["market"] for item in api_data]

        created_count = 0
        updated_count = 0
        
        # 스텝 1: 살아있는 모든 마켓(KRW, BTC, USDT) 추가/변경 및 활성화
        for item in api_data:
            caution = item.get("market_event", {}).get("caution", {})
            defaults = {
                "korean_name":    item["korean_name"],
                "english_name":   item["english_name"],
                "warning":        item.get("market_event", {}).get("warning", False),
                "price_fluc":     caution.get("PRICE_FLUCTUATIONS", False),
                "vol_soar":       caution.get("TRADING_VOLUME_SOARING", False),
                "dep_amt_soar":   caution.get("DEPOSIT_AMOUNT_SOARING", False),
                "global_diff":    caution.get("GLOBAL_PRICE_DIFFERENCES", False),
                "small_acc_conc": caution.get("CONCENTRATION_OF_SMALL_ACCOUNTS", False),
                "is_active":      True, 
            }
            _, created = Market.objects.update_or_create(
                market=item["market"],
                defaults=defaults
            )
            if created:
                created_count += 1
            else:
                updated_count += 1
                
        logger.info(f"[생존 코인] 신규 적재: {created_count}건 / 기존 정보 갱신: {updated_count}건 완료.")

        # ── [핵심 구현] 스텝 2: 전체 시장 기준 상장 폐지 쿼리 집행 ──────────────────
        shutup_targets = Market.objects.filter(is_active=True).exclude(market__in=current_api_markets)
                
        del_coins_list = list(shutup_targets.values_list('market', flat=True))
        
        if del_coins_list:
            logger.warning(f"[상장 폐지 발생] 업비트 명단에서 누락된 코인 비활성화 진입: {del_coins_list}")
            updated_del_count = shutup_targets.update(is_active=False)
            logger.info(f"[상장 폐지 조치 완료] 총 {updated_del_count}개의 코인 비활성화 완료")
            context['ti'].xcom_push(key='actual_del_coins', value=del_coins_list)
        else:
            # 💡 이제 두 번째 돌릴 때부터는 이미 다 False로 내려갔기 때문에 이 아래 로그가 정상적으로 출력됩니다.
            logger.info("[상장 폐지 깨끗함] 새로운 상장 폐지 타겟 코인이 없습니다.")
            context['ti'].xcom_push(key='actual_del_coins', value=[])

    # ── 태스크 4: 결과 리포트 ──────────────────────────────────────
    def report(**context):
        logger = logging.getLogger("airflow.task")

        new_coins = context['ti'].xcom_pull(task_ids='compare_markets', key='new_coins')
        actual_del_coins = context['ti'].xcom_pull(task_ids='update_markets', key='actual_del_coins')

        logger.info("=" * 50)
        logger.info(f"[최종 리포트] 신규 상장 고지: {len(new_coins)}개 → {new_coins}")
        logger.info(f"[최종 리포트] 상장 폐지 고지: {len(actual_del_coins)}개 → {actual_del_coins}")
        logger.info("=" * 50)

    # 태스크 순서 배정
    task1 = PythonOperator(task_id='fetch_upbit_markets', python_callable=fetch_upbit_markets)
    task2 = PythonOperator(task_id='compare_markets',     python_callable=compare_markets)
    task3 = PythonOperator(task_id='update_markets',      python_callable=update_markets)
    task4 = PythonOperator(task_id='report',              python_callable=report)

    task1 >> task2 >> task3 >> task4