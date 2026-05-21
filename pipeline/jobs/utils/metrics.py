from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from datetime import datetime, timezone
import os
class DropDetectorAggregate(AggregateFunction):
    """
    5분 윈도우 바구니 내부에서 가장 첫 가격과 마지막 가격을 찾아
    변동률을 계산하고 경고 등급을 매기는 요리사 클래스
    """

    def create_accumulator(self):
        # [중간 장소 초기화] (가장 작은 값용, 가장 큰 값용)
        # (최초_데이터, 최신_데이터) 구조로 저장합니다.
        return (None, None)

    def add(self, value, accumulator):
        # print(f"데이터 유입 완료 코인: {value.get('code')}, 데이터 시각(TS): {value.get('timestamp')}")
        # 데이터가 바구니에 한 건씩 들어올 때마다 실행됩니다.
        first_data, last_data = accumulator

        # 1. 첫 데이터 채우기 (기준점 잡기)
        if first_data is None or value['timestamp'] < first_data['timestamp']:
            first_data = value

        # 2. 최신 데이터 갱신하기
        if last_data is None or value['timestamp'] > last_data['timestamp']:
            last_data = value

        return (first_data, last_data)

    def get_result(self, accumulator):
        # 5분이 끝나고 최종 결과물을 낼 때 딱 한 번 실행됩니다.
        # print(f"실제 실행중인 metrics.py의 위치: {os.path.abspath(__file__)}")
        first_data, last_data = accumulator

        if first_data is None or last_data is None:
            return None

        start_price = float(first_data['trade_price'])
        end_price = float(last_data['trade_price'])
        
        if start_price == 0:
            return None

        # 7단계: 변동률 계산
        change_rate = ((end_price - start_price) / start_price) * 100

        # 8단계: 업비트 기준 등급 판단
        status = "normal"

        if change_rate >= 3.0:
            status = 'danger'
        elif change_rate >= 1.0:
            status = "warning"
        elif change_rate >= 0.5:
            status = 'caution'
        elif change_rate <= -3.0:
            status = 'danger'
        elif change_rate <= -1.0:
            status = 'warning'
        elif change_rate <= -0.5:
            status = 'caution'

        return {
            "code": first_data['code'],
            "start_price": start_price,
            "end_price": end_price,
            "change_rate": round(change_rate, 2),
            "status": status
        }

    def merge(self, a, b): 
        # 분산 환경에서 결과를 합칠 때 쓰는 함수 (형식상 구현)
        return a
    
class WindowMetaFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        # elements에는 AggregateFunction의 get_result() 결과가 들어온다.
        result = list(elements)[0]

        if result is None:
            return

        # 윈도우 시작/끝 시간 (밀리초 단위)
        window_start = context.window().start
        window_end = context.window().end

        # 밀리초 -> datetime으로 변환
        window_start_dt = datetime.fromtimestamp(window_start / 1000, tz=timezone.utc)
        window_end_dt = datetime.fromtimestamp(window_end / 1000, tz=timezone.utc)

        result['window_start'] = window_start_dt
        result['window_end'] = window_end_dt

        yield result