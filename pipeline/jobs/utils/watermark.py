from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration

class TickerTimestampAssigner(TimestampAssigner):
    """
    딕셔너리 데이터 내부의 'timestamp' 값을 파싱하여
    플링크 엔진의 이벤트 시간 기준으로 등록하는 클래스
    """

    def extract_timestamp(self, element, record_timestamp):
        # 업비트 데이터의 'timestamp' (ms) 추출
        # 만약 문자열이나 다른 형태로 들어올 수 있으니 안전하게 int로 변환
        return int(element.get('timestamp', record_timestamp))

def get_ticker_watermark_strategy():
    """
    1초의 데이터 지연을 허용하는
    워터마크 전략을 생성하여 반환합니다.
    """
    return (WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(1))
            .with_timestamp_assigner(TickerTimestampAssigner()))