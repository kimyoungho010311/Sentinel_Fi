from django.db import models

class FlinkMetric(models.Model):
    # Flink가 연산을 확정한 과거 시점의 타임스탬프를 직접 기록하기 위해 db_index를 추가합니다.
    checked_at = models.DateTimeField(db_index=True, verbose_name='체크 타임스탬프')

    time_unit = models.CharField(max_length=5, default='1s')
    
    # 데이터 개수는 소수점이 없는 정수형이므로 BigIntegerField와 IntegerField로 명세합니다.
    total_collected_count = models.BigIntegerField(verbose_name='총 소비 데이터 개수')
    total_error_count = models.IntegerField(verbose_name='에러 수')
    
    # 에러율 및 추가 성능 메트릭 배관을 정의합니다.
    error_rate_percentage = models.FloatField(verbose_name='에러율 퍼센티지')
    
    # 시스템 성능 및 지연 속도 지표입니다.
    average_latency_ms = models.FloatField(verbose_name='평균 처리 지연 시간')
    max_latency_ms = models.FloatField(verbose_name='최대 처리 지연 시간')

    # 암호화폐 시장 요약 정보 지표입니다.
    unique_market_count = models.IntegerField(verbose_name='활성 마켓 수')
    total_trade_volume_krw = models.FloatField(verbose_name='초당 총 거래 대금')

    class Meta:
        db_table = 'flink_realtime_metric'
        ordering = ['-checked_at']