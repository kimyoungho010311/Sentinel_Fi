from django.db import models

class Market(models.Model):
    market = models.CharField(max_length=20, primary_key=True)
    korean_name = models.CharField(max_length=255)
    english_name = models.CharField(max_length=255)
    warning = models.BooleanField(default=False, null=True)
    price_fluc = models.BooleanField(default=False, null=True)
    vol_soar = models.BooleanField(default=False, null=True)
    dep_amt_soar = models.BooleanField(default=False, null=True)
    global_diff = models.BooleanField(default=False, null=True)
    small_acc_conc = models.BooleanField(default=False, null=True)

    class Meta:
        db_table = 'market'

class TickerDate(models.Model):
    # cd: 마켓 코드 (예: KRW-BTC) -> 외래키 연결
    market_code = models.ForeignKey(Market, on_delete=models.CASCADE, db_column='market_code')
    
    # ty: 데이터 타입 (ticker)
    data_type = models.CharField(max_length=20, default='ticker')
    
    # [가격 관련 필드]
    opening_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="시가(op)")
    high_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="고가(hp)")
    low_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="저가(lp)")
    trade_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="현재체결가(tp)")
    prev_closing_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="전일종가(pcp)")
    
    # [변동 정보 필드]
    change = models.CharField(max_length=10, verbose_name="전일대비상태(c)") # RISE, EVEN, FALL
    change_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="전일대비변동액(cp)")
    signed_change_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="부호있는변동액(scp)")
    change_rate = models.DecimalField(max_digits=10, decimal_places=6, verbose_name="전일대비변동률(cr)")
    signed_change_rate = models.DecimalField(max_digits=10, decimal_places=6, verbose_name="부호있는변동률(scr)")
    
    # [체결 및 거래량 필드]
    ask_bid = models.CharField(max_length=3, verbose_name="매수매도구분(ab)") # ASK, BID
    trade_volume = models.DecimalField(max_digits=25, decimal_places=8, verbose_name="최근체결량(tv)")
    acc_trade_volume = models.DecimalField(max_digits=30, decimal_places=8, verbose_name="당일누적체결량(atv)")
    acc_trade_price = models.DecimalField(max_digits=30, decimal_places=4, verbose_name="당일누적거래대금(atp)")
    acc_ask_volume = models.DecimalField(max_digits=30, decimal_places=8, verbose_name="누적매도량")
    acc_bid_volume = models.DecimalField(max_digits=30, decimal_places=8, verbose_name="누적매수량")
    
    # [시간 및 타임스탬프 필드]
    trade_date = models.CharField(max_length=8, verbose_name="최근체결날짜st(tdt)") # yyyyMMdd
    trade_time = models.CharField(max_length=6, verbose_name="최근체결시각st(ttm)") # HHmmss
    trade_timestamp = models.BigIntegerField(verbose_name="체결타임스탬프ms(ttms)")
    timestamp = models.BigIntegerField(db_index=True, verbose_name="타임스탬프ms(tms)") # ⚠️ 메인 인덱스
    
    # [52주 최고/최저 필드]
    highest_52_week_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="52주최고가(h52wp)")
    highest_52_week_date = models.CharField(max_length=10, verbose_name="52주최고가달성일(h52wdt)") # yyyy-MM-dd
    lowest_52_week_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="52주최저가(l52wp)")
    lowest_52_week_date = models.CharField(max_length=10, verbose_name="52주최저가달성일(l52wdt)") # yyyy-MM-dd
    
    # [마켓 상태 및 기타 필드]
    market_state = models.CharField(max_length=40, verbose_name="마켓상태(ms)") # ACTIVE 등
    is_trading_suspended = models.BooleanField(default=False, verbose_name="거래정지여부(its)")
    delisting_date = models.JSONField(max_length=10, null=True, blank=True, verbose_name="상장폐지일(dd)")
    market_warning = models.CharField(max_length=20, verbose_name="유의종목여부(mw)") # NONE, CAUTION 등
    
    # [24시간 누적 필드]
    acc_trade_price_24h = models.DecimalField(max_digits=30, decimal_places=4, verbose_name="24시간누적거래대금(atp24h)")
    acc_trade_volume_24h = models.DecimalField(max_digits=30, decimal_places=8, verbose_name="24시간누적거래량(atv24h)")
    stream_type = models.CharField(max_length=20, verbose_name="정산상태(st)") # REALTIME 등
    
    # 시스템 적재 시간
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'ticker_date'


class BadTickerDate(models.Model):

    raw_data = models.JSONField() # 원본 데이터 전체
    error_log = models.TextField() # 에러 원인
    created_at = models.DateTimeField(auto_now_add=True)
    class Meta:
        db_table = 'bad_ticker_date'