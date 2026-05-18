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
    market_code = models.ForeignKey(Market, on_delete=models.CASCADE, db_column='market_code')
    trade_price = models.DecimalField(max_digits=20, decimal_places=4)
    change_rate = models.DecimalField(max_digits=10, decimal_places=6)
    trade_vol = models.DecimalField(max_digits=20, decimal_places=8)
    acc_vol_24h = models.DecimalField(max_digits=30, decimal_places=8)
    acc_amt_24h = models.DecimalField(max_digits=30, decimal_places=4)
    ask_bid = models.CharField(max_length=3)
    trade_ts = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'ticker_date'