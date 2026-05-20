from django.db import models
from market_data.models import Market
# Create your models here.

class DropLogs(models.Model):
    market_code = models.ForeignKey(Market, on_delete=models.CASCADE, db_column='market_code')
    start_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="윈도우 시작가")
    end_price = models.DecimalField(max_digits=20, decimal_places=4, verbose_name="윈도우 끝가")
    change_rate = models.FloatField(verbose_name='변동률')
    status = models.CharField(max_length=15, verbose_name='상태')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'drop_logs'
        verbose_name = '급락 로그'
        verbose_name_plural = '급락 로그 목록'