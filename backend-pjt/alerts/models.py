from django.db import models
from accounts.models import User
from market_data.models import Market

class AlertLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    market_code = models.ForeignKey(Market, on_delete=models.CASCADE, db_column='market_code')
    detected_price = models.DecimalField(max_digits=20, decimal_places=4)
    detected_change = models.DecimalField(max_digits=10, decimal_places=6)
    alert_type = models.CharField(max_length=20)
    sent_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'alert_log'