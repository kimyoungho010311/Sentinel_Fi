from django.db import models

class User(models.Model):
    email = models.EmailField(max_length=255, unique=True)
    nickname = models.CharField(max_length=50)
    password_hash = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'users'


class Watchlist(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    market_code = models.ForeignKey('market_data.Market', on_delete=models.CASCADE, db_column='market_code')
    alert_threshold = models.DecimalField(max_digits=5, decimal_places=2, default=-5.00, null=True)
    is_enabled = models.BooleanField(default=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'watchlist'