from django.urls import path

from . import views

urlpatterns = [
    path("latest/", views.latest_metric, name="flink-metric-latest"),
    path("recent/", views.recent_metrics, name="flink-metric-recent"),
]
