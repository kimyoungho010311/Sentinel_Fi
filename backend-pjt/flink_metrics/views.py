from django.http import JsonResponse
from django.views.decorators.http import require_GET

from .models import FlinkMetric


def serialize_metric(metric):
    return {
        "checked_at": metric.checked_at.isoformat(),
        "time_unit": metric.time_unit,
        "total_collected_count": metric.total_collected_count,
        "total_error_count": metric.total_error_count,
        "error_rate_percentage": metric.error_rate_percentage,
        "average_latency_ms": metric.average_latency_ms,
        "max_latency_ms": metric.max_latency_ms,
        "unique_market_count": metric.unique_market_count,
        "total_trade_volume_krw": metric.total_trade_volume_krw,
    }


@require_GET
def latest_metric(request):
    metric = FlinkMetric.objects.order_by("-checked_at").first()

    if metric is None:
        return JsonResponse({
            "message": "No metric data yet"
        }, status=404)

    return JsonResponse(serialize_metric(metric))


@require_GET
def recent_metrics(request):
    try:
        limit = int(request.GET.get("limit", 60))
    except ValueError:
        limit = 60

    limit = min(max(limit, 1), 300)
    metrics = FlinkMetric.objects.order_by("-checked_at")[:limit]

    return JsonResponse({
        "count": len(metrics),
        "results": [serialize_metric(metric) for metric in metrics],
    })
