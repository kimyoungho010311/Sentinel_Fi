import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


DEFAULT_MARKETS = [
    "KRW-BTC",
    "KRW-ETH",
    "KRW-XRP",
    "KRW-SOL",
    "KRW-DOGE",
    "KRW-ADA",
    "KRW-AVAX",
    "KRW-LINK",
    "KRW-DOT",
    "KRW-ID",
    "KRW-AQT",
    "KRW-CBK",
    "KRW-RE",
    "KRW-IRYS",
    "KRW-SUI",
    "KRW-NEAR",
    "KRW-SEI",
    "KRW-APT",
    "KRW-ARB",
    "KRW-OP",
]


def build_ticker_event(market):
    now_ms = int(time.time() * 1000)
    trade_price = round(random.uniform(10, 150_000_000), 4)
    trade_volume = round(random.uniform(0.0001, 5000), 8)
    signed_change_rate = random.uniform(-0.05, 0.05)
    change = "RISE" if signed_change_rate > 0 else "FALL" if signed_change_rate < 0 else "EVEN"
    now = datetime.now(timezone.utc)

    return {
        "type": "ticker",
        "code": market,
        "opening_price": round(trade_price * random.uniform(0.95, 1.05), 4),
        "high_price": round(trade_price * random.uniform(1.0, 1.08), 4),
        "low_price": round(trade_price * random.uniform(0.92, 1.0), 4),
        "trade_price": trade_price,
        "prev_closing_price": round(trade_price / (1 + signed_change_rate), 4),
        "acc_trade_price": round(random.uniform(10_000_000, 50_000_000_000), 4),
        "change": change,
        "change_price": abs(round(trade_price * signed_change_rate, 4)),
        "signed_change_price": round(trade_price * signed_change_rate, 4),
        "change_rate": abs(round(signed_change_rate, 10)),
        "signed_change_rate": round(signed_change_rate, 10),
        "ask_bid": random.choice(["ASK", "BID"]),
        "trade_volume": trade_volume,
        "acc_trade_volume": round(random.uniform(100, 10_000_000), 8),
        "trade_date": now.strftime("%Y%m%d"),
        "trade_time": now.strftime("%H%M%S"),
        "trade_timestamp": now_ms,
        "acc_ask_volume": round(random.uniform(100, 10_000_000), 8),
        "acc_bid_volume": round(random.uniform(100, 10_000_000), 8),
        "highest_52_week_price": round(trade_price * random.uniform(1.1, 2.0), 4),
        "highest_52_week_date": "2026-01-01",
        "lowest_52_week_price": round(trade_price * random.uniform(0.1, 0.9), 4),
        "lowest_52_week_date": "2026-01-01",
        "market_state": "ACTIVE",
        "is_trading_suspended": False,
        "delisting_date": None,
        "market_warning": "NONE",
        "timestamp": now_ms,
        "acc_trade_price_24h": round(random.uniform(10_000_000, 50_000_000_000), 4),
        "acc_trade_volume_24h": round(random.uniform(100, 10_000_000), 8),
        "stream_type": "REALTIME",
    }


def maybe_make_bad_event(event, bad_rate):
    if random.random() >= bad_rate:
        return event

    bad_type = random.choice(["negative_price", "missing_timestamp", "malformed_json"])
    if bad_type == "negative_price":
        event["trade_price"] = -1
        return event
    if bad_type == "missing_timestamp":
        event.pop("timestamp", None)
        return event
    return "{not-valid-json"


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Produce synthetic Upbit-like ticker events to Kafka for load testing."
    )
    parser.add_argument("--broker", default="localhost:29092")
    parser.add_argument("--topic", default="ticker")
    parser.add_argument("--rate", type=int, default=500, help="Target events per second.")
    parser.add_argument("--duration", type=int, default=600, help="Run duration in seconds.")
    parser.add_argument("--bad-rate", type=float, default=0.0, help="Bad event ratio from 0.0 to 1.0.")
    parser.add_argument("--markets", type=int, default=20, help="Number of synthetic markets to use.")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = Producer({
        "bootstrap.servers": args.broker,
        "linger.ms": 5,
        "batch.num.messages": 10000,
    })
    markets = DEFAULT_MARKETS[:max(1, min(args.markets, len(DEFAULT_MARKETS)))]

    print(
        "Synthetic producer started "
        f"broker={args.broker} topic={args.topic} rate={args.rate}/s "
        f"duration={args.duration}s markets={len(markets)} bad_rate={args.bad_rate}"
    )

    produced = 0
    started_at = time.time()
    next_tick = started_at

    for _ in range(args.duration):
        tick_start = time.time()

        for _ in range(args.rate):
            market = random.choice(markets)
            event = maybe_make_bad_event(build_ticker_event(market), args.bad_rate)
            value = event if isinstance(event, str) else json.dumps(event, ensure_ascii=False)

            producer.produce(
                args.topic,
                key=market,
                value=value,
                callback=delivery_report,
            )
            produced += 1

        producer.poll(0)
        next_tick += 1
        sleep_for = next_tick - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            print(f"Producer is behind schedule by {abs(sleep_for):.2f}s")

        elapsed = int(time.time() - started_at)
        if elapsed and elapsed % 30 == 0:
            actual_rate = produced / max(time.time() - started_at, 1)
            print(f"elapsed={elapsed}s produced={produced} actual_rate={actual_rate:.2f}/s")

        producer.poll(0)

    producer.flush()
    elapsed = time.time() - started_at
    print(f"Done. produced={produced} elapsed={elapsed:.2f}s actual_rate={produced / elapsed:.2f}/s")


if __name__ == "__main__":
    main()
