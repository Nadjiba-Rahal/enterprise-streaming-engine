import asyncio
from datetime import datetime, timedelta, timezone

from app.pipeline import StreamingPipeline


def event(**overrides):
    base = {
        "event_id": f"evt_test_{datetime.now(timezone.utc).timestamp()}",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "user_id": "user_1",
        "session_id": "session_1",
        "ip_address": "198.51.100.10",
        "location": "Paris",
        "previous_location": "",
        "seconds_since_location_change": None,
        "product_id": "sku_1000",
        "category": "electronics",
        "action": "page_view",
        "amount": 0.0,
        "quantity": 0,
        "status": "ok",
        "device": "desktop",
        "segment": "normal_user",
        "bot_likelihood": 0.1,
        "anomaly_type": "",
        "is_injected_anomaly": False,
        "checkout_failures_30s": 0,
    }
    base.update(overrides)
    return base


def test_amount_spike_scores_but_does_not_auto_block():
    pipeline = StreamingPipeline()
    score, is_fraud, reasons = pipeline.score_event(event(action="checkout_completed", amount=425.0))

    assert score == 0.45
    assert is_fraud is False
    assert reasons == ["amount_spike"]


def test_card_velocity_plus_amount_blocks_transaction():
    pipeline = StreamingPipeline()
    score, is_fraud, reasons = pipeline.score_event(event(action="card_velocity", amount=550.0))

    assert score == 0.95
    assert is_fraud is True
    assert set(reasons) == {"card_velocity", "amount_spike"}


def test_impossible_travel_adds_risk():
    pipeline = StreamingPipeline()
    first = event(user_id="traveler", location="Algiers", event_ts=datetime.now(timezone.utc).isoformat())
    second = event(
        user_id="traveler",
        location="Tokyo",
        event_ts=(datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat(),
        amount=350.0,
        action="checkout_completed",
    )

    pipeline.score_event(first)
    score, is_fraud, reasons = pipeline.score_event(second)

    assert score == 0.8
    assert is_fraud is True
    assert "impossible_travel" in reasons


def test_repetitive_checkout_failures_under_same_ip_blocks():
    pipeline = StreamingPipeline()
    timestamp = datetime.now(timezone.utc)

    scores = []
    for index in range(3):
        score, is_fraud, reasons = pipeline.score_event(
            event(
                event_id=f"failed_{index}",
                event_ts=(timestamp + timedelta(seconds=index * 4)).isoformat(),
                action="checkout_failed",
                status="failed",
                ip_address="203.0.113.9",
            )
        )
        scores.append((score, is_fraud, reasons))

    assert scores[-1][0] == 0.5
    assert scores[-1][1] is False
    assert "repetitive_checkout_failures" in scores[-1][2]


def test_duckdb_event_insertion_through_queue():
    pipeline = StreamingPipeline()
    asyncio.run(pipeline.publish(event(event_id="insert_1", action="checkout_completed", amount=125.0, quantity=2)))
    processed = asyncio.run(pipeline.drain())

    assert len(processed) == 1
    row = pipeline.conn.execute("SELECT COUNT(*), SUM(amount), SUM(quantity) FROM events_stream").fetchone()
    assert row == (1, 125.0, 2)


def test_prune_clears_stale_in_memory_state_and_old_rows():
    pipeline = StreamingPipeline(prune_every=1, retention_minutes=10)
    now = datetime.now(timezone.utc)
    old_ts = now - timedelta(minutes=40)

    pipeline.process_event(
        event(
            event_id="old_failed",
            event_ts=old_ts.isoformat(),
            action="checkout_failed",
            status="failed",
            ip_address="203.0.113.77",
            user_id="stale_user",
        )
    )
    assert "203.0.113.77" in pipeline.checkout_failures
    assert "stale_user" in pipeline.user_locations

    # A fresh event triggers a prune pass (prune_every=1) whose cutoff is
    # relative to *this* event's timestamp, so the 40-minute-old state above
    # is now well outside the 5-minute in-memory retention window.
    pipeline.process_event(
        event(event_id="fresh_view", event_ts=now.isoformat(), action="page_view", user_id="active_user")
    )

    assert "203.0.113.77" not in pipeline.checkout_failures
    assert "stale_user" not in pipeline.user_locations

    total_rows = pipeline.conn.execute("SELECT COUNT(*) FROM events_stream").fetchone()[0]
    remaining_ids = {
        row[0] for row in pipeline.conn.execute("SELECT event_id FROM events_stream").fetchall()
    }
    assert total_rows == 1
    assert remaining_ids == {"fresh_view"}
    pipeline = StreamingPipeline()
    for index in range(31):
        pipeline.process_event(event(event_id=f"view_{index}", action="product_view", category="luxury"))

    pricing = pipeline.dynamic_pricing()
    row = pricing[pricing["category"] == "luxury"].iloc[0].to_dict()

    assert row["views_60s"] == 31
    assert row["suggested_markup_pct"] == 10
