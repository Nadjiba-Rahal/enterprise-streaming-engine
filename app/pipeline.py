"""In-memory queue, fraud scoring, DuckDB storage, and analytics layer."""

from __future__ import annotations

import asyncio
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import duckdb


STARTING_STOCK = {
    "electronics": 72,
    "luxury": 28,
    "beauty": 54,
    "sports": 46,
    "home": 62,
    "fashion": 80,
    "travel": 34,
    "books": 44,
}


@dataclass
class StreamingPipeline:
    """Async in-memory streaming pipeline backed by an in-memory DuckDB database."""

    queue_maxsize: int = 25_000
    inventory_threshold: int = 10
    retention_minutes: int = 30
    prune_every: int = 200
    queue: asyncio.Queue = field(init=False)
    conn: duckdb.DuckDBPyConnection = field(init=False)
    user_locations: dict[str, tuple[str, datetime]] = field(default_factory=dict)
    checkout_failures: dict[str, list[datetime]] = field(default_factory=lambda: defaultdict(list))
    session_carts: dict[str, dict[str, Any]] = field(default_factory=dict)
    stock_levels: dict[str, int] = field(default_factory=lambda: STARTING_STOCK.copy())
    events_since_prune: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self.conn = duckdb.connect(":memory:")
        self._setup_schema()

    def _setup_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events_stream (
                event_id VARCHAR PRIMARY KEY,
                event_ts TIMESTAMP,
                user_id VARCHAR,
                session_id VARCHAR,
                ip_address VARCHAR,
                location VARCHAR,
                previous_location VARCHAR,
                seconds_since_location_change INTEGER,
                product_id VARCHAR,
                category VARCHAR,
                action VARCHAR,
                amount DOUBLE,
                quantity INTEGER,
                status VARCHAR,
                device VARCHAR,
                segment VARCHAR,
                bot_likelihood DOUBLE,
                anomaly_type VARCHAR,
                is_injected_anomaly BOOLEAN,
                checkout_failures_30s INTEGER,
                risk_score DOUBLE,
                is_fraud BOOLEAN,
                risk_reasons VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_event_ts ON events_stream(event_ts)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_user_ts ON events_stream(user_id, event_ts)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_category_action ON events_stream(category, action)")

    @staticmethod
    def parse_ts(value: str | datetime | None) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if not value:
            return datetime.now(timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    def score_event(self, event: dict[str, Any]) -> tuple[float, bool, list[str]]:
        score = 0.0
        reasons: list[str] = []
        amount = float(event.get("amount") or 0.0)
        action = str(event.get("action", ""))
        event_ts = self.parse_ts(event.get("event_ts"))
        user_id = str(event.get("user_id", ""))
        location = str(event.get("location", ""))
        ip_address = str(event.get("ip_address", ""))

        if amount > 300.0:
            score += 0.45
            reasons.append("amount_spike")

        if action == "card_velocity":
            score += 0.50
            reasons.append("card_velocity")

        if action == "checkout_failed":
            failures = self.checkout_failures[ip_address]
            cutoff = event_ts.timestamp() - 30
            failures[:] = [ts for ts in failures if ts.timestamp() >= cutoff]
            failures.append(event_ts)
            if len(failures) >= 3:
                score += 0.50
                reasons.append("repetitive_checkout_failures")

        explicit_fast_move = event.get("seconds_since_location_change")
        if explicit_fast_move is not None and int(explicit_fast_move) < 60 and event.get("previous_location"):
            score += 0.35
            reasons.append("impossible_travel")
        else:
            previous = self.user_locations.get(user_id)
            if previous and previous[0] != location:
                seconds = abs((event_ts - previous[1]).total_seconds())
                if seconds < 60:
                    score += 0.35
                    reasons.append("impossible_travel")

        if user_id and location:
            self.user_locations[user_id] = (location, event_ts)

        normalized = min(1.0, round(score, 2))
        return normalized, normalized >= 0.70, reasons

    async def publish(self, event: dict[str, Any]) -> None:
        await self.queue.put(event)

    async def consume_once(self) -> dict[str, Any] | None:
        if self.queue.empty():
            return None
        event = await self.queue.get()
        enriched = self.process_event(event)
        self.queue.task_done()
        return enriched

    async def drain(self, limit: int | None = None) -> list[dict[str, Any]]:
        processed: list[dict[str, Any]] = []
        while not self.queue.empty() and (limit is None or len(processed) < limit):
            event = await self.consume_once()
            if event is not None:
                processed.append(event)
        return processed

    def process_event(self, event: dict[str, Any]) -> dict[str, Any]:
        risk_score, is_fraud, reasons = self.score_event(event)
        enriched = {
            **event,
            "event_ts": self.parse_ts(event.get("event_ts")),
            "amount": float(event.get("amount") or 0.0),
            "quantity": int(event.get("quantity") or 0),
            "bot_likelihood": float(event.get("bot_likelihood") or 0.0),
            "risk_score": risk_score,
            "is_fraud": is_fraud,
            "risk_reasons": ",".join(reasons),
            "ingested_at": datetime.now(timezone.utc),
        }
        self._insert_event(enriched)
        self._update_business_state(enriched)

        self.events_since_prune += 1
        if self.events_since_prune >= self.prune_every:
            self.events_since_prune = 0
            self._prune_stale_state(enriched["event_ts"])

        return enriched

    def _prune_stale_state(self, now: datetime) -> None:
        """Bound memory growth for a long-lived demo/production session.

        Without this, `checkout_failures`, `user_locations`, and the DuckDB
        `events_stream` table grow forever, which contradicts the
        "production-grade" claim for anything that runs longer than a demo.
        """
        cutoff = now.timestamp() - 300
        stale_ips = [
            ip for ip, timestamps in self.checkout_failures.items()
            if not timestamps or timestamps[-1].timestamp() < cutoff
        ]
        for ip in stale_ips:
            del self.checkout_failures[ip]

        stale_users = [
            user_id for user_id, (_, seen_at) in self.user_locations.items()
            if seen_at.timestamp() < cutoff
        ]
        for user_id in stale_users:
            del self.user_locations[user_id]

        stale_sessions = [
            session_id
            for session_id, cart in self.session_carts.items()
            if cart["status"] != "cart_open" and self.parse_ts(cart["last_seen"]).timestamp() < cutoff
        ]
        for session_id in stale_sessions:
            del self.session_carts[session_id]

        retention_minutes = int(self.retention_minutes)
        self.conn.execute(f"DELETE FROM events_stream WHERE event_ts < now() - INTERVAL {retention_minutes} MINUTE")

    def _insert_event(self, event: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO events_stream VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                event.get("event_id"),
                event.get("event_ts"),
                event.get("user_id"),
                event.get("session_id"),
                event.get("ip_address"),
                event.get("location"),
                event.get("previous_location", ""),
                event.get("seconds_since_location_change"),
                event.get("product_id"),
                event.get("category"),
                event.get("action"),
                event.get("amount"),
                event.get("quantity"),
                event.get("status"),
                event.get("device"),
                event.get("segment"),
                event.get("bot_likelihood"),
                event.get("anomaly_type", ""),
                bool(event.get("is_injected_anomaly", False)),
                int(event.get("checkout_failures_30s") or 0),
                event.get("risk_score"),
                bool(event.get("is_fraud")),
                event.get("risk_reasons", ""),
                event.get("ingested_at"),
            ],
        )

    def _update_business_state(self, event: dict[str, Any]) -> None:
        action = event["action"]
        session_id = event["session_id"]
        category = event["category"]
        now = event["event_ts"]

        if action == "add_to_cart":
            self.session_carts[session_id] = {
                "session_id": session_id,
                "user_id": event["user_id"],
                "category": category,
                "product_id": event["product_id"],
                "amount": max(float(event["amount"]), 24.0),
                "first_seen": now,
                "last_seen": now,
                "status": "cart_open",
            }
        elif action == "checkout_completed":
            if session_id in self.session_carts:
                self.session_carts[session_id]["status"] = "converted"
            self.stock_levels[category] = max(0, self.stock_levels.get(category, 0) - int(event["quantity"] or 1))

    def query_df(self, sql: str):
        return self.conn.execute(sql).fetchdf()

    def metrics(self) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total_events,
                COUNT(DISTINCT user_id) AS active_users,
                COALESCE(SUM(CASE WHEN action = 'checkout_completed' AND NOT is_fraud THEN amount ELSE 0 END), 0) AS gmv,
                COALESCE(AVG(risk_score), 0) AS avg_risk,
                COALESCE(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END), 0) AS blocked,
                COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed
            FROM events_stream
            """
        ).fetchone()
        total = int(row[0] or 0)
        blocked = int(row[4] or 0)
        failed = int(row[5] or 0)
        recent = self.conn.execute(
            """
            SELECT COUNT(*) FROM events_stream
            WHERE event_ts >= now() - INTERVAL 10 SECOND
            """
        ).fetchone()[0]
        return {
            "total_events": total,
            "active_users": int(row[1] or 0),
            "gmv": float(row[2] or 0),
            "avg_risk": float(row[3] or 0),
            "blocked": blocked,
            "fraud_block_rate": blocked / total if total else 0.0,
            "failed_events": failed,
            "error_rate": failed / total if total else 0.0,
            "events_per_second": float(recent or 0) / 10.0,
            "queue_depth": self.queue.qsize(),
        }

    def fraud_feed(self, limit: int = 20):
        return self.query_df(
            f"""
            SELECT event_ts, risk_score, risk_reasons, user_id, ip_address, location, action, amount
            FROM events_stream
            WHERE risk_score >= 0.35
            ORDER BY event_ts DESC
            LIMIT {int(limit)}
            """
        )

    def dynamic_pricing(self):
        return self.query_df(
            """
            SELECT
                category,
                COUNT(*) FILTER (WHERE action IN ('page_view', 'product_view')) AS views_60s,
                CASE
                    WHEN COUNT(*) FILTER (WHERE action IN ('page_view', 'product_view')) > 30 THEN 10
                    WHEN COUNT(*) FILTER (WHERE action IN ('page_view', 'product_view')) > 20 THEN 5
                    ELSE 0
                END AS suggested_markup_pct
            FROM events_stream
            WHERE event_ts >= now() - INTERVAL 60 SECOND
            GROUP BY category
            ORDER BY views_60s DESC
            """
        )

    def abandoned_carts(self, age_seconds: int = 45) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        offers = []
        for cart in self.session_carts.values():
            if cart["status"] != "cart_open":
                continue
            age = (now - self.parse_ts(cart["last_seen"])).total_seconds()
            if age >= age_seconds:
                category = cart["category"]
                offers.append(
                    {
                        **cart,
                        "age_seconds": int(age),
                        "offer_payload": json.dumps(
                            {
                                "type": "recovery_offer",
                                "session_id": cart["session_id"],
                                "category": category,
                                "incentive": "free_shipping" if category in {"luxury", "electronics"} else "8pct_discount",
                            }
                        ),
                    }
                )
        return sorted(offers, key=lambda item: item["age_seconds"], reverse=True)

    def inventory_alerts(self) -> list[dict[str, Any]]:
        alerts = []
        for category, stock in sorted(self.stock_levels.items(), key=lambda item: item[1]):
            status = "stockout_alert" if stock <= self.inventory_threshold else "healthy"
            alerts.append({"category": category, "stock": stock, "threshold": self.inventory_threshold, "status": status})
        return alerts

    def ingestion_timeseries(self):
        return self.query_df(
            """
            SELECT
                date_trunc('second', event_ts) AS second,
                COUNT(*) AS events,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_events,
                AVG(risk_score) AS avg_risk
            FROM events_stream
            WHERE event_ts >= now() - INTERVAL 5 MINUTE
            GROUP BY second
            ORDER BY second
            """
        )

    def health_alarm(self) -> dict[str, Any]:
        metrics = self.metrics()
        rate = metrics["error_rate"]
        return {
            "error_rate_pct": round(rate * 100, 2),
            "alarm": rate > 0.10,
            "message": "Technical alarm: failed event rate above 10%" if rate > 0.10 else "Ingestion health within operating band",
        }

    def risk_distribution(self):
        return self.query_df(
            """
            SELECT
                CASE
                    WHEN risk_score < 0.35 THEN 'Low'
                    WHEN risk_score < 0.70 THEN 'Review'
                    ELSE 'Blocked'
                END AS risk_band,
                COUNT(*) AS events
            FROM events_stream
            GROUP BY risk_band
            ORDER BY risk_band
            """
        )

    def category_sales(self):
        rows = [{"category": category, "stock": stock, "sold": STARTING_STOCK.get(category, 0) - stock} for category, stock in self.stock_levels.items()]
        return rows

