"""Stochastic event simulator for the in-memory streaming engine."""

from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
from faker import Faker


NORMAL_USER_TRANSITIONS = {
    "page_view": {"page_view": 0.35, "product_view": 0.45, "add_to_cart": 0.08, "checkout_started": 0.04, "checkout_completed": 0.04, "checkout_failed": 0.04},
    "product_view": {"page_view": 0.16, "product_view": 0.34, "add_to_cart": 0.30, "checkout_started": 0.08, "checkout_completed": 0.04, "checkout_failed": 0.08},
    "add_to_cart": {"page_view": 0.10, "product_view": 0.18, "add_to_cart": 0.20, "checkout_started": 0.35, "checkout_completed": 0.08, "checkout_failed": 0.09},
    "checkout_started": {"page_view": 0.05, "product_view": 0.10, "add_to_cart": 0.14, "checkout_started": 0.12, "checkout_completed": 0.46, "checkout_failed": 0.13},
    "checkout_completed": {"page_view": 0.30, "product_view": 0.48, "add_to_cart": 0.08, "checkout_started": 0.04, "checkout_completed": 0.04, "checkout_failed": 0.06},
    "checkout_failed": {"page_view": 0.18, "product_view": 0.24, "add_to_cart": 0.20, "checkout_started": 0.22, "checkout_completed": 0.04, "checkout_failed": 0.12},
}

FRAUD_BOT_TRANSITIONS = {
    "page_view": {"page_view": 0.06, "product_view": 0.16, "add_to_cart": 0.16, "checkout_started": 0.22, "checkout_completed": 0.05, "checkout_failed": 0.23, "card_velocity": 0.12},
    "product_view": {"page_view": 0.04, "product_view": 0.10, "add_to_cart": 0.22, "checkout_started": 0.20, "checkout_completed": 0.06, "checkout_failed": 0.24, "card_velocity": 0.14},
    "add_to_cart": {"page_view": 0.03, "product_view": 0.08, "add_to_cart": 0.12, "checkout_started": 0.26, "checkout_completed": 0.05, "checkout_failed": 0.24, "card_velocity": 0.22},
    "checkout_started": {"page_view": 0.02, "product_view": 0.06, "add_to_cart": 0.10, "checkout_started": 0.16, "checkout_completed": 0.05, "checkout_failed": 0.31, "card_velocity": 0.30},
    "checkout_completed": {"page_view": 0.03, "product_view": 0.08, "add_to_cart": 0.14, "checkout_started": 0.20, "checkout_completed": 0.04, "checkout_failed": 0.25, "card_velocity": 0.26},
    "checkout_failed": {"page_view": 0.02, "product_view": 0.06, "add_to_cart": 0.12, "checkout_started": 0.20, "checkout_completed": 0.03, "checkout_failed": 0.25, "card_velocity": 0.32},
    "card_velocity": {"page_view": 0.02, "product_view": 0.05, "add_to_cart": 0.10, "checkout_started": 0.18, "checkout_completed": 0.03, "checkout_failed": 0.26, "card_velocity": 0.36},
}


@dataclass
class SyntheticUser:
    user_id: str
    session_id: str
    segment: str
    state: str
    ip_address: str
    location: str
    last_category: str = "electronics"
    bot_likelihood: float = 0.0


@dataclass
class StochasticEventGenerator:
    """Generate realistic e-commerce events with stochastic behavior and injected anomalies."""

    seed: int | None = None
    anomaly_rate: float = 0.065
    user_pool_size: int = 180
    faker: Faker = field(init=False)
    rng: np.random.Generator = field(init=False)
    users: list[SyntheticUser] = field(init=False)

    categories: tuple[str, ...] = (
        "electronics",
        "luxury",
        "beauty",
        "sports",
        "home",
        "fashion",
        "travel",
        "books",
    )
    cities: tuple[str, ...] = (
        "Paris",
        "Berlin",
        "Amsterdam",
        "Madrid",
        "Milan",
        "Stockholm",
        "Dublin",
        "Lisbon",
        "Algiers",
        "Tokyo",
        "Singapore",
        "New York",
    )

    def __post_init__(self) -> None:
        self.faker = Faker()
        if self.seed is not None:
            Faker.seed(self.seed)
            random.seed(self.seed)
        self.rng = np.random.default_rng(self.seed)
        self.users = [self._new_user(index) for index in range(self.user_pool_size)]

    def _new_user(self, index: int) -> SyntheticUser:
        is_bot = self.rng.random() < 0.12
        return SyntheticUser(
            user_id=f"usr_{index:04d}_{uuid.uuid4().hex[:8]}",
            session_id=f"sess_{uuid.uuid4().hex[:12]}",
            segment="fraud_bot" if is_bot else "normal_user",
            state="page_view",
            ip_address=self.faker.ipv4_public(),
            location=str(self.rng.choice(self.cities)),
            last_category=str(self.rng.choice(self.categories)),
            bot_likelihood=0.75 if is_bot else 0.08,
        )

    def _transition(self, user: SyntheticUser) -> str:
        matrix = FRAUD_BOT_TRANSITIONS if user.segment == "fraud_bot" else NORMAL_USER_TRANSITIONS
        transitions = matrix[user.state]
        states = list(transitions)
        probabilities = list(transitions.values())
        user.state = str(self.rng.choice(states, p=probabilities))
        return user.state

    def _amount(self, action: str) -> float:
        if action not in {"checkout_completed", "checkout_failed", "card_velocity"}:
            return 0.0
        return round(float(self.rng.lognormal(mean=3.5, sigma=0.75)), 2)

    def _inject_anomaly(self, event: dict[str, Any], user: SyntheticUser) -> dict[str, Any]:
        anomaly_type = str(self.rng.choice(["impossible_travel", "carding_velocity", "high_amount_spike"]))
        event["anomaly_type"] = anomaly_type
        event["is_injected_anomaly"] = True

        if anomaly_type == "impossible_travel":
            event["previous_location"] = user.location
            event["location"] = "Tokyo" if user.location != "Tokyo" else "Algiers"
            user.location = event["location"]
            event["seconds_since_location_change"] = int(self.rng.integers(5, 45))
        elif anomaly_type == "carding_velocity":
            event["action"] = "card_velocity"
            event["amount"] = round(float(self.rng.lognormal(mean=4.0, sigma=0.65)), 2)
            event["checkout_failures_30s"] = int(self.rng.integers(3, 8))
            event["quantity"] = 0
            event["status"] = "failed"
        elif anomaly_type == "high_amount_spike":
            event["action"] = "checkout_completed"
            event["amount"] = round(float(self.rng.uniform(1001, 4200)), 2)
            event["status"] = "ok"
            # `quantity` was computed from the *pre-anomaly* action and would
            # otherwise stay at 0, which silently breaks inventory depletion
            # and understates GMV for exactly the high-value orders the
            # dashboard is supposed to highlight.
            if not event.get("quantity"):
                event["quantity"] = int(self.rng.integers(1, 3))

        return event

    def generate_event(self) -> dict[str, Any]:
        user = random.choice(self.users)
        action = self._transition(user)

        if self.rng.random() < 0.18:
            user.session_id = f"sess_{uuid.uuid4().hex[:12]}"
        if self.rng.random() < 0.20:
            user.last_category = str(self.rng.choice(self.categories))

        event = {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "user_id": user.user_id,
            "session_id": user.session_id,
            "ip_address": user.ip_address,
            "location": user.location,
            "previous_location": "",
            "seconds_since_location_change": None,
            "product_id": f"sku_{self.rng.integers(1000, 9999)}",
            "category": user.last_category,
            "action": action,
            "amount": self._amount(action),
            "quantity": int(self.rng.integers(1, 4)) if action == "checkout_completed" else 0,
            "status": "failed" if action == "checkout_failed" else "ok",
            "device": str(self.rng.choice(["mobile", "desktop", "tablet"], p=[0.58, 0.34, 0.08])),
            "segment": user.segment,
            "bot_likelihood": user.bot_likelihood,
            "anomaly_type": "",
            "is_injected_anomaly": False,
            "checkout_failures_30s": 1 if action == "checkout_failed" else 0,
        }

        if self.rng.random() < self.anomaly_rate:
            return self._inject_anomaly(event, user)
        return event

    def poisson_batch_size(self, lam: float) -> int:
        return max(1, int(self.rng.poisson(lam=max(0.1, lam))))

    def exponential_delay(self, rate_per_second: float) -> float:
        return float(self.rng.exponential(1.0 / max(0.1, rate_per_second)))

    async def emit_forever(self, queue: asyncio.Queue, lam: float = 4.0, stop_after: int | None = None) -> None:
        emitted = 0
        while stop_after is None or emitted < stop_after:
            batch_size = self.poisson_batch_size(lam)
            for _ in range(batch_size):
                await queue.put(self.generate_event())
                emitted += 1
                if stop_after is not None and emitted >= stop_after:
                    break
            await asyncio.sleep(min(self.exponential_delay(lam), 1.25))

