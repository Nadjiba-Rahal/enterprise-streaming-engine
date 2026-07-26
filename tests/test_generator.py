from app.generator import StochasticEventGenerator


class _ForcedAnomalyRNG:
    """Wrap a real numpy Generator but force the anomaly-type choice.

    Used to deterministically exercise a specific branch of
    `_inject_anomaly` while every other random draw (lognormal, integers,
    uniform, ...) still goes through the real generator.
    """

    def __init__(self, real_rng, forced_anomaly: str):
        self._real = real_rng
        self._forced_anomaly = forced_anomaly

    def choice(self, a, p=None):
        options = set(a) if hasattr(a, "__iter__") else set()
        if options == {"impossible_travel", "carding_velocity", "high_amount_spike"}:
            return self._forced_anomaly
        return self._real.choice(a, p=p) if p is not None else self._real.choice(a)

    def __getattr__(self, name):
        return getattr(self._real, name)


def _force_anomaly(generator: StochasticEventGenerator, anomaly_type: str) -> None:
    generator.rng = _ForcedAnomalyRNG(generator.rng, anomaly_type)
    generator.anomaly_rate = 1.0


def test_high_amount_spike_always_produces_a_saleable_quantity():
    """Regression test: the anomaly used to overwrite `action` to
    `checkout_completed` *after* `quantity` had already been computed from
    the pre-anomaly action, so a large "sale" could silently carry
    quantity=0 and never deplete inventory or count cleanly toward GMV.
    """
    generator = StochasticEventGenerator(seed=7)
    _force_anomaly(generator, "high_amount_spike")

    user = generator.users[0]
    user.state = "page_view"  # pre-anomaly action would yield quantity=0

    event = generator.generate_event()

    assert event["anomaly_type"] == "high_amount_spike"
    assert event["action"] == "checkout_completed"
    assert event["status"] == "ok"
    assert event["amount"] > 1000
    assert event["quantity"] >= 1


def test_carding_velocity_anomaly_has_zero_quantity_and_failed_status():
    generator = StochasticEventGenerator(seed=7)
    _force_anomaly(generator, "carding_velocity")

    event = generator.generate_event()

    assert event["anomaly_type"] == "carding_velocity"
    assert event["action"] == "card_velocity"
    assert event["quantity"] == 0
    assert event["status"] == "failed"
    assert event["checkout_failures_30s"] >= 3
