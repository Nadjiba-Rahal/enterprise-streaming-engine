# Real-Time Event Streaming & Commerce Intelligence Platform

Production-grade, zero-cost real-time analytics built for portfolio review. The application simulates e-commerce behavior, streams events through an in-memory `asyncio.Queue`, scores fraud risk, persists analytical state in `duckdb.connect(":memory:")`, and presents five enterprise operating views in Streamlit.

No Kafka, no Docker, no cloud credentials, no paid services.

## Executive Summary

This project is an enterprise-style real-time decision platform for digital commerce teams. It demonstrates how a modern data product can combine event streaming, fraud prevention, pricing intelligence, cart recovery, inventory telemetry, and infrastructure monitoring without external infrastructure.

Recruiter signal:

- Event-driven Python architecture with asynchronous ingestion.
- Mathematical simulation using Poisson traffic, exponential inter-arrival timing, log-normal spend, and Markov chain user behavior.
- In-memory analytical storage using DuckDB SQL.
- Streamlit command center with operational metrics and interactive event injection.
- Unit-tested risk scoring and storage behavior.

## Architecture Schema

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                    IN-MEMORY STREAMING ANALYTICS PLATFORM                   │
├─────────────────────────────────┬───────────────────────────────────────────┤
│ Source A                        │ Source B                                  │
│ Stochastic Event Generator      │ Streamlit Ingest Form                     │
│ - Poisson traffic batches       │ - Manual event injection                  │
│ - Exponential delays            │ - Custom risk scenarios                   │
│ - Log-normal purchase amounts   │ - Recruiter demo controls                 │
│ - Markov behavior transitions   │                                           │
└─────────────────┬───────────────┴───────────────────┬───────────────────────┘
                  │                                   │
                  └───────────────┬───────────────────┘
                                  ▼
                  ┌───────────────────────────────────┐
                  │ asyncio.Queue                     │
                  │ In-memory event bus               │
                  └─────────────────┬─────────────────┘
                                    ▼
                  ┌───────────────────────────────────┐
                  │ StreamingPipeline                 │
                  │ - Risk scoring engine             │
                  │ - Impossible travel detection     │
                  │ - Carding velocity detection      │
                  │ - Business analytics layers       │
                  └─────────────────┬─────────────────┘
                                    ▼
                  ┌───────────────────────────────────┐
                  │ DuckDB :memory:                   │
                  │ SQL analytics over event stream   │
                  └─────────────────┬─────────────────┘
                                    ▼
                  ┌───────────────────────────────────┐
                  │ Streamlit Command Center          │
                  │ 5 enterprise tabs + live controls │
                  └───────────────────────────────────┘
```

## Reliability & Data Hygiene

This project has been through a review pass to close the gaps that usually separate a "demo" from something you can defend in an interview:

- **Inventory/GMV consistency** — high-value and card-velocity anomalies now carry a consistent `quantity`/`status`, so a large injected "sale" actually depletes stock and reports GMV correctly instead of silently leaving inventory untouched.
- **No false positives from browsing** — the manual event form used to let a `page_view` or `add_to_cart` event carry a leftover order value, which the risk engine scored as an `amount_spike`. Only checkout/card events can carry a monetary amount now.
- **Bounded memory** — a long-running session used to grow `checkout_failures`, `user_locations`, and the DuckDB `events_stream` table forever. The pipeline now prunes state older than a configurable in-memory window (5 minutes) and DuckDB retention window (`retention_minutes`, default 30) every `prune_every` events, so the "production-grade" claim actually holds for a session that runs for hours, not just minutes.
- **Dependency hygiene** — `requirements.txt` no longer lists an unused `polars` dependency and explicitly pins `pandas`, which `dashboard.py` imports directly instead of relying on it as an indirect dependency of Streamlit.
- **Deterministic demo scenarios** — the "Abandoned cart" scenario used to fake a cart's age by rewriting its timestamp to the year 2020. It now ages the cart by exactly the recovery threshold, which is both correct and easy to reason about.

### Known limitations / roadmap

- The event bus is a single-process `asyncio.Queue` drained synchronously on every Streamlit rerun — it is not a background/always-on stream. A natural next step is a background worker thread feeding a thread-safe queue that the UI drains, which would make the "real-time" framing literal rather than simulated.
- DuckDB `:memory:` means state resets whenever the process restarts (by design, for a zero-cost deployment). For anything beyond a demo, swap `duckdb.connect(":memory:")` for a file-backed DuckDB database or an external warehouse.
- The risk engine is a simple weighted rule set for explainability; a natural extension is to log engineered features and train a real classifier (e.g. gradient boosting) against them, keeping the same rule set as a fallback/explainer.

## Business ROI & Enterprise Use Cases

### 1. Fraud & Chargeback Prevention

The risk engine scores every event from `0.0` to `1.0`.

- `+0.45` for transaction amount above `€300`.
- `+0.50` for `card_velocity` or repeated checkout failures.
- `+0.35` for rapid location change consistent with impossible travel.
- Transactions with risk score `>= 0.70` are flagged as fraud.

Business value: fewer chargebacks, faster fraud triage, and visible explainability through risk reason codes.

### 2. Dynamic Pricing & Demand Surge

The pricing engine calculates rolling 60-second category views. When a category exceeds `30 views/min`, the dashboard suggests a `+10%` markup.

Business value: demand-sensitive pricing during traffic spikes, campaign surges, or flash-sale behavior.

### 3. Cart Abandonment Recovery

Sessions with `add_to_cart` and no `checkout_completed` after 45 seconds become recovery candidates. The engine emits a structured offer payload such as free shipping or category-specific discounts.

Business value: recover high-intent demand before it disappears.

### 4. Real-Time Inventory Alerts

The pipeline tracks category stock depletion from completed checkouts. When stock drops to `10` units or below, the dashboard raises a stockout alert.

Business value: earlier replenishment decisions and fewer lost sales from silent stockouts.

### 5. Infrastructure Health

The health layer measures failed events over total events:

```text
error_rate = failed_events / total_events * 100
```

If the rate rises above `10%`, the dashboard raises a technical alarm.

Business value: operations teams can separate demand problems from system reliability problems.

### 6. Reviewer & Recruiter Conveniences

- **Reset demo** in the sidebar clears accumulated state (pipeline + generator) without restarting the process, so a reviewer can start clean between scenarios.
- **CSV export** on the Action Plan and Fraud tabs downloads the current recommendations / fraud feed for a tangible artifact.

## Mathematical Simulation Model

### Traffic Rate

Traffic arrives in stochastic batches:

```text
X ~ Poisson(lambda)
```

The dashboard exposes `lambda` as a traffic-speed control.

### Inter-Event Delay

Generator sleep intervals follow an exponential distribution:

```text
T ~ Exponential(1 / lambda)
```

This reflects bursty traffic where many events arrive close together and quiet periods are still possible.

### Purchase Amounts

Checkout values use a log-normal distribution:

```text
amount ~ LogNormal(mu=3.5, sigma=0.75)
```

This better matches retail spend: many ordinary transactions and a smaller number of large baskets.

### Markov Behavior Transitions

Users move through commerce states using two transition matrices:

- `NORMAL_USER`: more browsing, carting, and successful checkout behavior.
- `FRAUD_BOT`: higher rates of checkout failure, card velocity, and suspicious conversion attempts.

### Injected Anomalies

The simulator injects anomalies at a configurable 5-8% rate:

- Impossible travel, for example Algiers to Tokyo in under 60 seconds.
- Carding velocity from repeated suspicious checkout behavior.
- High amount spikes above `€1,000`.

## Project Structure

```text
realtime-fraud-streaming-engine/
├── app/
│   ├── __init__.py
│   ├── generator.py
│   ├── pipeline.py
│   └── dashboard.py
├── tests/
│   ├── test_pipeline.py
│   └── test_generator.py
├── requirements.txt
└── README.md
```

## Run Locally

```bash
cd realtime-fraud-streaming-engine
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
streamlit run app/dashboard.py
```

On macOS or Linux:

```bash
source .venv/bin/activate
streamlit run app/dashboard.py
```

## Run Tests

```bash
pytest
```

Coverage includes the fraud/risk scoring rules, DuckDB insertion through the queue, dynamic pricing thresholds, the anomaly-injection quantity/status fixes, and the in-memory/DuckDB pruning behavior (`tests/test_pipeline.py`, `tests/test_generator.py`).

## Streamlit Community Cloud Deployment

1. Push this folder to a public GitHub repository.
2. Go to [Streamlit Community Cloud](https://streamlit.io/cloud).
3. Sign in with GitHub.
4. Select the repository and branch.
5. Set the main file path to:

```text
app/dashboard.py
```

6. Deploy.

The app uses only the dependencies in `requirements.txt` and stores all runtime data in memory, so it does not need a database server, Kafka broker, cloud account, API key, credit card, or paid managed service.

## Notes For Reviewers

This project intentionally uses `asyncio.Queue` instead of Kafka to satisfy a zero-cost deployment constraint while keeping event-driven design principles visible. DuckDB is used as an in-process analytical engine for real-time SQL aggregations. The result is easy to deploy, easy to inspect, and credible as an architecture pattern for prototyping enterprise streaming analytics before introducing managed infrastructure.
