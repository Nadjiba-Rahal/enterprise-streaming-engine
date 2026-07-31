
# Real-Time Event Streaming & Commerce Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30%2B-FF4B4B?style=for-the-badge&logo=streamlit)](https://streamlit.io/)
[![DuckDB](https://img.shields.io/badge/DuckDB-In--Memory-FFF000?style=for-the-badge&logo=duckdb)](https://duckdb.org/)
[![Pytest](https://img.shields.io/badge/Pytest-Automated_Tests-0A9EDC?style=for-the-badge&logo=pytest)](https://docs.pytest.org/)

An enterprise-grade, zero-cost real-time event-streaming and analytics engine. The platform simulates real-time e-commerce behavior, streams telemetry events through an asynchronous in-memory `asyncio.Queue` pipeline, evaluates fraud risk scores via rule engines, persists analytical states in an in-process DuckDB engine, and presents real-time command views in Streamlit.

Designed specifically to demonstrate production system design, mathematical traffic simulation, stateful event processing, and memory-safe stream windowing without external infrastructure dependencies.

---

## Executive Summary

This architecture implements a high-throughput, real-time decision framework for digital commerce systems. It demonstrates how event-driven pipelines can combine fraud prevention, dynamic pricing intelligence, cart recovery engines, stock depletion telemetry, and system health metrics within a self-contained environment.

### Core Architectural Competencies
* **Event-Driven Python Architecture**: Asynchronous event ingestion using non-blocking queues and structured pipeline execution.
* **Stochastic Mathematical Simulation**: Synthetic traffic models based on Poisson arrival processes, exponential inter-arrival timing, log-normal basket distribution, and Markov chain state transition matrices.
* **In-Memory OLAP Analytics**: High-performance SQL analytics executing over live stream windows using embedded DuckDB (`duckdb.connect(":memory:")`).
* **Command & Control Operations**: Streamlit dashboard presenting real-time business telemetry alongside manual event injection for operational edge-case testing.
* **Memory & State Bounding**: Automated window pruning mechanisms preventing unbounded memory leaks across long-running event processing sessions.

---

## System Architecture

```plain
+-----------------------------------------------------------------------------+
|                     IN-MEMORY STREAMING ANALYTICS PLATFORM                  |
+---------------------------------+-------------------------------------------+
| Source A                        | Source B                                  |
| Stochastic Event Generator      | Streamlit Ingest Form                     |
| - Poisson traffic batches       | - Manual event injection                  |
| - Exponential delays            | - Custom risk scenario injection          |
| - Log-normal purchase amounts   | - Operational controls                    |
| - Markov behavior transitions   |                                           |
+-----------------+---------------+-------------------+-----------------------+
                  |                                   |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------------------------+
                  | asyncio.Queue                     |
                  | In-memory asynchronous event bus  |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------------------------+
                  | StreamingPipeline                 |
                  | - Rule-based risk scoring engine  |
                  | - Impossible travel detection     |
                  | - Carding velocity analytics      |
                  | - Business intelligence layers    |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------------------------+
                  | DuckDB :memory:                   |
                  | Real-time SQL query execution     |
                  +-----------------+-----------------+
                                    |
                                    v
                  +-----------------------------------+
                  | Streamlit Command Center          |
                  | Operational tabs & live controls  |
                  +-----------------------------------+

```

---

## Reliability Engineering & State Hygiene

To maintain production alignment, key operational considerations have been built directly into the event engine:

* **Stateful Transactional Consistency**: High-value transactions and carding velocity anomalies propagate uniform `quantity` and `status` metadata. Large injected sales correctly deplete inventory and update gross merchandise value (GMV) deterministically.
* **Event Payload Isolation**: Strictly segregates financial fields (`amount`) to checkout and payment events, preventing false-positive fraud flags during browsing phases (`page_view`, `add_to_cart`).
* **Bounded Memory Windowing**: Implements automated cleanup routines. The pipeline prunes state objects older than 5 minutes and executes DuckDB table retention policies (defaulting to a 30-minute window) every `N` processed events to eliminate memory bloat during prolonged execution.
* **Deterministic Scenario Testing**: Cart abandonment recovery workflows age abandoned carts by precise threshold values, ensuring repeatable test execution across simulation runs.

---

## Business Logic & Technical Implementation

### 1. Fraud & Chargeback Detection Engine

Scores every ingested event on a continuous scale from `0.0` to `1.0`:

* `+0.45` weight for purchase amounts exceeding standard deviation thresholds (> €300).
* `+0.50` weight for elevated carding velocity or consecutive checkout failures.
* `+0.35` weight for impossible travel anomalies (e.g., location delta > 1000km in under 60 seconds).
* Events scoring `>= 0.70` trigger immediate automated mitigation flags and reason code audits.

### 2. Dynamic Demand-Based Pricing Engine

Calculates rolling 60-second window aggregations per product category using DuckDB SQL. If category views exceed `30 views/min`, the system suggests an automated dynamic markup (`+10%`) to optimize margin capture during demand spikes.

### 3. Automated Cart Recovery Pipeline

Evaluates active sessions containing `add_to_cart` events lacking a `checkout_completed` event within 45 seconds. The pipeline constructs and emits structured recovery payloads (e.g., free delivery vouchers, category incentives).

### 4. Real-Time Stock Depletion Telemetry

Tracks inventory decrements across active order flows. Depletion reaching stock thresholds `<= 10 units` triggers priority replenishment events to prevent revenue loss from unmanaged stockouts.

### 5. System Health & Infrastructure Telemetry

Monitors pipeline throughput and processing failure ratios:

$$\text{Error Rate} = \left( \frac{\text{Failed Events}}{\text{Total Processed Events}} \right) \times 100$$

Error rates exceeding `10%` raise automated system health alerts to separate infrastructure anomalies from commercial trends.

---

## Mathematical Simulation Mechanics

### Traffic Ingestion Rate

Stochastic event batching is modeled using a Poisson distribution:

$$X \sim \text{Poisson}(\lambda)$$

Where $\lambda$ represents configurable traffic velocity.

### Inter-Event Arrival Timing

Delays between generated event batches follow an exponential distribution:

$$T \sim \text{Exponential}\left(\frac{1}{\lambda}\right)$$

This effectively simulates real-world bursty network traffic profile characteristics.

### Financial Value Distribution

Order transaction values follow a log-normal distribution to reflect retail consumer purchasing behaviors:

$$\text{amount} \sim \text{LogNormal}(\mu = 3.5, \sigma = 0.75)$$

### Behavioral State Transitions

User behaviors transition via discrete Markov chain matrices, isolating organic user patterns (`NORMAL_USER`) from automated malicious activity (`FRAUD_BOT`).

---

## Technology Stack

| Domain | Technology | Implementation |
| --- | --- | --- |
| **Language** | Python 3.10+ | Core pipeline, simulation, and analytics engine |
| **Concurrency** | `asyncio` | Asynchronous event bus implementation via `asyncio.Queue` |
| **Data Engine** | DuckDB | Embedded OLAP database executing in-memory SQL over streams |
| **UI Framework** | Streamlit | Real-time operations panel and manual testing interface |
| **Test Suite** | Pytest | Automated testing covering rules, state retention, and queues |

---

## Repository Structure

```plain
realtime-fraud-streaming-engine/
├── app/
│   ├── __init__.py
│   ├── generator.py       # Mathematical traffic generator & Markov models
│   ├── pipeline.py        # Streaming pipeline, rule engine & DuckDB storage
│   └── dashboard.py       # Streamlit operational command center
├── tests/
│   ├── test_pipeline.py   # Pipeline rule tests, queue ingestion, state pruning
│   └── test_generator.py  # Simulation mathematical consistency tests
├── requirements.txt       # Strict production dependency declarations
└── README.md

```

---

## Local Development & Setup

### 1. Environment Setup

```bash
# Clone repository
git clone [https://github.com/your-username/realtime-fraud-streaming-engine.git](https://github.com/your-username/realtime-fraud-streaming-engine.git)
cd realtime-fraud-streaming-engine

# Create and activate virtual environment
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate

```

### 2. Dependency Installation

```bash
pip install -r requirements.txt

```

### 3. Execution

Launch the operational command dashboard:

```bash
streamlit run app/dashboard.py

```

---

## Automated Testing Suite

Execute the unit test suite covering fraud rules, memory retention, DuckDB streaming ingestion, and simulation logic:

```bash
pytest -v

```

---

## Deployment Architecture

This application is designed for zero-dependency edge execution on **Streamlit Community Cloud**:

1. Push the code repository to GitHub.
2. Connect the repository to Streamlit Cloud.
3. Configure entrypoint path to: `app/dashboard.py`.
4. Deploy instantly—no external databases, API keys, or cloud infrastructure required.

---

## License

This project is open-source and available under the [MIT License](https://www.google.com/search?q=LICENSE).

```

