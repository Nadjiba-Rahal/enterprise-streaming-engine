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
