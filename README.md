# Intelligent Real-Time Fraud Detection & AI Analytics Platform

An end-to-end data engineering portfolio project that demonstrates real-time financial transaction processing, fraud detection, distributed analytics, and AI-powered insights using Microsoft Azure and modern data engineering technologies.

## Project Overview

This project simulates a banking transaction environment and builds an event-driven data platform to identify potentially suspicious transactions in near real time.

The platform combines streaming data ingestion, event processing, cloud storage, distributed analytics, data visualization, and Retrieval-Augmented Generation (RAG).

## Architecture

**Planned data flow:**

Python Transaction Generator → Azure Event Hubs → Azure Functions → ADLS Gen2 → PySpark / Delta Lake → Power BI and RAG Assistant.

## Technology Stack

| Area | Technology |
|---|---|
| Programming | Python |
| Streaming ingestion | Azure Event Hubs |
| Stream processing | Azure Functions |
| Cloud storage | Azure Data Lake Storage Gen2 |
| Distributed processing | PySpark |
| Data lake format | Delta Lake |
| Visualization | Power BI |
| AI | Generative AI and RAG |
| Version control | Git and GitHub |

## Key Capabilities

- Continuous transaction event generation and ingestion.
- Near-real-time suspicious transaction detection.
- Raw and processed data storage.
- Historical transaction analytics using Spark.
- Fraud monitoring dashboards.
- AI-powered question answering over curated project information.

## Repository Structure

- `src/` — Application source code.
- `streaming/` — Streaming implementation.
- `docs/` — Architecture and technical documentation.
- `data/` — Sample data and schemas.
- `spark/` — Spark jobs and notebooks.
- `analytics/` — Power BI deliverables.
- `ai/` — RAG and AI implementation.
- `tests/` — Automated tests.
- `infrastructure/` — Azure deployment configuration.

## Project Status

**Status:** Foundation and architecture planning.

Implementation will be delivered incrementally, with documentation, tests, and deployment instructions maintained alongside the code.

## Disclaimer

This project uses synthetic financial transaction data for educational and portfolio purposes. It is not connected to real banking systems and does not authorize or block real financial transactions.