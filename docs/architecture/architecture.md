# System Architecture — Real-Time Fraud Detection Platform

**Project:** Intelligent Real-Time Fraud Detection & AI Analytics Platform  
**Architecture Version:** 0.1  
**Status:** Proposed  
**Cloud Platform:** Microsoft Azure

## 1. Introduction

The objective of this project is to design and implement an event-driven data engineering platform that processes simulated financial transactions in near real time, identifies potentially suspicious activity, and provides historical analytics and AI-powered insights.

The platform uses a combination of streaming ingestion, event processing, cloud storage, distributed processing, data visualization, and Retrieval-Augmented Generation (RAG).

All financial transactions used in this project are synthetic.

## 2. Architecture Objectives

- Ingest transaction events continuously.
- Process events independently of the transaction generator.
- Identify suspicious transactions using configurable fraud detection rules.
- Preserve original transaction events for auditing and reprocessing.
- Support historical data processing and analytics.
- Provide dashboards for fraud monitoring.
- Enable natural-language questions over curated project information.
- Maintain modular, testable and maintainable application components.

## 3. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data generation | Python | Generate synthetic transactions |
| Event ingestion | Azure Event Hubs | Receive transaction events |
| Stream processing | Azure Functions | Process events and apply fraud rules |
| Raw storage | ADLS Gen2 | Store original transaction events |
| Processed storage | ADLS Gen2 | Store classified transaction results |
| Distributed processing | PySpark | Historical data transformation |
| Table format | Delta Lake | Reliable analytical storage |
| Visualization | Power BI | Fraud analytics and monitoring |
| AI | RAG | Answer questions using curated information |

## 4. High-Level Architecture

The architecture consists of six layers:

### 4.1 Transaction Generation

A Python application generates simulated banking transactions.

Each transaction includes fields such as:

- Transaction ID
- Customer ID
- Transaction amount
- Transaction timestamp
- Merchant ID
- Transaction location
- Transaction channel

### 4.2 Streaming Ingestion

Azure Event Hubs receives transaction events from the Python producer.

The Event Hub distributes events across partitions. A customer identifier may be used as a partition key when customer-level ordering is required.

Independent consumer groups are planned for fraud processing and archival.

### 4.3 Real-Time Processing

Azure Functions consumes transaction events and applies configurable fraud detection rules.

Examples include:

- Unusually high transaction amounts.
- Multiple transactions within a short interval.
- Transactions from unusual locations.
- Repeated failed transaction attempts.

The processing output classifies transactions as normal or suspicious.

This project generates alerts for investigation. It does not authorize, reject or block actual banking payments.

### 4.4 Data Lake Storage

ADLS Gen2 stores transaction data in logical layers.

**Raw layer:** Original transaction events are retained for historical reference and reprocessing.

**Processed layer:** Transaction events with processing results and fraud classifications are stored.

The storage layout and file formats will be finalized during implementation.

### 4.5 Historical Analytics

PySpark processes stored transaction data to produce historical analytical datasets.

Delta Lake is planned as the analytical table format.

Power BI will consume curated data to display transaction volumes, suspicious activity, fraud patterns and trends.

### 4.6 AI and RAG

The AI component will provide natural-language question answering over curated project data and relevant technical documentation.

The initial implementation will be evaluated locally before selecting managed Azure AI services.

## 5. Data Flow

1. Python generates a transaction.
2. The producer sends the event to Azure Event Hubs.
3. Event Hubs distributes the event to a partition.
4. The fraud consumer group reads the event.
5. Azure Functions applies fraud detection rules.
6. The archival consumer group independently reads the original event.
7. Original and processed records are stored in ADLS Gen2.
8. PySpark transforms stored data.
9. Delta Lake stores analytical tables.
10. Power BI presents the results.
11. The RAG assistant provides natural-language insights using curated information.

## 6. Reliability and Data Quality Considerations

The implementation will address:

- Event replay and duplicate processing.
- Idempotent processing.
- Checkpoint management.
- Schema validation.
- Missing and invalid transaction values.
- Logging and error handling.
- Failed event handling and retry strategies.
- Monitoring of consumer lag and processing failures.

## 7. Security Considerations

- Store secrets outside source code.
- Use environment variables for local development.
- Use managed identities where supported in Azure.
- Follow least-privilege access principles.
- Avoid storing real customer or financial information.
- Review repository contents before publishing.

## 8. Deployment Strategy

Development will begin locally using Python 3.10 and VS Code.

Azure resources will be deployed incrementally after service availability, SKU, quota and pricing validation.

The initial cloud deployment will focus on the minimum infrastructure required to demonstrate event ingestion and processing.

## 9. Current Implementation Status

**Current status:** Architecture and repository foundation established.

All Azure services and processing components described in this document are proposed until implemented and validated.

## 10. Architecture Diagram

Refer to `architecture-diagram.mmd` in this directory for the editable Mermaid architecture diagram.

## 11. Future Enhancements

- Advanced anomaly detection.
- Improved fraud scoring.
- Data quality automation.
- Monitoring and operational dashboards.
- Automated deployment through GitHub Actions.
- RAG evaluation and response quality measurement.