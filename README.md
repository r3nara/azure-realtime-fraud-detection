# Intelligent Real-Time Fraud Detection & AI Analytics Platform

An end-to-end data engineering portfolio project demonstrating real-time financial transaction ingestion, event-driven processing, fraud detection, distributed analytics, and AI-powered insights using Microsoft Azure and modern data engineering technologies.

## Project Overview

This project simulates a banking transaction environment and builds an event-driven data platform to identify potentially suspicious transactions in near real time.

The platform combines:

- Continuous synthetic transaction generation
- Real-time event ingestion using Azure Event Hubs
- Python-based event consumption and validation
- Cloud storage using Azure Data Lake Storage Gen2
- Distributed processing using PySpark
- Delta Lake and medallion architecture
- Power BI dashboards
- Retrieval-Augmented Generation (RAG) for AI-powered analytics

The project is being developed incrementally, with an emphasis on understanding the underlying architecture, reliability, scalability, data quality, and cloud cost management.

## Architecture

### Target Architecture

The planned end-to-end architecture is:

```text
             Synthetic Transaction Generator
                         |
                         v
                  Azure Event Hubs
                  (4 Partitions)
                         |
             +-----------+-----------+
             |                       |
             v                       v
     Fraud Detection Consumer   Raw Archive Consumer
             |                       |
             v                       v
     Transaction Validation       ADLS Gen2
     and Fraud Detection          (Raw Layer)
             |
             v
          ADLS Gen2
       (Processed Layer)
             |
             v
       PySpark Processing
             |
             v
          Delta Lake
     (Bronze / Silver / Gold)
             |
          +--+--+
          |     |
          v     v
       Power BI  RAG Assistant
                 |
                 v
          AI-Powered Insights
```

### Current Implementation

The initial streaming foundation is implemented and tested:

```text
Python Transaction Generator
             |
             v
       Azure Event Hubs
             |
             v
    Python Event Hubs Consumer
             |
             v
     Transaction Validation
             |
             v
    Azure Blob Checkpoint Store
```

The consumer uses a dedicated Event Hubs consumer group and persists partition checkpoint metadata in Azure Blob Storage.

The independent raw archival consumer and downstream analytics components are planned for subsequent milestones.

## Technology Stack

| Area | Technology |
|---|---|
| Programming | Python 3.12 |
| Streaming ingestion | Azure Event Hubs |
| Event production | Azure Event Hubs Python SDK |
| Event consumption | Azure Event Hubs Python SDK |
| Authentication | Microsoft Entra ID / Azure CLI |
| Cloud storage | Azure Data Lake Storage Gen2 |
| Checkpoint management | Azure Blob Storage |
| Data validation | Pydantic |
| Testing | Pytest |
| Distributed processing | PySpark |
| Data lake format | Delta Lake |
| Visualization | Power BI |
| AI | Generative AI and RAG |
| Version control | Git and GitHub |

## Key Capabilities

### Implemented

- Synthetic financial transaction generation.
- Continuous transaction event generation.
- Event publishing to Azure Event Hubs.
- Four-partition event streaming.
- Customer-based partition key routing.
- Independent Event Hubs consumer groups.
- Python-based Event Hubs consumer.
- Pydantic transaction schema validation.
- Azure Blob Storage checkpoint persistence.
- Consumer restart and checkpoint recovery testing.
- Unit and integration tests.

### Planned

- Rule-based suspicious transaction detection.
- Idempotent transaction processing.
- Independent raw transaction archival.
- Invalid-event quarantine and data quality monitoring.
- PySpark distributed processing.
- Delta Lake medallion architecture.
- Power BI fraud monitoring dashboards.
- RAG-based AI analytics assistant.
- Automated testing and CI/CD.

## Repository Structure

```text
azure-realtime-fraud-detection/
│
├── src/
│   ├── common/
│   │   └── transaction_schema.py
│   │
│   ├── transaction_generator/
│   │   ├── generator.py
│   │   └── stream.py
│   │
│   ├── event_producer/
│   │   └── producer.py
│   │
│   └── fraud_processor/
│       └── consumer.py
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── connectivity tests
│
├── docs/
│   ├── architecture/
│   └── design-decisions/
│
├── data/
│   └── samples and schemas
│
├── spark/
│
├── analytics/
│
├── ai/
│
├── infrastructure/
│
├── .github/
│   └── workflows/
│
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

Some directories are reserved for upcoming implementation milestones.

## Local Development Setup

### Prerequisites

- Python 3.12
- Git
- Visual Studio Code
- Azure CLI
- An active Azure subscription
- Azure Event Hubs namespace and event hub
- Azure Storage account with hierarchical namespace enabled

### 1. Clone the repository

```powershell
git clone https://github.com/r3nara/azure-realtime-fraud-detection.git

cd azure-realtime-fraud-detection
```

### 2. Create a virtual environment

```powershell
py -3.12 -m venv .venv
```

Activate it in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 4. Authenticate with Azure

```powershell
az login --use-device-code
```

Verify the active subscription:

```powershell
az account show
```

Ensure the authenticated identity has the necessary Azure data-plane permissions.

### 5. Run unit tests

```powershell
python -m pytest tests/unit -v
```

### 6. Run the transaction producer integration test

```powershell
python -m tests.integration.test_transaction_producer
```

### 7. Run the checkpoint-enabled consumer

```powershell
python -m tests.integration.test_checkpoint_consumer
```

The consumer currently processes a bounded number of events for controlled integration testing.

**Note:** Azure resource configuration and authentication must be configured for your own environment before running the cloud integration tests. Never commit access keys, credentials, or personal environment configuration.

## Testing

The project uses Pytest for automated testing.

Current unit test coverage includes:

- Transaction generation.
- Transaction ID uniqueness.
- Positive transaction amounts.
- Required customer identifiers.
- Transaction schema validation.
- Invalid transaction rejection.
- Continuous transaction generation.
- Generator parameter validation.

Integration testing includes:

- Azure authentication.
- ADLS Gen2 connectivity.
- Event Hubs connectivity.
- Transaction publishing.
- Event consumption.
- Checkpoint persistence.
- Consumer restart and recovery.

The initial unit test suite currently passes 11 tests.

## Project Status

**Current milestone: Azure streaming foundation implemented and tested.**

| Component | Status |
|---|---|
| Azure infrastructure foundation | Completed |
| Python development environment | Completed |
| Transaction generator | Implemented |
| Transaction schema validation | Implemented |
| Event Hubs producer | Implemented |
| Event Hubs consumer | Implemented |
| Blob checkpoint persistence | Verified |
| Consumer restart and recovery | Demonstrated |
| Rule-based fraud detection | Upcoming |
| Idempotent processing | Upcoming |
| Raw archival consumer | Upcoming |
| PySpark and Delta Lake | Planned |
| Power BI dashboards | Planned |
| RAG assistant | Planned |
| CI/CD | Planned |

Implementation will continue incrementally, with source code, automated tests, architecture documentation, and deployment instructions maintained alongside the project.

## Design and Engineering Principles

The project focuses on practical data engineering principles:

- Event-driven architecture.
- Partition-aware streaming.
- Independent consumer groups.
- Checkpoint-based recovery.
- At-least-once processing considerations.
- Idempotent data processing.
- Data validation and quarantine.
- Medallion architecture.
- Distributed data processing.
- Cloud cost optimization.
- Secure authentication and least-privilege access.

## Disclaimer

This project uses synthetic financial transaction data for educational and portfolio purposes. It is not connected to real banking systems and does not authorize, approve, or block real financial transactions.
