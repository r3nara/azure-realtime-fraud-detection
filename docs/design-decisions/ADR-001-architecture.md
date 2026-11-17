# ADR-001: Selection of Event-Driven Architecture

**Status:** Accepted for initial implementation  
**Date:** 17 November 2026

## Context

The project requires continuous ingestion and near-real-time processing of simulated banking transactions.

A traditional batch-oriented architecture would process accumulated transactions at scheduled intervals. This would introduce processing delays.

The platform therefore requires an architecture capable of receiving events continuously and allowing multiple applications to consume the same transaction stream independently.

## Decision

Adopt an event-driven architecture using Azure Event Hubs as the streaming ingestion platform.

Use separate consumer groups for:

1. Real-time fraud detection.
2. Independent transaction archival.

Use Azure Functions as the initial custom Python processing engine.

## Alternatives Considered

### Batch-only processing

Not selected as the primary architecture because scheduled processing introduces latency.

Batch processing remains useful for historical analytics and reprocessing.

### Direct producer-to-consumer integration

Not selected because it tightly couples the transaction generator with downstream processing applications.

### Azure Stream Analytics as the primary processor

Deferred as an optional learning exercise. The initial architecture will use Azure Functions to implement custom Python fraud detection logic.

## Consequences

### Benefits

- Producers and consumers are decoupled.
- Multiple applications can independently consume transaction events.
- Supports near-real-time processing.
- Enables independent archival and fraud processing.
- Provides an opportunity to learn partitioning, checkpoints and consumer groups.

### Trade-offs

- Requires checkpoint and duplicate-processing management.
- Introduces cloud infrastructure and operating costs.
- Requires monitoring of consumer lag and processing failures.
- Requires careful partition and capacity planning.

## Implementation Status

Architecture decision recorded. Actual implementation and service configuration remain pending.