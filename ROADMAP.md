# Queue Processing System Roadmap

A lightweight, durable, and extensible message broker built from scratch in Go. The goal is to learn distributed systems, storage engines, and broker internals while creating a production-quality open-source project.

---

# v0.1 — Foundation ✅

## Features
- ✅ TCP Protocol
- ✅ Producer Registration
- ✅ Consumer Registration
- ✅ FIFO Queue
- ✅ Basic Message Transfer

**Status:** Complete

---

# v0.2 — Broker Core ✅

## Features
- ✅ Broker Architecture
- ✅ Dispatcher
- ✅ Consumer State Management
- ✅ Thread Safety
- ✅ Producer/Consumer Management

**Status:** Complete

---

# v0.3 — Reliable Delivery ✅

## Features
- ✅ ACK
- ✅ NACK
- ✅ Retry Mechanism
- ✅ Dead Letter Queue (DLQ)

**Status:** Complete

---

# v0.4 — Message Lifecycle ✅

## Features
- ✅ Visibility Timeout
- ✅ Delayed Retry

**Status:** Complete

---

# v0.5 — Durable Storage 🚧 (Current)

## Write-Ahead Log (WAL)

### Completed
- ✅ WAL Architecture
- ✅ Event Model
- ✅ Event ID Design
- ✅ Replay Strategy

### Remaining
- ⬜ WAL Rotation
- ⬜ WAL Cleanup

---

## Snapshots

### Completed
- ✅ Snapshot Architecture
- ✅ Snapshot Metadata
  - Version
  - LastAppliedEventID
- ✅ Snapshot Durable Data Design

### Remaining
- ✅ Snapshot Creation
- ✅ Snapshot Loading
- ⬜ Snapshot Recovery

---

## Recovery

### Remaining
- ⬜ Startup Recovery Flow
- ⬜ Crash Recovery Tests
- ⬜ Integration Tests

**Status:** In Progress

---

# v0.6 — Advanced Queue Features

## Features
- ⬜ Multiple Named Queues
- ⬜ Queue Configuration
- ⬜ Queue Metadata
- ⬜ Delay Queue
- ⬜ Priority Queue

**Status:** Planned

---

# v0.7 — Routing

## Features
- ⬜ Exchange System
- ⬜ Direct Exchange
- ⬜ Fanout Exchange
- ⬜ Topic Exchange
- ⬜ Publisher Confirms

**Status:** Planned

---

# v0.8 — Operations & High Availability

## Features
- ⬜ HTTP Management API
- ⬜ Metrics API
- ⬜ Authentication
- ⬜ Leader/Follower Replication

**Status:** Planned

---

# v1.0 — Production Ready

## Features
- ⬜ Stable APIs
- ⬜ Client SDK
- ⬜ Documentation
- ⬜ Benchmarks
- ⬜ Docker Support
- ⬜ Production Release

**Status:** Planned

---

# Current Progress

```text
████████████████████░░░░░░░░░░░░░░░░░░░░

v0.1 ✅ Foundation
v0.2 ✅ Broker Core
v0.3 ✅ Reliable Delivery
v0.4 ✅ Message Lifecycle
v0.5 🚧 Durable Storage
v0.6 ⬜ Advanced Queue Features
v0.7 ⬜ Routing
v0.8 ⬜ Operations & High Availability
v1.0 ⬜ Production Ready
```

---

# Current Focus (v0.5)

## ✅ Designed

### WAL
- Event Model
- Event IDs
- Replay Strategy

### Snapshot
- Architecture
- Metadata
- Durable Message Fields

### Recovery
- High-Level Recovery Flow

---

## 🚧 Currently Working On

- Snapshot Creation Policy
- WAL Rotation
- Snapshot Lifecycle

---

## ⏳ Next Steps

1. Implement WAL
2. Implement Snapshot System
3. Implement Recovery Process
4. Add Crash Recovery Tests
5. Add Integration Tests
6. Complete v0.5

---

# Long-Term Vision

This project aims to evolve from a simple message queue into a production-grade message broker by gradually introducing advanced distributed systems concepts rather than implementing everything at once.

The focus is on:
- Reliability
- Simplicity
- Performance
- Learning through implementation
- Clean architecture
- Open-source collaboration