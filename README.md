# 6.5840 — Distributed Sharded Key-Value Database

MIT 6.5840 (Distributed Systems) lab implementations, building from MapReduce through a fully sharded, replicated, and reconfigurable key-value store.

## Architecture Overview

```
┌──────────────┐     ┌─────────────────┐
│   Clerk      │────▶│ Shard Controller │  (lease-based leader election)
│  (Client)    │     │  (config mgmt)   │
└──────────────┘     └────────┬────────┘
        │                     │ reconfiguration
        ▼                     ▼
┌──────────────────────────────────────┐
│            Shard Groups              │
│  ┌──────────┐ ┌──────────┐          │
│  │ Group 0  │ │ Group 1  │  ...     │  Each group:
│  │ (Raft)   │ │ (Raft)   │          │  Leader + Followers
│  │ Shard 0-5│ │ Shard 6-11│          │  Raft log + Snapshot
│  └──────────┘ └──────────┘          │
└──────────────────────────────────────┘
```

## Labs

| Lab | Module | Description |
|-----|--------|-------------|
| 1   | `mr/`  | MapReduce framework — coordinator + worker with fault tolerance |
| 2   | `kvsrv1/` | Single-server KV store with version-based optimistic concurrency |
| 3   | `raft1/` | Full Raft consensus: leader election, log replication, snapshotting |
| 4   | `kvraft1/` | Replicated KV store on Raft via Replicated State Machine (RSM) |
| 5   | `shardkv1/` | Sharded KV with dynamic reconfiguration and shard migration |

## Project Structure

```
src/
├── mr/            Lab 1: MapReduce coordinator & worker
├── mrapps/        MapReduce application plugins (word count, indexing, etc.)
├── kvsrv1/        Lab 2: Simple key-value server & client
├   ├── rpc/       RPC types (GetArgs, PutArgs, Err types)
├   ├── server.go  Single-threaded in-memory KV store
├   └── client.go  Retry logic with ErrMaybe semantics
├── raft1/         Lab 3: Raft consensus implementation
├   ├── raft.go    Core Raft: election, log replication, commitment, snapshot
├   ├── server.go  Raft server wrapper with snapshot lifecycle
├   ├── rpc.go     AppendEntries & RequestVote RPC definitions
├   └── util.go    Log entry, persisted state helpers
├── raftapi/       Raft interface definition
├── kvraft1/       Lab 4: KV store replicated on Raft
├   ├── rsm/       Replicated State Machine — generic Raft abstraction
├   │   └── rsm.go Submit() with op-ID tracking & leadership-loss detection
├   ├── server.go  KVServer: DoOp, Snapshot, Restore, Get/Put handlers
├   └── client.go  Client with leader rotation
├── shardkv1/      Lab 5: Sharded KV store
├   ├── shardcfg/  Shard configuration & rebalancing (12 shards, FNV-1a hash)
├   ├── shardctrler/ Shard controller with lease-based leadership
├   ├── shardgrp/  Shard group server (owns shards, handles migration)
├   │   ├── server.go  DoOp: Get/Put/FreezeShard/InstallShard/DeleteShard
├   │   ├── client.go  Group-level client with leader rotation
├   │   └── shardrpc/  Freeze/Install/Delete RPC types
├   └── client.go  Top-level clerk: routes by shard → group
├── labgob/        Gob serialization wrapper (field validation)
├── labrpc/        Channel-based RPC simulator (drop, delay, partition)
├── models1/       Porcupine linearizability model
├── kvtest1/       KV linearizability test helpers
├── tester1/       Test harness (config, groups, server lifecycle)
└── main/          Test runners & benchmarks
```

## Key Design Decisions

### Consistency Model
**Linearizability** — all operations (including reads) go through Raft consensus. The leader never serves reads from local state, preventing stale reads from partitioned ex-leaders. Verified by Porcupine model checker.

### Version-Based Concurrency Control
Every value carries a version number. `Put(key, value, version)` succeeds only if the version matches — CAS semantics. Clients retry on `ErrVersion`.

### Replicated State Machine (RSM)
A generic abstraction that connects Raft to any state machine via `Submit(req)`. Uses globally unique operation IDs and `pendingLogByOpID` mapping to detect lost leadership when an unexpected operation lands at a reserved log index.

### Shard Migration Protocol
```
Source Group                  Controller                   Destination Group
    │                            │                              │
    │◀── FreezeShard ──────────│── InstallShard ──────────▶│
    │    frozen[shard]=true      │    owned[shard]=true         │
    │    return state            │    frozen[shard]=false       │
    │                            │                              │
    │◀── DeleteShard ──────────│                              │
    │    remove state & ownership│                              │
```

Frozen shards reject client reads and writes, clients receive `ErrWrongGroup`, re-read the config, and route to the new group.

### Controller Fault Tolerance
The shard controller stores configuration in a Raft-replicated KV store (Group 0). Leadership is managed via **leases** with 5-second timeouts. On controller failure, a new controller acquires the expired lease and resumes any in-progress reconfiguration from the persisted state.

### Raft Safety
- Only entries from the leader's current term can be committed (§5.4.1)
- Snapshots enable log compaction and full state recovery for lagging/crashed followers
- Election timeouts randomized (150–300ms) per peer to reduce split votes

## Getting Started

### Requirements
- Go 1.22+
- Git LFS (for test data)

### Running Tests

```bash
# Individual lab tests
cd src
go test -run 2A -race ./kvsrv1/...    # Lab 2
go test -run 3A -race ./raft1/...     # Lab 3
go test -run 4A -race ./kvraft1/...   # Lab 4
go test -run 5A -race ./shardkv1/...  # Lab 5

# All tests
go test -race ./...
```

### Submitting

```bash
make lab1    # → lab1-handin.tar.gz
make lab5c   # → lab5c-handin.tar.gz
```

## Comparison with Redis

A detailed comparison is available in [doc/distributed-kv-vs-redis.md](doc/distributed-kv-vs-redis.md). In short:

| | This Implementation | Redis |
|---|:---:|:---:|
| Consistency | Linearizable | Eventually consistent (default) |
| Replication | Synchronous (Raft) | Asynchronous |
| Data model | String KV with versions | Rich data structures |
| Performance | Raft-latency bound | Microsecond latency |
| Cluster topology | Centralized controller | Decentralized gossip |

## References

- [Raft Paper](https://raft.github.io/raft.pdf) — Ongaro & Ousterhout, 2014
- [MIT 6.5840](https://pdos.csail.mit.edu/6.824/) — Distributed Systems course
- [Porcupine](https://github.com/anishathalye/porcupine) — Linearizability checker
