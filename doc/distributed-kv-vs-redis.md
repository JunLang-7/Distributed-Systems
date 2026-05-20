# 分布式分片键值数据库 vs Redis 对比分析

> 基于 MIT 6.5840 分布式系统课程 Lab 1-5 实现，面试准备材料。

---

## 一、项目概述

本项目实现了一个完整的分布式分片键值数据库，核心组件包括：

| 层级 | 模块 | 功能 |
|------|------|------|
| Lab 1 | MapReduce | 基础 MapReduce 框架 |
| Lab 2 | kvsrv | 单机 KV 存储，版本化操作 |
| Lab 3 | Raft | Raft 共识算法完整实现 |
| Lab 4 | kvraft | 基于 Raft 的复制 KV 存储 (RSM) |
| Lab 5 | shardkv | 分片、可动态重配置的分布式 KV |

**核心架构**：Raft 共识 + 复制状态机 (RSM) + 分片控制器 (Shard Controller) + 分片迁移协议。

---

## 二、相同点

| 方面 | 本实现 | Redis |
|------|--------|-------|
| **核心数据模型** | 键值存储 (string → value) | 键值存储 (string → value) |
| **哈希分片** | FNV-1a 哈希将 key 映射到 12 个 shard | CRC16 哈希将 key 映射到 16384 个 hash slot（Cluster 模式） |
| **分片迁移** | Freeze → Install → Delete 协议迁移 shard | MIGRATE / CLUSTER SETSLOT 迁移 hash slot |
| **复制机制** | 每个 shard group 内部通过 Raft 复制 | 主从异步复制（replicaof） |
| **基础操作** | Get / Put(带版本) / Append | GET / SET / APPEND |
| **配置中心** | 独立 Shard Controller 管理分片分配 | Cluster 模式 gossip 协议 + 节点间通信 |

---

## 三、关键差异

### 1. 一致性模型（最大差异）

| | 本实现 | Redis |
|------|--------|-------|
| **模型** | **强一致性 / 线性一致性 (Linearizability)** | 默认**最终一致性** |
| **读操作** | 所有读也走 Raft 共识，绝不读 follower | 可从从节点读，可能读到过期数据 |
| **验证** | 通过 Porcupine 模型做线性一致性验证 | 无形式化验证 |
| **强一致支持** | 架构层面保证 | 仅 `WAIT` 命令提供有限保证，非默认 |

### 2. 数据模型丰富度

| 本实现 | Redis |
|--------|-------|
| 仅 `string → (value, version)` | Strings, Lists, Sets, Sorted Sets, Hashes, HyperLogLog, Bitmaps, Geospatial, Streams 等 |

### 3. 并发控制

| | 本实现 | Redis |
|------|--------|-------|
| **机制** | 乐观并发控制（版本号），CAS 语义 | 单线程天然原子，无版本号 |
| **冲突处理** | 应用层处理 `ErrVersion`，重试 | WATCH/MULTI/EXEC 事务实现乐观锁 |
| **隔离性** | 线性一致，无并发写冲突 | 命令级原子，事务有隔离 |

### 4. 集群拓扑管理

| | 本实现 | Redis |
|------|--------|-------|
| **架构** | 集中式 Shard Controller（lease 机制选主） | 去中心化 gossip 协议 |
| **元数据存储** | Controller 将配置存储在 kvraft 中（复用 Raft） | 每个节点维护完整的 slot→node 映射 |
| **变更方式** | Controller 单点协调所有迁移 | 节点间通过 cluster bus 分布式协商 |

### 5. 故障检测与故障转移

| | 本实现 | Redis |
|------|--------|-------|
| **检测** | Raft election timeout，无心跳则发起选举 | Sentinel: 周期性 PING；Cluster: gossip + PING/PONG |
| **切换** | Raft 自动选举新 leader | Sentinel 投票决定；Cluster 基于 epoch 的内部选举 |
| **恢复** | 重启后从 Snapshot + Raft log 恢复 | RDB/AOF 恢复 |

### 6. 持久化

| | 本实现 | Redis |
|------|--------|-------|
| **机制** | Raft 日志 + 周期性 Snapshot | RDB（全量快照）+ AOF（追加日志） |
| **灵活性** | Snapshot 周期固定 | RDB/AOF 可独立或组合使用，配置灵活 |
| **目的** | 主要用于日志压缩和重启恢复 | 数据持久化与灾备 |

### 7. 性能模型

| | 本实现 | Redis |
|------|--------|-------|
| **延迟** | 高（每次操作需 Raft 共识：RPC → 日志持久化 → 多数确认 → apply） | 极低（微秒级，单线程内存操作） |
| **吞吐** | 受限于 Raft 共识开销 | 极高（10w+ QPS 单节点） |
| **复制延迟** | 同步复制，commit 即持久 | 异步复制，不阻塞主节点写入 |

### 8. 扩展性

| | 本实现 | Redis |
|------|--------|-------|
| **分片数** | 固定 12 shard，不可变 | 固定 16384 hash slot，不可变 |
| **节点扩展** | 增加 group 后重新分配 shard | 增加节点后重新分配 slot |
| **水平扩展** | 支持（增加 group） | 支持（增加 master 节点） |

### 9. 功能特性

| 特性 | 本实现 | Redis |
|------|:--:|:--:|
| Pub/Sub | ✗ | ✓ |
| Lua 脚本 | ✗ | ✓ (EVAL) |
| 事务 | ✗ | ✓ (MULTI/EXEC) |
| Key 过期 (TTL) | ✗ | ✓ (EXPIRE) |
| 流水线 (Pipelining) | ✗ | ✓ |
| Streams 消息队列 | ✗ | ✓ |
| 乐观并发控制 (CAS) | ✓ (版本号) | ✓ (WATCH) |

---

## 四、架构对比图

```
本实现架构:
┌──────────────┐     ┌─────────────────┐
│   Clerk      │────▶│ Shard Controller │  (lease 选主)
│  (客户端)    │     │  (集中式配置)     │
└──────────────┘     └────────┬────────┘
        │                     │ 配置变更
        ▼                     ▼
┌──────────────────────────────────────┐
│            Shard Groups              │
│  ┌──────────┐ ┌──────────┐          │
│  │ Group 0  │ │ Group 1  │  ...     │  每 Group 内部:
│  │ (Raft)   │ │ (Raft)   │          │  Leader + Follower(s)
│  │ Shard 0-3│ │ Shard 4-7│          │  Raft log + Snapshot
│  └──────────┘ └──────────┘          │
└──────────────────────────────────────┘

Redis Cluster 架构:
┌──────────────────────────────────────┐
│         Redis Cluster Nodes          │
│  ┌──────────┐ ┌──────────┐          │
│  │ Master A │◄─│ Master B │  ...     │  Gossip 协议
│  │Slot 0-5k │ │Slot 5k-10k│         │  去中心化
│  │          │ │          │          │
│  │ Slave A1 │ │ Slave B1 │          │  主从异步复制
│  └──────────┘ └──────────┘          │
└──────────────────────────────────────┘
```

---

## 五、面试要点总结

### 本实现解决的核心分布式问题

1. **共识 (Consensus)** — Raft 算法：leader 选举、log replication、snapshot、membership change
2. **复制状态机 (RSM)** — 如何将任意操作通过共识转换为线性一致的状态机
3. **分片 (Sharding)** — 静态哈希分片 + 动态重配置
4. **分片迁移** — Freeze → Install → Delete 协议，保证迁移期间无数据丢失
5. **配置管理** — 集中式 controller + lease 机制实现容错

### 与 Redis 设计哲学的差异

| 维度 | 本实现 (学术/教学) | Redis (工业/生产) |
|------|-------------------|-------------------|
| **目标** | 正确性可证明 (Linearizability) | 极高性能 + 实用功能 |
| **权衡** | 牺牲性能换一致性 | 牺牲强一致性换性能 |
| **复杂度来源** | 分布式共识的正确实现 | 丰富数据结构 + 生态功能 |
| **适用场景** | 教学、研究、一致性要求极高的场景 | 缓存、会话存储、排行榜、消息队列等 |

### 可能的面试追问

- **Q: 为什么读也要走 Raft？** A: 保证线性一致性。如果直接读本地状态，可能读到 stale data（老 leader 以为自己还是 leader）。
- **Q: 分片迁移期间客户端请求怎么处理？** A: 源 group Freeze 后返回 `ErrWrongGroup`，客户端重读配置后路由到目标 group。
- **Q: Controller 挂了怎么办？** A: Lease 机制 → 超时后其他 controller 可接管，从 kvraft 读取未完成的重配置并继续。
- **Q: 和 Redis Cluster 的 slot 迁移有什么不同？** A: 本质相同（状态转移），但 Redis 支持在线迁移（slot 可部分迁移，key 逐个转移），本实现是整个 shard 原子迁移。
- **Q: 12 个 shard 会不会太少？** A: 教学中足够，实际生产需要更多以减少单 shard 热点。Redis 用 16384 个 slot 也是同理。

---

## 六、容错机制深度分析

### Q1: Controller 单点故障怎么办？

Controller 并不是真正意义上的"单点"——它通过 **lease 机制** 实现了故障接管。

**正常工作流程：**

```
Controller A                    kvraft (Group 0, Raft 复制)
     │                                │
     │── tryAcquireLease() ──────────▶│  读取当前 lease
     │◀── lease 为空或已过期 ────────│
     │── Put("ctrller/lease", A, T+5s)─▶  写入自己的 lease (带版本号 CAS)
     │◀── OK ────────────────────────│  A 成为合法 Controller
     │                                │
     │── renewLease() 每秒一次 ──────▶│  续租，防止超时
     │                                │
     │── ChangeConfigTo(newCfg) ─────▶│  执行分片迁移
```

**故障场景：**

```
Controller A 崩溃
     ✗
     │  lease 到期 (5s 后)
     ▼
Controller B (新实例或备份)
     │
     │── tryAcquireLease() ──────────▶│  lease 已过期
     │◀── lease 已过期 ──────────────│
     │── Put("ctrller/lease", B, T+5s)─▶  CAS 写入成功
     │◀── OK ────────────────────────│  B 成为新 Controller
     │
     │── 读取 cfgNextKey ───────────▶│  检查是否有未完成的重配置
     │◀── 有未完成的 cfgNext ───────│
     │── ChangeConfigTo(cfgNext) ────▶│  继续执行中断的迁移
     │   (从上次断点继续)             │
```

**关键设计点：**

1. **Lease 超时自动失效**：Controller A 崩溃后，不再续租，5 秒后 lease 自动过期。不需要心跳检测，不需要人工介入。
2. **CAS 写入防脑裂**：`tryAcquireLease()` 用版本号写入 kvraft，即使两个 Controller 同时尝试，只有一个会成功（先到先得，另一个得 `ErrVersion`）。
3. **可恢复的重配置**：当前配置和新配置都持久化在 kvraft 中（Raft 复制，不会丢）。新 Controller 通过 `InitController()` 读取 `cfgNextKey`，发现未完成的重配置就调用 `ChangeConfigTo()` 继续执行。`shouldAbortChange()` 在每个 shard 迁移步骤间检查自己是否还是 leader，发现自己被取代就中止。
4. **依赖 kvraft 的高可用**：lease 和配置数据都存在 kvraft（Group 0）中，而 kvraft 本身是 Raft 复制的（通常 3-5 个节点），只要多数节点存活就不会丢数据。

**极端情况：Controller A 假死（网络分区）**

```
Controller A (网络隔离，以为自己还是 leader)
     │── renewLease() ───X  网络不通
     │                      
     │  5s 后...            
     │                      
Controller B (网络正常)
     │── tryAcquireLease() ──▶  成功，lease 已过期
     
Controller A 尝试继续操作
     │── FreezeShard() ──────▶  kvraft 返回 ErrVersion (lease 已被 B 修改)
     │── shouldAbortChange() 检测到自己不是 leader，中止
     
     // 或者 A 直接操作 shard group
     │── FreezeShard() ──────▶  shard group 检查 configNum
     │                          FreezeShard 请求带的 Num 已过期
     │                          返回拒绝 (seen[shard] 版本检查)
```

即使 Controller A 绕过了 lease 检查直接操作 shard group，每个 shard group 的 `DoOp` 在 `FreezeShardArgs` 处理中也会检查 `req.Num < seen[shard]`，拒绝过期配置的迁移请求——双重防护。

**与 Redis Sentinel 的对比：**
- Redis Sentinel：多个 Sentinel 节点投票决定 failover（需要至少 3 个 Sentinel 避免脑裂），投票机制本身依赖多数派。
- 本实现：lease + kvraft（Raft 多数派），本质上也是依赖多数派，但更简洁——不需要额外的 Sentinel 进程，lease 本身就在高可用的 kvraft 中。

---

### Q2: 多台机器中如果因某一台机器故障导致该机器中数据丢失怎么办？

这个问题需要分两个层面来回答：**内存/进程故障**（最常见）和 **磁盘永久损坏**（极端情况）。

#### 场景一：进程崩溃 / 机器重启（最常

见）

```
Group 1 (3 个节点, 管理 Shard 0-5)

  Node 1 (Leader)    Node 2 (Follower)   Node 3 (Follower)
      │                   │                    │
      │  log[1..100]      │  log[1..98]        │  log[1..99]
      │  snapshot@50       │  snapshot@50        │  snapshot@50
      │                   │                    │
      │                   │  ✗ CRASH           │
      │                   │  (进程崩溃)         │
      │                   │                    │
      │  Raft 多数派仍在   │                    │  Raft 多数派仍在
      │  2/3 > 3/2 = 1.5  │                    │  2/3 > 3/2 = 1.5
      │                   │                    │
      │  集群正常工作      │                    │  集群正常工作
      │                   │                    │
      │                   │  ◀ 重启 ──────────│
      │                   │  Persister.Save()  │
      │                   │  读取: snapshot@50  │
      │                   │       + raftstate  │
      │                   │  恢复: term, vote  │
      │                   │  重建: log[51..98] │
      │                   │  lastApplied = 50  │
      │                   │                    │
      │── AppendEntries ──▶  追赶日志          │
      │   (log[99..100])  │                    │
      │                   │  commitIndex → 100 │
      │                   │  lastApplied → 100 │
      │                   │  apply log[51..100]│
      │                   │  状态恢复完成       │
```

**恢复过程详解：**

1. **持久化状态恢复**：Raft 节点的 `Persister` 将 `raftstate`（currentTerm, votedFor, log）和 `snapshot` 原子写入磁盘。节点重启时：
   - 读取 `snapshot` → 恢复状态机数据（Store, owned, frozen, seen maps）
   - 读取 `raftstate` → 恢复 Raft 状态（term, vote, 以及 snapshot 之后的 log entries）
2. **日志追赶**：恢复后 `lastApplied = snapshotIndex = 50`，但 leader 已经 commit 到 index 100。Leader 的下一次 `AppendEntries` 会发送 log[51..100]，follower 追上来。
3. **状态机重放**：`applier` goroutine 将 log[51..100] 通过 `applyCh` 逐个 apply 到状态机，执行 `DoOp`，状态恢复到和 leader 一致。

**KV 数据的持久化路径：**

```
Client Put("key1", "val1", version=0)
  → Leader Start(PutArgs)
  → 写入 Raft log entry (持久化到 Persister)
  → 复制到多数 followers (各自 Persister 持久化)
  → commitIndex 推进
  → applyCh ApplyMsg
  → DoOp(PutArgs): store["key1"] = ValueStore{"val1", 1}
  → Snapshot(每 10 个命令或达到 maxraftstate): 
      编码 store + owned + frozen + seen
      → Persister.Save(snapshot, raftstate)
```

数据在 apply 到状态机时就已在内存中，但持久化是通过两个机制保证的：
- **Raft log**：每个操作在 commit 前必须持久化到多数节点的 log 中
- **Snapshot**：定期将状态机全量快照也持久化

#### 场景二：磁盘永久损坏（数据完全丢失）

```
Node 2 磁盘损坏
     ✗ 所有持久化数据丢失

重启后:
     Node 2 (空白状态, term=0, log=空, snapshot=空)
         │
         │◀── InstallSnapshot ──── Node 1 (Leader)
         │    snapshot@100          snapshot = {store, owned, frozen, seen}
         │    (全量状态机数据)       
         │
         │  保存 snapshot @ index 100
         │  丢弃 index <= 100 的 log
         │  设置 commitIndex = lastApplied = 100
         │
         │  状态恢复完成，继续接收新日志
```

Leader 在 `leaderReplication()` 中检查 `nextIndex[peer] <= raftLog.firstIndex()` 时，说明该 peer 落后太多（或完全没有数据），log 已经被 snapshot 截断，无法用 `AppendEntries` 追赶。此时 Leader 发送 `InstallSnapshot` RPC，将**完整的快照数据**（包含所有 KV 数据 + 元数据）直接发送给 follower。

**Raft 的 `InstallSnapshot` 流程：**

```
Leader                                Follower (磁盘丢失)
  │                                       │
  │── InstallSnapshot ──────────────────▶│
  │   {Term, LastIncludedIndex,           │
  │    LastIncludedTerm, Data}            │
  │                                       │── raft.checkInstallSnapshot()
  │                                       │   如果 LastIncludedIndex 
  │                                       │   <= commitIndex: 拒绝(旧快照)
  │                                       │   保存 snapshot + raftstate
  │                                       │   截断 log
  │                                       │   重置 commitIndex, lastApplied
  │                                       │── applyCh ← SnapshotValid
  │                                       │── sm.Restore(snapshot.Data)
  │                                       │   状态机恢复完整状态
  │◀── OK ──────────────────────────────│
  │                                       │
  │── nextIndex[peer] = LastIncludedIndex+1
```

**数据恢复的完整性保证：**

| 故障类型 | 数据是否能恢复 | 原理 |
|---------|:--:|------|
| 少数节点进程崩溃 | ✓ 无数据丢失 | 多数派仍然持有完整 log，崩溃节点重启后从 log 追赶 |
| 少数节点磁盘损坏 | ✓ 无数据丢失 | Leader 通过 InstallSnapshot 发送全量快照 |
| 多数节点同时损坏 | ✗ 数据丢失 | Raft 需要多数派存活才能 commit，多数派数据丢失意味着已 commit 的数据也丢了 |
| Leader 崩溃 | ✓ 无数据丢失 | 新 Leader 选举，新 Leader 至少包含所有已 commit 的日志 |
| 整个 Group 全部宕机 | ✗ 暂时不可用 | 所有节点恢复后可从持久化数据恢复，期间该 group 的 shard 不可用 |

#### 对比 Redis

| 故障场景 | 本实现 | Redis |
|---------|--------|-------|
| Slave 崩溃 | Raft follower 重启后从 leader 日志追赶或接收 snapshot | Slave 重启后全量 RDB 同步（耗时）或部分重同步 |
| Master 崩溃 | Raft 自动选举新 leader，已 commit 数据不丢 | Sentinel 手动/自动 failover，异步复制可能丢失未同步数据 |
| 磁盘损坏 | InstallSnapshot 从 leader 全量恢复 | 从 master 全量 RDB 同步，但 master 自身损坏需从 AOF/RDB 备份恢复 |
| 数据一致性 | 同步复制，commit = 多数持久化 | 异步复制，可能丢数据 |

**Redis 数据丢失风险示例：**

```
Redis Master                     Redis Slave
    │                                │
    │── SET key "val" ─────────────▶│  (异步，还在网络传输中)
    │── +OK ◀──── 客户端              │
    │                                │
    │  ✗ Master 宕机                  │
    │                                │
    │  客户端认为写入成功              │  Slave 未收到，数据丢失
    │  Slave 提升为 Master             │  val 不存在
```

而在本实现中，`Put` 只有在 Raft 多数派持久化日志后才返回成功给客户端，不会出现这种情况。

---

## 七、总结：容错机制一览

```
                    故障域                    防护机制
    ┌─────────────────────────────────────────────────────┐
    │  Controller 故障          Lease 超时 + 新 Controller 接管 │
    │                          配置存储在 kvraft (Raft 复制)   │
    ├─────────────────────────────────────────────────────┤
    │  Group 内少数节点故障       Raft 多数派继续工作          │
    │  (进程崩溃/网络分区)        恢复后追赶日志               │
    ├─────────────────────────────────────────────────────┤
    │  Group 内节点数据丢失        InstallSnapshot 全量恢复    │
    │  (磁盘损坏)                从 Leader 拉取快照            │
    ├─────────────────────────────────────────────────────┤
    │  Group 内 Leader 故障       Raft 自动选举新 Leader      │
    │                           已 commit 数据不丢失          │
    ├─────────────────────────────────────────────────────┤
    │  整个 Group 全部宕机        该 Group 的 Shard 暂时不可用 │
    │                           重启后从持久化数据恢复         │
    ├─────────────────────────────────────────────────────┤
    │  脑裂 (网络分区)            Raft 多数派原则             │
    │                           少数派无法 commit             │
    │                           Controller lease CAS 防双主   │
    └─────────────────────────────────────────────────────┘
```

---

## 八、线性一致性 (Linearizability) 的实现

线性一致性是分布式系统中最强的一致性模型，它要求：**所有操作看起来像是按某个全局顺序原子执行的，且这个顺序与真实时间顺序一致**（即如果操作 A 在操作 B 开始之前就结束了，那么 A 必须排在 B 前面）。

本实现通过四层机制协同保证线性一致性：

### 8.1 核心机制：读写都走 Raft

这是最关键的设计决策。无论是 `Get`（读）还是 `Put`（写），都提交给 Raft 共识后再执行，**而不是直接从本地状态机读取**。

```
客户端 Get("key1")                        客户端 Put("key1", "val", v=0)
     │                                          │
     ▼                                          ▼
  KVServer.Get()                           KVServer.Put()
     │                                          │
     ▼                                          ▼
  rsm.Submit(GetArgs{Key:"key1"})     rsm.Submit(PutArgs{Key:"key1", Value:"val", Version:0})
     │                                          │
     ▼                                          ▼
  rf.Start(op)                            rf.Start(op)
     │                                          │
     ▼                                          ▼
  ┌─────────────────────────────────────────────────────────┐
  │                     Raft 共识层                          │
  │                                                         │
  │  1. Leader 将 Op 追加到本地 log                          │
  │  2. Leader 复制到所有 follower                            │
  │  3. 等待多数派确认 (quorum)                               │
  │  4. advanceCommitIndex → commitIndex 推进                │
  │  5. applyCh ← ApplyMsg                                  │
  └─────────────────────────────────────────────────────────┘
     │                                          │
     ▼                                          ▼
  reader() goroutine                       reader() goroutine
     │                                          │
     ▼                                          ▼
  sm.DoOp(GetArgs)                        sm.DoOp(PutArgs)
  → 读 store["key1"]                      → 写 store["key1"] = {val, 1}
     │                                          │
     ▼                                          ▼
  返回 GetReply{Value, Version}           返回 PutReply{OK}
```

**为什么读也必须走 Raft？**

假设读直接访问本地状态机（不走 Raft）：

```
时间线:
  T1: Leader 节点 A 网络分区，与其他节点隔离
  T2: 其他节点选举 B 为新 Leader
  T3: B 处理 Put("x", "v2")，commit
  T4: A 仍然认为自己是 Leader
  T5: 客户端向 A 发 Get("x")，A 返回旧值 "v1"  ← 违反线性一致性！
```

由于网络分区，A 已成为少数派，不可能 commit 任何新数据。但如果允许它直接返回本地状态，客户端就读到了**已过期的旧数据**。写入已经发生在 B 上（按真实时间在 T3 已经完成），但读操作（T5）返回了旧值，打破了 "后续读必须看到之前写的结果" 的约束。

**通过 Raft 的解决方案**：A 收到 Get 请求后调用 `rf.Start()`，但发现自己不是 Leader（或 term 已变），返回 `ErrWrongLeader`，客户端重试到真正的 Leader B，从而读到最新的 "v2"。

### 8.2 RSM 的 Submit 机制：如何把操作绑定到 Raft 日志顺序

`rsm.Submit()` 是整个一致性的桥梁。它的核心逻辑 (`src/kvraft1/rsm/rsm.go:163-216`)：

```go
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
    // 1. 生成全局唯一的 Op ID（epoch + 递增序号）
    op := Op{Me: rsm.me, Req: req, ID: rsm.epoch<<32 | rsm.nexSeq}
    rsm.nexSeq++

    // 2. 提交给 Raft，Raft 返回日志的 index 和当前 term
    index, startTerm, isLeader := rsm.rf.Start(op)
    if !isLeader {
        return rpc.ErrWrongLeader, nil  // 不是 Leader，立即返回
    }

    // 3. 注册等待通道：记录 log index → op ID 的映射
    rsm.pendingLogByOpID[index] = op.ID
    ch := make(chan submitResult, 1)
    rsm.waitChByOpID[op.ID] = ch

    // 4. 循环等待：要么收到 committed 结果，要么检测到丢权
    for {
        select {
        case res := <-ch:           // reader goroutine 通知结果
            return res.err, res.rep
        case <-ticker.C:            // 每 10ms 检查
            currentTerm, isLeader := rsm.rf.GetState()
            if currentTerm != startTerm || !isLeader {
                // 领导权丢失，清理等待，返回错误
                delete(rsm.pendingLogByOpID, index)
                delete(rsm.waitChByOpID, op.ID)
                return rpc.ErrWrongLeader, nil
            }
        }
    }
}
```

**关键设计：pendingLogByOpID**

`pendingLogByOpID` 记录的是 "log index → op ID" 的映射。当 reader goroutine 在特定 log index 处 apply 了一个 Op，它会检查该 Op 的 ID 是否与等待中的一致：

```go
// reader goroutine (rsm.go:93-154)
for msg := range rsm.applyCh {
    if msg.CommandValid {
        op := msg.Command.(Op)
        rep := rsm.sm.DoOp(op.Req)

        if expectID, ok := rsm.pendingLogByOpID[msg.CommandIndex]; ok {
            if expectID == op.ID {
                // 匹配！正是我们等待的操作
                ch <- submitResult{rpc.OK, rep}
            } else {
                // 不匹配！有别人占了这个 index → 我们已经不是 Leader
                ch <- submitResult{rpc.ErrWrongLeader, nil}
            }
        }
    }
}
```

这个 ID 匹配检查解决了**两个 Leader 同时提议**的经典问题：

```
场景:
  T1: Server A (term=5 Leader) 调用 Start(opA, ID=0x50001) → log index 10
  T2: A 还没来得及复制就崩溃 / 网络分区
  T3: Server B 当选 term=6 Leader
  T4: B 也在 index 10 处写入了 opB, ID=0x60001
  T5: opB commit 了，apply 到 A 时
      → pendingLogByOpID[10] = 0x50001
      → 实际 op.ID = 0x60001
      → 不匹配 → 通知 A 的等待者 ErrWrongLeader
```

这确保了：即使一个旧 Leader 的提议碰巧在同一个 log index 被 commit，它也能正确检测到自己已不再是 Leader，不会错误地将其他 Leader 的操作结果返回给客户端。

### 8.3 Raft 的 Commitment 安全保证

`advanceCommitIndex()` (`src/raft1/raft.go:403-418`) 有两个关键的安全检查：

```go
func (rf *Raft) advanceCommitIndex() {
    // 1. 中位数 = 多数派的 matchIndex
    newCommitIndex := sortMatchIndex[n-(n/2+1)]

    if newCommitIndex > rf.commitIndex {
        // 2. 安全条件（Raft 论文 §5.4.1）：
        //    只能 commit 当前 term 的 entry
        if rf.currentTerm == rf.log[newCommitIndex].Term {
            rf.commitIndex = newCommitIndex
            rf.applyCond.Signal()
        }
    }
}
```

**为什么只能 commit 当前 term 的 entry？**（Raft 论文 Figure 8 的问题）

```
term 2: S1 Leader, 在 index 2 写 entry, 复制到 S2 就崩溃了
term 3: S5 当选 Leader (S3,S4,S5 投票), 在 index 2 写新 entry
term 4: S1 当选 Leader (S1,S2,S3,S4 投票), 
        如果允许 term2 的旧 entry @index2 被 commit，
        但它与 term3 的 entry @index2 冲突 → 不一致！
```

通过限制 "只能 commit 当前 term 的 entry"，Raft 保证了：一旦一个 entry 被 commit，它的 log index 位置就不可被其他 entry 替代（Leader Completeness Property）。

### 8.4 版本号与乐观并发控制

每次 `Put` 操作完成后，版本号递增：

```go
// DoOp in kvraft1/server.go:31-61
case rpc.PutArgs:
    if req.Version != state.Version {
        return rpc.PutReply{Err: rpc.ErrVersion}  // 版本不匹配 → 冲突
    }
    state.Value = req.Value
    state.Version += 1   // 版本递增
    kv.Store[req.Key] = state
    return rpc.PutReply{Err: rpc.OK}
```

这实现了 **CAS (Compare-And-Swap)** 语义：
- 客户端先 `Get` 获取当前版本号
- 修改时携带这个版本号
- 如果中途被别人修改（版本号变了），返回 `ErrVersion` 让客户端重试

**一致性层面的意义**：版本号让客户端能检测到并发冲突，避免 "last write wins" 的无声覆盖。虽然 Raft 已经保证了操作的顺序性，但版本号实现了**应用层的冲突检测**——它让客户端能实现 read-modify-write 的原子性。

### 8.5 线性一致性验证（Porcupine 模型）

代码中通过 Porcupine 线性一致性检查器做形式化验证 (`src/models1/kv.go`)：

```go
var KvModel = porcupine.Model{
    // 按 key 分区：不同 key 的操作可以并发
    Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
        // 将操作按 key 分组，每个 key 独立验证
    },

    // 初始状态：空字符串 + 版本号 0
    Init: func() interface{} {
        return KvState{"", 0}
    },

    // 状态转移规则（这是线性一致性的形式化定义）
    Step: func(state, input, output interface{}) (bool, interface{}) {
        switch inp.Op {
        case 0: // Get
            // Get 必须返回当前状态的 value
            return out.Value == st.Value, state

        case 1: // Put
            if st.Version == inp.Version {
                // 版本匹配 → Put 成功 → 状态更新
                return out.Err == "OK" || out.Err == "ErrMaybe",
                       KvState{inp.Value, st.Version + 1}
            } else {
                // 版本不匹配 → 返回 ErrVersion
                return out.Err == "ErrVersion" || out.Err == "ErrMaybe",
                       st  // 状态不变
            }
        }
    },
}
```

**`ErrMaybe` 的特殊意义**：当客户端重试 Put 后收到 `ErrVersion`（第一次尝试可能已成功但回复丢失），客户端返回 `ErrMaybe`。线性一致性模型允许 `ErrMaybe` 出现在任何状态——它等价于 "这个操作可能发生了，也可能没发生"。这是分布式中**无法区分 "失败了" 和 "成功了但回复丢了"** 的经典问题的优雅处理。

### 8.6 一致性的完整链路（端到端时序）

```
客户端                     RSM                     Raft                    状态机
  │                         │                       │                        │
  │── Put("x","v2",v=1) ──▶│                       │                        │
  │                         │── Start(op) ────────▶│                        │
  │                         │                       │── 追加到本地 log       │
  │                         │                       │── persist()           │
  │                         │                       │── 复制到 Follower     │
  │                         │                       │◀── Follower ACK ──    │
  │                         │                       │── 多数派确认          │
  │                         │                       │── advanceCommitIdx()  │
  │                         │                       │── applyCh ← ApplyMsg  │
  │                         │◀── reader() 收到 ────│                        │
  │                         │── DoOp(PutArgs) ───────────────────────────▶│
  │                         │◀── PutReply{OK} ───────────────────────────│
  │                         │── ch ← submitResult  │                       │
  │◀── PutReply{OK} ──────│                       │                       │
  │                         │                       │                       │
  │ 此时 "x"="v2", v=2     │                       │                       │
  │                         │                       │                       │
  │── Get("x") ──────────▶│                       │                       │
  │                         │── Start(op) ────────▶│  (同上完整流程)        │
  │                         │                       │── commit, apply       │
  │                         │── DoOp(GetArgs) ──────────────────────────▶│
  │                         │◀── v2, v=2 ───────────────────────────────│
  │◀── GetReply{v2, v=2}──│                       │                       │
```

每次操作（读或写）都经过完整的 Raft 共识流程：Propose → 持久化 → 复制到多数派 → Commit → Apply → 返回。

### 8.7 与 Redis 的一致性对比总结

```
                     本实现                      Redis (默认配置)
              ┌───────────────────┐       ┌───────────────────┐
  读操作       │  Raft 共识       │       │  直接读本地内存    │
              │  (线性一致)      │       │  (可能 stale)     │
              ├───────────────────┤       ├───────────────────┤
  写操作       │  Raft 共识       │       │  写 Master 内存    │
              │  (多数派确认)    │       │  (异步复制)       │
              ├───────────────────┤       ├───────────────────┤
  复制        │  同步复制        │       │  异步复制         │
              │  commit 前需 ACK  │       │  Master 不等 Slave │
              ├───────────────────┤       ├───────────────────┤
  冲突检测    │  版本号 CAS      │       │  WATCH/MULTI      │
              │  (内置)          │       │  (可选)           │
              ├───────────────────┤       ├───────────────────┤
  线性一致性  │  架构保证        │       │  需 WAIT 命令     │
  保证        │  + Porcupine 验证 │       │  (非默认)         │
              └───────────────────┘       └───────────────────┘
```

**一句话总结**：本实现的一致性核心思路是 "所有操作都经过 Raft 共识后才生效"——读不绕过 Leader，写不绕过多数派。这是教科书式的 Raft 复制状态机实现，也是 etcd、Consul、TiKV 等一致性 KV 存储的通用做法。代价是每次操作都需要至少一轮网络 RTT + 磁盘写入，延迟远高于 Redis。Redis 选择的是 "高性能优先，一致性靠配置补偿" 的路线。
