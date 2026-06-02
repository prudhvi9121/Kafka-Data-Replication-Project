# Kafka MirrorMaker 2 — Enhanced Fault-Tolerant Replication

**Docker Hub Images:**
- [`prudhvikarri/enhanced-mirrormaker:4.0.0`](https://hub.docker.com/r/prudhvikarri/enhanced-mirrormaker)
- [`prudhvikarri/commit-log-producer:1.0`](https://hub.docker.com/r/prudhvikarri/commit-log-producer)

**Repository Links:**
- **Kafka Fork:** [https://github.com/prudhvi9121/kafka/](https://github.com/prudhvi9121/kafka/)
- **Pull Request:** [MirrorMaker 2 — Fault-Tolerant Replication (Log Truncation + Topic Reset)](https://github.com/prudhvi9121/kafka/pull/2)

A production-hardened MirrorMaker 2 (MM2) that adds **log truncation detection** (fail-fast) and **topic reset recovery** (graceful resubscription) on top of Apache Kafka 4.0.0.

## Quick Start

```bash
# 1. Build the enhanced MM2 JAR (from the kafka/ directory)
cd ../kafka
./gradlew :connect:mirror:jar -x test --no-daemon

# 2. Build Docker images and run all three test scenarios
cd ../kafka-test
docker compose build
./run_challenge.sh
```

Expected output:
```
✔ Scenario 1 Passed! Replicated 1000/1000 records successfully.
✔ Scenario 2 Passed! Enhanced MirrorMaker detected the truncation gap and failed-fast.
✔ Scenario 3 Passed! Topic reset detected and handled gracefully.
ALL CORE CHALLENGE EVALUATION TASKS PASSED !
```

---

## Architecture

```
Primary Cluster (port 9092)          Standby / DR Cluster (port 9192)
┌─────────────────────────┐          ┌─────────────────────────┐
│  Topic: commit-log       │          │  Topic: primary.commit-  │
│  (Write-Ahead Log)       │          │  log (replicated copy)   │
│                          │          │                          │
│  Partition 0             │          │  Partition 0             │
│  offset 0 → N            │          │  offset 0 → N            │
└──────────┬───────────────┘          └──────────▲──────────────┘
           │                                     │
           │      Enhanced MirrorMaker 2          │
           │   ┌───────────────────────────┐      │
           └──►│  MirrorSourceTask         ├──────┘
               │  • verifyOffsetSequence() │
               │  • handleExceptionBounds()│
               │  • initializeConsumer()   │
               └───────────────────────────┘
```

**Topic naming:** `DefaultReplicationPolicy` is used (not `IdentityReplicationPolicy`), so the DR topic is prefixed with the source alias: `primary.commit-log`. This matches the assignment spec and avoids cycle-detection issues.

---

## Source Code Changes

Only **one** Kafka source file was modified:

### [`connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`](../kafka/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java)

Total lines changed: **~120 lines** across additions and modifications.

#### Change 1 — Data Loss Exception Sentinel

```java
// New inner class added to MirrorSourceTask
public static class DataLossException extends ConnectException {
    public DataLossException(String message) { super(message); }
}
```

A typed exception that distinguishes deliberate fail-fast terminations from ordinary Connect errors.

#### Change 2 — Compacted Topic Allowlist (Task 2 prerequisite)

```java
private final Set<String> compactedTopics = new java.util.HashSet<>();
```

Compacted topics legitimately skip offsets (log compaction removes duplicate keys). The set is consulted before raising a data loss alarm so compacted topics are exempt from the offset-gap check.

#### Change 3 — `verifyOffsetSequence()` (Tasks 2 & 3)

Called on every record consumed from the source topic to verify the offset is exactly the one expected.

```java
private boolean verifyOffsetSequence(TopicPartition tp, long actualOffset) {
    Long expectedOffset = expectedOffsets.get(tp);
    if (expectedOffset == null) return false;

    // TASK 2: Gap detected → data was purged between last replicated and current offset
    if (actualOffset > expectedOffset) {
        if (compactedTopics.contains(tp.topic())) { /* skip */ }
        log.error("[CRITICAL REPLICATION GAP] ...");
        System.exit(1);          // Connect swallows exceptions; must exit the JVM directly
        throw new DataLossException("...");  // unreachable; satisfies compiler
    }

    // TASK 3: Backward offset → topic was deleted and recreated
    if (actualOffset < expectedOffset) {
        log.warn("[TOPIC RESET DETECTED] ... at {}", Instant.now(), ...);
        consumer.seekToBeginning(Collections.singletonList(tp));
        expectedOffsets.put(tp, actualOffset + 1L); // accept this record as valid post-reset data
        return false;
    }

    return false;
}
```

**Design decision — why `System.exit(1)` instead of `throw`?**  
Kafka Connect's `WorkerSourceTask` catches all `RuntimeException` (and `ConnectException`) thrown from `poll()` or `start()`, marks the task as FAILED, and continues running the JVM. Throwing alone does not stop the container. `System.exit(1)` is the only reliable way to make the container exit and signal failure to the orchestrator.

**Design decision — why accept the reset-triggering record?**  
The original code returned `true` from the reset branch, causing `poll()` to discard the entire batch including the record at offset 0. That record is valid post-reset data. The fix sets `expectedOffsets[tp] = actualOffset + 1` and returns `false`, allowing the record to be forwarded to the DR cluster.

#### Change 4 — `handleExceptionBounds()` (Task 3, startup path)

Handles `OffsetOutOfRangeException` thrown by the consumer when MM2 tries to seek to an offset that no longer exists (topic was recreated while MM2 was down).

```java
private void handleExceptionBounds(OffsetOutOfRangeException e) {
    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(...);
    for (TopicPartition tp : ...) {
        long expected  = expectedOffsets.get(tp);   // where MM2 expects to resume
        long beginning = beginningOffsets.get(tp);  // earliest available offset

        // Data was purged before MM2 could replicate it → unrecoverable
        if (expected < beginning) {
            log.error("[CRITICAL DATA LOSS AT STARTUP] ...");
            System.exit(1);
        }

        // Topic was recreated (beginning rolled back to 0)
        if (beginning == 0 && expected > 0) {
            log.warn("[TOPIC RESET DETECTED] ... at {}", Instant.now(), ...);
            consumer.seekToBeginning(...);
            expectedOffsets.put(tp, 0L);
        }
    }
}
```

#### Change 5 — `initializeConsumer()` (Task 2, startup gap check)

Compares MM2's last committed offset against the topic's current beginning offset at startup. If the beginning has advanced past the next-expected offset, records were purged before replication — immediate exit.

```java
void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
    Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
    Map<TopicPartition, Long> allBeginningOffsets = consumer.beginningOffsets(...); // all partitions

    for (TopicPartition tp : committedPartitions) {
        long earliestAvailable = allBeginningOffsets.get(tp);
        long lastCommitted     = topicPartitionOffsets.get(tp);

        if (earliestAvailable > (lastCommitted + 1)) {
            log.error("[CRITICAL DATA LOSS AT STARTUP] ...");
            System.exit(1);
        }
    }
    // ... seek each partition to nextOffset
}
```

**Design decision — `auto.offset.reset = none`**  
Set in `start()` to override whatever the operator configured. Without this, the consumer would silently seek to the earliest or latest offset on `OffsetOutOfRangeException`, bypassing our detection logic entirely.

#### Change 6 — Per-record `INFO` log demoted to `DEBUG`

The original code logged every single record at `INFO` level. At 1,000 records/sec this floods logs and makes debugging impossible. Changed to `DEBUG`.

---

## Infrastructure Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Two KRaft Kafka clusters + MM2 (custom image) + producer service |
| `Dockerfile.mirrormaker` | Extends `apache/kafka:4.0.0`, replaces stock JAR with enhanced one |
| `commit-log-producer/Dockerfile` | Multi-stage Maven build → minimal JRE runtime image |
| `commit-log-producer/pom.xml` | Added `maven-shade-plugin` to produce an executable fat-jar |
| `mm2.properties` | Switched to `DefaultReplicationPolicy`; disabled `sync.topic.configs` to prevent retention settings propagating to DR |
| `run_challenge.sh` | Automated end-to-end test suite for all 3 scenarios |

---

## Test Scenario Details

### Scenario 1 — Normal High-Velocity Replication
Produces 1,000 JSON events to `commit-log` on the primary cluster and polls `primary.commit-log` on the DR cluster until all 1,000 records are confirmed replicated (60-second timeout).
![alt text](<Screenshot 2026-05-29 114108.png>)

### Scenario 2 — Log Truncation Fail-Fast
1. MM2 stopped cleanly (offsets committed: 999)
2. `retention.ms=1000` applied; 10 new messages produced (offsets 1000–1009)
3. Wait 70 seconds for log retention to purge those messages
4. 1 fresh message produced (offset 1010) to create a visible gap
5. MM2 restarted; `initializeConsumer` detects `earliestAvailable=1010 > lastCommitted+1=1000`
6. MM2 calls `System.exit(1)` → container exits → test verifies `State.Running=false`
![alt text](<Screenshot 2026-05-29 114145.png>)

### Scenario 3 — Graceful Topic Reset Recovery
1. `commit-log` deleted and recreated (beginning offset resets to 0) **before** MM2 starts
2. MM2 starts; startup check: `beginning=0 > lastCommitted+1=1000`? → NO → no crash
3. MM2 seeks to offset 1000 → `OffsetOutOfRangeException`
4. `handleExceptionBounds`: `beginning=0, expected=1000` → `[TOPIC RESET DETECTED]` logged
5. `seekToBeginning()` called → MM2 resumes from offset 0 and replicates normally
![alt text](<Screenshot 2026-05-29 114219.png>)

---

### AI Usage

AI assistance was used mainly for research, debugging support, and understanding Kafka MirrorMaker 2 internals during development. It helped analyze runtime issues, validate edge-case handling, and speed up investigation of Kafka Connect behavior.

