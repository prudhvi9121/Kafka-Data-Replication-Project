# Kafka MirrorMaker 2 Enhanced Fault-Tolerant Replication

**Docker Hub Images:**
- [`prudhvikarri/enhanced-mirrormaker:4.0.0`](https://hub.docker.com/r/prudhvikarri/enhanced-mirrormaker)
- [`prudhvikarri/commit-log-producer:1.0`](https://hub.docker.com/r/prudhvikarri/commit-log-producer)

**Repository Links:**
- **Kafka Fork:** [https://github.com/prudhvi9121/kafka/](https://github.com/prudhvi9121/kafka/)
- **Pull Request:** [MirrorMaker 2 — Fault-Tolerant Replication (Log Truncation + Topic Reset)](https://github.com/prudhvi9121/kafka/pull/1/changes)

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
┌─────────────────────────┐           ┌─────────────────────────┐
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

All changes were made in two files in the Kafka repository:
- [`MirrorSourceTask.java`](../kafka/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java) (Main implementation)
- [`MirrorSourceTaskTest.java`](../kafka/connect/mirror/src/test/java/org/apache/kafka/connect/mirror/MirrorSourceTaskTest.java) (Unit tests)

Here is a simple explanation of the changes:

1. **Log Truncation Detection (Preventing Silent Data Loss):**
   - **On Startup:** MirrorMaker checks the beginning offset of the source partition. If the beginning offset is greater than the last offset replicated (plus one), it means some messages were deleted by Kafka's log retention before replication could complete.
   - **Mid-Stream:** While running, MirrorMaker continuously checks if the offset of incoming records matches the expected next offset. If the incoming offset is greater, a gap is detected.
   - **Action:** In both cases, MirrorMaker prints a critical data loss warning and immediately terminates the task (fails-fast) to prevent replica mismatch. Compacted topics are exempt from this check.
   ```java
   // From MirrorSourceTask.java: Startup data-loss check
   if (earliestAvailable > (lastCommitted + 1)) {
       log.error("[CRITICAL DATA LOSS AT STARTUP] Partition {} has purged records...", tp);
       exitOrThrow("Data loss at startup on partition " + tp, null);
   }
   ```

2. **Graceful Topic Reset Recovery:**
   - **On Startup:** MirrorMaker checks if the topic was deleted and recreated (indicated by the earliest offset resetting to `0` while the last committed offset is positive and exceeds the broker's current end offset).
   - **Mid-Stream:** MirrorMaker detects if the offset of polled records rolls back to a value lower than expected (meaning the topic was recreated mid-stream).
   - **Action:** Instead of crashing or stalling, MirrorMaker logs a warning and automatically seeks to the beginning offset (`0`) to seamlessly continue replication.
   ```java
   // From MirrorSourceTask.java: Mid-stream reset recovery
   if (actualOffset < expectedOffset) {
       log.warn("[TOPIC RESET DETECTED] Source topic-partition {} reset...", tp);
       consumer.seekToBeginning(Collections.singletonList(tp));
       expectedOffsets.put(tp, actualOffset + 1L);
   }
   ```

3. **Explicit Offset Reset Control:**
   - We override the Kafka consumer configuration and set `auto.offset.reset = none`. 
   - This ensures that Kafka does not silently reset offsets when they go out of bounds, allowing MirrorMaker to catch the exception and run our custom data loss or reset detection logic.
   ```java
   // From MirrorSourceTask.java: Disable automatic consumer reset
   consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
   ```

4. **Cleaner Log Output:**
   - Changed the per-record logging level from `INFO` to `DEBUG` to prevent flooding the logs when replicating at high speeds (1,000+ messages/sec).
   ```java
   // From MirrorSourceTask.java: Log check sequence at DEBUG level
   log.debug("Checking offset sequence: partition={}, actualOffset={}, expectedOffset={}",
           tp, actualOffset, expectedOffset);
   ```

5. **Updated Unit Tests:**
   - Added multiple new unit tests in `MirrorSourceTaskTest.java` to verify data-loss detection, compacted topic boundaries, out-of-range exception behaviors, and mid-stream/startup reset scenarios.
   - Updated the legacy test `testSeekBehaviorDuringStart()` to mock partition beginning/end offsets and prevent false-positive topic reset triggers. (*Justification: Since the task now checks partition boundaries during startup validation, the consumer mock must return valid offset bounds to verify normal seek behavior rather than defaulting to empty maps.*)
   ```java
   // From MirrorSourceTaskTest.java: Stub offsets in legacy seek test
   when(mockConsumer.beginningOffsets(any())).thenReturn(beginningOffsets);
   when(mockConsumer.endOffsets(any())).thenReturn(endOffsets);
   ```

---

## Infrastructure Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Two KRaft Kafka clusters + MM2 (custom image) + producer service |
| `Dockerfile.mirrormaker` | Based on `eclipse-temurin:17-jre-jammy`; extracts the full Kafka release tarball built from our modified source. Swapping only the connect-mirror JAR into `apache/kafka:4.0.0` fails with `NoSuchMethodError` because SNAPSHOT JARs call APIs not present in `kafka-clients-4.0.0`. The tarball guarantees all JARs are version-consistent. |
| `commit-log-producer/Dockerfile` | Multi-stage Maven build → minimal JRE runtime image |
| `commit-log-producer/pom.xml` | Added `maven-shade-plugin` to produce an executable fat-jar |
| `mm2.properties` | Switched to `DefaultReplicationPolicy`; disabled `sync.topic.configs` to prevent retention settings propagating to DR |
| `run_challenge.sh` | Automated end-to-end test suite for all 3 scenarios |

---

## Test Scenario Details

### Scenario 1 — Normal High-Velocity Replication
Produces 1,000 JSON events to `commit-log` on the primary cluster and polls `primary.commit-log` on the DR cluster until all 1,000 records are confirmed replicated (60-second timeout).

![Scenario 1 — Normal Replication](screenshots/Screenshot%202026-05-29%20114108.png)

### Scenario 2 — Log Truncation Fail-Fast
1. MM2 stopped cleanly (offsets committed: 999)
2. `retention.ms=1000` applied; 10 new messages produced (offsets 1000–1009)
3. Wait 70 seconds for log retention to purge those messages
4. 1 fresh message produced (offset 1010) to create a visible gap
5. MM2 restarted; `initializeConsumer` detects `earliestAvailable=1010 > lastCommitted+1=1000`
6. MM2 calls `Exit.exit(1)` → container exits → test verifies `State.Running=false`

![Scenario 2 — Log Truncation Fail-Fast](screenshots/Screenshot%202026-05-29%20114145.png)

### Scenario 3 — Graceful Topic Reset Recovery
1. `commit-log` deleted and recreated (beginning offset resets to 0) **before** MM2 starts
2. MM2 starts; startup check: `beginning=0 > lastCommitted+1=1000`? → NO → no crash
3. MM2 seeks to offset 1000 → `OffsetOutOfRangeException`
4. `handleExceptionBounds`: `beginning=0, expected=1000` → `[TOPIC RESET DETECTED]` logged
5. `seekToBeginning()` called → MM2 resumes from offset 0 and replicates normally

![Scenario 3 — Topic Reset Recovery](screenshots/Screenshot%202026-05-29%20114219.png)

---

### AI Usage

AI assistance was used mainly for research, debugging support, and understanding Kafka MirrorMaker 2 internals during development. It helped analyze runtime issues, validate edge-case handling, and speed up investigation of Kafka Connect behavior.