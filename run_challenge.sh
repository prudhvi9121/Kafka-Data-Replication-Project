#!/usr/bin/env bash
# =========================================================================
# RUN_CHALLENGE.SH: AUTOMATED END-TO-END VALIDATION SUITE
#
# Tests three scenarios:
#   1. Normal replication: 1000 events produced → replicated to DR cluster
#   2. Log truncation (fail-fast): MM2 detects data loss and crashes
#   3. Topic reset (recovery): MM2 detects topic recreation and re-subscribes
#
# Windows Git Bash note: MSYS_NO_PATHCONV=1 prevents MSYS from converting
# Unix-style paths in docker exec commands on Windows. Harmless on Linux/Mac.
# =========================================================================
set -euo pipefail

# ANSI Terminal Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${YELLOW}====================================================${NC}"
echo -e "${YELLOW}   KAFKA REPLICATION FAULT TOLERANCE TEST SUITE     ${NC}"
echo -e "${YELLOW}====================================================${NC}"

# ---------------------------------------------------------------------------
# Helper: wait_for_topic_count <container> <bootstrap> <topic> <expected_count>
# Polls until the topic end-offset reaches expected_count or times out.
# ---------------------------------------------------------------------------
wait_for_topic_count() {
    local container="$1"
    local bootstrap="$2"
    local topic="$3"
    local expected="$4"
    local timeout=60   # seconds
    local elapsed=0

    echo "  Waiting for $topic on $container to reach $expected messages (timeout ${timeout}s)..."
    while [ "$elapsed" -lt "$timeout" ]; do
        local count
        count=$(MSYS_NO_PATHCONV=1 docker exec "$container" \
            /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell \
            --bootstrap-server "$bootstrap" --topic "$topic" --time -1 2>/dev/null \
            | awk -F ":" '{sum+=$3} END{print sum+0}')
        if [ "$count" -ge "$expected" ]; then
            echo "  ✔ $topic count=$count (target=$expected)"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    echo "  ✘ Timed out: $topic count did not reach $expected within ${timeout}s"
    return 1
}

# =========================================================================
# STEP 1: Clean slate — build images and bring up all services
# =========================================================================
echo -e "\n${YELLOW}[STEP 1/4] Provisioning Clean Docker Environment...${NC}"
cd "$SCRIPT_DIR"

# Build custom images and start clusters
docker compose down --volumes --remove-orphans 2>/dev/null || true
docker compose build mirror-maker commit-log-producer

# Start Kafka clusters first, then MirrorMaker
docker compose up -d primary-kafka standby-kafka
echo "Waiting 20s for KRaft leader election to complete..."
sleep 20

# =========================================================================
# STEP 2: Create commit-log topic on primary cluster ONLY.
# MirrorMaker will auto-create 'primary.commit-log' on standby.
# =========================================================================
echo -e "\n${YELLOW}[STEP 2/4] Creating 'commit-log' topic on Primary Cluster...${NC}"

MSYS_NO_PATHCONV=1 docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --create --topic commit-log \
  --partitions 1 --replication-factor 1 \
  --if-not-exists

echo "Starting Enhanced MirrorMaker 2..."
docker compose up -d mirror-maker
sleep 10

# =========================================================================
# SCENARIO 1: NORMAL REPLICATION FLOW
# Produce 1000 messages and verify they appear on the DR cluster.
# =========================================================================
echo -e "\n${GREEN}[SCENARIO 1] Normal High-Velocity Replication (1000 Events)...${NC}"

docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=primary-kafka:29092 \
    commit-log-producer --count 1000

echo "Verifying replication on Standby DR cluster (topic: primary.commit-log)..."
# DR topic is named 'primary.commit-log' because DefaultReplicationPolicy prefixes with source alias
if wait_for_topic_count standby-kafka localhost:29192 primary.commit-log 1000; then
    echo -e "${GREEN}✔ SUCCESS: Scenario 1 Passed! Replicated 1000/1000 records successfully.${NC}"
else
    echo -e "${RED}✘ FAILURE: Scenario 1 Failed. primary.commit-log count did not reach 1000.${NC}"
    docker compose logs mirror-maker | tail -30
    exit 1
fi

# =========================================================================
# SCENARIO 2: LOG TRUNCATION DETECTION (FAIL-FAST)
# Stop MM2, shrink retention to force data purge, produce more messages,
# wait for retention to kick in, then restart MM2.
# Enhanced MM2 should detect the gap and crash (fail-fast).
# =========================================================================
echo -e "\n${GREEN}[SCENARIO 2] Simulating Log Truncation (Fail-Fast Detection)...${NC}"
echo "Pausing MirrorMaker 2..."
docker compose stop mirror-maker

echo "Dynamically shrinking retention on commit-log (retention.ms=1000)..."
MSYS_NO_PATHCONV=1 docker exec primary-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --alter \
  --entity-type topics \
  --entity-name commit-log \
  --add-config retention.ms=1000,segment.ms=1000,file.delete.delay.ms=1000

echo "Producing 10 more messages that will be purged by retention..."
docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=primary-kafka:29092 \
    commit-log-producer --count 10

echo "Waiting 70s for the log retention worker to purge those 10 messages..."
sleep 70

echo "Producing 1 fresh message to advance head offset beyond the gap..."
docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=primary-kafka:29092 \
    commit-log-producer --count 1

echo "Restarting MirrorMaker 2 (it should detect the truncation gap and crash)..."
docker compose up -d mirror-maker

echo "Waiting up to 60s for MirrorMaker 2 to detect the truncation gap and exit..."
CRASHED=false
for i in $(seq 1 20); do
    sleep 3
    STATUS=$(docker inspect -f '{{.State.Running}}' mirror-maker 2>/dev/null || echo "false")
    if [ "$STATUS" = "false" ]; then
        CRASHED=true
        echo "  MM2 exited after ~$((i * 3)) seconds."
        break
    fi
    echo "  Still running... ($((i * 3))s elapsed)"
done

if [ "$CRASHED" = "true" ]; then
    echo -e "${GREEN}✔ SUCCESS: Scenario 2 Passed! Enhanced MirrorMaker detected the truncation gap and failed-fast.${NC}"
    echo "  Key log line:"
    docker compose logs mirror-maker 2>&1 | grep -m1 "CRITICAL REPLICATION GAP\|CRITICAL DATA LOSS\|DataLossException" || true
else
    echo -e "${RED}✘ FAILURE: Scenario 2 Failed. MirrorMaker is still running (expected crash).${NC}"
    echo "  Recent logs:"
    docker compose logs --tail=30 mirror-maker
    exit 1
fi

# =========================================================================
# SCENARIO 3: GRACEFUL TOPIC RESET HANDLING
# Delete and recreate commit-log FIRST, THEN start MM2.
# When MM2 starts it sees committed=999 but beginning=0 (fresh topic).
# Startup gap check: 0 > (999+1)? NO → no crash.
# MM2 seeks to offset 1000 → OffsetOutOfRangeException → handleExceptionBounds
# detects beginning=0 with expected=1000 → logs [TOPIC RESET DETECTED] → recovers.
# =========================================================================
echo -e "\n${GREEN}[SCENARIO 3] Simulating Topic Reset (Delete + Recreate)...${NC}"

# Reset aggressive retention config from Scenario 2
MSYS_NO_PATHCONV=1 docker exec primary-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --alter \
  --entity-type topics \
  --entity-name commit-log \
  --delete-config retention.ms,segment.ms,file.delete.delay.ms 2>/dev/null || true

# Delete and recreate BEFORE starting MM2 so the topic starts at offset 0.
# With beginning=0 and committed=999: startup check (0 > 1000?) = false → no crash.
# MM2 then seeks to 1000, hits OffsetOutOfRangeException, and enters recovery.
echo "Deleting and recreating 'commit-log' on Primary cluster..."
MSYS_NO_PATHCONV=1 docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --delete --topic commit-log
sleep 3
MSYS_NO_PATHCONV=1 docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --create --topic commit-log \
    --partitions 1 --replication-factor 1

echo "Starting fresh MirrorMaker 2 instance (topic already reset at offset 0)..."
docker compose up -d mirror-maker

echo "Producing 5 post-reset events..."
docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=primary-kafka:29092 \
    commit-log-producer --count 5

echo "Waiting 20s for MM2 to detect the reset and recover..."
sleep 20

echo "Checking MM2 logs for automatic recovery indicators..."
# Use 'docker compose logs' (not raw 'docker logs') so the log stream is read through
# Compose's aggregator — the same path used for the failure dump below. Raw 'docker logs'
# multiplexes stdout/stderr in a binary framing that grep can mishandle on Windows/Git-Bash.
if docker compose logs mirror-maker 2>&1 | grep -q "TOPIC RESET DETECTED"; then
    echo -e "${GREEN}✔ SUCCESS: Scenario 3 Passed! Topic reset detected and handled gracefully.${NC}"
    echo "  Recovery log:"
    docker compose logs mirror-maker 2>&1 | grep "TOPIC RESET DETECTED" | head -3
else
    echo -e "${RED}✘ FAILURE: Scenario 3 Failed. 'TOPIC RESET DETECTED' not found in MM2 logs.${NC}"
    echo "  Recent MM2 logs:"
    docker compose logs --tail=40 mirror-maker
    exit 1
fi

echo -e "\n${GREEN}====================================================${NC}"
echo -e "${GREEN}   ALL CORE CHALLENGE EVALUATION TASKS PASSED !     ${NC}"
echo -e "${GREEN}====================================================${NC}"