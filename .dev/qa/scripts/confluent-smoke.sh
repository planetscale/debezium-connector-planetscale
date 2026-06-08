#!/usr/bin/env bash
#
# Confluent Cloud smoke test — the real cloud gate. Uploads the built connector as a Custom
# Connector plugin, creates a connector instance, polls until RUNNING, verifies one migration cycle
# (snapshot.mode=initial → at least one record lands on a Kafka data topic), then tears everything
# down. Intended for CI (the local Docker harness in run.sh is the fast inner loop; this is the
# slower, authoritative check).
#
# Prereqs:
#   * confluent CLI installed and logged in:           confluent login
#   * an environment + Kafka cluster already exist in Confluent Cloud
#   * admin RBAC (required to upload custom-connector archives)
#
# Required env vars:
#   CONFLUENT_ENV         e.g. env-abc123      (confluent environment list)
#   CONFLUENT_CLUSTER     e.g. lkc-abc123      (confluent kafka cluster list)
#   KAFKA_API_KEY / KAFKA_API_SECRET           (for the connector's Kafka auth)
#   PSDB_HOST PSDB_USER PSDB_PASSWORD PSDB_KEYSPACE TOPIC_PREFIX   (PlanetScale connection)
# Optional:
#   PLUGIN_NAME (default planetscale-debezium)  KEEP=1 (skip teardown)
#   CLOUD (aws|gcp|azure; auto-detected from the target cluster if unset)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
ZIP_GLOB="$ROOT/debezium-planetscale/build/connect/dist/planetscale-debezium-connector-planetscale-*.zip"
PLUGIN_NAME="${PLUGIN_NAME:-planetscale-debezium}"
CLOUD="${CLOUD:-}"  # auto-detected from the target cluster below unless set explicitly
CONNECTOR_NAME="psdb-smoke-$$"

need() { [ -n "${!1:-}" ] || { echo "!! missing required env var: $1" >&2; exit 2; }; }
for v in CONFLUENT_ENV CONFLUENT_CLUSTER KAFKA_API_KEY KAFKA_API_SECRET PSDB_HOST PSDB_USER PSDB_PASSWORD PSDB_KEYSPACE TOPIC_PREFIX; do need "$v"; done
command -v confluent >/dev/null || { echo "!! confluent CLI not found"; exit 2; }
command -v jq >/dev/null || { echo "!! jq not found (required to parse Confluent CLI JSON output)"; exit 2; }

# Tolerant, schema-agnostic status reader: first string-valued "status"/"state" anywhere in the JSON
# (the CLI's describe/list shapes vary by version, so we don't hard-code a path).
json_status() { jq -r 'first(.. | objects | (.status, .state) | select(type == "string")) // empty' 2>/dev/null; }

# 1. Build the archive (assembleConnectZip → dist/*.zip) if needed.
ZIP=$(ls $ZIP_GLOB 2>/dev/null | head -1 || true)
if [ -z "$ZIP" ]; then
  echo ">> Building connector archive..."
  ( cd "$ROOT" && ./gradlew :debezium-planetscale:connectDist -x test --console=plain )
  ZIP=$(ls $ZIP_GLOB | head -1)
fi
echo ">> Archive: $ZIP"

cleanup() {
  [ "${KEEP:-0}" = 1 ] && { echo ">> KEEP=1: leaving connector + plugin in place."; return; }
  echo ">> Tearing down..."
  [ -n "${LCC:-}" ] && confluent connect cluster delete "$LCC" --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" --force 2>/dev/null || true
  [ -n "${CCP:-}" ] && confluent connect custom-plugin delete "$CCP" --force 2>/dev/null || true
}
trap cleanup EXIT

# A custom plugin is uploaded for a specific cloud and can only provision connectors on a cluster in
# that same cloud ("cannot use connector plugin from aws to provision custom connector in gcp ...").
# Auto-detect the target cluster's cloud (aws/gcp/azure) unless CLOUD was set explicitly.
if [ -z "$CLOUD" ]; then
  CLOUD=$(confluent kafka cluster describe "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" -o json 2>/dev/null \
          | jq -r '[.. | strings | ascii_downcase | select(. == "aws" or . == "gcp" or . == "azure")] | first // empty' || true)
fi
CLOUD="${CLOUD:-aws}"
echo ">> Target cloud: $CLOUD (cluster $CONFLUENT_CLUSTER)"

# 2. Upload the plugin (CLI handles the presigned-URL upload). Custom plugins are ORG-scoped, so
#    `custom-plugin create` takes NO --environment (unlike the cluster/connector commands below).
echo ">> Uploading custom-connector plugin..."
CCP=$(confluent connect custom-plugin create "$PLUGIN_NAME-$$" \
  --plugin-file "$ZIP" \
  --connector-type source \
  --connector-class com.planetscale.debezium.PlanetscaleConnector \
  --sensitive-properties database.password,kafka.api.secret \
  --cloud "$CLOUD" \
  -o json | jq -r '.id // empty')
[ -n "$CCP" ] || { echo "!! failed to obtain custom-plugin id from create output"; exit 1; }
echo "   plugin id: $CCP"

# 3. Create the connector instance.
CFG=$(mktemp)
cat > "$CFG" <<JSON
{
  "name": "$CONNECTOR_NAME",
  "config": {
    "connector.class": "com.planetscale.debezium.PlanetscaleConnector",
    "confluent.connector.type": "CUSTOM",
    "confluent.custom.plugin.id": "$CCP",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "$KAFKA_API_KEY",
    "kafka.api.secret": "$KAFKA_API_SECRET",
    "tasks.max": "1",
    "topic.prefix": "$TOPIC_PREFIX",
    "database.hostname": "$PSDB_HOST",
    "database.port": "443",
    "database.user": "$PSDB_USER",
    "database.password": "$PSDB_PASSWORD",
    "snapshot.mode": "initial",
    "vitess.tablet.type": "REPLICA",
    "vitess.keyspace": "$PSDB_KEYSPACE"
  }
}
JSON
echo ">> Creating connector $CONNECTOR_NAME..."
confluent connect cluster create --config-file "$CFG" --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" -o json

# Resolve the connector's lcc- id by name. `describe`/`delete` take the id positionally (not the
# name), and the connector can take a few seconds to appear in the listing after create.
LCC=""
for _ in $(seq 1 12); do
  LCC=$(confluent connect cluster list --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" -o json 2>/dev/null \
        | jq -r --arg n "$CONNECTOR_NAME" 'first(.. | objects | select(.name? == $n) | .id?) // empty' || true)
  if [ -n "$LCC" ]; then break; fi
  sleep 5
done
if [ -n "$LCC" ]; then echo "   connector id: $LCC"; else echo "   (warning: could not resolve connector id; polling by name via list)"; fi

# 4. Poll until RUNNING or FAILED (provisioning takes minutes).
echo ">> Polling connector status (provisioning can take several minutes)..."
status=""; trace=""
for _ in $(seq 1 60); do
  if [ -n "$LCC" ]; then
    out=$(confluent connect cluster describe "$LCC" --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" -o json 2>/dev/null || true)
  else
    out=$(confluent connect cluster list --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" -o json 2>/dev/null \
          | jq -c --arg n "$CONNECTOR_NAME" 'first(.. | objects | select(.name? == $n)) // {}' || true)
  fi
  status=$(echo "$out" | json_status)
  echo "   status: ${status:-<provisioning>}"
  case "$status" in
    RUNNING) break ;;
    FAILED|DEGRADED|PAUSED) trace="$out"; break ;;
  esac
  sleep 10
done

# 5. Verify one migration cycle: snapshot.mode=initial publishes existing rows to Kafka. Assert that
#    at least one record actually lands on a data topic under the connector's prefix. We only assert
#    delivery here — payload/type fidelity (incl. geometry) is covered by the local GeoReplication
#    and Vitess integration tests.
DATA_OK=0
if [ "$status" = RUNNING ]; then
  echo ">> Verifying one migration cycle (snapshot → Kafka topic)..."
  cerr=$(mktemp)
  for attempt in $(seq 1 5); do
    all_topics=$(confluent kafka topic list --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" -o json 2>"$cerr" \
                 | jq -r '.[]? | (if type == "object" then .name else . end) | select(type == "string")' 2>/dev/null || true)
    if [ "$attempt" = 1 ]; then
      echo "   topics on cluster: $(printf '%s\n' "$all_topics" | grep -c . || true) total"
      printf '%s\n' "$all_topics" | grep . | sed 's/^/     - /' | head -40 || true
      [ -s "$cerr" ] && echo "   (topic list note: $(tr '\n' ' ' < "$cerr" | head -c 300))"
    fi
    # data topics are <prefix>.<keyspace>.<table>; exclude internal transaction/schema/heartbeat topics
    topics=$(printf '%s\n' "$all_topics" | grep -E "^${TOPIC_PREFIX}\." | grep -vE '\.(transaction|schema-changes|heartbeat)$' | head -n 4 || true)
    while IFS= read -r t; do
      [ -n "$t" ] || continue
      # head -n 1 makes this return as soon as a record arrives; timeout caps the wait (the CLI
      # consumer is slow to start, so give it room).
      out=$(timeout 45 confluent kafka topic consume "$t" --from-beginning --value-format string \
              --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" \
              --api-key "$KAFKA_API_KEY" --api-secret "$KAFKA_API_SECRET" 2>"$cerr" | head -n 1 || true)
      n=$(printf '%s' "$out" | grep -c . || true)
      if [ "${n:-0}" -gt 0 ]; then
        echo "   ✔ consumed a record from $t"
        DATA_OK=1; break
      fi
      echo "   $t: 0 records$( [ -s "$cerr" ] && echo " — $(tr '\n' ' ' < "$cerr" | head -c 200)")"
    done <<< "$topics"
    if [ "$DATA_OK" = 1 ]; then break; fi
    echo "   no snapshot data yet (round $attempt); waiting..."
    sleep 15
  done
  rm -f "$cerr"

  # Diagnosis: if no data topics appeared, the connector itself logs why (no tables matched, 0 rows
  # snapshotted, auth/permission error, ...) to its <connector-id>-app-logs topic. Dump it before
  # teardown so the failing run shows the root cause.
  if [ "$DATA_OK" != 1 ] && [ -n "$LCC" ]; then
    echo ">> No data observed — sampling connector app-logs (${LCC}-app-logs) for diagnosis:"
    alog=$(mktemp)
    timeout 45 confluent kafka topic consume "${LCC}-app-logs" --from-beginning --value-format string \
      --cluster "$CONFLUENT_CLUSTER" --environment "$CONFLUENT_ENV" \
      --api-key "$KAFKA_API_KEY" --api-secret "$KAFKA_API_SECRET" 2>/dev/null > "$alog" || true
    echo "   (captured $(grep -c . "$alog" || true) app-log lines)"
    echo "   --- snapshot / table / error lines ---"
    grep -iE "snapshot|table|keyspace|vstream|vgtid|row|error|exception|denied|permission|no tables|empty|complet" "$alog" | tail -n 40 | sed 's/^/     | /' || true
    echo "   --- last 20 app-log lines ---"
    tail -n 20 "$alog" | sed 's/^/     | /' || true
    rm -f "$alog"
  fi
fi

echo
echo "================= CONFLUENT SMOKE RESULT ================="
echo "  connector : $CONNECTOR_NAME"
echo "  status    : ${status:-unknown}"
echo "  data flow : $([ "$DATA_OK" = 1 ] && echo 'snapshot records reached Kafka' || echo 'no records observed')"
if echo "$trace" | grep -qi "not a subtype"; then
  echo "  !! gRPC 'not a subtype' STILL PRESENT — packaging regression"; echo "$trace" | grep -i "not a subtype" | head -1
fi
echo "========================================================="

if [ "$status" = RUNNING ] && [ "$DATA_OK" = 1 ]; then
  echo ">> PASS: connector RUNNING and snapshot completed one migration cycle to Kafka."; exit 0
elif [ "$status" = RUNNING ]; then
  echo ">> CHECK: connector RUNNING but no snapshot data observed on Kafka within timeout."; exit 1
else
  echo ">> CHECK: connector not RUNNING (status=$status). Inspect trace above."; exit 1
fi
