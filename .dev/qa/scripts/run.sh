#!/usr/bin/env bash
#
# End-to-end local QA: boot Kafka + a Connect worker that emulates Confluent's runtime
# (foreign gRPC on the worker classpath), load our connector, deploy it, and assert
# whether the two known Confluent-Cloud failures appear.
#
#   EXPECT=red    -> the run PASSES only if BOTH failures reproduce (proves the diagnosis)
#   EXPECT=green  -> the run PASSES only if NEITHER failure is present (proves the fix)   [default]
#   KEEP=1        -> leave the stack running afterwards (for poking via REST on :8083)
#   REBUILD=1     -> force a connector rebuild before running
#
# Requires: docker (compose v2+), curl. No Confluent account.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QA="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$QA/../.." && pwd)"
PKG="$ROOT/debezium-planetscale/build/connect/pkg"
COMPOSE=(docker compose -f "$QA/docker-compose.yml")
CONNECT_URL="http://localhost:8083"
NAME="planetscale-qa"
EXPECT="${EXPECT:-green}"
KEEP="${KEEP:-0}"

cd "$ROOT"

# 1. Ensure the connector archive is built.
if [[ "${REBUILD:-0}" == 1 || ! -d "$PKG/lib" ]]; then
  echo ">> Building connector dist (./gradlew :debezium-planetscale:connectDist)..."
  ./gradlew :debezium-planetscale:connectDist -x test --console=plain
fi
echo ">> Using plugin archive: $PKG ($(ls -1 "$PKG/lib"/*.jar | wc -l | tr -d ' ') jars)"

# 2. Foreign gRPC for the worker classpath.
echo ">> Preparing foreign gRPC (emulating Confluent runtime)..."
"$HERE/download-host-grpc.sh"

# 3. Fresh stack.
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
echo ">> Starting Kafka + Connect..."
"${COMPOSE[@]}" up -d --quiet-pull

# 4. Wait for the Connect REST API.
echo -n ">> Waiting for Connect REST API"
ok=""
for _ in $(seq 1 120); do
  if curl -sf "$CONNECT_URL/connectors" >/dev/null 2>&1; then ok=1; break; fi
  echo -n "."; sleep 2
done
echo
if [[ "$ok" != 1 ]]; then
  echo "!! Connect did not become ready; recent logs:"
  "${COMPOSE[@]}" logs --no-color connect 2>&1 | tail -60
  [[ "$KEEP" == 1 ]] || "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  exit 2
fi
sleep 5  # let plugin scanning finish and logs flush

# 5. Deploy the connector (PlanetscaleConnector.validateConnection is a no-op, so the
#    gRPC channel — and the NameResolver registry init — fires at TASK START).
echo ">> Deploying connector..."
code=$(curl -s -o /tmp/psdb-deploy.json -w '%{http_code}' \
  -XPOST -H 'Content-Type: application/json' \
  --data @"$QA/config/connector.json" "$CONNECT_URL/connectors" || true)
echo "   POST /connectors -> HTTP $code"

# 6. Let the TASK (not the connector) settle. The connector reports RUNNING almost
#    immediately while the task is still starting; the gRPC channel — and thus the
#    'not a subtype' failure — only fires a bit later, when the task builds the channel.
#    So we must watch the task state, not the connector state.
#
#    From here on, grep/pipeline "no match" is normal control flow, so relax -e/pipefail
#    (otherwise task_state's grep aborts the whole script before any verdict prints).
set +e +o pipefail
task_state() {
  # extract the first task's state from the status JSON (no jq dependency)
  echo "$1" | sed 's/.*"tasks":\[//' | grep -o '"state":"[A-Z_]*"' | head -1 | sed 's/.*"state":"//; s/"//'
}
echo -n ">> Waiting for task to settle"
for i in $(seq 1 40); do
  st=$(curl -s "$CONNECT_URL/connectors/$NAME/status" 2>/dev/null || true)
  ts=$(task_state "$st")
  [ "$ts" = "FAILED" ] && break                         # failed (gRPC error in RED; maybe connectivity in GREEN)
  [ "$i" -ge 14 ] && [ "$ts" = "RUNNING" ] && break      # GREEN happy path: task stably running (~40s grace)
  echo -n "."; sleep 3
done
echo

# 7. Collect evidence from logs + REST and assert.
#    The detection greps legitimately return non-zero on "no match", so relax -e/pipefail
#    for this section (otherwise the verdict never prints).
set +e +o pipefail

LOGS="$("${COMPOSE[@]}" logs --no-color connect 2>&1)"
STATUS="$(curl -s "$CONNECT_URL/connectors/$NAME/status" 2>/dev/null)"
DEPLOY="$(cat /tmp/psdb-deploy.json 2>/dev/null)"
HAY="$LOGS
$STATUS
$DEPLOY"

err2=0; echo "$HAY" | grep -q "io.debezium.embedded.Transformations" && echo "$HAY" | grep -qi "NoSuchMethodException" && err2=1
err1=0; echo "$HAY" | grep -qi "not a subtype" && err1=1
gapis=0; echo "$HAY" | grep -q "GoogleCloudToProdExperimentalNameResolverProvider" && gapis=1
# Packaging/linkage failure IN THE TASK (runtime) — guards against a false GREEN where the gRPC
# error is gone but the relocation broke the channel another way. Scoped to the task status trace
# on purpose: non-fatal plugin-DISCOVERY warnings for unrelated bundled plugins (e.g. Debezium's
# CloudEventsConverter vs the host JsonConverter) appear in worker logs but never fail the task.
pkgerr=0; echo "$STATUS" | grep -qiE "NoClassDefFoundError|ClassNotFoundException|NoSuchMethodError|VerifyError|LinkageError|UnsatisfiedLinkError|IncompatibleClassChangeError" && pkgerr=1
# positive evidence the RELOCATED gRPC is actually exercised at runtime
reloc=0; echo "$HAY" | grep -q "com.planetscale.labs.io.grpc" && reloc=1
# informational: non-fatal plugin-discovery warnings (not a pass/fail signal)
scanwarn="$(echo "$LOGS" | grep -oiE "Failed to discover [A-Za-z]+ in [^:]+" | sort -u | tr '\n' ';')"

state_line="$(echo "$STATUS" | grep -o '"state":"[A-Z_]*"' | tr '\n' ' ')"
# the task's failure cause, ignoring the gRPC-collision line (confirms GREEN failed only for
# connectivity/auth — i.e. the channel actually built)
cause_line="$(echo "$STATUS $LOGS" | grep -oiE "StatusRuntimeException: [A-Z_]+|Caused by: [^\"]+" | grep -vi "not a subtype" | tail -1 | sed 's/^[[:space:]]*//')"

echo
echo "================================ VERDICT ================================"
printf "  Error 2  debezium-embedded Transformations\$1 scan : %s\n" "$([ "$err2" = 1 ] && echo '*** PRESENT ***' || echo 'absent')"
printf "  Error 1  gRPC NameResolverProvider 'not a subtype' : %s\n" "$([ "$err1" = 1 ] && echo '*** PRESENT ***' || echo 'absent')"
printf "           (matched googleapis provider by name)     : %s\n" "$([ "$gapis" = 1 ] && echo yes || echo no)"
printf "  Task-level packaging/linkage error                 : %s\n" "$([ "$pkgerr" = 1 ] && echo '*** PRESENT ***' || echo 'absent')"
printf "  Relocated gRPC (com.planetscale.labs.io.grpc) live : %s\n" "$([ "$reloc" = 1 ] && echo yes || echo no)"
printf "  Connector/task state                               : %s\n" "${state_line:-<none>}"
[ -n "$cause_line" ] && printf "  Task outcome (non-gRPC cause)                      : %s\n" "${cause_line:0:80}"
[ -n "$scanwarn" ] && printf "  Note: non-fatal plugin-discovery warning(s)        : %s\n" "$scanwarn"
echo "========================================================================="

if [ "$err1" = 1 ]; then
  echo; echo "---- gRPC failure excerpt ----"
  echo "$HAY" | grep -i -m1 "not a subtype" | sed 's/^/  /'
fi

[ "$KEEP" = 1 ] || "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1
[ "$KEEP" = 1 ] && echo ">> KEEP=1: stack left running at $CONNECT_URL (scripts/down.sh to stop)."

# 8. Exit code per expectation.
if [ "$EXPECT" = red ]; then
  if [ "$err1" = 1 ] && [ "$err2" = 1 ]; then
    echo ">> RESULT: RED as expected — both Confluent failures reproduced locally."; exit 0
  fi
  echo ">> RESULT: expected RED (both errors) but did not reproduce both."; exit 1
else
  if [ "$err1" = 0 ] && [ "$err2" = 0 ] && [ "$pkgerr" = 0 ]; then
    echo ">> RESULT: GREEN — neither Confluent failure nor any packaging/linkage error present."; exit 0
  fi
  echo ">> RESULT: expected GREEN but a failure is still present."; exit 1
fi
