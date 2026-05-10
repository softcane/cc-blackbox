#!/usr/bin/env bash
# Deterministic cc-blackbox end-to-end test.
#
# This intentionally does not run a real Claude Code session. Instead it routes
# Envoy to test/fake-anthropic.py and drives Claude Code hook payloads directly.
# Usage: ./test/e2e.sh
set -euo pipefail

cd "$(dirname "$0")/.."

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { printf "%bPASS%b: %s\n" "$GREEN" "$NC" "$1"; }
info() { printf "%bINFO%b: %s\n" "$YELLOW" "$NC" "$1"; }
fail() { printf "%bFAIL%b: %s\n" "$RED" "$NC" "$1"; exit 1; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

for cmd in docker curl python3 sqlite3; do
    require_cmd "$cmd"
done

RUN_ID="${CC_BLACKBOX_E2E_RUN_ID:-e2e-$(date +%s)-$$}"
CORE_URL="http://localhost:9091"
ENVOY_URL="http://localhost:10000"
PROM_URL="http://localhost:9092"
GRAFANA_URL="http://localhost:3000"
COMPOSE_FILES=(-f docker-compose.yml -f test/docker-compose.e2e.yml)
E2E_COMPLETED=0

compose() {
    docker compose "${COMPOSE_FILES[@]}" "$@"
}

cleanup_e2e_stack_on_failure() {
    if [ "$E2E_COMPLETED" = "1" ] || [ "${CC_BLACKBOX_E2E_KEEP_STACK:-0}" = "1" ]; then
        return
    fi
    compose down --remove-orphans -t 5 >/dev/null 2>&1 || true
}

restore_normal_stack() {
    if [ "${CC_BLACKBOX_E2E_KEEP_STACK:-0}" = "1" ]; then
        info "Leaving fake Anthropic E2E stack running because CC_BLACKBOX_E2E_KEEP_STACK=1"
        return
    fi

    info "Stopping fake Anthropic E2E stack..."
    compose down --remove-orphans -t 5 >/dev/null

    if [ "${CC_BLACKBOX_E2E_RESTORE_NORMAL_STACK:-1}" = "1" ]; then
        info "Restoring normal cc-blackbox stack..."
        docker compose up -d >/dev/null
        pass "Normal cc-blackbox stack restored"
    else
        pass "Fake Anthropic E2E stack stopped"
    fi
}

trap cleanup_e2e_stack_on_failure EXIT

curl_core() {
    curl -fsS "$CORE_URL$1"
}

wait_for_core() {
    for _ in $(seq 1 60); do
        if [ "$(curl -fsS "$CORE_URL/health" 2>/dev/null || true)" = "ok" ]; then
            return 0
        fi
        sleep 2
    done
    compose ps
    fail "cc-blackbox-core did not become healthy"
}

wait_for_envoy() {
    for _ in $(seq 1 60); do
        if curl -fsS --max-time 2 "$ENVOY_URL/" >/dev/null 2>&1; then
            return 0
        fi
        if curl -sS --max-time 2 "$ENVOY_URL/" >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    compose ps
    fail "envoy did not open localhost:10000"
}

post_hook() {
    local payload=$1
    local code
    code=$(curl -sS -o /dev/null -w "%{http_code}" --max-time 10 \
        -H "content-type: application/json" \
        -d "$payload" \
        "$CORE_URL/api/hooks/claude-code")
    [ "$code" = "204" ] || fail "hook returned HTTP $code for payload: $payload"
}

proxy_payload() {
    local skill=$1
    local label=$2
    local model=${3:-claude-sonnet-4-6-20250514}
    local stream=${4:-true}
    python3 - "$RUN_ID" "$skill" "$label" "$model" "$stream" <<'PY'
import json
import sys

run_id, skill, label, model, stream = sys.argv[1:]
payload = {
    "model": model,
    "max_tokens": 128,
    "stream": stream.lower() == "true",
    "system": f"Primary working directory: /tmp/cc-blackbox-e2e/{run_id}. Read-only E2E.",
    "messages": [
        {
            "role": "user",
            "content": (
                f"[{run_id}] [{label}] Use the {skill} skill to inspect "
                "read-only platform engineering context."
            ),
        }
    ],
}
print(json.dumps(payload, separators=(",", ":")))
PY
}

hook_json() {
    python3 - "$@" <<'PY'
import json
import sys

kind = sys.argv[1]
session = sys.argv[2]
if kind == "expansion":
    print(json.dumps({
        "session_id": session,
        "hook_event_name": "UserPromptExpansion",
        "expansion_type": "slash_command",
        "command_name": sys.argv[3],
        "command_args": f"synthetic e2e hook {sys.argv[4]}",
    }, separators=(",", ":")))
elif kind == "skill":
    print(json.dumps({
        "session_id": session,
        "hook_event_name": "PreToolUse",
        "tool_name": "Skill",
        "tool_input": {
            "skill_name": sys.argv[3],
            "prompt": f"synthetic e2e skill call {sys.argv[4]}",
        },
    }, separators=(",", ":")))
elif kind == "mcp_called":
    print(json.dumps({
        "session_id": session,
        "hook_event_name": "PreToolUse",
        "tool_name": "mcp__github__get_issue",
        "tool_input": {"query": f"synthetic e2e issue lookup {sys.argv[3]}"},
    }, separators=(",", ":")))
elif kind == "mcp_succeeded":
    print(json.dumps({
        "session_id": session,
        "hook_event_name": "PostToolUse",
        "tool_name": "mcp__github__get_issue",
        "tool_input": {"query": f"synthetic e2e issue lookup {sys.argv[3]}"},
    }, separators=(",", ":")))
elif kind == "stop":
    print(json.dumps({
        "session_id": session,
        "hook_event_name": "Stop",
    }, separators=(",", ":")))
else:
    raise SystemExit(f"unknown hook kind: {kind}")
PY
}

send_proxy_turn() {
    local label=$1
    local skill=$2
    local model=${3:-claude-sonnet-4-6-20250514}
    local stream=${4:-true}
    local payload response
    payload=$(proxy_payload "$skill" "$label" "$model" "$stream")
    response=$(curl -fsS --max-time 30 -N \
        -H "x-api-key: test-e2e-fake" \
        -H "anthropic-version: 2023-06-01" \
        -H "content-type: application/json" \
        -H "accept-encoding: gzip" \
        -d "$payload" \
        "$ENVOY_URL/v1/messages")
    if [ "$stream" = "true" ]; then
        grep -q "message_stop" <<<"$response" \
            || fail "fake upstream response did not contain message_stop: $response"
    else
        if ! RESPONSE_JSON="$response" python3 - <<'PY'
import json
import os
d = json.loads(os.environ["RESPONSE_JSON"])
assert d["type"] == "message"
assert d["usage"]["output_tokens"] == 90
assert d["usage"]["cache_creation"]["ephemeral_1h_input_tokens"] == 100
PY
        then
            fail "fake non-streaming response was not valid Claude JSON: $response"
        fi
    fi
    grep -q "$skill" <<<"$response" \
        || fail "fake upstream response did not contain $skill: $response"
}

send_delayed_proxy_turn_and_assert_live_watch() {
    local payload watch_file response_file watch_pid proxy_pid response live_seen still_running
    payload=$(proxy_payload "$SKILL_A" "proxy-delayed" "claude-sonnet-4-6-20250514" "true")
    watch_file=$(mktemp)
    response_file=$(mktemp)

    curl -sS -N --max-time 12 -H "Accept: text/event-stream" "$CORE_URL/watch" >"$watch_file" 2>/dev/null &
    watch_pid=$!
    sleep 0.2

    curl -fsS --max-time 30 -N \
        -H "x-api-key: test-e2e-fake" \
        -H "anthropic-version: 2023-06-01" \
        -H "content-type: application/json" \
        -H "accept-encoding: gzip" \
        -d "$payload" \
        "$ENVOY_URL/v1/messages" >"$response_file" &
    proxy_pid=$!

    live_seen=0
    still_running=0
    for _ in $(seq 1 30); do
        sleep 0.2
        if grep -q "proxy-delayed-live" "$watch_file"; then
            live_seen=1
            if kill -0 "$proxy_pid" 2>/dev/null; then
                still_running=1
            fi
            break
        fi
        if ! kill -0 "$proxy_pid" 2>/dev/null; then
            break
        fi
    done

    if ! wait "$proxy_pid"; then
        kill "$watch_pid" 2>/dev/null || true
        wait "$watch_pid" 2>/dev/null || true
        rm -f "$watch_file" "$response_file"
        fail "delayed proxy curl failed"
    fi
    kill "$watch_pid" 2>/dev/null || true
    wait "$watch_pid" 2>/dev/null || true

    response=$(cat "$response_file")
    rm -f "$watch_file" "$response_file"

    grep -q "message_stop" <<<"$response" \
        || fail "delayed fake upstream response did not contain message_stop: $response"
    grep -q "$SKILL_A" <<<"$response" \
        || fail "delayed fake upstream response did not contain $SKILL_A: $response"
    [ "$live_seen" = "1" ] && [ "$still_running" = "1" ] \
        || fail "watch did not receive delayed SSE tool event before upstream response completed"
}

send_hook_turn() {
    local session=$1
    local skill=$2
    local label=$3
    post_hook "$(hook_json expansion "$session" "$skill" "$label")"
    post_hook "$(hook_json skill "$session" "$skill" "$label")"
    post_hook "$(hook_json mcp_called "$session" "$label")"
    post_hook "$(hook_json mcp_succeeded "$session" "$label")"
    post_hook "$(hook_json stop "$session")"
}

prom_value() {
    local query=$1
    curl -fsSG --data-urlencode "query=$query" "$PROM_URL/api/v1/query" \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); vals=[float(r["value"][1]) for r in d.get("data",{}).get("result",[])]; print(max(vals) if vals else 0)'
}

float_ge() {
    python3 - "$1" "$2" <<'PY'
import sys
actual = float(sys.argv[1])
minimum = float(sys.argv[2])
raise SystemExit(0 if actual >= minimum else 1)
PY
}

wait_prom_ge() {
    local query=$1
    local minimum=$2
    local label=$3
    local value=0
    for _ in $(seq 1 12); do
        value=$(prom_value "$query" 2>/dev/null || echo 0)
        if float_ge "$value" "$minimum"; then
            pass "$label (value=$value)"
            return 0
        fi
        sleep 5
    done
    fail "$label stayed below $minimum; last value=$value; query=$query"
}

sqlite_scalar() {
    local db=$1
    local sql=$2
    sqlite3 "$db" "$sql"
}

assert_sql_ge() {
    local db=$1
    local sql=$2
    local minimum=$3
    local label=$4
    local value
    value=$(sqlite_scalar "$db" "$sql" | tr -d '[:space:]')
    value=${value:-0}
    if [ "$value" -ge "$minimum" ]; then
        pass "$label (count=$value)"
    else
        fail "$label expected >= $minimum, got $value"
    fi
}

assert_json_python() {
    local label=$1
    local script=$2
    if python3 -c "$script"; then
        pass "$label"
    else
        fail "$label"
    fi
}

echo "=== cc-blackbox deterministic E2E Test ==="
info "run_id=$RUN_ID"
info "Starting docker compose with fake Anthropic upstream..."
compose down --remove-orphans -t 5 2>/dev/null || true
compose up -d --build

info "Waiting for cc-blackbox-core and envoy..."
wait_for_core
wait_for_envoy

running=$(compose ps --status running -q | wc -l | tr -d ' ')
[ "$running" -ge 5 ] && pass "E2E stack running ($running services)" || fail "Only $running services running"

health=$(curl_core /health)
[ "$health" = "ok" ] && pass "cc-blackbox-core /health" || fail "cc-blackbox-core /health returned: '$health'"

info "Exercising Claude Code hook telemetry..."
HOOK_A="hook_${RUN_ID}_kustomize"
HOOK_B="hook_${RUN_ID}_cicd"
SKILL_A="fixture-skills:config-review"
SKILL_B="fixture-skills:pipeline-review"
send_hook_turn "$HOOK_A" "$SKILL_A" "a"
send_hook_turn "$HOOK_B" "$SKILL_B" "b"
pass "Synthetic hook turns accepted"

watch_output=$(curl -sS --max-time 4 -H "Accept: text/event-stream" "$CORE_URL/watch?session=$HOOK_A" 2>/dev/null || true)
grep -q "$SKILL_A" <<<"$watch_output" \
    && pass "/watch replays hook skill events" \
    || fail "/watch did not replay $SKILL_A; output: $watch_output"

info "Sending fake Anthropic streaming turns through Envoy..."
send_proxy_turn "proxy-a" "$SKILL_A"
send_proxy_turn "proxy-b" "$SKILL_B"
send_proxy_turn "proxy-1m" "$SKILL_A" "claude-opus-4-7[1m]"
send_delayed_proxy_turn_and_assert_live_watch
send_proxy_turn "proxy-json" "$SKILL_B" "claude-haiku-4-5-20251001" "false"
pass "Envoy ext_proc observed fake streaming, delayed streaming, and JSON turns"

sleep 4

metrics=$(curl_core /metrics)
grep -q "cc_blackbox_requests_total" <<<"$metrics" && pass "Core request metrics registered" || fail "No request metrics"
grep -q "cc_blackbox_tool_calls_total" <<<"$metrics" && pass "Core tool metrics registered" || fail "No tool metrics"
grep -q "cc_blackbox_skill_events_total" <<<"$metrics" && pass "Core skill metrics registered" || fail "No skill metrics"
grep -q "cc_blackbox_mcp_events_total" <<<"$metrics" && pass "Core MCP metrics registered" || fail "No MCP metrics"

summary=$(curl_core /api/summary)
SUMMARY_JSON="$summary" assert_json_python "/api/summary includes sessions" 'import json, os; d=json.loads(os.environ["SUMMARY_JSON"]); assert d["today"]["sessions"] >= 2'

sessions_resp=$(curl_core /api/sessions)
SESSIONS_JSON="$sessions_resp" assert_json_python "/api/sessions returns session list" 'import json, os; d=json.loads(os.environ["SESSIONS_JSON"]); assert isinstance(d.get("sessions"), list)'

info "Waiting for Prometheus to scrape E2E metrics..."
wait_prom_ge 'cc_blackbox_tool_calls_total{tool="skill"}' 2 "Prometheus sees Skill tool calls"
wait_prom_ge 'cc_blackbox_tool_calls_total{tool="read"}' 1 "Prometheus sees Read tool calls"
wait_prom_ge 'cc_blackbox_tool_calls_total{tool="glob"}' 1 "Prometheus sees Glob tool calls"
wait_prom_ge 'cc_blackbox_skill_events_total{skill="fixture-skills_config-review",event_type="fired",source="hook"}' 1 "Prometheus sees hook skill fired"
wait_prom_ge 'cc_blackbox_skill_events_total{skill="fixture-skills_pipeline-review",event_type="fired",source="proxy"}' 1 "Prometheus sees proxy skill fired"
wait_prom_ge 'cc_blackbox_mcp_events_total{server="github",tool="get_issue",event_type="called",source="hook"}' 1 "Prometheus sees hook MCP call"

info "Checking Grafana API and provisioned dashboard..."
grafana_health=$(curl -fsS "$GRAFANA_URL/api/health")
GRAFANA_HEALTH="$grafana_health" assert_json_python "Grafana API healthy" 'import json, os; d=json.loads(os.environ["GRAFANA_HEALTH"]); assert d.get("database") == "ok"'
grafana_search=$(curl -fsS "$GRAFANA_URL/api/search?query=cc-blackbox")
GRAFANA_SEARCH="$grafana_search" assert_json_python "Grafana dashboard provisioned" 'import json, os; data=json.loads(os.environ["GRAFANA_SEARCH"]); assert any("cc-blackbox" in item.get("title","") for item in data)'

info "Copying SQLite database and asserting run-scoped records..."
CORE_CID=$(compose ps -q cc-blackbox-core)
compose stop cc-blackbox-core >/dev/null
DB_COPY="/tmp/cc-blackbox-e2e-db-${RUN_ID}"
rm -rf "$DB_COPY"
mkdir -p "$DB_COPY"
docker cp "$CORE_CID:/data/." "$DB_COPY" >/dev/null
DB_FILE="$DB_COPY/cc-blackbox.db"
[ -f "$DB_FILE" ] || fail "SQLite database not found at $DB_FILE"

PROXY_SESSION_FILTER="SELECT session_id FROM sessions WHERE working_dir LIKE '/tmp/cc-blackbox-e2e/${RUN_ID}%'"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM sessions WHERE working_dir LIKE '/tmp/cc-blackbox-e2e/${RUN_ID}%';" 2 "SQLite captured run-scoped proxy sessions"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM requests WHERE session_id IN (${PROXY_SESSION_FILTER});" 2 "SQLite captured run-scoped requests"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM turn_snapshots WHERE session_id IN (${PROXY_SESSION_FILTER});" 2 "SQLite captured run-scoped turn snapshots"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM tool_calls WHERE request_id IN (SELECT request_id FROM requests WHERE session_id IN (${PROXY_SESSION_FILTER})) AND lower(tool_name) = 'skill';" 2 "SQLite captured proxy Skill tool calls"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM requests WHERE session_id IN (${PROXY_SESSION_FILTER}) AND cache_creation_1h_tokens > 0;" 2 "SQLite captured 1h cache creation bucket"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM requests WHERE session_id IN (${PROXY_SESSION_FILTER}) AND model LIKE '%haiku-4-5%' AND input_tokens = 1200 AND output_tokens = 90 AND cache_read_tokens = 80 AND cache_creation_5m_tokens = 220 AND cache_creation_1h_tokens = 100 AND cost_dollars > 0;" 1 "SQLite captured parsed non-streaming Haiku JSON usage"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM skill_events WHERE session_id IN ('$HOOK_A', '$HOOK_B') AND source = 'hook' AND event_type = 'fired';" 4 "SQLite captured hook skill fired events"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM skill_events WHERE session_id IN (${PROXY_SESSION_FILTER}) AND source = 'proxy' AND event_type = 'fired';" 2 "SQLite captured proxy skill fired events"
assert_sql_ge "$DB_FILE" "SELECT COUNT(*) FROM mcp_events WHERE session_id IN ('$HOOK_A', '$HOOK_B') AND source = 'hook' AND event_type IN ('called', 'succeeded');" 4 "SQLite captured hook MCP lifecycle"

info "Restarting core and verifying persisted data is readable..."
compose start cc-blackbox-core >/dev/null
wait_for_core
summary_after=$(curl_core /api/summary)
SUMMARY_AFTER_JSON="$summary_after" assert_json_python "Data readable after core restart" 'import json, os; d=json.loads(os.environ["SUMMARY_AFTER_JSON"]); assert d["today"]["sessions"] >= 2'

metrics_after=$(curl_core /metrics)
grep -q "cc_blackbox_history_estimated_spend_dollars" <<<"$metrics_after" \
    && pass "Historical spend metrics present after restart" \
    || fail "Historical spend metrics missing after restart"

info "Testing Envoy failure_mode_allow against fake upstream..."
compose stop cc-blackbox-core >/dev/null
sleep 3
failopen_payload=$(proxy_payload "$SKILL_A" "fail-open")
failopen_response=$(curl -fsS --max-time 30 -N \
    -H "x-api-key: test-e2e-fake" \
    -H "anthropic-version: 2023-06-01" \
    -H "content-type: application/json" \
    -d "$failopen_payload" \
    "$ENVOY_URL/v1/messages")
compose start cc-blackbox-core >/dev/null
wait_for_core
grep -q "message_stop" <<<"$failopen_response" \
    && pass "failure_mode_allow preserves upstream traffic when cc-blackbox-core is stopped" \
    || fail "failure_mode_allow response did not reach fake upstream: $failopen_response"

size=$(docker images cc-blackbox-cc-blackbox-core --format '{{.Size}}' | head -1)
pass "Docker image size: $size"

echo ""
echo "=== All deterministic E2E checks passed ==="
E2E_COMPLETED=1
restore_normal_stack
echo "(normal stack left running - use 'docker compose down' to stop)"
