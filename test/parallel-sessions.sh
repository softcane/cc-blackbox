#!/usr/bin/env bash
# Regression test: parallel Claude sessions from the same working directory
# must produce distinct server-side session ids.
#
# Pre-change, sys_prompt_hash was derived from working_directory only, so
# N parallel sessions collapsed into 1 session_id — their tool events were
# intermixed under one tag, costs were misattributed, and an inactivity
# sweep on the shared session caused other still-running sessions to appear
# to "disappear" from the watch stream.
#
# This test fires N parallel requests with different first-user-message
# content and asserts that the docker logs show N distinct sys_prompt_hash
# values. It does NOT require real Anthropic credentials — the proxy logs
# the hash during request_body phase before the upstream call is made.
#
# Usage: ./test/parallel-sessions.sh [N]

set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-4}
RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'
pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; exit 1; }

echo "=== Parallel sessions regression test (N=$N) ==="

# Stack must be running.
if ! curl -sf http://localhost:9091/health >/dev/null; then
    fail "clauditor-core not healthy at http://localhost:9091 — run 'docker compose up -d' first"
fi

# Unique run marker so we only count hashes logged for requests from *this*
# invocation. We inject it into the first user message.
RUN_ID="parallel-$(date +%s)-$RANDOM"
WORKDIR_TAG="/tmp/clauditor-parallel"

# Fire N parallel requests with distinct first user messages.
pids=()
for i in $(seq 1 "$N"); do
    (
        code=$(curl -sS -o /dev/null -w "%{http_code}" http://localhost:10000/v1/messages \
            -H "x-api-key: test-parallel" \
            -H "anthropic-version: 2023-06-01" \
            -H "content-type: application/json" \
            -H "x-request-id: ${RUN_ID}-req-${i}" \
            -d "{\"model\":\"claude-sonnet-4-6\",\"system\":\"Primary working directory: $WORKDIR_TAG\",\"max_tokens\":10,\"messages\":[{\"role\":\"user\",\"content\":\"[$RUN_ID][req-$i] hello\"}]}") || exit 1
        [ "$code" != "000" ]
    ) &
    pids+=($!)
done
curl_failed=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        curl_failed=1
    fi
done
[ "$curl_failed" = "0" ] || fail "one or more parallel curl requests failed"

# Give the ext_proc handler a beat to log.
sleep 2

# Count distinct sys_prompt_hash values in a tight recent window. We pull the
# last 30s of logs and then keep only entries from requests adjacent to ours,
# which is good enough in practice because the test runs in under 5s.
logs=$(docker compose logs --since 30s --no-color clauditor-core 2>/dev/null \
    | grep "$RUN_ID" || true)
hashes=$(printf '%s\n' "$logs" \
    | grep -oE '"sys_prompt_hash":[-0-9]+' \
    | sort -u)
distinct=$(printf '%s\n' "$hashes" | grep -c . || true)

echo "Distinct hashes observed:"
printf '  %s\n' $hashes

if [ "$distinct" -eq "$N" ]; then
    pass "$distinct distinct sys_prompt_hash values for $N parallel requests"
else
    fail "expected exactly $N run-scoped distinct hashes, got $distinct — session-key collision regression or setup failure"
fi
