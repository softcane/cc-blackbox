# Clauditor

**Clauditor gives you a redacted postmortem when Claude Code gets slow, stuck, expensive, or weird.**

Run Claude Code through Clauditor and, when the session ends, get a compact report that explains the likely cause, evidence, confidence, token/cost impact, and the next action. The live watcher and Grafana dashboard are still there, but the first useful thing is the postmortem.

Clauditor runs on your machine. It does not phone home and does not send Clauditor telemetry to a hosted service. Claude Code still sends its normal API traffic to Anthropic, exactly as it would without Clauditor, but no additional observability data leaves your machine/network.

![demo](docs/demo.gif)

## First Aha: The Postmortem

```bash
clauditor run claude --watch
# work normally, then end the session
clauditor postmortem last --redact
```

Watched sessions print a redacted postmortem automatically when they end. You can also rerun the latest report at any time.

```markdown
# Clauditor Session Postmortem

## Summary
- Session: `session_1776_abcd`
- Status: redacted
- Outcome: degraded
- Model: claude-opus-4
- Duration: 18m
- Turns: 7, tokens: 214k
- Initial prompt: Initial prompt captured (redacted, 12 words).

## Likely Cause
- Cause: repeated tool loop after cache rebuild
- Detail: Claude repeatedly read and edited the same files after a cache miss, then hit high context pressure.
- Confidence: high
- Next action: restart with a shorter prompt and the generated summary, then inspect the repeated Read/Edit path first.

## Evidence
- [direct] cache: cache miss followed by 62k cache creation tokens
- [direct] tools: 14 Read/Edit calls against `<path>`
- [heuristic] context: peaked at 87% full with about 1 turn to compact
- [direct] model: requested opus, response used sonnet fallback

## Token And Cost Impact
- Total tokens: 214k
- Estimated likely waste: 76k tokens, $1.84
- Caveat: costs are estimates, not billing truth. Built-in pricing may exclude contract discounts, data residency, fast-mode modifiers, and server-tool charges unless you reconcile billed costs.

## Claude Analysis
This session likely went bad because Claude lost the useful cached context, rebuilt a large prompt, and then repeated edits while near compaction. Start fresh with the final summary and ask for one file-level change at a time.
```

Claude-assisted synthesis is on by default, using only redacted evidence. For deterministic local-only output, run:

```bash
clauditor postmortem last --redact --no-analyze-with-claude
```

## Quick Start

Install Clauditor:

```bash
curl -fsSL https://raw.githubusercontent.com/softcane/clauditor/main/install.sh | sh
```

Start using it:

```bash
clauditor doctor
clauditor up
clauditor run claude --watch
```

`clauditor doctor` checks Docker, Docker Compose, Claude Code, tmux, local ports, health endpoints, and environment variables. `clauditor up` starts the local Clauditor stack. `clauditor run claude --watch` routes only that Claude Code process through Clauditor and starts a watcher; it does not permanently modify shell config, shell startup files, or other Claude Code sessions. Claude Code stays in the terminal where you ran it. When tmux is installed, the live watch view opens separately in a detached tmux session and the CLI prints the `tmux attach` command. When the watched Claude process exits, Clauditor prints the final redacted postmortem back in the original terminal.

## Supporting Workflows

Postmortems explain one session. Watch mode and Grafana help when you want live status or history across many sessions.

- **Read the latest postmortem:** `clauditor postmortem last --redact`
- **Force local-only postmortem synthesis:** `clauditor postmortem last --redact --no-analyze-with-claude`
- **Watch all active sessions:** `clauditor watch --url http://127.0.0.1:9091`
- **Skip automatic postmortems while watching:** `clauditor watch --no-postmortem`
- **Watch all sessions in tmux:** `clauditor watch --tmux`
- **Watch one session:** `clauditor watch --session session_1776... --url http://127.0.0.1:9091`
- **Review recent sessions:** `clauditor sessions --limit 20 --days 7`
- **Open the local session API:** `curl -s 'http://127.0.0.1:9091/api/sessions?limit=5'`
- **Read the current local summary:** `curl -s http://127.0.0.1:9091/api/summary`
- **Inspect one session diagnosis:** `curl -s http://127.0.0.1:9091/api/diagnosis/<session_id>`
- **Recall where you left off:** `clauditor recall "auth middleware"`
- **Advanced hook setup:** [Claude Code hook telemetry](docs/reference/advanced.md#claude-code-hook-telemetry)

Open Grafana at [http://127.0.0.1:3000/d/clauditor-main](http://127.0.0.1:3000/d/clauditor-main). Anonymous viewer mode is enabled, and the local admin login is `admin` / `admin`.

![grafana dashboard](docs/grafana-overview.png)

Live watch output stays compact:

```text
session-api      READ     src/routes.rs
session-api      CACHE    expires in 2m14s · est. rebuild $0.43
session-worker   CONTEXT  82% full · ~1 turn to auto-compact
session-auth     ⚠ MODEL ROUTE requested opus, got sonnet
```

Replace `session_1776...` and `<session_id>` with real session IDs from `/api/sessions`. Recall searches the cleaned first prompt and compact final summary for each stored session. If you subscribe after a session already started, Clauditor injects a synthetic `SessionStart` so the watcher still gets the session header and cleaned initial prompt.

## What Clauditor Catches

- **Stuck or runaway sessions:** See which session is active, idle, blocked, or burning through repeated work.
- **Tool loops and repeated tool failures:** Spot noisy reads, edits, bash calls, MCP activity, and failure streaks while they are happening.
- **Cache expiry and cache rebuild waste:** Watch cache hits, misses, TTL countdowns, and estimated rebuild cost.
- **Context pressure and turns-to-compact.** See when a session is close to auto-compaction before the next confusing slowdown.
- **Model route mismatch:** Detect when the response model differs from the model Claude Code requested, without assuming the cause.
- **Multi-session chaos:** Keep several long-running Claude Code terminals understandable from one watch view.
- **Quota and cost trends:** Track token use, estimated spend, reset timing, and budget pressure.
- **Session diagnosis after the run:** Get post-session hints for cache expiry, tool thrash, compaction pressure, and other degradation signals.
- **Lightweight recall without storing full transcripts:** Search cleaned first prompts and compact final summaries when you need to remember where a session left off.

## Why It Is Safe To Run Locally

Clauditor is designed to be safe to try because it stays local and is easy to stop using.

- **It runs on your machine:** The local proxy, core service, database, metrics, dashboard, and CLI all run locally.
- **It does not phone home:** Clauditor does not send observability data to a hosted Clauditor service.
- **Ports stay local by default:** Docker Compose binds the published ports to `127.0.0.1`.
- **Claude Code still talks to Anthropic:** Normal Claude Code API requests still go to Anthropic, exactly as they would without Clauditor.
- **No full transcript storage:** By default, Clauditor stores a cleaned first prompt and compact final summary for recall. It does not persist full conversation history, raw file contents, or raw tool payloads.
- **It fails open:** If the observability service stops, Claude Code traffic can keep going.
- **Estimates are estimates:** Cost, compaction runway, cache rebuild cost, cache miss causes, and diagnosis are best-effort signals, not billing truth or provider-confirmed root cause.

## What Clauditor Surfaces

- **Tool activity:** Reads, edits, bash commands, grep/glob calls, MCP server/tool usage, and tool failures.
- **Skill telemetry.** Expected, fired, missed, misfired, and failed skills from hooks plus conservative proxy inference.
- **MCP activity:** MCP server and tool lifecycle events from hooks and proxy-derived metrics.
- **Cache intelligence:** Cache hits, misses, expiry countdown, and estimated rebuild cost.
- **Context pressure:** Fill percentage and projected turns-to-compact as a heuristic. Fill percentage is computed against the detected context window for the current request, including extended-context requests such as `sonnet[1m]` or `opus[1m]`.
- **Model route mismatch:** Detection when the response model differs from the requested one.
- **Quota burn:** Weekly token use, reset time, and projected exhaustion if you set a budget.
- **Session diagnosis:** Post-session hints for cache expiry, thrash, tool failure streaks, compaction loops, and context pressure.
- **Session postmortems:** Redacted markdown summaries that explain likely cause, evidence, confidence, token/cost impact, and next action. Claude-assisted synthesis is used by default from redacted evidence and falls back to deterministic local markdown if unavailable.
- **Session history:** Recent sessions in SQLite plus lightweight recall.
- **Prometheus metrics:** Local `/metrics` output for scripting, dashboards, and alerts you control.
- **Grafana trends:** Local dashboards for sessions, quality, cache reuse, model route mismatches, and estimated cost over time.

## Reference

- [Advanced setup and architecture](docs/reference/advanced.md)
- [Developing on Clauditor](docs/reference/developing.md)
