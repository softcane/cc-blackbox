# Clauditor

Claude Code sessions sometimes go bad in ways that are hard to explain after the fact. A run gets slow, repeats tool calls, loses cache, nears compaction, routes to a different model, or burns far more tokens than expected. By the time you notice, the terminal scrollback rarely tells you what actually happened.

Clauditor runs Claude Code through a local proxy and gives you a redacted postmortem for the session. It tells you the likely cause, the evidence behind it, the token and cost impact, and the next action worth taking. Live watch and Grafana are there when you need them, but the main output is simple: a useful postmortem when the run ends.

The proxy, database, metrics, dashboard, and CLI run on your machine. Clauditor does not send telemetry to a hosted Clauditor service. Claude Code API traffic is proxied to Anthropic, and Claude-assisted postmortems ask Claude to analyze redacted evidence unless you disable that step.

![demo](docs/demo.gif)

## Postmortem

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
- Started: 2026-05-05T14:03:10Z
- Ended: 2026-05-05T14:21:18Z
- Duration: 18m
- Turns: 7, tokens: 214K
- Initial prompt: Initial prompt captured (redacted, 12 words).
- Final response: Final response summary captured (redacted, 27 words).

## Likely Cause
- Cause: repeated tool loop after cache rebuild
- Detail: Claude repeatedly read and edited the same files after a cache miss, then hit high context pressure.
- Confidence: high
- Next action: restart with a shorter prompt and the generated summary, then inspect the repeated Read/Edit path first.

## Evidence
- [direct] cache: cache miss followed by 62K cache creation tokens
- [direct] tools: 14 Read/Edit calls against `<path>`
- [heuristic] context: peaked at 87% full with about 1 turn to compact
- [direct] model: requested opus, response used sonnet fallback

## Token And Cost Impact
- Total tokens: 214K (input 88K, cache read 64K, cache create 62K, output 432)
- Estimated total cost: $4.91 (builtin_model_family_pricing)
- Estimated likely waste: 76K tokens, $1.84
- Cache reusable-prefix ratio: 42%
- Total input cache rate: 36%
- Context max: 87% full; turns to compact: 1
- Caveat: costs are estimates, not billing truth. Built-in pricing may exclude contract discounts, data residency, fast-mode modifiers, and server-tool charges unless you reconcile billed costs.

## Timeline Highlights
- 2026-05-05T14:03:10Z: session_started: Session row created.
- 2026-05-05T14:17:42Z: turn 6, turn_signal: cache rebuild without long idle gap; context 87% full
- 2026-05-05T14:21:18Z: session_ended: Degraded

## Recommendations
- Restart with a shorter prompt and the generated summary.
- Inspect the repeated Read/Edit path first.

## Caveats
- This report is generated from local Clauditor SQLite data.
- Context runway and some degradation causes are heuristics.
- Redacted output omits raw prompts, absolute paths, query strings, secret-like values, and structured tool payloads.

## Claude Analysis
- Likely cause: Claude lost useful cached context, rebuilt a large prompt, and repeated edits while near compaction.
- Confidence: high, because cache, tool, context, and model evidence all point at the same turn range.
- What changed the user's decision: restarting is cheaper than continuing the degraded session.
- Next action: start fresh with the final summary and ask for one file-level change at a time.

## Restart Prompt
Continue from this summary. Make one file-level change at a time, and inspect the repeated Read/Edit path before editing.
```

Claude-assisted synthesis is on by default. It sends only the redacted postmortem JSON to Claude for analysis. For deterministic output without that extra Claude analysis call, run:

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
[session-api]     14:03:11  READ    src/routes.rs
[session-api]     14:03:13  CACHE   ○ miss (TTL inferred) · expires in 2m14s · est. rebuild $0.43
[session-worker]  14:04:02  CONTEXT 82% full · ~2 turns to auto-compact
[session-auth]    14:05:20  ⚠ MODEL ROUTE requested opus, got sonnet
```

Replace `session_1776...` and `<session_id>` with real session IDs from watch output or `/api/sessions`. Recall searches the cleaned first prompt and compact final summary for each stored session. If you subscribe to a known in-progress session after it already started, Clauditor injects a synthetic `SessionStart` so the watcher still gets the session header and cleaned initial prompt.

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
- **It does not phone home to Clauditor:** Clauditor does not send observability data to a hosted Clauditor service. The default Claude-assisted postmortem asks Claude to analyze redacted evidence; use `--no-analyze-with-claude` for deterministic local markdown.
- **Ports stay local by default:** Docker Compose binds the published ports to `127.0.0.1`.
- **Claude Code still talks to Anthropic:** Claude Code API requests are proxied through local Envoy to `api.anthropic.com`. Clauditor strips `Accept-Encoding` and may adjust retired context-window aliases so response parsing stays reliable.
- **No full transcript storage:** By default, Clauditor stores request/session metrics, a cleaned first-prompt excerpt, compact response summaries, and tool names/summaries. It does not persist full conversation history or raw file contents.
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
