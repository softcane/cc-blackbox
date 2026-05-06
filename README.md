# Clauditor

Claude Code sessions sometimes go bad in ways that are hard to explain after the fact. A run gets slow, repeats tool calls, loses cache, nears compaction, routes to a different model, or burns far more tokens than expected. By the time you notice, the terminal scrollback rarely tells you what actually happened.

Clauditor runs Claude Code through a local proxy and gives you a redacted postmortem for the session. It tells you the likely cause, the evidence behind it, the token and cost impact, and the next action worth taking. Live watch and Grafana are there when you need them, but the main output is simple: a useful postmortem after a watched `clauditor run` finishes.

The proxy, database, metrics, dashboard, and CLI run on your machine. Clauditor does not send telemetry to a hosted Clauditor service. Claude Code API traffic is proxied to Anthropic, and Claude-assisted postmortems ask Claude to analyze redacted evidence unless you disable that step.

![demo](docs/demo.gif)

## Postmortem

```bash
clauditor run claude --watch
# work normally, then end the session
clauditor postmortem last --redact
```

`clauditor run ... --watch` prints a redacted postmortem after the child Claude process exits. A standalone `clauditor watch` process prints postmortems only while it is still running, either after the idle-postmortem checkpoint or after Clauditor later times out the session. You can also rerun the latest report at any time.

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
- [direct] model: requested opus, response used sonnet

## Token And Cost Impact
- Total tokens: 214K (input 88K, cache read 64K, cache create 62K, output 432)
- Estimated total cost: $4.91 (builtin_model_family_pricing)
- Estimated likely waste: 76K tokens, $1.84
- Cache reusable-prefix ratio: 42%
- Total input cache rate: 36%
- Context max: 87% full; inferred turns to compact: 1
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
- Likely cause: Claude likely lost useful cached context, rebuilt a large prompt, and repeated edits while near compaction.
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

## What Clauditor Catches

- **Sessions going sideways:** Tool loops, repeated failures, cache rebuilds, context pressure, and model route mismatches.
- **Token and cost waste:** Estimated spend, likely wasted tokens, cache reuse, quota burn, and budget pressure.
- **Multi-session confusion:** Which Claude sessions are active, idle, blocked, expensive, or worth restarting.

## Why It Is Safe To Run Locally

Clauditor is designed to be safe to try because it stays local and is easy to stop using.

- **Local-first:** The proxy, core service, SQLite database, metrics, dashboard, and CLI run on your machine. Ports bind to `127.0.0.1` by default.
- **No full transcript storage:** Clauditor stores metrics, cleaned first-prompt excerpts, compact response summaries, and tool summaries, not full conversation history or raw file contents.
- **Fails open:** If Clauditor stops, Claude Code traffic can keep going to Anthropic.
- **Evidence is labeled:** Costs are estimates, context runway is heuristic, and model route mismatch reports observed requested/actual models without claiming provider cause.

## What Clauditor Surfaces

- **Live watch:** Tool activity, cache state, context pressure, model route mismatch, quota burn, and active session status.
- **Postmortems:** Redacted reports with likely cause, direct/heuristic evidence, confidence, token/cost impact, and the next action.
- **History and dashboards:** Recent sessions, lightweight recall, local `/metrics`, and Grafana trends.

## Reference

### Advanced

#### Supporting Workflows

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
[session-worker]  14:04:02  CONTEXT 82% full · inferred ~2 turns to auto-compact
[session-auth]    14:05:20  ⚠ MODEL ROUTE requested opus, got sonnet
```

Replace `session_1776...` and `<session_id>` with real session IDs from watch output or `/api/sessions`. Recall searches the cleaned first prompt and compact final summary for each stored session. If you subscribe to a known in-progress session after it already started, Clauditor injects a synthetic `SessionStart` so the watcher still gets the session header and cleaned initial prompt.

### Links

- [Advanced setup and architecture](docs/reference/advanced.md)
- [Developing on Clauditor](docs/reference/developing.md)
