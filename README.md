# Clauditor

**Clauditor tells you why Claude Code got slow, stuck, expensive, or weird.**

Clauditor is a local flight recorder and live dashboard for Claude Code. Run Claude Code through Clauditor and it shows what is happening while the session runs: tool activity, cache behavior, context pressure, model fallback, quota burn, session history, and diagnosis after the run.

Clauditor runs on your machine. It does not phone home and does not send Clauditor telemetry to a hosted service. Claude Code still sends its normal API traffic to Anthropic, exactly as it would without Clauditor, but no additional observability data leaves your machine/network.

![demo](docs/demo.gif)

![grafana dashboard](docs/grafana-overview.png)

## What Clauditor Catches

- **Stuck or runaway sessions.** See which session is active, idle, blocked, or burning through repeated work.
- **Tool loops and repeated tool failures.** Spot noisy reads, edits, bash calls, MCP activity, and failure streaks while they are happening.
- **Cache expiry and cache rebuild waste.** Watch cache hits, misses, TTL countdowns, and estimated rebuild cost.
- **Context pressure and turns-to-compact.** See when a session is close to auto-compaction before the next confusing slowdown.
- **Silent model fallback.** Detect when the model used in the response differs from the model Claude Code requested.
- **Multi-session chaos.** Keep several long-running Claude Code terminals understandable from one watch view.
- **Quota and cost trends.** Track token use, estimated spend, reset timing, and budget pressure.
- **Session diagnosis after the run.** Get post-session hints for cache expiry, tool thrash, compaction pressure, and other degradation signals.
- **Lightweight recall without storing full transcripts.** Search cleaned first prompts and compact final summaries when you need to remember where a session left off.

## Why It Is Safe To Run Locally

Clauditor is designed to be safe to try because it stays local and is easy to stop using.

- **It runs on your machine.** The local proxy, core service, database, metrics, dashboard, and CLI all run locally.
- **It does not phone home.** Clauditor does not send observability data to a hosted Clauditor service.
- **Ports stay local by default.** Docker Compose binds the published ports to `127.0.0.1`.
- **Claude Code still talks to Anthropic.** Normal Claude Code API requests still go to Anthropic, exactly as they would without Clauditor.
- **No full transcript storage.** By default, Clauditor stores a cleaned first prompt and compact final summary for recall. It does not persist full conversation history, raw file contents, or raw tool payloads.
- **It fails open.** If the observability service stops, Claude Code traffic can keep going.
- **Estimates are estimates.** Cost, compaction runway, cache rebuild cost, and diagnosis are best-effort signals, not billing truth.

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

`clauditor doctor` checks Docker, Docker Compose, Claude Code, tmux, local ports, health endpoints, and environment variables. `clauditor up` starts the local Clauditor stack. `clauditor run claude --watch` routes only that Claude Code process through Clauditor and starts a watcher; it does not permanently modify shell config, shell startup files, or other Claude Code sessions.

Open Grafana at [http://127.0.0.1:3000/d/clauditor-main](http://127.0.0.1:3000/d/clauditor-main). Anonymous viewer mode is enabled, and the local admin login is `admin` / `admin`.

## What You See Live

```text
session-api      READ     src/routes.rs
session-api      CACHE    expires in 2m14s · est. rebuild $0.43
session-worker   CONTEXT  82% full · ~1 turn to auto-compact
session-auth     ⚠ MODEL FALLBACK requested opus, got sonnet
```

## Core Workflows

- **Watch all active sessions:** `clauditor watch --url http://127.0.0.1:9091`
- **Watch all sessions in tmux:** `clauditor watch --tmux`
- **Watch one session:** `clauditor watch --session session_1776... --url http://127.0.0.1:9091`
- **Review recent sessions:** `clauditor sessions --limit 20 --days 7`
- **Open the local session API:** `curl -s 'http://127.0.0.1:9091/api/sessions?limit=5'`
- **Read the current local summary:** `curl -s http://127.0.0.1:9091/api/summary`
- **Inspect one session diagnosis:** `curl -s http://127.0.0.1:9091/api/diagnosis/<session_id>`
- **Recall where you left off:** `clauditor recall "auth middleware"`
- **Advanced hook setup:** [Claude Code hook telemetry](#claude-code-hook-telemetry)

Replace `session_1776...` and `<session_id>` with real session IDs from `/api/sessions`. Recall searches the cleaned first prompt and compact final summary for each stored session. If you subscribe after a session already started, Clauditor injects a synthetic `SessionStart` so the watcher still gets the session header and cleaned initial prompt.

## What Clauditor Surfaces

- **Tool activity.** Reads, edits, bash commands, grep/glob calls, MCP server/tool usage, and tool failures.
- **Skill telemetry.** Expected, fired, missed, misfired, and failed skills from hooks plus conservative proxy inference.
- **MCP activity.** MCP server and tool lifecycle events from hooks and proxy-derived metrics.
- **Cache intelligence.** Cache hits, misses, expiry countdown, and estimated rebuild cost.
- **Context pressure.** Fill percentage and projected turns-to-compact as a heuristic. Fill percentage is computed against the detected context window for the current request, including extended-context requests such as `sonnet[1m]` or `opus[1m]`.
- **Model fallback.** Detection when the response model differs from the requested one.
- **Quota burn.** Weekly token use, reset time, and projected exhaustion if you set a budget.
- **Session diagnosis.** Post-session hints for cache expiry, thrash, tool failure streaks, compaction loops, and context pressure.
- **Session history.** Recent sessions in SQLite plus lightweight recall.
- **Prometheus metrics.** Local `/metrics` output for scripting, dashboards, and alerts you control.
- **Grafana trends.** Local dashboards for sessions, quality, cache reuse, model fallback, and estimated cost over time.

## Advanced

### Claude Code Hook Telemetry

Add hooks like these to your Claude Code settings, for example `.claude/settings.local.json` for one project or `~/.claude/settings.json` globally. The Docker Compose stack exposes Clauditor on host port `9091`; if you run `clauditor-core` directly, use its default `9090` port instead.

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ],
    "UserPromptExpansion": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "^(Skill|mcp__.*)$",
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "^mcp__.*$",
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ],
    "PostToolUseFailure": [
      {
        "matcher": "^(Skill|mcp__.*)$",
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ],
    "PermissionDenied": [
      {
        "matcher": "^(Skill|mcp__.*)$",
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ],
    "StopFailure": [
      {
        "hooks": [
          {
            "type": "http",
            "url": "http://127.0.0.1:9091/api/hooks/claude-code",
            "timeout": 2
          }
        ]
      }
    ]
  }
}
```

## How It Works

Claude Code points to a local proxy. The proxy forwards normal API traffic to Anthropic and streams request/response metadata to `clauditor-core`. `clauditor-core` parses streamed responses, tracks sessions, writes local SQLite history, exposes `/watch`, `/metrics`, and JSON APIs, and feeds Prometheus and Grafana.

Fail-open behavior means the local proxy keeps routing even if `clauditor-core` dies, so observability is not a hard dependency for Claude Code traffic.

```text
Claude Code  (ANTHROPIC_BASE_URL=http://127.0.0.1:10000)
    |
    v
+---------------------------------------------+
| Local proxy :10000                          |
|   +-- metadata --> clauditor-core :50051    |
|   +-- router   --> api.anthropic.com :443   |
+---------------------------------------------+
                    |
                    v
          +---------------------------+
          | clauditor-core            |
          | Rust + tonic + axum       |
          | SSE parser                |
          | watch broadcaster         |
          | diagnosis + SQLite        |
          | HTTP API + /metrics       |
          +---------------------------+
                    ^
                    |
          clauditor watch / Grafana
```

## Developing On It

```bash
docker compose up -d --build
docker compose logs -f clauditor-core
bash test/e2e.sh
bash test/parallel-sessions.sh 4
cargo fmt
cargo clippy -- -W clippy::all
```

Before touching the request path or the broadcast path, read [CONTRIBUTING.md](CONTRIBUTING.md). That guide has the public invariants and the parts that are easy to break by accident.
