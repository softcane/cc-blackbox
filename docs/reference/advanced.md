# Advanced

## Claude Code Hook Telemetry

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

Claude Code points to a local envoy proxy. The proxy forwards normal API traffic to Anthropic and streams request/response metadata to `clauditor-core`. `clauditor-core` parses streamed responses, tracks sessions, writes local SQLite history, exposes `/watch`, `/metrics`, and JSON APIs, and feeds Prometheus and Grafana.

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
