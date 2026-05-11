# cc-blackbox

Claude Code can look busy after the useful work has already stopped.

cc-blackbox watches a Claude Code run locally and answers the questions that matter while you are working:

- Is this session still worth continuing?
- What failed?
- How many tokens did it burn?
- Should I restart, and with what prompt?

## Real Example

In one real read-only validation run, Claude kept moving through a fixed checklist. From the terminal, it looked productive: docs were read, tests ran, `kubectl` was tried, and Helm rendered YAML.

The postmortem caught the part I cared about:

- 15 assistant turns in 31 seconds.
- 321K total tokens, including 299K cache-read tokens.
- 6 failed Bash tool results.
- Missing `go.sum` data in the nested scheduler module.
- `kubectl` checks hitting `localhost:8080` with no real cluster available.
- No final ship/no-ship verdict captured.

![cc-blackbox postmortem showing a degraded Claude Code validation session](docs/degraded-postmortem-screenshot.png)

The postmortem gives a concrete next step: **stop this session, fix the module boundary and `go.sum`, attach a real cluster, then rerun validation.**

For live runs, cc-blackbox can also warn before the next request when a configured guard condition is hit.

## What You Get

- **Live guard:** warning, critical, blocked, cooldown, or healthy.
- **Session facts:** tool use, cache state, context pressure, model route mismatch, quota burn, and token totals.
- **Postmortem:** what happened, why it matters, direct versus heuristic evidence, and the next action.
- **Restart prompt:** a compact prompt for a fresh Claude Code session when continuing is no longer worth it.
- **Local storage:** derived metadata stays on your machine.

cc-blackbox runs locally. Claude Code traffic still goes to Anthropic. cc-blackbox stores derived metadata, not raw full transcripts or file contents. Optional postmortem analysis sends only redacted evidence to Claude, and you can turn that off.

## Quick Start

Install cc-blackbox:

```bash
curl -fsSL https://raw.githubusercontent.com/softcane/cc-blackbox/main/install.sh | sh
```

Start the local guard stack:

```bash
cc-blackbox doctor
cc-blackbox guard start
cc-blackbox guard policy
```

Run Claude Code through the proxy:

```bash
cc-blackbox run claude
```

In another terminal, watch the guard state. Add `--watch` to `cc-blackbox run` only if you also want the lower-level event stream next to the Claude process.

```bash
cc-blackbox guard status
cc-blackbox guard watch
```

When the session is over or idle, read the postmortem:

```bash
cc-blackbox postmortem latest
```

## Guard Mode

Guard mode is what you keep open next to Claude Code. It tells you when a session is still healthy, when it is burning budget, and when the next prompt should be blocked or restarted.

```bash
cc-blackbox guard start     # start or validate the local proxy/core stack
cc-blackbox guard policy    # show the effective policy and config source
cc-blackbox guard status    # show current sessions and guard state
cc-blackbox guard watch     # stream live findings in plain language
```

The states are: `Healthy`, `Watching`, `Warning`, `Critical`, `Blocked`, `Cooldown`, and `Ended`.
A warning means "pay attention." Critical means the next request may be blocked if policy says so. Blocked and Cooldown mean cc-blackbox already returned a policy response instead of forwarding that request.

Guard findings are evidence-labeled. A model mismatch is reported as a route mismatch. Context runway and compaction risk are marked as heuristics. Tool and JSONL findings say where the evidence came from.

## What Can Block

cc-blackbox fails open by default. If policy cannot load, a detector fails, or the guard is unhealthy, traffic is allowed rather than making the proxy a hard dependency.

By default, request blocking is conservative:

| Signal | Default action | Stops next request? | Policy key |
| --- | --- | --- | --- |
| Session token budget | Block when a token limit is set | Yes | `per_session_token_budget_exceeded` |
| Trusted session dollar budget | Block when a trusted dollar limit is set | Yes | `per_session_trusted_dollar_budget_exceeded` |
| API error streak | Cooldown | Yes | `api_error_circuit_breaker_cooldown` |
| Repeated cache rebuilds | Warn | No | `repeated_cache_rebuilds` |
| Context pressure / near compaction | Warn | No | `context_near_warning_threshold` |
| Suspected compaction loop | Warn | No | `suspected_compaction_loop` |
| Model route mismatch | Warn | No | `model_mismatch` |
| Tool failure streak | Warn | No | `tool_failure_streak` |
| Weekly/project quota burn | Warn | No | `high_weekly_project_quota_burn` |

Blocking happens on the next request, not by stopping a response already in flight. Response-side analysis updates guard state after the stream continues, and request-side policy enforcement decides whether the next call is forwarded upstream.

That split matters. A false warning is annoying. A false block interrupts work. cc-blackbox should only block when the policy is explicit and the signal is strong enough.

Only token budget, trusted dollar budget, and API cooldown stop a request today. The other signals change guard severity and postmortem output; they do not interrupt Claude Code traffic.

For simple budget controls, set limits before starting the guard stack:

```bash
export CC_BLACKBOX_SESSION_BUDGET_TOKENS=1000000
export CC_BLACKBOX_SESSION_BUDGET_DOLLARS=5
export CC_BLACKBOX_CIRCUIT_BREAKER_THRESHOLD=5
export CC_BLACKBOX_CIRCUIT_BREAKER_COOLDOWN_SECS=30
cc-blackbox guard start
```

Token budgets are straightforward hard stops. Dollar budgets only hard-stop when the active pricing source is trusted for enforcement; otherwise they stay visible as estimates.

For a TOML policy file, edit `~/.config/cc-blackbox/guard-policy.toml`. The bundled guard stack mounts that file into the core service as `/config/guard-policy.toml`.

```bash
mkdir -p ~/.config/cc-blackbox
$EDITOR ~/.config/cc-blackbox/guard-policy.toml
cc-blackbox guard start
cc-blackbox guard policy
```

Example policy:

```toml
fail_open = true

[rules.per_session_token_budget_exceeded]
action = "block"
limit_tokens = 1000000

[rules.per_session_trusted_dollar_budget_exceeded]
action = "block"
limit_dollars = 5.00

[rules.api_error_circuit_breaker_cooldown]
action = "cooldown"
threshold_count = 5
cooldown_secs = 30

[rules.context_near_warning_threshold]
action = "critical"
threshold_count = 90

[rules.repeated_cache_rebuilds]
action = "critical"

[rules.model_mismatch]
action = "warn"
```

`cc-blackbox guard policy` shows the effective policy, where it was loaded from, and any ignored keys. The core reads the file when policy is checked, so editing the TOML is enough for the next request/status read.

## Postmortem

The guard tells you what is happening now. The postmortem tells you what happened after the session has enough evidence to be useful.

Postmortems are redacted by default. They combine proxy data with local Claude Code JSONL when a confident match exists. Without a confident JSONL match, cc-blackbox produces a proxy-only report instead of mixing sessions.

```markdown
# cc-blackbox Postmortem

## Snapshot
  Session       `session_1776_abcd`
  State         final postmortem
  Outcome       Degraded
  Model         claude-sonnet-4-6
  Duration      18m
  Turns/tokens  7 turns, 214K
  Cost          $4.91

## Signals
  Cause    Repeated cache rebuilds [heuristic]
  Cache    Low: 42% reusable prompt cache; 36% of input from cache
  Context  High: 87% full; about 1 turn before auto-compaction
  Waste    Likely waste: 76K tokens, $1.84
  Tools    14 calls, 0 failures; repeated: Read, Edit
  Skills   No failed skill events detected
  MCP      No failed MCP calls detected
  Next     Restart with a shorter prompt and inspect the repeated Read/Edit path first.

## Evidence
  Type        Signal        Turn   Detail
  ----------  ------------  -----  ------
  direct      cache         6      cache miss followed by 62K cache creation tokens
  direct      tools         7      14 Read/Edit calls against the same redacted path

## Analysis
  What happened  The session got stuck in repeated Read/Edit work after a cache rebuild.
  What it means  The direct tool loop is enough to act; the context estimate is heuristic.
  What to do     Restart with the summary and ask for one file-level change at a time.

## Restart Prompt
  Continue from this summary. Make one file-level change at a time, and inspect the repeated Read/Edit path before editing.
```

Model-assisted analysis is on by default. It sends only the redacted postmortem JSON to Claude. For deterministic local output without that extra analysis call, run:

```bash
cc-blackbox postmortem latest --no-analyze-with-claude
```

`cc-blackbox postmortem last` is also accepted.

## What cc-blackbox Catches

- **A session going sideways while it still looks busy:** cache rebuilds, repeated tool failures, compaction risk, model route mismatch, and API error streaks.
- **Budget burn before the bill is obvious:** session spend, trusted budget enforcement, token pressure, cache waste, and weekly/project quota pressure.
- **The "should I restart?" moment:** current guard state, why it changed, and the next action worth taking.
- **Post-session evidence:** what degraded, where the money went, whether JSONL added tool/task evidence, and what to do differently next time.

## Why It Is Safe To Run Locally

cc-blackbox is designed to be safe to try because it stays local and is easy to stop using.

- **Local-first:** cc-blackbox runs on your machine, and local services bind to `127.0.0.1` by default.
- **Derived metadata storage:** cc-blackbox stores metrics, session facts, first-message hashes, and derived findings. It does not persist raw prompts, assistant text, tool outputs, file contents, or raw JSONL message text.
- **Fails open:** If cc-blackbox stops or cannot evaluate policy, Claude Code traffic can keep going to Anthropic.
- **Explicit blocks:** block responses come from policy decisions, not hidden guesses.
- **Evidence is labeled:** Costs are estimates unless the pricing source is trusted, context runway is heuristic, and model route mismatch reports observed requested/actual models without claiming provider cause.

## What cc-blackbox Surfaces

- **Guard:** current state, active findings, policy, warnings, blocks, and cooldowns.
- **Watch:** lower-level live activity for tools, cache, context, model route mismatch, quota burn, and sessions.
- **Postmortems:** redacted reports with likely cause, direct/proxy/JSONL/heuristic evidence, confidence, token/cost impact, and the next action.
- **Advanced views:** recent sessions, local `/metrics`, and Grafana trends.

## Reference

### Advanced

#### Supporting Workflows

Guard mode is the normal live workflow. Watch mode, APIs, and Grafana are still useful when you want lower-level events or history across many sessions.

- **Start or validate the guard stack:** `cc-blackbox guard start`
- **Show effective guard policy:** `cc-blackbox guard policy`
- **Show current guard state:** `cc-blackbox guard status`
- **Watch guard findings:** `cc-blackbox guard watch`
- **Read the latest postmortem:** `cc-blackbox postmortem latest`
- **Force local-only postmortem synthesis:** `cc-blackbox postmortem latest --no-analyze-with-claude`
- **Render local unredacted evidence:** `cc-blackbox postmortem latest --no-redact`
- **Watch all active sessions:** `cc-blackbox watch --url http://127.0.0.1:9091`
- **Opt into automatic watch postmortems:** `cc-blackbox watch --postmortem`
- **Watch all sessions in tmux:** `cc-blackbox watch --tmux`
- **Watch one session:** `cc-blackbox watch --session session_1776... --url http://127.0.0.1:9091`
- **Review recent sessions:** `cc-blackbox sessions --limit 20 --days 7`
- **Open the local session API:** `curl -s 'http://127.0.0.1:9091/api/sessions?limit=5'`
- **Read the current local summary:** `curl -s http://127.0.0.1:9091/api/summary`
- **Inspect one session diagnosis:** `curl -s http://127.0.0.1:9091/api/diagnosis/<session_id>`
- **Advanced hook setup:** [Claude Code hook telemetry](docs/reference/advanced.md#claude-code-hook-telemetry)

Open Grafana at [http://127.0.0.1:3000/d/cc-blackbox-main](http://127.0.0.1:3000/d/cc-blackbox-main) when you want longer-running trends. Anonymous viewer mode is enabled, and the local admin login is `admin` / `admin`.

![Grafana dashboard showing the last 5 minutes](docs/grafana-overview.png)

### Links

- [Advanced setup and architecture](docs/reference/advanced.md)
- [Developing on cc-blackbox](docs/reference/developing.md)
