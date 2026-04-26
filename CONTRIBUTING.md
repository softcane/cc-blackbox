# Contributing to Clauditor

Clauditor is a local observability layer for Claude Code. The project is fairly
compact, but parts of the proxy path are load-bearing. This guide is the
public, human-facing version of the contribution workflow and the invariants you
should keep in mind before changing request handling, streaming, session
identity, or `/watch`.

## Development setup

Prerequisites:

- Rust toolchain
- Docker / Docker Compose
- tmux for `clauditor watch --tmux`

Useful commands:

```bash
cargo build --release -p clauditor-cli
docker compose up -d --build
docker compose logs -f clauditor-core
```

## Checks to run before opening a PR

```bash
cargo fmt --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
bash test/parallel-sessions.sh 4
bash test/e2e.sh
```

If your change touches session identity, broadcast ordering, `/watch`, or tmux
watch mode, also exercise `clauditor watch --tmux` manually.

## Load-bearing invariants

- **Response-side ext_proc phases must return `CONTINUE` immediately.**
  Request-body parsing is the only synchronous step we keep on the hot path,
  because session identity and per-session budget gates have to be resolved
  before the request goes upstream.
- **Response body mode must stay `STREAMED`.**
  Buffering the whole SSE response would break live `/watch`.
- **Envoy must stay fail-open.**
  `failure_mode_allow: true` is deliberate: if `clauditor-core` crashes,
  Claude traffic should still reach Anthropic.
- **`Accept-Encoding` is stripped on request headers.**
  Anthropic otherwise returns gzip SSE, and the parser expects plaintext.
- **Broadcasting must stay off the hot path.**
  Deferred watch events are broadcast only after Envoy has already been told to
  continue.
- **Session identity is `hash(working_dir, full messages[0] text)`.**
  `working_dir` alone collides for parallel Claude sessions in one repo; short
  prompt prefixes also collide. Use the full first user message.
- **`/watch` replay behavior is intentional.**
  New subscribers need recent event history, and `GET /watch?session=X` needs a
  synthetic `SessionStart` so mid-session joins still render a useful header.
- **No panics in ext_proc phase handlers.**
  Avoid `unwrap()` in the request/response phase path.
- **Quota burn is computed locally.**
  Claude Code OAuth traffic does not reliably expose the headers you would need
  for authoritative rate-limit accounting.
- **Request rows are immutable.**
  Raw history stays append-only; derived summaries and repaired aggregates can
  be recalculated.

## Code map

- `clauditor-core/src/main.rs` — ext_proc server, HTTP server, session
  lifecycle, response finalization
- `clauditor-core/src/metrics.rs` — Prometheus registration and metric helpers
- `clauditor-core/src/watch.rs` — watch event model and replay broadcaster
- `clauditor-core/src/diagnosis.rs` — diagnosis and degradation analysis
- `clauditor-cli/src/main.rs` — CLI commands and inline watch rendering
- `clauditor-cli/src/tmux.rs` — tmux bootstrap and multi-pane orchestrator
- `envoy/envoy.yaml` — the proxy/filter chain
- `test/parallel-sessions.sh` — session identity regression
- `test/e2e.sh` — full-stack smoke test

## Change-specific guidance

- If you change session identity or request parsing, run
  `bash test/parallel-sessions.sh 4`.
- If you change watch replay, synthetic session start handling, or tmux
  orchestration, test `clauditor watch --tmux` manually.
- If you change metrics names, update Grafana and tests in the same patch.
- If you change the proxy behavior in `envoy/envoy.yaml`, be conservative:
  small misconfigurations can silently break all Claude Code traffic.

## Releases

Pushing a tag like `v0.1.0` runs `.github/workflows/release.yml`.

The release workflow:

- builds CLI tarballs for macOS and Linux on x86_64 and arm64/aarch64
- pushes `ghcr.io/softcane/clauditor-core:v0.1.0`
- publishes SHA-256 checksums and `install.sh` to the GitHub Release

The installed CLI can run `clauditor up` without a git checkout. If it cannot
find a repo-level compose file, it writes bundled stack assets under
`$CLAUDITOR_HOME` or `~/.local/share/clauditor` and uses the released
`clauditor-core` container image.
