# Developing On Clauditor

```bash
docker compose up -d --build
docker compose logs -f clauditor-core
bash test/e2e.sh
bash test/parallel-sessions.sh 4
cargo fmt
cargo clippy -- -W clippy::all
```

Before touching the request path or the broadcast path, read [CONTRIBUTING.md](../../CONTRIBUTING.md). That guide has the public invariants and the parts that are easy to break by accident.
