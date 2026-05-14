use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};
use serde_json::Value;

static CORE_PROCESS_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct CoreProcess {
    child: Child,
}

impl CoreProcess {
    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().expect("poll child process")
    }
}

impl Drop for CoreProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn unused_loopback_addr() -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    listener.local_addr().expect("read local addr").to_string()
}

fn unique_db_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "cc-blackbox-core-http-smoke-{}-{nanos}.db",
        std::process::id()
    ))
}

fn start_core(http_addr: &str, grpc_addr: &str, db_path: &PathBuf) -> CoreProcess {
    let child = Command::new(env!("CARGO_BIN_EXE_cc-blackbox-core"))
        .env("CC_BLACKBOX_HTTP_ADDR", http_addr)
        .env("CC_BLACKBOX_GRPC_ADDR", grpc_addr)
        .env("CC_BLACKBOX_DB_PATH", db_path)
        .env("RUST_LOG", "error")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn cc-blackbox-core");

    CoreProcess { child }
}

fn http_get(addr: &str, path: &str) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|err| err.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| err.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| err.to_string())?;

    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|err| err.to_string())?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| err.to_string())?;

    parse_http_response(&response)
}

fn wait_for_health(process: &mut CoreProcess, http_addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(status) = process.try_wait() {
            panic!("cc-blackbox-core exited before health check: {status}");
        }
        if let Ok((200, body)) = http_get(http_addr, "/health") {
            if body.trim() == "ok" {
                return;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }

    panic!("cc-blackbox-core did not become healthy at {http_addr}");
}

fn open_db_after_schema_ready(db_path: &PathBuf) -> Connection {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let conn = Connection::open(db_path).expect("open fixture db");
        let schema_ready = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sessions'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;
        if schema_ready {
            return conn;
        }

        if Instant::now() >= deadline {
            panic!("cc-blackbox-core did not initialize sqlite schema");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn http_post_json(addr: &str, path: &str, body: &str) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|err| err.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| err.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| err.to_string())?;

    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    )
    .map_err(|err| err.to_string())?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| err.to_string())?;
    parse_http_response(&response)
}

fn http_get_stream_until(addr: &str, path: &str, expected: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(addr).map_err(|err| err.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| err.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| err.to_string())?;

    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
    )
    .map_err(|err| err.to_string())?;

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut response = Vec::new();
    let mut buffer = [0u8; 1024];
    while Instant::now() < deadline {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                response.extend_from_slice(&buffer[..n]);
                let text = String::from_utf8_lossy(&response);
                if text.contains(expected) {
                    return Ok(text.into_owned());
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err.to_string()),
        }
    }

    Err(format!(
        "stream did not contain {expected:?}; got:\n{}",
        String::from_utf8_lossy(&response)
    ))
}

fn http_get_stream_for(addr: &str, path: &str, duration: Duration) -> Result<String, String> {
    let mut stream = TcpStream::connect(addr).map_err(|err| err.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .map_err(|err| err.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| err.to_string())?;

    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
    )
    .map_err(|err| err.to_string())?;

    let deadline = Instant::now() + duration;
    let mut response = Vec::new();
    let mut buffer = [0u8; 1024];
    while Instant::now() < deadline {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&buffer[..n]),
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err.to_string()),
        }
    }

    Ok(String::from_utf8_lossy(&response).into_owned())
}

fn parse_http_response(response: &str) -> Result<(u16, String), String> {
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| format!("missing HTTP status in response: {response:?}"))?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();

    Ok((status, body))
}

fn http_get_json(addr: &str, path: &str) -> Value {
    let (status, body) = http_get(addr, path).expect("GET JSON endpoint");
    assert_eq!(status, 200, "unexpected response body: {body}");
    serde_json::from_str(&body).expect("response body is JSON")
}

fn json_array<'a>(value: &'a Value, key: &str) -> &'a Vec<Value> {
    value
        .get(key)
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("missing JSON array field {key}: {value}"))
}

fn insert_api_fixture(conn: &Connection) {
    conn.execute(
        "INSERT INTO sessions (
            session_id, started_at, ended_at, model, initial_prompt
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            "session_api",
            "2999-01-01T00:00:00Z",
            "2999-01-01T00:30:00Z",
            "claude-sonnet-4-5-20250929",
            "Investigate auth cache behavior"
        ],
    )
    .expect("insert session");

    conn.execute(
        "INSERT INTO requests (
            request_id, session_id, timestamp, model, input_tokens, output_tokens,
            cache_read_tokens, cache_creation_tokens, cost_dollars, cost_source,
            trusted_for_budget_enforcement, duration_ms, tool_calls, cache_event
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            "request_api_1",
            "session_api",
            "2999-01-01T00:05:00Z",
            "claude-sonnet-4-5-20250929",
            1000,
            200,
            150,
            50,
            1.25,
            "pricing_file:test_contract",
            1,
            800,
            r#"["Read","Bash"]"#,
            "hit"
        ],
    )
    .expect("insert request");

    conn.execute(
        "INSERT INTO session_diagnoses (
            session_id, completed_at, outcome, total_turns, total_cost,
            cache_hit_ratio, degraded, degradation_turn, causes_json, advice_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            "session_api",
            "2999-01-01T00:30:00Z",
            "Likely Completed",
            3,
            1.25,
            0.75,
            1,
            2,
            r#"[{"turn_first_noticed":2,"cause_type":"cache_miss_ttl","detail":"cache expired","estimated_cost":0.42,"is_heuristic":false}]"#,
            r#"["Send a small keepalive before cache TTL expires."]"#
        ],
    )
    .expect("insert diagnosis");

    conn.execute(
        "INSERT INTO billing_reconciliations (
            session_id, imported_at, source, billed_cost_dollars
        ) VALUES (?1, ?2, ?3, ?4)",
        params!["session_api", "2999-01-01T01:00:00Z", "invoice_test", 1.11],
    )
    .expect("insert billing reconciliation");

    conn.execute(
        "INSERT INTO session_recall (
            session_id, initial_prompt, final_response_summary
        ) VALUES (?1, ?2, ?3)",
        params![
            "session_api",
            "Investigate auth cache behavior",
            "Found the auth middleware cache regression and fixed the warm path."
        ],
    )
    .expect("insert recall");
}

#[allow(clippy::too_many_arguments)]
fn insert_turn_snapshot(
    conn: &Connection,
    session_id: &str,
    turn_number: i64,
    timestamp: &str,
    input_tokens: i64,
    cache_read_tokens: i64,
    cache_creation_tokens: i64,
    output_tokens: i64,
    ttft_ms: i64,
    gap_from_prev_secs: f64,
    context_window_tokens: i64,
    tool_failures: i64,
    requested_model: &str,
    actual_model: &str,
) {
    conn.execute(
        "INSERT INTO turn_snapshots (
            session_id, turn_number, timestamp, input_tokens, cache_read_tokens,
            cache_creation_tokens, output_tokens, ttft_ms, tool_calls, tool_failures,
            gap_from_prev_secs, context_utilization, context_window_tokens,
            frustration_signals, requested_model, actual_model, response_summary
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, '[]', ?9, ?10, 0.0, ?11, 0, ?12, ?13, NULL)",
        params![
            session_id,
            turn_number,
            timestamp,
            input_tokens,
            cache_read_tokens,
            cache_creation_tokens,
            output_tokens,
            ttft_ms,
            tool_failures,
            gap_from_prev_secs,
            context_window_tokens,
            requested_model,
            actual_model,
        ],
    )
    .expect("insert turn snapshot");
}

#[test]
fn core_serves_health_and_metrics_on_configured_http_addr() {
    let _guard = CORE_PROCESS_TEST_LOCK
        .lock()
        .expect("lock core process test");
    let http_addr = unused_loopback_addr();
    let grpc_addr = unused_loopback_addr();
    let db_path = unique_db_path();
    let mut core = start_core(&http_addr, &grpc_addr, &db_path);

    wait_for_health(&mut core, &http_addr);

    let (status, metrics) = http_get(&http_addr, "/metrics").expect("GET /metrics");
    assert_eq!(status, 200);
    assert!(
        metrics.contains("cc_blackbox_active_sessions"),
        "metrics body should expose active session gauge: {metrics}"
    );

    drop(core);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[test]
fn core_http_apis_render_fixture_backed_session_data() {
    let _guard = CORE_PROCESS_TEST_LOCK
        .lock()
        .expect("lock core process test");
    let http_addr = unused_loopback_addr();
    let grpc_addr = unused_loopback_addr();
    let db_path = unique_db_path();
    let mut core = start_core(&http_addr, &grpc_addr, &db_path);
    wait_for_health(&mut core, &http_addr);

    let conn = open_db_after_schema_ready(&db_path);
    insert_api_fixture(&conn);
    insert_turn_snapshot(
        &conn,
        "session_api",
        1,
        "2999-01-01T00:05:00Z",
        100,
        0,
        500,
        20,
        100,
        0.0,
        200_000,
        0,
        "claude-sonnet",
        "claude-sonnet",
    );
    insert_turn_snapshot(
        &conn,
        "session_api",
        2,
        "2999-01-01T00:10:00Z",
        130_000,
        0,
        1_000,
        30,
        500,
        400.0,
        200_000,
        2,
        "claude-opus",
        "claude-sonnet",
    );
    insert_turn_snapshot(
        &conn,
        "session_api",
        3,
        "2999-01-01T00:15:00Z",
        100,
        0,
        800,
        25,
        100,
        10.0,
        200_000,
        0,
        "claude-sonnet",
        "claude-sonnet",
    );

    let summary = http_get_json(&http_addr, "/api/summary");
    assert_eq!(summary["today"]["sessions"], 1);
    assert_eq!(summary["today"]["estimated_cost_dollars"], 1.25);
    assert_eq!(summary["today"]["billed_cost_dollars"], 1.11);
    assert_eq!(summary["today"]["cache_hit_ratio"], 0.75);

    let sessions = http_get_json(&http_addr, "/api/sessions?limit=5&days=30");
    let session_rows = json_array(&sessions, "sessions");
    assert_eq!(session_rows.len(), 1);
    assert_eq!(session_rows[0]["session_id"], "session_api");
    assert_eq!(session_rows[0]["primary_cause"], "cache_miss_ttl");
    assert_eq!(session_rows[0]["billed_cost_dollars"], 1.11);

    let diagnosis = http_get_json(&http_addr, "/api/diagnosis/session_api");
    assert_eq!(diagnosis["session_id"], "session_api");
    assert_eq!(diagnosis["degraded"], true);
    assert_eq!(diagnosis["degradation_turn"], 2);
    assert_eq!(diagnosis["causes"][0]["cause_type"], "cache_miss_ttl");

    let recall = http_get_json(
        &http_addr,
        "/api/recall?q=captured%20redacted&limit=1&days=30",
    );
    let hits = json_array(&recall, "hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["session_id"], "session_api");
    assert!(hits[0]["score"].as_i64().unwrap_or_default() > 0);

    let cache_rebuilds = http_get_json(&http_addr, "/api/cache-rebuilds?days=30");
    assert_eq!(cache_rebuilds["total_rebuilds"], 2);
    assert_eq!(cache_rebuilds["cold_start_builds"], 1);
    assert_eq!(cache_rebuilds["rebuilds_from_idle_gaps"], 1);
    assert_eq!(cache_rebuilds["rebuilds_without_idle_gap"], 1);
    assert_eq!(cache_rebuilds["tokens_wasted_on_rebuilds"], 1_800);

    let degradation = http_get_json(&http_addr, "/api/degradation/session_api");
    assert_eq!(degradation["degraded"], true);
    assert_eq!(degradation["degradation_turn"], 2);
    let turn_flags = degradation["turns"][1]["flags"]
        .as_array()
        .expect("turn flags")
        .iter()
        .map(|value| value.as_str().expect("flag string"))
        .collect::<Vec<_>>();
    assert!(turn_flags.contains(&"cache_miss_ttl"));
    assert!(turn_flags.contains(&"context_bloat"));
    assert!(turn_flags.contains(&"model_fallback"));
    assert!(turn_flags.contains(&"tool_failures"));

    drop(conn);
    drop(core);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[test]
fn core_billing_reconciliation_api_validates_and_persists_payloads() {
    let _guard = CORE_PROCESS_TEST_LOCK
        .lock()
        .expect("lock core process test");
    let http_addr = unused_loopback_addr();
    let grpc_addr = unused_loopback_addr();
    let db_path = unique_db_path();
    let mut core = start_core(&http_addr, &grpc_addr, &db_path);
    wait_for_health(&mut core, &http_addr);

    let conn = open_db_after_schema_ready(&db_path);
    conn.execute(
        "INSERT INTO sessions (session_id, started_at, model) VALUES (?1, ?2, ?3)",
        params!["session_billable", "2999-01-01T00:00:00Z", "claude-sonnet"],
    )
    .expect("insert billable session");

    let (status, body) = http_post_json(
        &http_addr,
        "/api/billing-reconciliations",
        r#"{
          "session_id": "session_billable",
          "source": "invoice_test",
          "billed_cost_dollars": 2.75,
          "imported_at": "2999-01-01T01:00:00Z"
        }"#,
    )
    .expect("POST reconciliation");
    assert_eq!(status, 200, "body: {body}");
    let body: Value = serde_json::from_str(&body).expect("reconciliation JSON");
    assert_eq!(body["inserted"], 1);

    let stored_cost: f64 = conn
        .query_row(
            "SELECT billed_cost_dollars FROM billing_reconciliations WHERE session_id = ?1",
            params!["session_billable"],
            |row| row.get(0),
        )
        .expect("stored billing reconciliation");
    assert_eq!(stored_cost, 2.75);

    let (status, body) = http_post_json(
        &http_addr,
        "/api/billing-reconciliations",
        r#"{
          "session_id": "missing_session",
          "source": "invoice_test",
          "billed_cost_dollars": 1.00
        }"#,
    )
    .expect("POST unknown session reconciliation");
    assert_eq!(status, 404);
    assert!(body.contains("unknown session_id: missing_session"));

    let (status, body) = http_post_json(
        &http_addr,
        "/api/billing-reconciliations",
        r#"{
          "session_id": "session_billable",
          "source": "",
          "billed_cost_dollars": -1.00
        }"#,
    )
    .expect("POST invalid reconciliation");
    assert_eq!(status, 400);
    assert!(body.contains("invalid reconciliation payload"));

    drop(conn);
    drop(core);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[test]
fn core_finalize_sessions_api_is_process_visible_and_idempotent() {
    let _guard = CORE_PROCESS_TEST_LOCK
        .lock()
        .expect("lock core process test");
    let http_addr = unused_loopback_addr();
    let grpc_addr = unused_loopback_addr();
    let db_path = unique_db_path();
    let mut core = start_core(&http_addr, &grpc_addr, &db_path);
    wait_for_health(&mut core, &http_addr);

    let conn = open_db_after_schema_ready(&db_path);
    conn.execute(
        "INSERT INTO sessions (session_id, started_at, ended_at, model, initial_prompt)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            "session_http_finalized",
            "2999-01-01T00:00:00Z",
            "2999-01-01T00:05:00Z",
            "claude-sonnet",
            "already done"
        ],
    )
    .expect("insert finalized session");

    let (status, body) = http_post_json(
        &http_addr,
        "/api/sessions/finalize",
        r#"{
          "session_ids": ["session_http_finalized", "session_http_missing"],
          "reason": "child_exit",
          "child_exit_code": 0
        }"#,
    )
    .expect("POST finalize sessions");
    assert_eq!(status, 200, "body: {body}");
    let body: Value = serde_json::from_str(&body).expect("finalize JSON");
    assert_eq!(body["reason"], "child_exit");
    assert_eq!(body["results"][0]["session_id"], "session_http_finalized");
    assert_eq!(body["results"][0]["outcome"], "already_finalized");
    assert_eq!(body["results"][1]["session_id"], "session_http_missing");
    assert_eq!(body["results"][1]["outcome"], "not_found");

    let (status, body) = http_post_json(
        &http_addr,
        "/api/sessions/finalize",
        r#"{"session_ids":[],"reason":"child_exit"}"#,
    )
    .expect("POST invalid finalize sessions");
    assert_eq!(status, 400);
    assert!(body.contains("session_ids must not be empty"));

    drop(conn);
    drop(core);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[test]
fn core_watch_replays_hook_events_for_late_subscribers() {
    let _guard = CORE_PROCESS_TEST_LOCK
        .lock()
        .expect("lock core process test");
    let http_addr = unused_loopback_addr();
    let grpc_addr = unused_loopback_addr();
    let db_path = unique_db_path();
    let mut core = start_core(&http_addr, &grpc_addr, &db_path);
    wait_for_health(&mut core, &http_addr);

    let (status, body) = http_post_json(
        &http_addr,
        "/api/hooks/claude-code",
        r#"{
          "session_id": "session_watch",
          "hook_event_name": "PreToolUse",
          "tool_name": "Skill",
          "tool_input": {
            "skill_name": "tdd",
            "prompt": "write one failing test"
          }
        }"#,
    )
    .expect("POST hook event");
    assert_eq!(status, 204, "body: {body}");

    let stream = http_get_stream_until(
        &http_addr,
        "/watch?session=session_watch",
        r#""skill_name":"tdd""#,
    )
    .expect("watch stream replay");
    assert!(stream.contains(r#""type":"skill_event""#));
    assert!(stream.contains(r#""event_type":"fired""#));
    assert!(stream.contains(r#""source":"hook""#));

    drop(core);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[test]
fn core_watch_replay_false_suppresses_history_for_run_owned_watchers() {
    let _guard = CORE_PROCESS_TEST_LOCK
        .lock()
        .expect("lock core process test");
    let http_addr = unused_loopback_addr();
    let grpc_addr = unused_loopback_addr();
    let db_path = unique_db_path();
    let mut core = start_core(&http_addr, &grpc_addr, &db_path);
    wait_for_health(&mut core, &http_addr);

    let (status, body) = http_post_json(
        &http_addr,
        "/api/hooks/claude-code",
        r#"{
          "session_id": "session_watch_no_replay",
          "hook_event_name": "PreToolUse",
          "tool_name": "Skill",
          "tool_input": {
            "skill_name": "tdd",
            "prompt": "write one failing test"
          }
        }"#,
    )
    .expect("POST hook event");
    assert_eq!(status, 204, "body: {body}");

    let stream = http_get_stream_for(
        &http_addr,
        "/watch?session=session_watch_no_replay&replay=false",
        Duration::from_millis(250),
    )
    .expect("watch stream with replay disabled");
    assert!(
        stream.starts_with("HTTP/1.1 200 OK"),
        "unexpected stream response:\n{stream}"
    );
    assert!(
        !stream.contains(r#""type":"skill_event""#),
        "replay=false should suppress prior history:\n{stream}"
    );

    drop(core);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}
