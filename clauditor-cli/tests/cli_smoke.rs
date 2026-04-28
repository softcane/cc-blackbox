use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn clauditor(args: &[&str]) -> Output {
    clauditor_with_env(args, &[])
}

fn clauditor_with_env(args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_clauditor"));
    command.args(args).env("NO_COLOR", "1");
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("run clauditor")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn serve_json_once(body: &str) -> (String, mpsc::Receiver<String>) {
    serve_response_once(200, body)
}

fn serve_response_once(status: u16, body: &str) -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind test server");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let body = body.to_string();
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let n = stream.read(&mut buffer).expect("read request");
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..n]);
            if request_is_complete(&request) {
                break;
            }
        }

        tx.send(String::from_utf8_lossy(&request).into_owned())
            .expect("send captured request");

        write!(
            stream,
            "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            status,
            if status < 400 { "OK" } else { "ERROR" },
            body.len(),
            body
        )
        .expect("write response");
    });

    (url, rx)
}

fn request_is_complete(request: &[u8]) -> bool {
    let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);

    request.len() >= header_end + 4 + content_length
}

fn captured_request(rx: mpsc::Receiver<String>) -> String {
    rx.recv_timeout(Duration::from_secs(2))
        .expect("captured HTTP request")
}

#[test]
fn top_level_help_exposes_user_workflows() {
    let output = clauditor(&["--help"]);

    assert!(output.status.success(), "stderr:\n{}", stderr(&output));
    let out = stdout(&output);
    assert!(out.contains("CLI for clauditor observability proxy"));
    assert!(out.contains("doctor"));
    assert!(out.contains("up"));
    assert!(out.contains("run"));
    assert!(out.contains("watch"));
    assert!(out.contains("sessions"));
    assert!(out.contains("recall"));
    assert!(out.contains("reconcile"));
}

#[test]
fn run_help_documents_watch_and_trailing_child_command() {
    let output = clauditor(&["run", "--help"]);

    assert!(output.status.success(), "stderr:\n{}", stderr(&output));
    let out = stdout(&output);
    assert!(out.contains("Run a command through the local Clauditor proxy"));
    assert!(out.contains("--watch"));
    assert!(out.contains("Command and arguments to run"));
}

#[test]
fn run_command_requires_child_command() {
    let output = clauditor(&["run"]);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("required"));
}

#[test]
fn run_command_uses_proxy_env_after_core_health_check() {
    let (url, request_rx) = serve_json_once(r#"{"ok":true}"#);
    let health_url = format!("{url}/health");

    let output = clauditor_with_env(
        &[
            "run",
            "/bin/sh",
            "-c",
            "printf '%s' \"$ANTHROPIC_BASE_URL\"; exit 7",
        ],
        &[("CLAUDITOR_CORE_HEALTH_URL", &health_url)],
    );

    assert_eq!(
        output.status.code(),
        Some(7),
        "stderr:\n{}",
        stderr(&output)
    );
    let request = captured_request(request_rx);
    assert!(
        request.starts_with("GET /health "),
        "unexpected request:\n{request}"
    );
    assert_eq!(stdout(&output), "http://127.0.0.1:10000");
}

#[test]
fn watch_rejects_session_filter_with_tmux_mode() {
    let output = clauditor(&["watch", "--session", "session_demo", "--tmux"]);

    assert!(!output.status.success());
    let err = stderr(&output);
    assert!(
        err.contains("cannot be used with"),
        "expected clap conflict error, got:\n{err}"
    );
}

#[test]
fn sessions_command_renders_sessions_from_api() {
    let (url, request_rx) = serve_json_once(
        r#"{
          "cost_source": "builtin_model_family_pricing",
          "trusted_for_budget_enforcement": false,
          "sessions": [
            {
              "session_id": "session_abcdefghijklmnopqrstuvwxyz",
              "model": "claude-sonnet-4-5-20250929",
              "total_turns": 7,
              "outcome": "Likely Completed",
              "estimated_total_cost_dollars": 1.23,
              "billed_cost_dollars": 1.11,
              "cache_hit_ratio": 0.5,
              "primary_cause": ""
            }
          ]
        }"#,
    );

    let output = clauditor(&["sessions", "--url", &url, "--limit", "1", "--days", "2"]);

    assert!(output.status.success(), "stderr:\n{}", stderr(&output));
    let request = captured_request(request_rx);
    assert!(
        request.starts_with("GET /api/sessions?limit=1&days=2 "),
        "unexpected request:\n{request}"
    );
    let out = stdout(&output);
    assert!(out.contains("Estimated cost source: built-in model-family pricing"));
    assert!(out.contains("session_abcdefghijkl"));
    assert!(out.contains("sonnet-4-5-20250929"));
    assert!(out.contains("$1.23"));
    assert!(out.contains("$1.11"));
}

#[test]
fn sessions_command_reports_api_errors() {
    let (url, request_rx) = serve_response_once(503, r#"{"error":"db unavailable"}"#);

    let output = clauditor(&["sessions", "--url", &url]);

    assert!(!output.status.success());
    let request = captured_request(request_rx);
    assert!(
        request.starts_with("GET /api/sessions?limit=20&days=7 "),
        "unexpected request:\n{request}"
    );
    assert!(stderr(&output).contains("Error: HTTP 503"));
}

#[test]
fn recall_command_renders_ranked_hits_from_api() {
    let (url, request_rx) = serve_json_once(
        r#"{
          "query": "auth cache",
          "hits": [
            {
              "score": 88,
              "session_id": "session_recall",
              "started_at": "2026-04-28T10:15:00Z",
              "completed_at": "2026-04-28T10:45:00Z",
              "model": "claude-sonnet-4-5-20250929",
              "outcome": "Likely Completed",
              "initial_prompt": "Investigate auth cache",
              "final_response_summary": "Fixed the auth cache warm path."
            }
          ]
        }"#,
    );

    let output = clauditor(&[
        "recall", "--url", &url, "--limit", "2", "--days", "9", "auth", "cache",
    ]);

    assert!(output.status.success(), "stderr:\n{}", stderr(&output));
    let request = captured_request(request_rx);
    assert!(
        request.starts_with("GET /api/recall?"),
        "unexpected request:\n{request}"
    );
    assert!(request.contains("q=auth+cache"));
    assert!(request.contains("limit=2"));
    assert!(request.contains("days=9"));
    let out = stdout(&output);
    assert!(out.contains("Recall results for \"auth cache\":"));
    assert!(out.contains("session_recall"));
    assert!(out.contains("Outcome: Likely Completed"));
    assert!(out.contains("Prompt: Investigate auth cache"));
    assert!(out.contains("Landed: Fixed the auth cache warm path."));
}

#[test]
fn recall_command_reports_no_matches() {
    let (url, request_rx) = serve_json_once(r#"{"query":"missing","hits":[]}"#);

    let output = clauditor(&["recall", "--url", &url, "missing"]);

    assert!(output.status.success(), "stderr:\n{}", stderr(&output));
    let request = captured_request(request_rx);
    assert!(
        request.starts_with("GET /api/recall?"),
        "unexpected request:\n{request}"
    );
    assert!(stdout(&output).contains("No matches for \"missing\"."));
}

#[test]
fn reconcile_command_posts_billing_payload() {
    let (url, request_rx) = serve_json_once(r#"{"inserted":1}"#);

    let output = clauditor(&[
        "reconcile",
        "--url",
        &url,
        "--session",
        "session_demo",
        "--billed-cost",
        "3.5",
        "--source",
        "invoice_test",
        "--imported-at",
        "2026-04-28T00:00:00Z",
    ]);

    assert!(output.status.success(), "stderr:\n{}", stderr(&output));
    let request = captured_request(request_rx);
    assert!(
        request.starts_with("POST /api/billing-reconciliations "),
        "unexpected request:\n{request}"
    );
    assert!(request.contains(r#""session_id":"session_demo""#));
    assert!(request.contains(r#""source":"invoice_test""#));
    assert!(request.contains(r#""billed_cost_dollars":3.5"#));
    assert!(request.contains(r#""imported_at":"2026-04-28T00:00:00Z""#));
    assert!(stdout(&output).contains("Imported 1 billed reconciliation."));
}

#[test]
fn reconcile_command_reports_api_errors() {
    let (url, request_rx) = serve_response_once(404, r#"{"error":"unknown session"}"#);

    let output = clauditor(&[
        "reconcile",
        "--url",
        &url,
        "--session",
        "session_missing",
        "--billed-cost",
        "3.5",
        "--source",
        "invoice_test",
    ]);

    assert!(!output.status.success());
    let request = captured_request(request_rx);
    assert!(
        request.starts_with("POST /api/billing-reconciliations "),
        "unexpected request:\n{request}"
    );
    assert!(stderr(&output).contains("Error: HTTP 404"));
}
