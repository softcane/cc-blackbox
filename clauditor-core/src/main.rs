use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use rusqlite::{Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Proto modules
// ---------------------------------------------------------------------------
pub mod envoy {
    pub mod service {
        pub mod ext_proc {
            pub mod v3 {
                tonic::include_proto!("envoy.service.ext_proc.v3");
            }
        }
    }
    pub mod config {
        pub mod core {
            pub mod v3 {
                tonic::include_proto!("envoy.config.core.v3");
            }
        }
    }
    pub mod r#type {
        pub mod v3 {
            tonic::include_proto!("envoy.r#type.v3");
        }
    }
    pub mod extensions {
        pub mod filters {
            pub mod http {
                pub mod ext_proc {
                    pub mod v3 {
                        tonic::include_proto!("envoy.extensions.filters.http.ext_proc.v3");
                    }
                }
            }
        }
    }
}

pub mod diagnosis;
pub mod metrics;
pub mod pricing;
pub mod watch;

use envoy::config::core::v3::{
    HeaderValue as ProtoHeaderValue, HeaderValueOption as ProtoHeaderValueOption,
};
use envoy::service::ext_proc::v3::{
    common_response::ResponseStatus,
    external_processor_server::{ExternalProcessor, ExternalProcessorServer},
    processing_request::Request as ExtProcRequest,
    processing_response::Response as ExtProcResponse,
    BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, HttpHeaders, ImmediateResponse,
    ProcessingRequest, ProcessingResponse,
};

// ---------------------------------------------------------------------------
// Date formatting (minimal UTC ISO 8601, no chrono dependency)
// ---------------------------------------------------------------------------
pub fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn epoch_to_iso8601(secs: u64) -> String {
    let days_total = (secs / 86400) as i32;
    let tod = secs % 86400;
    let (y, m, d) = days_to_ymd(days_total);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    )
}

fn days_to_ymd(mut days: i32) -> (i32, u32, u32) {
    let mut y = 1970;
    loop {
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let diy = if leap { 366 } else { 365 };
        if days < diy {
            break;
        }
        days -= diy;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let md = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 0usize;
    while m < 12 && days >= md[m] {
        days -= md[m];
        m += 1;
    }
    (y, (m + 1) as u32, (days + 1) as u32)
}

fn now_iso8601() -> String {
    epoch_to_iso8601(now_epoch_secs())
}

/// Return ISO 8601 for start of today (UTC).
fn start_of_today_iso() -> String {
    let secs = now_epoch_secs();
    let day_start = secs - (secs % 86400);
    epoch_to_iso8601(day_start)
}

fn start_of_week_epoch_at(secs: u64) -> u64 {
    let days = (secs / 86400) as i32;
    // day-of-week: 0=Thu for epoch. Monday = (days + 3) % 7 offset.
    let dow = ((days + 3) % 7) as u64; // 0=Mon .. 6=Sun
    secs - (secs % 86400) - dow * 86400
}

fn start_of_week_iso_at(secs: u64) -> String {
    epoch_to_iso8601(start_of_week_epoch_at(secs))
}

fn start_of_week_iso() -> String {
    start_of_week_iso_at(now_epoch_secs())
}

fn start_of_month_iso() -> String {
    let secs = now_epoch_secs();
    let days_total = (secs / 86400) as i32;
    let (y, m, _) = days_to_ymd(days_total);
    format!("{y:04}-{m:02}-01T00:00:00Z")
}

// ---------------------------------------------------------------------------
// Per-request metadata
// ---------------------------------------------------------------------------
pub struct RequestMeta {
    pub request_id: String,
    pub session_id: String,
    pub model: String,
    pub message_count: usize,
    pub has_tools: bool,
    pub system_prompt_length: usize,
    pub estimated_input_tokens: usize,
    pub started_at: Instant,
}

static REQUEST_STATE: LazyLock<DashMap<String, RequestMeta>> = LazyLock::new(DashMap::new);

// ---------------------------------------------------------------------------
// Budget & circuit breaker state (Phase 7)
// ---------------------------------------------------------------------------
pub fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
pub fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub const ESTIMATED_COST_SOURCE: &str = pricing::BUILTIN_COST_SOURCE;

struct RuntimeState {
    total_spend: f64,
    total_tokens: u64,
    request_count: u64,
    consecutive_errors: u64,
    circuit_open_until: Option<Instant>,
}

impl RuntimeState {
    fn new() -> Self {
        Self {
            total_spend: 0.0,
            total_tokens: 0,
            request_count: 0,
            consecutive_errors: 0,
            circuit_open_until: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SessionBudgetState {
    total_spend: f64,
    total_tokens: u64,
    request_count: u64,
    estimated_cache_waste_dollars: f64,
}

static RUNTIME_STATE: LazyLock<Mutex<RuntimeState>> =
    LazyLock::new(|| Mutex::new(RuntimeState::new()));
static SESSION_BUDGETS: LazyLock<DashMap<String, SessionBudgetState>> = LazyLock::new(DashMap::new);

/// Check if request should be blocked by the process-wide circuit breaker.
fn check_circuit_breaker() -> Option<(&'static str, String)> {
    let runtime = RUNTIME_STATE.lock().unwrap();

    // Circuit breaker check.
    if let Some(until) = runtime.circuit_open_until {
        if Instant::now() < until {
            let remaining = until.duration_since(Instant::now()).as_secs();
            return Some((
                "circuit_breaker",
                format!(
                    "Clauditor: circuit breaker open. {} consecutive errors detected. \
                 Pausing requests for {}s to prevent runaway estimated cost.",
                    runtime.consecutive_errors, remaining
                ),
            ));
        }
    }

    None
}

/// Check if the current session has exceeded its configured budget.
fn check_session_budget(session_id: Option<&str>) -> Option<(&'static str, String)> {
    let session_id = session_id?;
    let state = SESSION_BUDGETS.get(session_id)?;

    // Estimated-dollar budget check. These dollars are only enforced when the
    // active price catalog is explicitly marked trusted for enforcement.
    let budget = env_f64("CLAUDITOR_SESSION_BUDGET_DOLLARS", 0.0);
    if budget > 0.0 && state.total_spend >= budget && pricing::trusted_for_budget_enforcement() {
        return Some(("budget_exceeded", format!(
            "Clauditor: estimated session budget exceeded (${:.2}). Estimated spend: ${:.2} across {} requests. \
             Estimated cache rebuild waste: ${:.2}. Reset with CLAUDITOR_SESSION_BUDGET_DOLLARS=0 or restart session.",
            budget,
            state.total_spend,
            state.request_count,
            state.estimated_cache_waste_dollars
        )));
    }

    // Token budget check.
    let token_budget = env_u64("CLAUDITOR_SESSION_BUDGET_TOKENS", 0);
    if token_budget > 0 && state.total_tokens >= token_budget {
        return Some((
            "budget_exceeded",
            format!(
                "Clauditor: token budget exceeded ({}). Used: {} tokens across {} requests. \
             Reset with CLAUDITOR_SESSION_BUDGET_TOKENS=0 or restart session.",
                token_budget, state.total_tokens, state.request_count
            ),
        ));
    }

    None
}

fn make_block_response(error_type: &str, message: &str) -> ProcessingResponse {
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": message
        }
    })
    .to_string();

    ProcessingResponse {
        response: Some(ExtProcResponse::ImmediateResponse(ImmediateResponse {
            status: Some(envoy::r#type::v3::HttpStatus {
                code: envoy::r#type::v3::StatusCode::TooManyRequests.into(),
            }),
            headers: Some(HeaderMutation {
                set_headers: vec![ProtoHeaderValueOption {
                    header: Some(ProtoHeaderValue {
                        key: "content-type".into(),
                        value: "application/json".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            body,
            ..Default::default()
        })),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// SQLite persistence (Phase 6)
// ---------------------------------------------------------------------------
fn db_path() -> String {
    std::env::var("CLAUDITOR_DB_PATH").unwrap_or_else(|_| "/data/clauditor.db".to_string())
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    total_input_tokens INTEGER DEFAULT 0,
    total_output_tokens INTEGER DEFAULT 0,
    total_cache_read_tokens INTEGER DEFAULT 0,
    total_cache_creation_tokens INTEGER DEFAULT 0,
    total_cost_dollars REAL DEFAULT 0.0,
    cache_waste_dollars REAL DEFAULT 0.0,
    request_count INTEGER DEFAULT 0,
    model TEXT,
    initial_prompt TEXT
);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER DEFAULT 0,
    cache_creation_tokens INTEGER DEFAULT 0,
    cost_dollars REAL,
    cost_source TEXT,
    trusted_for_budget_enforcement INTEGER DEFAULT 0,
    duration_ms INTEGER,
    tool_calls TEXT,
    cache_event TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    FOREIGN KEY (request_id) REFERENCES requests(request_id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at);
CREATE INDEX IF NOT EXISTS idx_requests_session ON requests(session_id);
CREATE INDEX IF NOT EXISTS idx_requests_timestamp ON requests(timestamp);

CREATE TABLE IF NOT EXISTS turn_snapshots (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id            TEXT NOT NULL,
    turn_number           INTEGER NOT NULL,
    timestamp             TEXT NOT NULL,
    input_tokens          INTEGER,
    cache_read_tokens     INTEGER DEFAULT 0,
    cache_creation_tokens INTEGER DEFAULT 0,
    output_tokens         INTEGER,
    ttft_ms               INTEGER,
    tool_calls            TEXT,
    tool_failures         INTEGER DEFAULT 0,
    gap_from_prev_secs    REAL,
    context_utilization   REAL,
    context_window_tokens INTEGER,
    frustration_signals   INTEGER DEFAULT 0,
    requested_model       TEXT,
    actual_model          TEXT,
    response_summary      TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS session_diagnoses (
    session_id        TEXT PRIMARY KEY,
    completed_at      TEXT NOT NULL,
    outcome           TEXT NOT NULL,
    total_turns       INTEGER,
    total_cost        REAL,
    cache_hit_ratio   REAL,
    degraded          INTEGER DEFAULT 0,
    degradation_turn  INTEGER,
    causes_json       TEXT,
    advice_json       TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS tool_outcomes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    turn_number INTEGER NOT NULL,
    timestamp   TEXT NOT NULL,
    tool_name   TEXT NOT NULL,
    input_summary TEXT,
    outcome     TEXT,
    duration_ms INTEGER
);

CREATE TABLE IF NOT EXISTS skill_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    skill_name  TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    confidence  REAL DEFAULT 1.0,
    source      TEXT NOT NULL,
    detail      TEXT
);

CREATE TABLE IF NOT EXISTS mcp_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    server      TEXT NOT NULL,
    tool        TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    source      TEXT NOT NULL,
    detail      TEXT
);

CREATE TABLE IF NOT EXISTS session_recall (
    session_id              TEXT PRIMARY KEY,
    initial_prompt          TEXT,
    final_response_summary  TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS billing_reconciliations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT NOT NULL,
    imported_at         TEXT NOT NULL,
    source              TEXT NOT NULL,
    billed_cost_dollars REAL NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_turns_session ON turn_snapshots(session_id, turn_number);
CREATE INDEX IF NOT EXISTS idx_diagnoses_completed ON session_diagnoses(completed_at);
CREATE INDEX IF NOT EXISTS idx_tool_outcomes_session ON tool_outcomes(session_id);
CREATE INDEX IF NOT EXISTS idx_skill_events_session ON skill_events(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_mcp_events_session ON mcp_events(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_session_recall_session ON session_recall(session_id);
CREATE INDEX IF NOT EXISTS idx_billing_reconciliations_session_imported
    ON billing_reconciliations(session_id, imported_at DESC);
";

enum DbCommand {
    InsertSession {
        session_id: String,
        started_at: String,
        model: String,
        initial_prompt: Option<String>,
    },
    RecordRequest {
        request_id: String,
        session_id: String,
        timestamp: String,
        model: String,
        input_tokens: u64,
        output_tokens: u64,
        cache_read_tokens: u64,
        cache_creation_tokens: u64,
        cost_dollars: f64,
        cost_source: String,
        trusted_for_budget_enforcement: bool,
        duration_ms: u64,
        tool_calls_json: String,
        tool_calls_list: Vec<(String, String)>,
        cache_event: String,
    },
    WriteTurnSnapshot {
        session_id: String,
        turn_number: u32,
        timestamp: String,
        input_tokens: u64,
        cache_read_tokens: u64,
        cache_creation_tokens: u64,
        output_tokens: u64,
        ttft_ms: u64,
        tool_calls_json: String,
        tool_failures: u32,
        gap_from_prev_secs: f64,
        context_utilization: f64,
        context_window_tokens: u64,
        frustration_signals: u32,
        requested_model: Option<String>,
        actual_model: Option<String>,
        response_summary: Option<String>,
    },
    WriteDiagnosis {
        session_id: String,
        completed_at: String,
        outcome: String,
        total_turns: u32,
        total_cost: f64,
        cache_hit_ratio: f64,
        degraded: bool,
        degradation_turn: Option<u32>,
        causes_json: String,
        advice_json: String,
    },
    WriteToolOutcome {
        session_id: String,
        turn_number: u32,
        timestamp: String,
        tool_name: String,
        input_summary: String,
        outcome: String,
        duration_ms: u64,
    },
    WriteSkillEvent {
        session_id: String,
        timestamp: String,
        skill_name: String,
        event_type: String,
        confidence: f64,
        source: String,
        detail: Option<String>,
    },
    WriteMcpEvent {
        session_id: String,
        timestamp: String,
        server: String,
        tool: String,
        event_type: String,
        source: String,
        detail: Option<String>,
    },
    WriteRecall {
        session_id: String,
        initial_prompt: String,
        final_response_summary: String,
    },
    WriteBillingReconciliation {
        session_id: String,
        imported_at: String,
        source: String,
        billed_cost_dollars: f64,
        response_tx: oneshot::Sender<Result<(), BillingReconciliationWriteError>>,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum BillingReconciliationWriteError {
    DbUnavailable,
    UnknownSession(String),
    Sqlite(String),
}

static DB_TX: LazyLock<std_mpsc::Sender<DbCommand>> = LazyLock::new(|| {
    let (tx, rx) = std_mpsc::channel();
    let path = db_path();
    std::thread::spawn(move || db_writer_loop(&path, rx));
    tx
});

fn ensure_turn_snapshot_model_columns(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(turn_snapshots)")?;
    let columns: HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|row| row.ok())
        .collect();

    if !columns.contains("requested_model") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN requested_model TEXT",
            [],
        )?;
    }
    if !columns.contains("actual_model") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN actual_model TEXT",
            [],
        )?;
    }
    if !columns.contains("response_summary") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN response_summary TEXT",
            [],
        )?;
    }
    if !columns.contains("context_window_tokens") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN context_window_tokens INTEGER",
            [],
        )?;
    }

    Ok(())
}

fn ensure_session_columns(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(sessions)")?;
    let columns: HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|row| row.ok())
        .collect();

    if !columns.contains("initial_prompt") {
        conn.execute("ALTER TABLE sessions ADD COLUMN initial_prompt TEXT", [])?;
    }
    if columns.contains("cache_hit_ratio") {
        conn.execute("ALTER TABLE sessions DROP COLUMN cache_hit_ratio", [])?;
    }

    Ok(())
}

fn ensure_request_cost_columns(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(requests)")?;
    let columns: HashSet<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|row| row.ok())
        .collect();

    if !columns.contains("cost_source") {
        conn.execute("ALTER TABLE requests ADD COLUMN cost_source TEXT", [])?;
    }
    if !columns.contains("trusted_for_budget_enforcement") {
        conn.execute(
            "ALTER TABLE requests ADD COLUMN trusted_for_budget_enforcement INTEGER DEFAULT 0",
            [],
        )?;
    }

    Ok(())
}

fn repair_turn_snapshot_context_windows(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT id, input_tokens, cache_read_tokens, cache_creation_tokens, \
         requested_model, actual_model \
         FROM turn_snapshots \
         WHERE context_window_tokens IS NULL OR context_window_tokens <= 0",
    )?;

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
                row.get::<_, i64>(2)?.max(0) as u64,
                row.get::<_, i64>(3)?.max(0) as u64,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    for (
        id,
        input_tokens,
        cache_read_tokens,
        cache_creation_tokens,
        requested_model,
        actual_model,
    ) in rows
    {
        let context_window_tokens = infer_context_window_tokens(
            requested_model.as_deref(),
            actual_model.as_deref(),
            input_tokens,
            cache_read_tokens,
            cache_creation_tokens,
        );
        let context_utilization = context_fill_ratio(
            input_tokens,
            cache_read_tokens,
            cache_creation_tokens,
            context_window_tokens,
        );
        conn.execute(
            "UPDATE turn_snapshots \
             SET context_window_tokens = ?2, context_utilization = ?3 \
             WHERE id = ?1",
            rusqlite::params![id, context_window_tokens as i64, context_utilization],
        )?;
    }

    Ok(())
}

fn repair_session_diagnosis_degradation_turns(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE session_diagnoses SET degradation_turn = NULL WHERE degraded = 0",
        [],
    )?;
    Ok(())
}

fn seed_live_metric_labels_from_db(conn: &Connection) -> rusqlite::Result<()> {
    metrics::ensure_tool_metric_labels("unknown");
    metrics::ensure_skill_metric_labels("unknown", "fired", "hook");

    let mut stmt = conn.prepare(
        "SELECT DISTINCT tool_name FROM tool_calls \
         UNION \
         SELECT DISTINCT tool_name FROM tool_outcomes",
    )?;
    let tool_names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    for tool_name in tool_names {
        metrics::ensure_tool_metric_labels(&tool_name);
    }

    let mut stmt =
        conn.prepare("SELECT DISTINCT skill_name, event_type, source FROM skill_events")?;
    let skill_events = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    for (skill_name, event_type, source) in skill_events {
        metrics::ensure_skill_metric_labels(&skill_name, &event_type, &source);
    }

    let mut stmt =
        conn.prepare("SELECT DISTINCT server, tool, event_type, source FROM mcp_events")?;
    let mcp_events = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    for (server, tool, event_type, source) in mcp_events {
        metrics::ensure_mcp_event_metric_labels(&server, &tool, &event_type, &source);
    }

    Ok(())
}

fn db_writer_loop(path: &str, rx: std_mpsc::Receiver<DbCommand>) {
    let conn = match Connection::open(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to open SQLite at {path}: {e}");
            return;
        }
    };
    let _ = conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
    if let Err(e) = conn.execute_batch(SCHEMA) {
        eprintln!("Failed to create schema: {e}");
        return;
    }
    if let Err(e) = ensure_turn_snapshot_model_columns(&conn) {
        eprintln!("Failed to migrate turn_snapshots model columns: {e}");
        return;
    }
    if let Err(e) = ensure_session_columns(&conn) {
        eprintln!("Failed to migrate sessions columns: {e}");
        return;
    }
    if let Err(e) = ensure_request_cost_columns(&conn) {
        eprintln!("Failed to migrate requests cost columns: {e}");
        return;
    }
    if let Err(e) = repair_turn_snapshot_context_windows(&conn) {
        eprintln!("Failed to repair turn_snapshots context windows: {e}");
        return;
    }
    if let Err(e) = repair_session_diagnosis_degradation_turns(&conn) {
        eprintln!("Failed to repair session_diagnoses degradation turns: {e}");
        return;
    }
    if let Err(e) = seed_live_metric_labels_from_db(&conn) {
        eprintln!("Failed to seed live metric labels from SQLite: {e}");
        return;
    }

    while let Ok(cmd) = rx.recv() {
        match cmd {
            DbCommand::InsertSession {
                session_id,
                started_at,
                model,
                initial_prompt,
            } => {
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO sessions (session_id, started_at, model, initial_prompt) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![session_id, started_at, model, initial_prompt],
                );
            }
            DbCommand::RecordRequest {
                request_id,
                session_id,
                timestamp,
                model,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_creation_tokens,
                cost_dollars,
                cost_source,
                trusted_for_budget_enforcement,
                duration_ms,
                tool_calls_json,
                tool_calls_list,
                cache_event,
            } => {
                let inserted_rows = conn
                    .execute(
                    "INSERT OR IGNORE INTO requests (request_id, session_id, timestamp, model, \
                     input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, \
                     cost_dollars, cost_source, trusted_for_budget_enforcement, duration_ms, tool_calls, cache_event) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                    rusqlite::params![
                        &request_id,
                        &session_id,
                        timestamp,
                        model,
                        input_tokens,
                        output_tokens,
                        cache_read_tokens,
                        cache_creation_tokens,
                        cost_dollars,
                        cost_source,
                        trusted_for_budget_enforcement as i32,
                        duration_ms,
                        tool_calls_json,
                        cache_event,
                    ],
                )
                    .unwrap_or(0);

                if inserted_rows == 0 {
                    continue;
                }
                // Individual tool_calls rows.
                for (name, ts) in &tool_calls_list {
                    let _ = conn.execute(
                        "INSERT INTO tool_calls (request_id, timestamp, tool_name) VALUES (?1,?2,?3)",
                        rusqlite::params![&request_id, ts, name],
                    );
                }
                // Update session totals.
                let _ = conn.execute(
                    "UPDATE sessions SET \
                     total_input_tokens = total_input_tokens + ?2, \
                     total_output_tokens = total_output_tokens + ?3, \
                     total_cache_read_tokens = total_cache_read_tokens + ?4, \
                     total_cache_creation_tokens = total_cache_creation_tokens + ?5, \
                     total_cost_dollars = total_cost_dollars + ?6, \
                     request_count = request_count + 1, \
                     ended_at = ?7 \
                     WHERE session_id = ?1",
                    rusqlite::params![
                        session_id,
                        input_tokens,
                        output_tokens,
                        cache_read_tokens,
                        cache_creation_tokens,
                        cost_dollars,
                        timestamp,
                    ],
                );
            }
            DbCommand::WriteTurnSnapshot {
                session_id,
                turn_number,
                timestamp,
                input_tokens,
                cache_read_tokens,
                cache_creation_tokens,
                output_tokens,
                ttft_ms,
                tool_calls_json,
                tool_failures,
                gap_from_prev_secs,
                context_utilization,
                context_window_tokens,
                frustration_signals,
                requested_model,
                actual_model,
                response_summary,
            } => {
                let _ = conn.execute(
                    "INSERT INTO turn_snapshots (session_id, turn_number, timestamp, \
                     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, \
                     ttft_ms, tool_calls, tool_failures, gap_from_prev_secs, \
                     context_utilization, context_window_tokens, frustration_signals, \
                     requested_model, actual_model, response_summary) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
                    rusqlite::params![
                        session_id,
                        turn_number,
                        timestamp,
                        input_tokens,
                        cache_read_tokens,
                        cache_creation_tokens,
                        output_tokens,
                        ttft_ms,
                        tool_calls_json,
                        tool_failures,
                        gap_from_prev_secs,
                        context_utilization,
                        context_window_tokens,
                        frustration_signals,
                        requested_model,
                        actual_model,
                        response_summary,
                    ],
                );
            }
            DbCommand::WriteDiagnosis {
                session_id,
                completed_at,
                outcome,
                total_turns,
                total_cost,
                cache_hit_ratio,
                degraded,
                degradation_turn,
                causes_json,
                advice_json,
            } => {
                let _ = conn.execute(
                    "INSERT OR REPLACE INTO session_diagnoses (session_id, completed_at, \
                     outcome, total_turns, total_cost, cache_hit_ratio, degraded, \
                     degradation_turn, causes_json, advice_json) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                    rusqlite::params![
                        session_id,
                        completed_at,
                        outcome,
                        total_turns,
                        total_cost,
                        cache_hit_ratio,
                        degraded as i32,
                        degradation_turn,
                        causes_json,
                        advice_json,
                    ],
                );
            }
            DbCommand::WriteToolOutcome {
                session_id,
                turn_number,
                timestamp,
                tool_name,
                input_summary,
                outcome,
                duration_ms,
            } => {
                let _ = conn.execute(
                    "INSERT INTO tool_outcomes (session_id, turn_number, timestamp, \
                     tool_name, input_summary, outcome, duration_ms) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    rusqlite::params![
                        session_id,
                        turn_number,
                        timestamp,
                        tool_name,
                        input_summary,
                        outcome,
                        duration_ms,
                    ],
                );
            }
            DbCommand::WriteSkillEvent {
                session_id,
                timestamp,
                skill_name,
                event_type,
                confidence,
                source,
                detail,
            } => {
                let _ = conn.execute(
                    "INSERT INTO skill_events (session_id, timestamp, skill_name, event_type, \
                     confidence, source, detail) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    rusqlite::params![
                        session_id,
                        timestamp,
                        skill_name,
                        event_type,
                        confidence,
                        source,
                        detail,
                    ],
                );
            }
            DbCommand::WriteMcpEvent {
                session_id,
                timestamp,
                server,
                tool,
                event_type,
                source,
                detail,
            } => {
                let _ = conn.execute(
                    "INSERT INTO mcp_events (session_id, timestamp, server, tool, event_type, \
                     source, detail) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    rusqlite::params![
                        session_id, timestamp, server, tool, event_type, source, detail,
                    ],
                );
            }
            DbCommand::WriteRecall {
                session_id,
                initial_prompt,
                final_response_summary,
            } => {
                match conn.execute(
                    "INSERT OR IGNORE INTO session_recall (session_id, initial_prompt, final_response_summary) \
                     VALUES (?1,?2,?3)",
                    rusqlite::params![&session_id, &initial_prompt, &final_response_summary],
                ) {
                    Ok(1) => {}
                    Ok(_) => {
                        warn!(
                            session_id,
                            "session recall row already existed; ignoring duplicate insert"
                        );
                    }
                    Err(err) => {
                        warn!(
                            session_id,
                            error = %err,
                            "failed to persist session recall"
                        );
                    }
                }
            }
            DbCommand::WriteBillingReconciliation {
                session_id,
                imported_at,
                source,
                billed_cost_dollars,
                response_tx,
            } => {
                let result = match conn
                    .query_row(
                        "SELECT 1 FROM sessions WHERE session_id = ?1 LIMIT 1",
                        rusqlite::params![&session_id],
                        |_| Ok(()),
                    )
                    .optional()
                {
                    Ok(Some(())) => match conn.execute(
                        "INSERT INTO billing_reconciliations (session_id, imported_at, source, billed_cost_dollars) \
                         VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![
                            &session_id,
                            &imported_at,
                            &source,
                            billed_cost_dollars
                        ],
                    ) {
                        Ok(1) => Ok(()),
                        Ok(rows) => Err(BillingReconciliationWriteError::Sqlite(format!(
                            "expected 1 inserted row, got {rows}"
                        ))),
                        Err(err) => Err(BillingReconciliationWriteError::Sqlite(err.to_string())),
                    },
                    Ok(None) => Err(BillingReconciliationWriteError::UnknownSession(
                        session_id.clone(),
                    )),
                    Err(err) => Err(BillingReconciliationWriteError::Sqlite(err.to_string())),
                };

                if let Err(err) = &result {
                    warn!(
                        session_id = %session_id,
                        error = ?err,
                        "failed to persist billing reconciliation"
                    );
                }

                let _ = response_tx.send(result);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct EstimatedAggregate {
    estimated_cost_dollars: f64,
    cost_source: String,
    trusted_for_budget_enforcement: bool,
}

#[derive(Clone, Debug)]
struct LatestBillingReconciliation {
    billed_cost_dollars: f64,
    source: String,
    imported_at: String,
}

#[derive(Clone, Debug)]
struct SummaryWindowData {
    sessions: i64,
    estimated_cost_dollars: f64,
    cost_source: String,
    trusted_for_budget_enforcement: bool,
    billed_cost_dollars: Option<f64>,
    billed_sessions: u64,
    cache_hit_ratio: f64,
}

struct CostAccumulator {
    total_cost_dollars: f64,
    sources: HashSet<String>,
    trusted_for_budget_enforcement: bool,
    saw_rows: bool,
}

impl CostAccumulator {
    fn new() -> Self {
        Self {
            total_cost_dollars: 0.0,
            sources: HashSet::new(),
            trusted_for_budget_enforcement: true,
            saw_rows: false,
        }
    }

    fn record(&mut self, breakdown: pricing::EstimatedCostBreakdown) {
        self.total_cost_dollars += breakdown.total_cost_dollars;
        self.sources.insert(breakdown.cost_source);
        self.trusted_for_budget_enforcement &= breakdown.trusted_for_budget_enforcement;
        self.saw_rows = true;
    }

    fn record_persisted(
        &mut self,
        total_cost_dollars: f64,
        cost_source: Option<String>,
        trusted_for_budget_enforcement: Option<bool>,
    ) {
        self.total_cost_dollars += total_cost_dollars.max(0.0);
        self.sources
            .insert(cost_source.unwrap_or_else(pricing::active_catalog_source));
        self.trusted_for_budget_enforcement &=
            trusted_for_budget_enforcement.unwrap_or_else(pricing::trusted_for_budget_enforcement);
        self.saw_rows = true;
    }

    fn finish(self) -> EstimatedAggregate {
        EstimatedAggregate {
            estimated_cost_dollars: self.total_cost_dollars.max(0.0),
            cost_source: pricing::summarize_cost_sources(&self.sources),
            trusted_for_budget_enforcement: if self.saw_rows {
                self.trusted_for_budget_enforcement
            } else {
                pricing::trusted_for_budget_enforcement()
            },
        }
    }
}

fn rounded_estimated_cost_dollars(amount: f64) -> f64 {
    (amount * 100.0).round() / 100.0
}

fn rounded_billed_cost_dollars(amount: Option<f64>) -> Option<f64> {
    amount.map(rounded_estimated_cost_dollars)
}

fn load_latest_billing_reconciliations(
    conn: &Connection,
    session_ids: &[String],
) -> rusqlite::Result<HashMap<String, LatestBillingReconciliation>> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders = vec!["?"; session_ids.len()].join(",");
    let sql = format!(
        "SELECT session_id, imported_at, source, billed_cost_dollars \
         FROM billing_reconciliations \
         WHERE session_id IN ({placeholders}) \
         ORDER BY imported_at DESC, id DESC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(session_ids.iter()), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, f64>(3)?,
        ))
    })?;

    let mut latest = HashMap::new();
    for row in rows {
        let (session_id, imported_at, source, billed_cost_dollars) = row?;
        latest
            .entry(session_id)
            .or_insert(LatestBillingReconciliation {
                billed_cost_dollars,
                source,
                imported_at,
            });
    }

    Ok(latest)
}

fn compute_estimated_costs_for_sessions(
    conn: &Connection,
    session_ids: &[String],
) -> rusqlite::Result<HashMap<String, EstimatedAggregate>> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders = vec!["?"; session_ids.len()].join(",");
    let sql = format!(
        "SELECT session_id, model, input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, \
         cost_dollars, cost_source, trusted_for_budget_enforcement \
         FROM requests WHERE session_id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(session_ids.iter()), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, Option<f64>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<i64>>(8)?,
        ))
    })?;

    let mut accumulators: HashMap<String, CostAccumulator> = HashMap::new();
    for row in rows {
        let (
            session_id,
            model,
            input,
            output,
            cache_read,
            cache_create,
            stored_cost,
            stored_source,
            stored_trusted,
        ) = row?;
        let accumulator = accumulators
            .entry(session_id)
            .or_insert_with(CostAccumulator::new);
        if let Some(cost) = stored_cost {
            accumulator.record_persisted(cost, stored_source, stored_trusted.map(|n| n != 0));
        } else {
            accumulator.record(pricing::estimate_cost_dollars(
                &model,
                input.max(0) as u64,
                output.max(0) as u64,
                cache_read.max(0) as u64,
                cache_create.max(0) as u64,
            ));
        }
    }

    let mut estimates = HashMap::new();
    for session_id in session_ids {
        let aggregate = accumulators
            .remove(session_id)
            .map(CostAccumulator::finish)
            .unwrap_or_else(|| CostAccumulator::new().finish());
        estimates.insert(session_id.clone(), aggregate);
    }

    Ok(estimates)
}

fn query_summary(conn: &Connection, since: &str) -> rusqlite::Result<SummaryWindowData> {
    let mut stmt = conn.prepare(
        "SELECT r.session_id, r.model, r.input_tokens, r.output_tokens, r.cache_read_tokens, \
                r.cache_creation_tokens, r.cost_dollars, r.cost_source, \
                r.trusted_for_budget_enforcement, CASE WHEN s.session_id IS NULL THEN 0 ELSE 1 END \
         FROM requests r \
         LEFT JOIN sessions s ON s.session_id = r.session_id \
         WHERE r.timestamp >= ?1",
    )?;
    let rows = stmt.query_map(rusqlite::params![since], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, Option<f64>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, i64>(9)? != 0,
        ))
    })?;

    let mut session_ids = HashSet::new();
    let mut cost_accumulator = CostAccumulator::new();
    let mut cache_read_tokens: i64 = 0;
    let mut cache_total_tokens: i64 = 0;

    for row in rows {
        let (
            session_id,
            model,
            input,
            output,
            cache_read,
            cache_create,
            stored_cost,
            stored_source,
            stored_trusted,
            is_real_session,
        ) = row?;
        if is_real_session {
            session_ids.insert(session_id);
        }
        cache_read_tokens += cache_read.max(0);
        cache_total_tokens += (cache_read + cache_create).max(0);
        if let Some(cost) = stored_cost {
            cost_accumulator.record_persisted(cost, stored_source, stored_trusted.map(|n| n != 0));
        } else {
            cost_accumulator.record(pricing::estimate_cost_dollars(
                &model,
                input.max(0) as u64,
                output.max(0) as u64,
                cache_read.max(0) as u64,
                cache_create.max(0) as u64,
            ));
        }
    }

    let billing_records = load_latest_billing_reconciliations(
        conn,
        &session_ids.iter().cloned().collect::<Vec<_>>(),
    )?;
    let billed_sessions = billing_records.len() as u64;
    let billed_cost_dollars = if billed_sessions > 0 {
        Some(
            billing_records
                .values()
                .map(|record| record.billed_cost_dollars)
                .sum::<f64>(),
        )
    } else {
        None
    };

    let cache_hit_ratio = if cache_total_tokens > 0 {
        cache_read_tokens as f64 / cache_total_tokens as f64
    } else {
        0.0
    };

    let estimate = cost_accumulator.finish();
    Ok(SummaryWindowData {
        sessions: session_ids.len() as i64,
        estimated_cost_dollars: estimate.estimated_cost_dollars,
        cost_source: estimate.cost_source,
        trusted_for_budget_enforcement: estimate.trusted_for_budget_enforcement,
        billed_cost_dollars,
        billed_sessions,
        cache_hit_ratio,
    })
}

fn summary_window_json(summary: &SummaryWindowData) -> Value {
    serde_json::json!({
        "sessions": summary.sessions,
        "estimated_cost_dollars": rounded_estimated_cost_dollars(summary.estimated_cost_dollars),
        "cost_source": summary.cost_source.clone(),
        "trusted_for_budget_enforcement": summary.trusted_for_budget_enforcement,
        "billed_cost_dollars": rounded_billed_cost_dollars(summary.billed_cost_dollars),
        "billed_sessions": summary.billed_sessions,
        "cache_hit_ratio": (summary.cache_hit_ratio * 100.0).round() / 100.0,
    })
}

fn build_summary_response_json(
    today: &SummaryWindowData,
    week: &SummaryWindowData,
    month: &SummaryWindowData,
) -> Value {
    serde_json::json!({
        "cost_source": pricing::active_catalog_source(),
        "trusted_for_budget_enforcement": pricing::trusted_for_budget_enforcement(),
        "today": summary_window_json(today),
        "this_week": summary_window_json(week),
        "this_month": summary_window_json(month),
    })
}

fn build_diagnosis_response_json(
    session_id: String,
    completed_at: String,
    outcome: String,
    total_turns: i64,
    estimated_total_cost_dollars: f64,
    cost_source: String,
    trusted_for_budget_enforcement: bool,
    billed_cost_dollars: Option<f64>,
    billing_source: Option<String>,
    billing_imported_at: Option<String>,
    cache_hit_ratio: f64,
    degraded: bool,
    degradation_turn: Option<i64>,
    causes: Value,
    advice: Value,
) -> Value {
    serde_json::json!({
        "session_id": session_id,
        "completed_at": completed_at,
        "outcome": outcome,
        "total_turns": total_turns,
        "estimated_total_cost_dollars": estimated_total_cost_dollars,
        "cost_source": cost_source,
        "trusted_for_budget_enforcement": trusted_for_budget_enforcement,
        "billed_cost_dollars": rounded_billed_cost_dollars(billed_cost_dollars),
        "billing_source": billing_source,
        "billing_imported_at": billing_imported_at,
        "cache_hit_ratio": cache_hit_ratio,
        "degraded": degraded,
        "degradation_turn": if degraded { degradation_turn } else { None },
        "causes": causes,
        "advice": advice,
    })
}

fn build_session_summary_json(
    session_id: String,
    started_at: Option<String>,
    outcome: String,
    degraded: bool,
    total_turns: i64,
    estimated_total_cost_dollars: f64,
    cost_source: String,
    trusted_for_budget_enforcement: bool,
    billed_cost_dollars: Option<f64>,
    billing_source: Option<String>,
    billing_imported_at: Option<String>,
    primary_cause: String,
    cache_hit_ratio: f64,
    model: Option<String>,
) -> Value {
    serde_json::json!({
        "session_id": session_id,
        "started_at": started_at,
        "outcome": outcome,
        "degraded": degraded,
        "total_turns": total_turns,
        "estimated_total_cost_dollars": estimated_total_cost_dollars,
        "cost_source": cost_source,
        "trusted_for_budget_enforcement": trusted_for_budget_enforcement,
        "billed_cost_dollars": rounded_billed_cost_dollars(billed_cost_dollars),
        "billing_source": billing_source,
        "billing_imported_at": billing_imported_at,
        "primary_cause": primary_cause,
        "cache_hit_ratio": cache_hit_ratio,
        "model": model,
    })
}

fn build_sessions_response_json(sessions: Vec<Value>) -> Value {
    serde_json::json!({
        "cost_source": pricing::active_catalog_source(),
        "trusted_for_budget_enforcement": pricing::trusted_for_budget_enforcement(),
        "sessions": sessions,
    })
}

#[derive(Deserialize)]
struct StoredDegradationCause {
    cause_type: String,
}

fn query_historical_window_from_db(
    conn: &Connection,
    since: &str,
    window: &'static str,
) -> rusqlite::Result<metrics::HistoricalWindowMetrics> {
    let mut stmt = conn.prepare(
        "SELECT model, input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, \
         cost_dollars, cost_source, trusted_for_budget_enforcement, cache_event \
         FROM requests WHERE timestamp >= ?1",
    )?;
    let rows = stmt.query_map(rusqlite::params![since], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Option<f64>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<String>>(8)?,
        ))
    })?;

    let mut cost_accumulator = CostAccumulator::new();
    let mut estimated_spend_dollars_by_model = std::collections::BTreeMap::new();
    let mut estimated_cache_waste_dollars_by_model = std::collections::BTreeMap::new();
    let mut cache_read_tokens: i64 = 0;
    let mut cache_total_tokens: i64 = 0;
    let mut cache_events = std::collections::BTreeMap::new();
    for row in rows {
        let (
            model,
            input,
            output,
            cache_read,
            cache_create,
            stored_cost,
            stored_source,
            stored_trusted,
            cache_event,
        ) = row?;
        let row_cost = if let Some(cost) = stored_cost {
            cost_accumulator.record_persisted(cost, stored_source, stored_trusted.map(|n| n != 0));
            cost
        } else {
            let estimated = pricing::estimate_cost_dollars(
                &model,
                input.max(0) as u64,
                output.max(0) as u64,
                cache_read.max(0) as u64,
                cache_create.max(0) as u64,
            );
            let total_cost = estimated.total_cost_dollars;
            cost_accumulator.record(estimated);
            total_cost
        };
        let model_label = metrics::historical_model_label(&model);
        if let Some(cache_event_label) = cache_event
            .as_deref()
            .and_then(metrics::historical_cache_event_label)
        {
            *cache_events.entry(cache_event_label).or_insert(0) += 1;
            let waste =
                pricing::estimate_cache_rebuild_waste_dollars(&model, cache_create.max(0) as u64)
                    .total_cost_dollars;
            *estimated_cache_waste_dollars_by_model
                .entry(model_label)
                .or_insert(0.0) += waste.max(0.0);
        }
        *estimated_spend_dollars_by_model
            .entry(model_label)
            .or_insert(0.0) += row_cost.max(0.0);
        cache_read_tokens += cache_read.max(0);
        cache_total_tokens += (cache_read + cache_create).max(0);
    }

    let cache_hit_ratio = if cache_total_tokens > 0 {
        cache_read_tokens.max(0) as f64 / cache_total_tokens.max(0) as f64
    } else {
        0.0
    };

    let mut sessions = 0u64;
    let mut degraded_sessions = 0u64;
    let mut degraded_causes = std::collections::BTreeMap::new();

    let mut stmt = conn.prepare(
        "SELECT degraded, causes_json \
         FROM session_diagnoses WHERE completed_at >= ?1",
    )?;

    let rows = stmt.query_map(rusqlite::params![since], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        let (degraded, causes_json) = row?;
        sessions += 1;
        if degraded == 0 {
            continue;
        }

        degraded_sessions += 1;
        let causes: Vec<StoredDegradationCause> =
            serde_json::from_str(&causes_json).unwrap_or_default();
        for cause in causes {
            if let Some(label) = metrics::historical_cause_label(&cause.cause_type) {
                *degraded_causes.entry(label).or_insert(0) += 1;
            }
        }
    }

    let degraded_session_ratio = if sessions > 0 {
        degraded_sessions as f64 / sessions as f64
    } else {
        0.0
    };

    let mut model_fallbacks = std::collections::BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT requested_model, actual_model \
         FROM turn_snapshots WHERE timestamp >= ?1",
    )?;
    let rows = stmt.query_map(rusqlite::params![since], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, Option<String>>(1)?,
        ))
    })?;
    for row in rows {
        let (requested_model, actual_model) = row?;
        let Some(requested_model) = requested_model else {
            continue;
        };
        let Some(actual_model) = actual_model else {
            continue;
        };
        let requested_label = metrics::historical_model_label(&requested_model);
        let actual_label = metrics::historical_model_label(&actual_model);
        if requested_label != actual_label {
            *model_fallbacks
                .entry((requested_label, actual_label))
                .or_insert(0) += 1;
        }
    }

    let mut tool_failures_by_tool = std::collections::BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT tool_name, COUNT(*) \
         FROM tool_outcomes \
         WHERE timestamp >= ?1 AND outcome = 'error' \
         GROUP BY tool_name",
    )?;
    let rows = stmt.query_map(rusqlite::params![since], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (tool_name, count) = row?;
        let tool_label = metrics::historical_tool_label(&tool_name);
        *tool_failures_by_tool.entry(tool_label).or_insert(0) += count.max(0) as u64;
    }

    let mut avg_estimated_session_cost_dollars_by_model = std::collections::BTreeMap::new();
    let mut model_totals = std::collections::BTreeMap::<&'static str, (f64, u64)>::new();
    let mut stmt = conn.prepare(
        "SELECT s.model, d.total_cost \
         FROM session_diagnoses d \
         LEFT JOIN sessions s ON s.session_id = d.session_id \
         WHERE d.completed_at >= ?1",
    )?;
    let rows = stmt.query_map(rusqlite::params![since], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, Option<f64>>(1)?,
        ))
    })?;
    for row in rows {
        let (model, total_cost) = row?;
        let label = model
            .as_deref()
            .map(metrics::historical_model_label)
            .unwrap_or("other");
        let entry = model_totals.entry(label).or_insert((0.0, 0));
        entry.0 += total_cost.unwrap_or(0.0).max(0.0);
        entry.1 += 1;
    }
    for (label, (total_cost, count)) in model_totals {
        let avg_cost = if count > 0 {
            total_cost / count as f64
        } else {
            0.0
        };
        avg_estimated_session_cost_dollars_by_model.insert(label, avg_cost);
    }

    Ok(metrics::HistoricalWindowMetrics {
        window,
        sessions,
        estimated_spend_dollars: cost_accumulator.finish().estimated_cost_dollars,
        estimated_spend_dollars_by_model,
        estimated_cache_waste_dollars_by_model,
        avg_estimated_session_cost_dollars_by_model,
        cache_hit_ratio,
        cache_events,
        degraded_sessions,
        degraded_session_ratio,
        degraded_causes,
        model_fallbacks,
        tool_failures_by_tool,
    })
}

fn query_historical_metrics(
    conn: &Connection,
    now_epoch: u64,
) -> rusqlite::Result<Vec<metrics::HistoricalWindowMetrics>> {
    let mut windows = Vec::with_capacity(metrics::HISTORY_WINDOWS.len());
    for (window, days) in metrics::HISTORY_WINDOWS {
        let since = epoch_to_iso8601(now_epoch.saturating_sub(days * 86_400));
        windows.push(query_historical_window_from_db(conn, &since, window)?);
    }
    Ok(windows)
}

// ---------------------------------------------------------------------------
// Cache intelligence tracker
// ---------------------------------------------------------------------------
struct CacheTracker {
    consecutive_misses: u64,
    last_request_time: Option<Instant>,
}

impl CacheTracker {
    fn new() -> Self {
        Self {
            consecutive_misses: 0,
            last_request_time: None,
        }
    }
}

static CACHE_TRACKERS: LazyLock<DashMap<String, CacheTracker>> = LazyLock::new(DashMap::new);
const CACHE_TTL_SECS: u64 = 300;
const THRASH_THRESHOLD: u64 = 3;

/// Returns cache event type: "hit", "partial", "cold_start", "miss_ttl",
/// "miss_thrash", or "none".
fn update_cache_intelligence(session_id: &str, cache_read: u64, cache_create: u64) -> &'static str {
    let mut t = CACHE_TRACKERS
        .entry(session_id.to_string())
        .or_insert_with(CacheTracker::new);
    let now = Instant::now();

    let is_full_miss = cache_create > 0 && cache_read == 0;
    let is_first = t.last_request_time.is_none();
    let gap = t
        .last_request_time
        .map(|p| now.duration_since(p).as_secs())
        .unwrap_or(0);
    let is_ttl = is_full_miss && gap > CACHE_TTL_SECS;

    let event = if cache_read == 0 && cache_create == 0 {
        "none"
    } else if is_full_miss {
        if is_first {
            t.consecutive_misses = 0;
            "cold_start"
        } else if is_ttl {
            t.consecutive_misses = 0;
            info!(gap, cache_create, "cache miss attributed to TTL expiry");
            "miss_ttl"
        } else {
            t.consecutive_misses += 1;
            if t.consecutive_misses >= THRASH_THRESHOLD {
                warn!(
                    session_id,
                    consecutive_misses = t.consecutive_misses,
                    "cache thrashing detected within a single session"
                );
            }
            "miss_thrash"
        }
    } else if cache_create > 0 {
        t.consecutive_misses = 0;
        "partial"
    } else {
        t.consecutive_misses = 0;
        "hit"
    };
    t.last_request_time = Some(now);
    event
}

// ---------------------------------------------------------------------------
// Estimated pricing
// ---------------------------------------------------------------------------
pub fn token_cost(tokens: u64, price_per_mtok: f64) -> f64 {
    pricing::token_cost(tokens, price_per_mtok)
}

// ---------------------------------------------------------------------------
// SSE response accumulator
// ---------------------------------------------------------------------------
// Frustration signal patterns — compiled once at startup.
struct FrustrationPattern {
    regex: regex::Regex,
    signal_type: &'static str,
}

static FRUSTRATION_PATTERNS: LazyLock<Vec<FrustrationPattern>> = LazyLock::new(|| {
    vec![
        FrustrationPattern {
            regex: regex::Regex::new(r"(?i)burning too many tokens").unwrap(),
            signal_type: "token_pressure",
        },
        FrustrationPattern {
            regex: regex::Regex::new(r"(?i)this has taken too many turns").unwrap(),
            signal_type: "early_stop",
        },
        FrustrationPattern {
            regex: regex::Regex::new(r"(?i)let me wrap up").unwrap(),
            signal_type: "early_stop",
        },
        FrustrationPattern {
            regex: regex::Regex::new(r"(?i)simplest (?:fix|solution)").unwrap(),
            signal_type: "simplest_fix",
        },
        FrustrationPattern {
            regex: regex::Regex::new(r"(?i)context (?:is|getting) (?:large|full)").unwrap(),
            signal_type: "context_pressure",
        },
    ]
});

/// Track what type of content block we're currently inside.
#[derive(Clone, PartialEq)]
enum ContentBlockType {
    Text,
    ToolUse,
    Other,
}

struct ResponseAccumulator {
    sse_buffer: Vec<u8>,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    tool_calls: Vec<String>,
    is_sse: bool,
    http_status: u32,
    error_body: Vec<u8>,
    /// Model Anthropic reported in `message_start` — may differ from what the
    /// client requested when a quota fallback silently routes the request.
    response_model: Option<String>,
    /// Concatenated assistant text from this response, used for compact
    /// session-end recall summaries.
    response_text: String,
    // Phase 1: text delta accumulation for frustration detection.
    text_buffer: String,
    // Phase 1: tool input JSON accumulation for summary extraction.
    tool_input_buffer: String,
    // Track current content block type.
    current_block_type: ContentBlockType,
    // Phase 2: count frustration signals detected in this response.
    frustration_signal_count: u32,
    // Deferred watch events — broadcast after session start in finalize_response.
    deferred_watch_events: Vec<watch::WatchEvent>,
}

impl ResponseAccumulator {
    fn new() -> Self {
        Self {
            sse_buffer: Vec::with_capacity(4096),
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            tool_calls: Vec::new(),
            is_sse: false,
            http_status: 0,
            error_body: Vec::new(),
            response_model: None,
            response_text: String::new(),
            text_buffer: String::new(),
            tool_input_buffer: String::new(),
            current_block_type: ContentBlockType::Other,
            frustration_signal_count: 0,
            deferred_watch_events: Vec::new(),
        }
    }

    fn process_chunk(&mut self, chunk: &[u8]) {
        self.sse_buffer.extend_from_slice(chunk);
        loop {
            let Some(pos) = self.sse_buffer.iter().position(|&b| b == b'\n') else {
                break;
            };
            let line = String::from_utf8_lossy(&self.sse_buffer[..pos]);
            let line = line.trim_end_matches('\r');
            if let Some(data) = line.strip_prefix("data: ") {
                if data != "[DONE]" {
                    if let Ok(v) = serde_json::from_str::<Value>(data) {
                        self.process_event(&v);
                    }
                }
            }
            self.sse_buffer.drain(..=pos);
        }
    }

    fn process_event(&mut self, v: &Value) {
        match v.get("type").and_then(|t| t.as_str()) {
            Some("message_start") => {
                if let Some(u) = v.pointer("/message/usage") {
                    self.input_tokens = u.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                    self.cache_read_tokens = u
                        .get("cache_read_input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    self.cache_creation_tokens = u
                        .get("cache_creation_input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                }
                // Capture the model Anthropic actually routed to. If it differs
                // from what the user requested we surface a silent-fallback
                // alert in `finalize_response`.
                if let Some(m) = v.pointer("/message/model").and_then(|m| m.as_str()) {
                    self.response_model = Some(m.to_string());
                }
            }
            Some("content_block_start") => {
                if let Some(b) = v.get("content_block") {
                    let block_type = b.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    match block_type {
                        "tool_use" => {
                            self.current_block_type = ContentBlockType::ToolUse;
                            self.tool_input_buffer.clear();
                            if let Some(n) = b.get("name").and_then(|n| n.as_str()) {
                                self.tool_calls.push(n.to_string());
                            }
                        }
                        "text" => {
                            self.current_block_type = ContentBlockType::Text;
                            self.text_buffer.clear();
                        }
                        _ => {
                            self.current_block_type = ContentBlockType::Other;
                        }
                    }
                }
            }
            Some("content_block_delta") => {
                if let Some(delta) = v.get("delta") {
                    let delta_type = delta.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    match delta_type {
                        "text_delta" => {
                            if let Some(text) = delta.get("text").and_then(|t| t.as_str()) {
                                self.text_buffer.push_str(text);
                            }
                        }
                        "input_json_delta" => {
                            if let Some(partial) =
                                delta.get("partial_json").and_then(|p| p.as_str())
                            {
                                self.tool_input_buffer.push_str(partial);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Some("content_block_stop") => {
                match self.current_block_type {
                    ContentBlockType::Text => {
                        // Run frustration detection on completed text block.
                        for pattern in FRUSTRATION_PATTERNS.iter() {
                            if pattern.regex.is_match(&self.text_buffer) {
                                self.frustration_signal_count += 1;
                                self.deferred_watch_events.push(
                                    watch::WatchEvent::FrustrationSignal {
                                        session_id: String::new(), // Filled in finalize_response.
                                        signal_type: pattern.signal_type.to_string(),
                                    },
                                );
                            }
                        }
                        let trimmed = self.text_buffer.trim();
                        if !trimmed.is_empty() {
                            if !self.response_text.is_empty() {
                                self.response_text.push('\n');
                            }
                            self.response_text.push_str(trimmed);
                        }
                        self.text_buffer.clear();
                    }
                    ContentBlockType::ToolUse => {
                        // Defer ToolUse event — broadcast after session start in finalize_response.
                        if let Some(tool_name) = self.tool_calls.last() {
                            let summary =
                                watch::extract_summary(tool_name, &self.tool_input_buffer);
                            self.deferred_watch_events.push(watch::WatchEvent::ToolUse {
                                session_id: String::new(), // Filled in finalize_response.
                                timestamp: now_iso8601(),
                                tool_name: tool_name.clone(),
                                summary,
                            });
                            if tool_name.eq_ignore_ascii_case("Skill") {
                                if let Some(skill_name) =
                                    skill_name_from_tool_input_json(&self.tool_input_buffer)
                                {
                                    self.deferred_watch_events.push(
                                        watch::WatchEvent::SkillEvent {
                                            session_id: String::new(), // Filled in finalize_response.
                                            timestamp: now_iso8601(),
                                            skill_name,
                                            event_type: "fired".to_string(),
                                            source: "proxy".to_string(),
                                            confidence: 0.65,
                                            detail: Some(
                                                "inferred from Skill tool_use".to_string(),
                                            ),
                                        },
                                    );
                                }
                            }
                        }
                        self.tool_input_buffer.clear();
                    }
                    ContentBlockType::Other => {}
                }
                self.current_block_type = ContentBlockType::Other;
            }
            Some("message_delta") => {
                if let Some(u) = v.get("usage") {
                    if let Some(o) = u.get("output_tokens").and_then(|v| v.as_u64()) {
                        self.output_tokens = o;
                    }
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Finalize: metrics + DB persistence
// ---------------------------------------------------------------------------
/// Relaxed model equivalence check: "claude-opus-4-7" and
/// "claude-opus-4-7-20260410" should be considered the same model. We treat
/// two strings as matching if either contains the other as a prefix.
fn normalize_model_for_match(model: &str) -> &str {
    model.strip_suffix("[1m]").unwrap_or(model)
}

pub(crate) fn model_matches(requested: &str, actual: &str) -> bool {
    let requested = normalize_model_for_match(requested);
    let actual = normalize_model_for_match(actual);
    requested == actual || requested.starts_with(actual) || actual.starts_with(requested)
}

/// Linear extrapolation: estimate how many turns remain before Claude Code
/// auto-compacts at ~85% of the resolved context window.
pub(crate) fn project_turns_until_compaction(
    prev_fill_percent: f64,
    current_fill_percent: f64,
) -> Option<u32> {
    const COMPACT_THRESHOLD: f64 = 85.0;
    if current_fill_percent >= COMPACT_THRESHOLD {
        return Some(0);
    }
    let delta = current_fill_percent - prev_fill_percent;
    if delta <= 0.0 {
        return None;
    }
    let remaining = COMPACT_THRESHOLD - current_fill_percent;
    Some((remaining / delta).ceil().max(1.0) as u32)
}

/// Linear extrapolation: look at the last two turns' fill % and project how
/// many turns until we cross 85%. Returns None if we don't have two turns yet
/// or the trajectory is flat / decreasing.
fn project_turns_to_compact(session_id: &str, current_fill_percent: f64) -> Option<u32> {
    let turns = diagnosis::SESSION_TURNS.get(session_id)?;
    let prev = turns.last()?;
    let prev_fill = context_fill_percent(
        prev.input_tokens as u64,
        prev.cache_read_tokens as u64,
        prev.cache_creation_tokens as u64,
        prev.context_window_tokens,
    );
    project_turns_until_compaction(prev_fill, current_fill_percent)
}

fn finalize_response(
    acc: &ResponseAccumulator,
    request_id: &str,
    model: &str,
    started_at: &Instant,
    tool_results: &[ParsedToolResult],
    sys_prompt_hash: u64,
    working_dir_str: &str,
    user_prompt_excerpt: &str,
    context_window_tokens: u64,
) {
    let duration = started_at.elapsed();
    let duration_secs = duration.as_secs_f64();
    let duration_ms = duration.as_millis() as u64;
    let tool_failures = tool_results.iter().filter(|result| result.is_error).count() as u32;

    // Look up or create the per-terminal session by system prompt hash.
    // Use get_mut first (cheap read lock), then entry() only if truly new.
    let session_id = if let Some(mut existing) = diagnosis::SESSIONS.get_mut(&sys_prompt_hash) {
        existing.last_activity = Instant::now();
        existing.cache_warning_sent = false;
        existing.session_id.clone()
    } else {
        // Derive display name BEFORE taking the entry lock to avoid deadlock
        // (derive_display_name iterates SESSIONS to check for name collisions).
        let name = derive_display_name(working_dir_str, model, sys_prompt_hash);
        let ts = now_epoch_secs();
        let hash_suffix = &format!("{:x}", sys_prompt_hash)[..4];
        let sid = format!("session_{ts}_{hash_suffix}");
        let sid_clone = sid.clone();
        diagnosis::SESSIONS.insert(
            sys_prompt_hash,
            diagnosis::SessionState {
                session_id: sid,
                display_name: name,
                model: model.to_string(),
                initial_prompt: if user_prompt_excerpt.is_empty() {
                    None
                } else {
                    Some(user_prompt_excerpt.to_string())
                },
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: false,
                cache_warning_sent: false,
            },
        );
        sid_clone
    };

    let billing_model = acc.response_model.as_deref().unwrap_or(model);
    let estimated_cost = pricing::estimate_cost_dollars(
        billing_model,
        acc.input_tokens,
        acc.output_tokens,
        acc.cache_read_tokens,
        acc.cache_creation_tokens,
    );
    let total_cost = estimated_cost.total_cost_dollars;

    metrics::record_request(
        billing_model,
        acc.input_tokens,
        acc.output_tokens,
        acc.cache_read_tokens,
        acc.cache_creation_tokens,
        total_cost,
        duration_secs,
    );

    // Skip requests that can't be attributed to a terminal:
    // - Haiku title-generation requests
    // - Any request with empty system prompt (e.g., Opus/Sonnet title-generation requests
    //   which have system_prompt_length=0 and no working directory)
    // These still get budget tracking, DB recording, and logging below.
    let is_title_request = model.contains("haiku") || working_dir_str.is_empty();

    let cache_event = if is_title_request {
        "none"
    } else {
        update_cache_intelligence(
            &session_id,
            acc.cache_read_tokens,
            acc.cache_creation_tokens,
        )
    };

    // Phase 7: update budget state.
    {
        let mut runtime = RUNTIME_STATE.lock().unwrap();
        runtime.total_spend += total_cost;
        runtime.total_tokens += acc.input_tokens
            + acc.output_tokens
            + acc.cache_read_tokens
            + acc.cache_creation_tokens;
        runtime.request_count += 1;
    }

    let estimated_cache_waste_dollars = if matches!(cache_event, "miss_ttl" | "miss_thrash") {
        Some(
            pricing::estimate_cache_rebuild_waste_dollars(billing_model, acc.cache_creation_tokens)
                .total_cost_dollars,
        )
    } else {
        None
    };
    {
        let mut entry = SESSION_BUDGETS.entry(session_id.clone()).or_default();
        entry.total_spend += total_cost;
        entry.total_tokens += acc.input_tokens
            + acc.output_tokens
            + acc.cache_read_tokens
            + acc.cache_creation_tokens;
        entry.request_count += 1;
        if let Some(waste) = estimated_cache_waste_dollars {
            entry.estimated_cache_waste_dollars += waste;
        }
    }

    if !is_title_request {
        // Ensure session row exists (uses the primary model, not haiku).
        let insert_info = {
            let mut entry = diagnosis::SESSIONS.get_mut(&sys_prompt_hash);
            if let Some(ref mut e) = entry {
                if !e.session_inserted {
                    e.session_inserted = true;
                    Some(e.display_name.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(display_name) = insert_info {
            let _ = DB_TX.send(DbCommand::InsertSession {
                session_id: session_id.clone(),
                started_at: now_iso8601(),
                model: model.to_string(),
                initial_prompt: if user_prompt_excerpt.is_empty() {
                    None
                } else {
                    Some(user_prompt_excerpt.to_string())
                },
            });
            watch::BROADCASTER.broadcast(watch::WatchEvent::SessionStart {
                session_id: session_id.clone(),
                display_name,
                model: model.to_string(),
                initial_prompt: if user_prompt_excerpt.is_empty() {
                    None
                } else {
                    Some(user_prompt_excerpt.to_string())
                },
            });
            metrics::set_active_sessions(active_session_count());
        }

        for tool_result in tool_results {
            if tool_result.is_error {
                metrics::record_tool_failures(&tool_result.tool_name, 1);
                if let Some((server, tool)) = metrics::mcp_tool_labels(&tool_result.tool_name) {
                    watch::BROADCASTER.broadcast(watch::WatchEvent::McpEvent {
                        session_id: session_id.clone(),
                        timestamp: now_iso8601(),
                        server,
                        tool,
                        event_type: "failed".to_string(),
                        source: "proxy".to_string(),
                        detail: Some(tool_result.outcome.clone()),
                    });
                }
                if tool_result.tool_name.eq_ignore_ascii_case("Skill") {
                    let skill_name = canonical_telemetry_name(&tool_result.input_summary)
                        .unwrap_or_else(|| "unknown".to_string());
                    emit_skill_event(SkillTelemetryEvent {
                        session_id: session_id.clone(),
                        timestamp: now_iso8601(),
                        skill_name,
                        event_type: "failed".to_string(),
                        source: "proxy".to_string(),
                        confidence: 0.65,
                        detail: Some(tool_result.outcome.clone()),
                    });
                }
            } else if let Some((server, tool)) = metrics::mcp_tool_labels(&tool_result.tool_name) {
                emit_mcp_event(McpTelemetryEvent {
                    session_id: session_id.clone(),
                    timestamp: now_iso8601(),
                    server,
                    tool,
                    event_type: "succeeded".to_string(),
                    source: "proxy".to_string(),
                    detail: Some(tool_result.outcome.clone()),
                });
            }
            watch::BROADCASTER.broadcast(watch::WatchEvent::ToolResult {
                session_id: session_id.clone(),
                tool_name: tool_result.tool_name.clone(),
                outcome: tool_result.outcome.clone(),
                duration_ms: tool_result.duration_ms,
            });
        }

        // Broadcast all cache events except "none" — hits confirm the stream is alive.
        if cache_event != "none" {
            let resolved = pricing::resolve_pricing(billing_model);
            let prompt_size_tokens =
                acc.input_tokens + acc.cache_read_tokens + acc.cache_creation_tokens;
            let rebuild_cost = token_cost(prompt_size_tokens, resolved.pricing.cache_create);
            let expires_at = now_epoch_secs() + CACHE_TTL_SECS;
            metrics::record_cache_event(billing_model, cache_event, estimated_cache_waste_dollars);
            watch::BROADCASTER.broadcast(watch::WatchEvent::CacheEvent {
                session_id: session_id.clone(),
                event_type: cache_event.to_string(),
                cache_expires_at_epoch: Some(expires_at),
                estimated_rebuild_cost_dollars: Some(rebuild_cost),
            });
        }

        // Flush deferred watch events (tool use, frustration signals) — inject session_id.
        for event in &acc.deferred_watch_events {
            let mut event = event.clone();
            match &mut event {
                watch::WatchEvent::ToolUse {
                    session_id: sid, ..
                }
                | watch::WatchEvent::FrustrationSignal {
                    session_id: sid, ..
                }
                | watch::WatchEvent::SkillEvent {
                    session_id: sid, ..
                }
                | watch::WatchEvent::McpEvent {
                    session_id: sid, ..
                } => {
                    *sid = session_id.clone();
                }
                _ => {}
            }
            match event {
                watch::WatchEvent::ToolUse { ref tool_name, .. } => {
                    metrics::record_tool_call(tool_name);
                    watch::BROADCASTER.broadcast(event);
                }
                watch::WatchEvent::SkillEvent {
                    session_id,
                    timestamp,
                    skill_name,
                    event_type,
                    source,
                    confidence,
                    detail,
                } => {
                    emit_skill_event(SkillTelemetryEvent {
                        session_id,
                        timestamp,
                        skill_name,
                        event_type,
                        source,
                        confidence,
                        detail,
                    });
                }
                watch::WatchEvent::McpEvent {
                    session_id,
                    timestamp,
                    server,
                    tool,
                    event_type,
                    source,
                    detail,
                } => {
                    emit_mcp_event(McpTelemetryEvent {
                        session_id,
                        timestamp,
                        server,
                        tool,
                        event_type,
                        source,
                        detail,
                    });
                }
                other => watch::BROADCASTER.broadcast(other),
            }
        }

        // Silent-fallback detector: Anthropic sometimes routes a request to a
        // different model than the client asked for (e.g. Opus→Sonnet when
        // the Opus quota is drained). We emit a one-shot alert whenever the
        // `message.model` we saw in the SSE doesn't match the requested model.
        // Matching is a contains-check so minor version suffix drift
        // (`claude-opus-4-7` vs `claude-opus-4-7-20260410`) doesn't misfire.
        if let Some(actual) = acc.response_model.as_ref() {
            if !model_matches(model, actual) {
                metrics::record_model_fallback(model, actual);
                watch::BROADCASTER.broadcast(watch::WatchEvent::ModelFallback {
                    session_id: session_id.clone(),
                    requested: model.to_string(),
                    actual: actual.clone(),
                });
            }
        }

        // Compaction runway: broadcast current context fill + projected turns
        // until Claude Code auto-compacts (~85% of the resolved context
        // window). If we have <2 turns of history we don't project — the
        // slope is meaningless.
        let fill_percent = context_fill_percent(
            acc.input_tokens,
            acc.cache_read_tokens,
            acc.cache_creation_tokens,
            context_window_tokens,
        );
        let turns_to_compact = project_turns_to_compact(&session_id, fill_percent);
        metrics::record_context_status(turns_to_compact);
        watch::BROADCASTER.broadcast(watch::WatchEvent::ContextStatus {
            session_id: session_id.clone(),
            fill_percent,
            context_window_tokens: Some(context_window_tokens),
            turns_to_compact,
        });

        // Build and store TurnSnapshot.
        let (turn_number, compaction_loop_signal) = {
            let mut entry = diagnosis::SESSION_TURNS
                .entry(session_id.clone())
                .or_default();
            let n = entry.len() as u32 + 1;
            let gap = if let Some(prev) = entry.last() {
                started_at.duration_since(prev.timestamp).as_secs_f64()
            } else {
                0.0
            };
            let response_summary = compact_response_summary(&acc.response_text, tool_results);
            let snapshot = diagnosis::TurnSnapshot {
                turn_number: n,
                timestamp: *started_at,
                input_tokens: acc.input_tokens as u32,
                cache_read_tokens: acc.cache_read_tokens as u32,
                cache_creation_tokens: acc.cache_creation_tokens as u32,
                output_tokens: acc.output_tokens as u32,
                ttft_ms: duration_ms,
                tool_calls: acc.tool_calls.clone(),
                tool_results_failed: tool_failures,
                gap_from_prev_secs: gap,
                context_utilization: context_fill_ratio(
                    acc.input_tokens,
                    acc.cache_read_tokens,
                    acc.cache_creation_tokens,
                    context_window_tokens,
                ),
                context_window_tokens,
                frustration_signals: acc.frustration_signal_count,
                requested_model: Some(model.to_string()),
                actual_model: acc.response_model.clone(),
                response_summary: response_summary.clone(),
            };
            entry.push(snapshot);
            let signal = diagnosis::compaction_loop_signal_ending_at(
                entry.as_slice(),
                entry.len().saturating_sub(1),
            );
            (n, signal)
        };

        if let Some(signal) = compaction_loop_signal {
            // Emit once when the heuristic first crosses the detection threshold.
            if signal.consecutive_turns == 3 {
                watch::BROADCASTER.broadcast(watch::WatchEvent::CompactionLoop {
                    session_id: session_id.clone(),
                    consecutive: signal.consecutive_turns,
                    wasted_tokens: signal.wasted_tokens,
                });
            }
        }

        // Persist turn snapshot to SQLite (async).
        let snap_session = session_id.clone();
        let snap_ts = now_iso8601();
        let snap_tools = serde_json::to_string(&acc.tool_calls).unwrap_or_default();
        let snap_input = acc.input_tokens;
        let snap_output = acc.output_tokens;
        let snap_cache_read = acc.cache_read_tokens;
        let snap_cache_create = acc.cache_creation_tokens;
        let snap_ttft = duration_ms;
        let snap_failures = tool_failures;
        let snap_gap = {
            let entry = diagnosis::SESSION_TURNS.get(&session_id);
            entry
                .as_ref()
                .and_then(|turns| turns.last())
                .map(|t| t.gap_from_prev_secs)
                .unwrap_or(0.0)
        };
        let snap_ctx = context_fill_ratio(
            acc.input_tokens,
            acc.cache_read_tokens,
            acc.cache_creation_tokens,
            context_window_tokens,
        );
        let snap_frust = acc.frustration_signal_count;
        let snap_requested_model = Some(model.to_string());
        let snap_actual_model = acc.response_model.clone();
        let snap_response_summary = compact_response_summary(&acc.response_text, tool_results);
        let _ = DB_TX.send(DbCommand::WriteTurnSnapshot {
            session_id: snap_session,
            turn_number,
            timestamp: snap_ts.clone(),
            input_tokens: snap_input,
            cache_read_tokens: snap_cache_read,
            cache_creation_tokens: snap_cache_create,
            output_tokens: snap_output,
            ttft_ms: snap_ttft,
            tool_calls_json: snap_tools,
            tool_failures: snap_failures,
            gap_from_prev_secs: snap_gap,
            context_utilization: snap_ctx,
            context_window_tokens,
            frustration_signals: snap_frust,
            requested_model: snap_requested_model,
            actual_model: snap_actual_model,
            response_summary: snap_response_summary,
        });

        for tool_result in tool_results {
            let _ = DB_TX.send(DbCommand::WriteToolOutcome {
                session_id: session_id.clone(),
                turn_number,
                timestamp: snap_ts.clone(),
                tool_name: tool_result.tool_name.clone(),
                input_summary: tool_result.input_summary.clone(),
                outcome: tool_result.outcome.clone(),
                duration_ms: tool_result.duration_ms,
            });
        }
    }

    // Persist request (always — including title requests for estimated-cost tracking).
    let ts = now_iso8601();
    let tool_calls_json = serde_json::to_string(&acc.tool_calls).unwrap_or_default();
    let tool_calls_list: Vec<(String, String)> = acc
        .tool_calls
        .iter()
        .map(|n| (n.clone(), ts.clone()))
        .collect();
    let _ = DB_TX.send(DbCommand::RecordRequest {
        request_id: request_id.to_string(),
        session_id: session_id.clone(),
        timestamp: ts,
        model: billing_model.to_string(),
        input_tokens: acc.input_tokens,
        output_tokens: acc.output_tokens,
        cache_read_tokens: acc.cache_read_tokens,
        cache_creation_tokens: acc.cache_creation_tokens,
        cost_dollars: total_cost,
        cost_source: estimated_cost.cost_source.clone(),
        trusted_for_budget_enforcement: estimated_cost.trusted_for_budget_enforcement,
        duration_ms,
        tool_calls_json,
        tool_calls_list,
        cache_event: cache_event.to_string(),
    });

    if acc.http_status >= 400 && !acc.error_body.is_empty() {
        let error_text = String::from_utf8_lossy(&acc.error_body);
        warn!(request_id, model, http_status = acc.http_status,
            error = %error_text, duration_s = format!("{:.2}", duration_secs),
            "request failed");
    } else {
        info!(
            request_id,
            model,
            http_status = acc.http_status,
            input_tokens = acc.input_tokens,
            output_tokens = acc.output_tokens,
            cache_read = acc.cache_read_tokens,
            cache_create = acc.cache_creation_tokens,
            context_window_tokens,
            fill_percent = format!(
                "{:.1}",
                context_fill_percent(
                    acc.input_tokens,
                    acc.cache_read_tokens,
                    acc.cache_creation_tokens,
                    context_window_tokens
                )
            ),
            estimated_cost = format!("${:.6}", total_cost),
            duration_s = format!("{:.2}", duration_secs),
            tools = acc.tool_calls.len(),
            cache_event,
            billed_as = billing_model,
            "request complete"
        );
    }
}

fn last_session_response_summary(turns: &[diagnosis::TurnSnapshot]) -> String {
    turns
        .iter()
        .rev()
        .filter_map(|t| t.response_summary.as_ref())
        .find(|summary| !summary.is_empty())
        .cloned()
        .unwrap_or_default()
}

fn session_timeout_secs() -> u64 {
    env_u64("CLAUDITOR_SESSION_TIMEOUT_MINUTES", 5) * 60
}

fn parse_tool_calls_json(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn load_turn_snapshots_from_db(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<Vec<diagnosis::TurnSnapshot>> {
    let mut stmt = conn.prepare(
        "SELECT turn_number, input_tokens, cache_read_tokens, cache_creation_tokens, \
         output_tokens, ttft_ms, tool_calls, tool_failures, gap_from_prev_secs, \
         context_utilization, context_window_tokens, frustration_signals, requested_model, actual_model, response_summary \
         FROM turn_snapshots WHERE session_id = ?1 ORDER BY turn_number ASC",
    )?;

    let turns = stmt
        .query_map(rusqlite::params![session_id], |row| {
            let tool_calls_raw = row.get::<_, String>(6)?;
            let input_tokens = row.get::<_, i64>(1)?.max(0) as u32;
            let cache_read_tokens = row.get::<_, i64>(2)?.max(0) as u32;
            let cache_creation_tokens = row.get::<_, i64>(3)?.max(0) as u32;
            let requested_model = row.get::<_, Option<String>>(12)?;
            let actual_model = row.get::<_, Option<String>>(13)?;
            let context_window_tokens = row
                .get::<_, Option<i64>>(10)?
                .map(|value| value.max(0) as u64)
                .filter(|value| *value > 0)
                .unwrap_or_else(|| {
                    infer_context_window_tokens(
                        requested_model.as_deref(),
                        actual_model.as_deref(),
                        input_tokens as u64,
                        cache_read_tokens as u64,
                        cache_creation_tokens as u64,
                    )
                });
            let response_summary = row.get::<_, Option<String>>(14)?;
            Ok(diagnosis::TurnSnapshot {
                turn_number: row.get::<_, i64>(0)?.max(0) as u32,
                timestamp: Instant::now(),
                input_tokens,
                cache_read_tokens,
                cache_creation_tokens,
                output_tokens: row.get::<_, i64>(4)?.max(0) as u32,
                ttft_ms: row.get::<_, i64>(5)?.max(0) as u64,
                tool_calls: parse_tool_calls_json(&tool_calls_raw),
                tool_results_failed: row.get::<_, i64>(7)?.max(0) as u32,
                gap_from_prev_secs: row.get::<_, f64>(8)?.max(0.0),
                context_utilization: context_fill_ratio(
                    input_tokens as u64,
                    cache_read_tokens as u64,
                    cache_creation_tokens as u64,
                    context_window_tokens,
                ),
                context_window_tokens,
                frustration_signals: row.get::<_, i64>(11)?.max(0) as u32,
                requested_model,
                actual_model,
                response_summary: response_summary.filter(|s| !s.trim().is_empty()),
            })
        })?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    Ok(turns)
}

fn stored_tool_recall_context(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<Option<String>> {
    let mut stmt = conn.prepare(
        "SELECT tool_name, input_summary, outcome \
         FROM tool_outcomes WHERE session_id = ?1 \
         ORDER BY turn_number DESC, id DESC LIMIT 12",
    )?;
    let rows = stmt.query_map(rusqlite::params![session_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;

    let mut seen = HashSet::new();
    let mut parts = Vec::new();
    for row in rows.flatten() {
        let (tool_name, input_summary, outcome) = row;
        let mut detail = tool_name;
        if let Some(summary) = input_summary {
            let summary = summary.trim();
            if !summary.is_empty() {
                detail.push(' ');
                detail.push_str(summary);
            }
        }
        if matches!(outcome.as_deref(), Some("error")) {
            detail.push_str(" (error)");
        }
        if seen.insert(detail.clone()) {
            parts.push(detail);
        }
        if parts.len() >= 3 {
            break;
        }
    }

    if parts.is_empty() {
        Ok(None)
    } else {
        Ok(Some(format!("Tools: {}", parts.join("; "))))
    }
}

fn latest_response_summary_from_db(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT response_summary FROM turn_snapshots \
         WHERE session_id = ?1 AND response_summary IS NOT NULL AND trim(response_summary) != '' \
         ORDER BY turn_number DESC LIMIT 1",
        rusqlite::params![session_id],
        |row| row.get::<_, String>(0),
    )
    .optional()
}

fn diagnosis_outcome_needs_refresh(outcome: Option<&str>) -> bool {
    matches!(
        outcome,
        Some("Completed" | "PartiallyCompleted" | "Abandoned")
    )
}

fn repair_persisted_session_artifacts(conn: &Connection) -> rusqlite::Result<()> {
    let _ = ensure_turn_snapshot_model_columns(conn);
    let _ = ensure_session_columns(conn);
    let _ = ensure_request_cost_columns(conn);
    let _ = repair_turn_snapshot_context_windows(conn);
    let _ = repair_session_diagnosis_degradation_turns(conn);
    let cutoff = epoch_to_iso8601(now_epoch_secs().saturating_sub(session_timeout_secs()));
    let mut stmt = conn.prepare(
        "SELECT s.session_id, s.ended_at, s.initial_prompt, d.outcome, \
                CASE WHEN r.session_id IS NULL THEN 0 ELSE 1 END \
         FROM sessions s \
         LEFT JOIN session_diagnoses d ON d.session_id = s.session_id \
         LEFT JOIN session_recall r ON r.session_id = s.session_id \
         WHERE s.ended_at IS NOT NULL AND s.ended_at <= ?1 \
           AND EXISTS (SELECT 1 FROM turn_snapshots t WHERE t.session_id = s.session_id) \
           AND (
                d.session_id IS NULL
                OR d.outcome IN ('Completed', 'PartiallyCompleted', 'Abandoned')
                OR (
                    r.session_id IS NULL
                    AND (
                        (s.initial_prompt IS NOT NULL AND trim(s.initial_prompt) != '')
                        OR EXISTS (
                            SELECT 1 FROM turn_snapshots t2
                            WHERE t2.session_id = s.session_id
                              AND t2.response_summary IS NOT NULL
                              AND trim(t2.response_summary) != ''
                        )
                        OR EXISTS (
                            SELECT 1 FROM tool_outcomes o
                            WHERE o.session_id = s.session_id
                        )
                    )
                )
           ) \
         ORDER BY s.ended_at ASC LIMIT 200",
    )?;

    let candidates = stmt
        .query_map(rusqlite::params![cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)? != 0,
            ))
        })?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    for (session_id, ended_at, initial_prompt, stored_outcome, recall_exists) in candidates {
        let turns = load_turn_snapshots_from_db(conn, &session_id)?;
        if turns.is_empty() {
            continue;
        }

        if stored_outcome.is_none() || diagnosis_outcome_needs_refresh(stored_outcome.as_deref()) {
            let report = diagnosis::analyze_session(&session_id, &turns);
            let completed_at = ended_at.clone().unwrap_or_else(now_iso8601);
            let _ = conn.execute(
                "INSERT OR REPLACE INTO session_diagnoses (session_id, completed_at, \
                 outcome, total_turns, total_cost, cache_hit_ratio, degraded, degradation_turn, \
                 causes_json, advice_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                rusqlite::params![
                    &session_id,
                    completed_at,
                    report.outcome,
                    report.total_turns,
                    report.estimated_total_cost_dollars,
                    report.cache_hit_ratio,
                    report.degraded as i32,
                    report.degradation_turn,
                    serde_json::to_string(&report.causes).unwrap_or_default(),
                    serde_json::to_string(&report.advice).unwrap_or_default(),
                ],
            );
        }

        if !recall_exists {
            let summary = latest_response_summary_from_db(conn, &session_id)?
                .or_else(|| stored_tool_recall_context(conn, &session_id).ok().flatten())
                .unwrap_or_default();
            let prompt = initial_prompt.unwrap_or_default();
            if !prompt.is_empty() || !summary.is_empty() {
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO session_recall (session_id, initial_prompt, final_response_summary) \
                     VALUES (?1,?2,?3)",
                    rusqlite::params![&session_id, prompt, summary],
                );
            }
        }
    }

    Ok(())
}

/// End a session: run diagnosis, broadcast SessionEnd, persist to DB, clean up.
fn end_session(session_id: &str, session_model: Option<String>, initial_prompt: Option<String>) {
    SESSION_BUDGETS.remove(session_id);
    CACHE_TRACKERS.remove(session_id);
    if let Some((_, turns)) = diagnosis::SESSION_TURNS.remove(session_id) {
        if turns.is_empty() {
            // No turns collected — still broadcast session end.
            watch::BROADCASTER.broadcast(watch::WatchEvent::SessionEnd {
                session_id: session_id.to_string(),
                outcome: "timeout".to_string(),
                total_tokens: 0,
                total_turns: 0,
            });
            return;
        }
        let report = diagnosis::analyze_session(session_id, &turns);

        watch::BROADCASTER.broadcast(watch::WatchEvent::SessionEnd {
            session_id: session_id.to_string(),
            outcome: report.outcome.clone(),
            total_tokens: report.total_tokens,
            total_turns: report.total_turns,
        });
        metrics::record_session_end(
            &report.outcome,
            session_model.as_deref(),
            report.estimated_total_cost_dollars,
            report.total_turns,
        );

        if report.degraded {
            metrics::record_degraded_session();
            for cause in &report.causes {
                metrics::record_degraded_cause(&cause.cause_type);
                if cause.cause_type == "compaction_suspected" {
                    metrics::record_compaction_suspected();
                }
            }
            watch::BROADCASTER.broadcast(watch::WatchEvent::Diagnosis {
                session_id: session_id.to_string(),
                report: report.clone(),
            });
        }

        let recall_initial_prompt = initial_prompt.unwrap_or_default();
        let recall_summary = last_session_response_summary(&turns);
        if !recall_initial_prompt.is_empty() || !recall_summary.is_empty() {
            if let Err(err) = DB_TX.send(DbCommand::WriteRecall {
                session_id: session_id.to_string(),
                initial_prompt: recall_initial_prompt,
                final_response_summary: recall_summary,
            }) {
                warn!(
                    session_id,
                    error = %err,
                    "failed to queue session recall for persistence"
                );
            }
        }

        let _ = DB_TX.send(DbCommand::WriteDiagnosis {
            session_id: session_id.to_string(),
            completed_at: now_iso8601(),
            outcome: report.outcome,
            total_turns: report.total_turns,
            total_cost: report.estimated_total_cost_dollars,
            cache_hit_ratio: report.cache_hit_ratio,
            degraded: report.degraded,
            degradation_turn: report.degradation_turn,
            causes_json: serde_json::to_string(&report.causes).unwrap_or_default(),
            advice_json: serde_json::to_string(&report.advice).unwrap_or_default(),
        });
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive a short display name from the working directory path.
/// Falls back to model+time if working_dir is empty.
/// Appends a 3-char hash suffix if the name collides with an existing session.
fn derive_display_name(working_dir: &str, model: &str, sys_prompt_hash: u64) -> String {
    let base = if !working_dir.is_empty() {
        // Extract the last path component: /Users/pradeep/code/idea/clauditor → clauditor
        working_dir
            .rsplit('/')
            .next()
            .unwrap_or(working_dir)
            .to_string()
    } else {
        let short_model = model.replace("claude-", "").replace("-20250514", "");
        let tod = now_epoch_secs() % 86400;
        format!(
            "{}\u{00b7}{:02}:{:02}",
            short_model,
            tod / 3600,
            (tod % 3600) / 60
        )
    };

    // Check if any existing session has the same base name — if so, add a 3-char hash suffix.
    let has_collision = diagnosis::SESSIONS
        .iter()
        .any(|entry| entry.display_name == base);
    if has_collision {
        let suffix = &format!("{:x}", sys_prompt_hash)[..3];
        format!("{}-{}", base, suffix)
    } else {
        base
    }
}

fn extract_header(h: &HttpHeaders, name: &str) -> Option<String> {
    h.headers
        .as_ref()?
        .headers
        .iter()
        .find(|hv| hv.key.eq_ignore_ascii_case(name))
        .map(|hv| {
            if hv.value.is_empty() {
                String::from_utf8_lossy(&hv.raw_value).into_owned()
            } else {
                hv.value.clone()
            }
        })
}

fn extract_headers(h: &HttpHeaders, name: &str) -> Vec<String> {
    h.headers
        .as_ref()
        .map(|headers| {
            headers
                .headers
                .iter()
                .filter(|hv| hv.key.eq_ignore_ascii_case(name))
                .map(|hv| {
                    if hv.value.is_empty() {
                        String::from_utf8_lossy(&hv.raw_value).into_owned()
                    } else {
                        hv.value.clone()
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

const STANDARD_CONTEXT_WINDOW_TOKENS: u64 = 200_000;
const EXTENDED_CONTEXT_WINDOW_TOKENS: u64 = 1_000_000;
const CONTEXT_1M_BETA_PREFIX: &str = "context-1m-";

fn configured_context_window_tokens() -> Option<u64> {
    let value = std::env::var("CLAUDITOR_CONTEXT_WINDOW_TOKENS").ok()?;
    let parsed = value.parse::<u64>().ok()?;
    if parsed == 0 {
        None
    } else {
        Some(parsed)
    }
}

fn model_requests_1m_context(model: &str) -> bool {
    model.to_ascii_lowercase().contains("[1m]")
}

fn request_uses_1m_context(headers: &HttpHeaders) -> bool {
    extract_headers(headers, "anthropic-beta")
        .iter()
        .flat_map(|value| value.split(','))
        .map(|beta| beta.trim().to_ascii_lowercase())
        .any(|beta| beta.starts_with(CONTEXT_1M_BETA_PREFIX))
}

fn resolve_context_window_tokens(header_hint: Option<u64>, model: &str) -> u64 {
    configured_context_window_tokens()
        .or(header_hint)
        .or_else(|| model_requests_1m_context(model).then_some(EXTENDED_CONTEXT_WINDOW_TOKENS))
        .unwrap_or(STANDARD_CONTEXT_WINDOW_TOKENS)
}

fn infer_context_window_tokens(
    requested_model: Option<&str>,
    actual_model: Option<&str>,
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
) -> u64 {
    if let Some(configured) = configured_context_window_tokens() {
        return configured;
    }

    let model = actual_model.or(requested_model).unwrap_or("");
    if model_requests_1m_context(model) {
        return EXTENDED_CONTEXT_WINDOW_TOKENS;
    }

    let total_input_tokens = input_tokens + cache_read_tokens + cache_creation_tokens;
    if total_input_tokens > STANDARD_CONTEXT_WINDOW_TOKENS {
        return EXTENDED_CONTEXT_WINDOW_TOKENS;
    }

    STANDARD_CONTEXT_WINDOW_TOKENS
}

fn context_fill_ratio(
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    context_window_tokens: u64,
) -> f64 {
    let total = input_tokens + cache_read_tokens + cache_creation_tokens;
    total as f64 / context_window_tokens.max(1) as f64
}

fn context_fill_percent(
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    context_window_tokens: u64,
) -> f64 {
    context_fill_ratio(
        input_tokens,
        cache_read_tokens,
        cache_creation_tokens,
        context_window_tokens,
    ) * 100.0
}

/// Local quota-burn tracker. Anthropic does not expose rate-limit headers to
/// Claude Code (OAuth) traffic, so instead we track our own token counters
/// and project burn from the delta over a sliding window. The user supplies
/// their subscription-tier weekly budget via `CLAUDITOR_WEEKLY_TOKEN_BUDGET`
/// (tokens); without it we still broadcast burn rate but skip the
/// "remaining" and "projected exhaustion" fields.
struct BurnTracker {
    /// (timestamp, cumulative_tokens_seen) samples. Bounded ring.
    samples: VecDeque<(Instant, u64)>,
}

impl BurnTracker {
    fn new() -> Self {
        Self {
            samples: VecDeque::with_capacity(64),
        }
    }

    fn record(&mut self, cumulative_tokens: u64) {
        self.samples.push_back((Instant::now(), cumulative_tokens));
        if self.samples.len() > 64 {
            self.samples.pop_front();
        }
    }

    /// Tokens per second over the last window. None when span too short or
    /// not enough samples.
    fn tokens_per_sec(&self) -> Option<f64> {
        let (first_t, first_v) = self.samples.front()?;
        let (last_t, last_v) = self.samples.back()?;
        let span = last_t.duration_since(*first_t).as_secs_f64();
        if span < 60.0 {
            return None;
        }
        let delta = last_v.saturating_sub(*first_v) as f64;
        if delta <= 0.0 {
            return None;
        }
        Some(delta / span)
    }
}

static BURN_TRACKER: LazyLock<Mutex<BurnTracker>> =
    LazyLock::new(|| Mutex::new(BurnTracker::new()));

const QUOTA_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
const QUOTA_WATCH_BROADCAST_INTERVAL: Duration = Duration::from_secs(5 * 60);

fn should_broadcast_quota_snapshot(
    since_last_broadcast: Option<Duration>,
    previous_alarm: Option<bool>,
    alarm: bool,
) -> bool {
    let interval_elapsed = since_last_broadcast
        .map(|elapsed| elapsed >= QUOTA_WATCH_BROADCAST_INTERVAL)
        .unwrap_or(true);
    let alarm_changed = previous_alarm
        .map(|previous| previous != alarm)
        .unwrap_or(false);
    interval_elapsed || alarm_changed
}

#[derive(Clone)]
struct AutoWeeklyBudget {
    tokens_limit: u64,
}

struct AutoWeeklyBudgetCache {
    refreshed_at: Option<Instant>,
    week_start_epoch: Option<u64>,
    suggestion: Option<AutoWeeklyBudget>,
}

impl AutoWeeklyBudgetCache {
    fn new() -> Self {
        Self {
            refreshed_at: None,
            week_start_epoch: None,
            suggestion: None,
        }
    }
}

static AUTO_WEEKLY_BUDGET_CACHE: LazyLock<Mutex<AutoWeeklyBudgetCache>> =
    LazyLock::new(|| Mutex::new(AutoWeeklyBudgetCache::new()));

fn percentile_nearest_rank(mut values: Vec<u64>, percentile: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let rank = ((percentile.clamp(0.0, 1.0) * values.len() as f64).ceil() as usize)
        .max(1)
        .min(values.len());
    values.get(rank - 1).copied()
}

fn query_auto_weekly_budget_suggestion(current_week_start: u64) -> Option<AutoWeeklyBudget> {
    let conn = Connection::open(db_path()).ok()?;
    let mut stmt = conn.prepare(
        "SELECT COALESCE(SUM(input_tokens + output_tokens + cache_read_tokens + cache_creation_tokens), 0) \
         FROM requests WHERE timestamp >= ?1 AND timestamp < ?2",
    ).ok()?;

    let mut weekly_totals = Vec::with_capacity(4);
    for weeks_back in 1..=4u64 {
        let week_start = current_week_start.saturating_sub(weeks_back * 7 * 86_400);
        let week_end = week_start + 7 * 86_400;
        let total = stmt
            .query_row(
                rusqlite::params![epoch_to_iso8601(week_start), epoch_to_iso8601(week_end)],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0);
        weekly_totals.push(total.max(0) as u64);
    }

    if weekly_totals.iter().all(|&t| t == 0) {
        return None;
    }

    // With only four completed weeks, p95 is effectively the max weekly total.
    percentile_nearest_rank(weekly_totals, 0.95)
        .map(|tokens_limit| AutoWeeklyBudget { tokens_limit })
}

fn auto_weekly_budget_suggestion() -> Option<AutoWeeklyBudget> {
    let now = Instant::now();
    let current_week_start = start_of_week_epoch_at(now_epoch_secs());

    {
        let cache = AUTO_WEEKLY_BUDGET_CACHE.lock().unwrap();
        let is_fresh = cache
            .refreshed_at
            .map(|t| now.duration_since(t) < Duration::from_secs(3600))
            .unwrap_or(false);
        if is_fresh && cache.week_start_epoch == Some(current_week_start) {
            return cache.suggestion.clone();
        }
    }

    let suggestion =
        tokio::task::block_in_place(|| query_auto_weekly_budget_suggestion(current_week_start));

    let mut cache = AUTO_WEEKLY_BUDGET_CACHE.lock().unwrap();
    cache.refreshed_at = Some(now);
    cache.week_start_epoch = Some(current_week_start);
    cache.suggestion = suggestion.clone();
    suggestion
}

/// Returns (model, message_count, has_tools, system_prompt_length, estimated_input_tokens, sys_prompt_hash, working_dir).
/// sys_prompt_hash: stable per-terminal session key derived from working directory.
/// working_dir: extracted working directory path — used for session naming.
/// user_prompt_excerpt: cleaned text of the first user message (no Claude Code preamble), for display.
fn parse_request_body(
    body: &[u8],
) -> Option<(String, usize, bool, usize, usize, u64, String, String)> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let model = v.get("model")?.as_str()?.to_string();
    let mc = v
        .get("messages")
        .and_then(|m| m.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let ht = v
        .get("tools")
        .and_then(|t| t.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false);

    let sl = match v.get("system") {
        Some(Value::String(s)) => s.len(),
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|i| i.get("text").and_then(|t| t.as_str()))
            .map(|s| s.len())
            .sum(),
        _ => 0,
    };

    // Search system blocks for the "Primary working directory:" line.
    // This is stable across requests from the same terminal and different across terminals.
    let mut working_dir = String::new();
    match v.get("system") {
        Some(Value::String(s)) => {
            if let Some(dir) = extract_working_dir(s) {
                working_dir = dir;
            }
        }
        Some(Value::Array(a)) => {
            for item in a {
                if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                    if working_dir.is_empty() {
                        if let Some(dir) = extract_working_dir(text) {
                            working_dir = dir;
                        }
                    }
                }
            }
        }
        _ => {}
    }

    // Hash: working_directory + full first user message.
    // Two Claude sessions in the same dir have different initial prompts, so
    // including the first user message distinguishes them. Within a single
    // session, messages[0] is stable across turns (history is appended).
    // We hash the whole message because Claude Code prepends a multi-KB
    // boilerplate preamble; a short prefix would collide across sessions.
    // Concatenate every text block in messages[0].content. Claude Code
    // packs the user's actual prompt into a LATER text block after preamble
    // blocks (system-reminders, command outputs, etc.), so taking only
    // the first block used to lose the user text entirely.
    let first_user_message = v
        .get("messages")
        .and_then(|m| m.as_array())
        .and_then(|a| a.first())
        .and_then(|msg| match msg.get("content") {
            Some(Value::String(s)) => Some(s.as_str().to_string()),
            Some(Value::Array(arr)) => Some(
                arr.iter()
                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            _ => None,
        })
        .unwrap_or_default();
    let sys_prompt_hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        working_dir.hash(&mut hasher);
        first_user_message.hash(&mut hasher);
        hasher.finish()
    };

    let user_prompt_excerpt = clean_user_prompt(&first_user_message);

    Some((
        model,
        mc,
        ht,
        sl,
        body.len() / 4,
        sys_prompt_hash,
        working_dir,
        user_prompt_excerpt,
    ))
}

/// Strip Claude Code's injected preamble (`<system-reminder>...`,
/// `<command-name>...`, etc.) from the first user message and return a short
/// excerpt of what the human actually typed. Best-effort heuristic — failures
/// degrade to an empty string, which renders as "no prompt captured" rather
/// than wrong text.
///
/// Rust's `regex` crate doesn't support backreferences, so we strip each
/// known Claude Code preamble tag explicitly rather than via a single
/// backreferencing pattern.
fn clean_user_prompt(raw: &str) -> String {
    // (?s) makes `.` match newlines; `.*?` is non-greedy so nested / repeated
    // blocks collapse one at a time.
    static PREAMBLE_TAGS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
        [
            r"(?s)<system-reminder\b[^>]*>.*?</system-reminder>",
            r"(?s)<command-name\b[^>]*>.*?</command-name>",
            r"(?s)<command-message\b[^>]*>.*?</command-message>",
            r"(?s)<command-args\b[^>]*>.*?</command-args>",
            r"(?s)<local-command-stdout\b[^>]*>.*?</local-command-stdout>",
            r"(?s)<local-command-stderr\b[^>]*>.*?</local-command-stderr>",
        ]
        .iter()
        .filter_map(|p| regex::Regex::new(p).ok())
        .collect()
    });

    let mut cleaned = raw.to_string();
    // Collapse repeatedly because removing an outer tag can expose another.
    for _ in 0..4 {
        let mut changed = false;
        for re in PREAMBLE_TAGS.iter() {
            let next = re.replace_all(&cleaned, "").to_string();
            if next != cleaned {
                cleaned = next;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    let trimmed: String = cleaned
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    const MAX_LEN: usize = 320;
    if trimmed.chars().count() <= MAX_LEN {
        trimmed
    } else {
        let mut out: String = trimmed.chars().take(MAX_LEN).collect();
        out.push('…');
        out
    }
}

fn looks_like_machine_recall_line(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.starts_with("```") {
        return true;
    }
    if matches!(trimmed, "{" | "}" | "[" | "]" | "}," | "],") {
        return true;
    }
    if (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
    {
        return true;
    }
    if trimmed.starts_with('"') && trimmed.contains("\":") {
        return true;
    }
    trimmed.starts_with('<') && trimmed.ends_with('>') && !trimmed.contains(' ')
}

fn tool_recall_context(tool_results: &[ParsedToolResult]) -> Option<String> {
    let mut seen = HashSet::new();
    let mut parts = Vec::new();

    for result in tool_results {
        let mut detail = result.tool_name.clone();
        let input_summary = result.input_summary.trim();
        if !input_summary.is_empty() {
            detail.push(' ');
            detail.push_str(input_summary);
        }
        if result.is_error {
            detail.push_str(" (error)");
        }
        if seen.insert(detail.clone()) {
            parts.push(detail);
        }
        if parts.len() >= 3 {
            break;
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(format!("Tools: {}", parts.join("; ")))
    }
}

fn compact_response_summary(raw: &str, tool_results: &[ParsedToolResult]) -> Option<String> {
    let trimmed = raw
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .filter(|l| !looks_like_machine_recall_line(l))
        .collect::<Vec<_>>()
        .join(" ");
    let summary = if trimmed.is_empty() {
        tool_recall_context(tool_results)?
    } else {
        trimmed
    };
    const MAX_LEN: usize = 360;
    if summary.chars().count() <= MAX_LEN {
        Some(summary)
    } else {
        let mut out: String = summary.chars().take(MAX_LEN).collect();
        out.push('…');
        Some(out)
    }
}

/// Extract the working directory path from a system prompt text block.
fn extract_working_dir(text: &str) -> Option<String> {
    for line in text.lines() {
        let trimmed = line.trim();
        // Match "- Primary working directory: /path/to/dir" or "Primary working directory: /path"
        if let Some(rest) = trimmed.strip_prefix("- Primary working directory:") {
            let path = rest.trim();
            if !path.is_empty() {
                return Some(path.to_string());
            }
        }
        if let Some(rest) = trimmed.strip_prefix("Primary working directory:") {
            let path = rest.trim();
            if !path.is_empty() {
                return Some(path.to_string());
            }
        }
    }
    None
}

#[derive(Clone, Debug)]
struct ParsedToolResult {
    tool_name: String,
    input_summary: String,
    outcome: String,
    duration_ms: u64,
    is_error: bool,
}

#[derive(Clone, Debug)]
struct SkillTelemetryEvent {
    session_id: String,
    timestamp: String,
    skill_name: String,
    event_type: String,
    source: String,
    confidence: f64,
    detail: Option<String>,
}

#[derive(Clone, Debug)]
struct McpTelemetryEvent {
    session_id: String,
    timestamp: String,
    server: String,
    tool: String,
    event_type: String,
    source: String,
    detail: Option<String>,
}

#[derive(Default)]
struct SkillTurnState {
    expected: HashSet<String>,
    fired: HashSet<String>,
}

static HOOK_SKILL_TURNS: LazyLock<Mutex<HashMap<String, SkillTurnState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn canonical_telemetry_name(raw: &str) -> Option<String> {
    let trimmed = raw
        .trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '/' | '$'));
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.ends_with("SKILL.md")
        || trimmed.ends_with("Skill.md")
        || trimmed.ends_with("skill.md")
    {
        let path = Path::new(trimmed);
        if let Some(parent_name) = path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
        {
            return canonical_telemetry_name(parent_name);
        }
    }

    let mut out = String::with_capacity(trimmed.len());
    let mut last_was_sep = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if matches!(ch, '-' | '_' | '.' | ':') {
            out.push(ch);
            last_was_sep = false;
        } else if ch.is_ascii_whitespace() && !last_was_sep && !out.is_empty() {
            out.push('-');
            last_was_sep = true;
        }
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn skill_name_from_tool_input_value(value: &Value) -> Option<String> {
    if let Some(raw) = value.as_str() {
        return canonical_telemetry_name(raw);
    }

    let fields = [
        "skill_name",
        "skill",
        "command_name",
        "command",
        "name",
        "id",
        "path",
        "file_path",
    ];
    for field in fields {
        if let Some(raw) = value.get(field).and_then(|value| value.as_str()) {
            if let Some(name) = canonical_telemetry_name(raw) {
                if name != "skill" {
                    return Some(name);
                }
            }
        }
    }

    None
}

fn skill_name_from_tool_input_json(raw: &str) -> Option<String> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| skill_name_from_tool_input_value(&value))
}

fn truncate_detail(raw: &str, max: usize) -> String {
    if raw.len() <= max {
        raw.to_string()
    } else {
        format!("{}...", &raw[..max.min(raw.len())])
    }
}

fn summarize_hook_tool_input(value: Option<&Value>) -> Option<String> {
    let value = value?;
    if let Some(command) = value.get("command").and_then(|value| value.as_str()) {
        return Some(truncate_detail(command, 100));
    }
    if let Some(path) = value
        .get("file_path")
        .or_else(|| value.get("path"))
        .and_then(|value| value.as_str())
    {
        return Some(truncate_detail(path, 100));
    }
    if let Some(query) = value.get("query").and_then(|value| value.as_str()) {
        return Some(truncate_detail(query, 100));
    }
    serde_json::to_string(value)
        .ok()
        .map(|json| truncate_detail(&json, 100))
}

fn remember_skill_turn_event(event: &SkillTelemetryEvent) {
    if event.session_id.trim().is_empty() || event.session_id == "unknown" {
        return;
    }
    let mut states = HOOK_SKILL_TURNS.lock().unwrap();
    let state = states.entry(event.session_id.clone()).or_default();
    match event.event_type.as_str() {
        "expected" => {
            state.expected.insert(event.skill_name.clone());
        }
        "fired" | "failed" => {
            state.fired.insert(event.skill_name.clone());
        }
        _ => {}
    }
}

fn emit_skill_event(event: SkillTelemetryEvent) {
    remember_skill_turn_event(&event);
    metrics::record_skill_event(&event.skill_name, &event.event_type, &event.source);
    let _ = DB_TX.send(DbCommand::WriteSkillEvent {
        session_id: event.session_id.clone(),
        timestamp: event.timestamp.clone(),
        skill_name: event.skill_name.clone(),
        event_type: event.event_type.clone(),
        confidence: event.confidence,
        source: event.source.clone(),
        detail: event.detail.clone(),
    });
    watch::BROADCASTER.broadcast(watch::WatchEvent::SkillEvent {
        session_id: event.session_id,
        timestamp: event.timestamp,
        skill_name: event.skill_name,
        event_type: event.event_type,
        source: event.source,
        confidence: event.confidence,
        detail: event.detail,
    });
}

fn emit_mcp_event(event: McpTelemetryEvent) {
    metrics::record_mcp_event(&event.server, &event.tool, &event.event_type, &event.source);
    let _ = DB_TX.send(DbCommand::WriteMcpEvent {
        session_id: event.session_id.clone(),
        timestamp: event.timestamp.clone(),
        server: event.server.clone(),
        tool: event.tool.clone(),
        event_type: event.event_type.clone(),
        source: event.source.clone(),
        detail: event.detail.clone(),
    });
    watch::BROADCASTER.broadcast(watch::WatchEvent::McpEvent {
        session_id: event.session_id,
        timestamp: event.timestamp,
        server: event.server,
        tool: event.tool,
        event_type: event.event_type,
        source: event.source,
        detail: event.detail,
    });
}

fn extract_explicit_skill_refs(prompt: &str) -> HashSet<String> {
    let mut refs = HashSet::new();
    let mut chars = prompt.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if !matches!(ch, '/' | '$') {
            continue;
        }
        let marker_at_token_boundary = prompt[..idx]
            .chars()
            .next_back()
            .is_none_or(|prev| prev.is_whitespace() || matches!(prev, '(' | '[' | '{' | ','));
        if !marker_at_token_boundary {
            continue;
        }
        let mut raw = String::new();
        while let Some((_, next)) = chars.peek().copied() {
            if next.is_ascii_alphanumeric() || matches!(next, '-' | '_' | '.' | ':') {
                raw.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if ch == '/'
            && !raw
                .chars()
                .next()
                .is_some_and(|first| first.is_ascii_lowercase())
        {
            continue;
        }
        if let Some(name) = canonical_telemetry_name(&raw) {
            refs.insert(name);
        }
    }
    refs
}

fn normalize_phrase_for_match(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_space = true;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    out.trim().to_string()
}

fn skill_name_from_skill_file(path: &Path, content: &str) -> Option<String> {
    let mut lines = content.lines();
    if lines.next()?.trim() == "---" {
        for line in lines {
            let trimmed = line.trim();
            if trimmed == "---" {
                break;
            }
            if let Some(raw) = trimmed.strip_prefix("name:") {
                return canonical_telemetry_name(raw);
            }
        }
    }
    path.parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .and_then(canonical_telemetry_name)
}

fn discover_skill_names(cwd: Option<&str>) -> HashSet<String> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    if let Some(cwd) = cwd {
        dirs.push(Path::new(cwd).join(".claude/skills"));
    }
    if let Some(home) = std::env::var_os("HOME") {
        dirs.push(Path::new(&home).join(".claude/skills"));
    }

    let mut names = HashSet::new();
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let skill_path = entry.path().join("SKILL.md");
            let Ok(content) = std::fs::read_to_string(&skill_path) else {
                continue;
            };
            if let Some(name) = skill_name_from_skill_file(&skill_path, &content) {
                names.insert(name);
            }
        }
    }
    names
}

fn expected_skill_names_from_prompt(prompt: &str, cwd: Option<&str>) -> HashSet<String> {
    let mut expected = extract_explicit_skill_refs(prompt);
    let known_skills = discover_skill_names(cwd);
    if known_skills.is_empty() {
        return expected;
    }

    let prompt_norm = normalize_phrase_for_match(prompt);
    for skill in known_skills {
        let skill_phrase = normalize_phrase_for_match(&skill.replace(['-', '_', ':'], " "));
        if skill_phrase.is_empty() {
            continue;
        }
        if prompt_norm.contains(&format!("use {skill_phrase}"))
            || prompt_norm.contains(&format!("use the {skill_phrase} skill"))
            || prompt_norm.contains(&format!("{skill_phrase} skill"))
        {
            expected.insert(skill);
        }
    }
    expected
}

fn finalize_hook_skill_turn(session_id: &str, timestamp: &str) {
    let state = {
        let mut states = HOOK_SKILL_TURNS.lock().unwrap();
        states.remove(session_id).unwrap_or_default()
    };

    for skill_name in state.expected.difference(&state.fired) {
        emit_skill_event(SkillTelemetryEvent {
            session_id: session_id.to_string(),
            timestamp: timestamp.to_string(),
            skill_name: skill_name.clone(),
            event_type: "missed".to_string(),
            source: "heuristic".to_string(),
            confidence: 0.9,
            detail: Some("expected skill did not fire before Stop".to_string()),
        });
    }

    for skill_name in state.fired.difference(&state.expected) {
        emit_skill_event(SkillTelemetryEvent {
            session_id: session_id.to_string(),
            timestamp: timestamp.to_string(),
            skill_name: skill_name.clone(),
            event_type: "misfired".to_string(),
            source: "heuristic".to_string(),
            confidence: 0.4,
            detail: Some("skill fired without an explicit expected signal".to_string()),
        });
    }
}

fn hook_session_id(payload: &Value) -> String {
    payload
        .get("session_id")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("unknown")
        .to_string()
}

fn process_claude_code_hook_payload(payload: &Value, timestamp: String) {
    let session_id = hook_session_id(payload);
    let hook_event_name = payload
        .get("hook_event_name")
        .and_then(|value| value.as_str())
        .unwrap_or("");

    match hook_event_name {
        "UserPromptSubmit" => {
            let prompt = payload
                .get("prompt")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let cwd = payload.get("cwd").and_then(|value| value.as_str());
            for skill_name in expected_skill_names_from_prompt(prompt, cwd) {
                emit_skill_event(SkillTelemetryEvent {
                    session_id: session_id.clone(),
                    timestamp: timestamp.clone(),
                    skill_name,
                    event_type: "expected".to_string(),
                    source: "heuristic".to_string(),
                    confidence: 0.8,
                    detail: Some("prompt matched explicit skill trigger".to_string()),
                });
            }
        }
        "UserPromptExpansion" => {
            if payload
                .get("expansion_type")
                .and_then(|value| value.as_str())
                == Some("slash_command")
            {
                let Some(skill_name) = payload
                    .get("command_name")
                    .and_then(|value| value.as_str())
                    .and_then(canonical_telemetry_name)
                else {
                    return;
                };
                let detail = payload
                    .get("command_args")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .map(|value| truncate_detail(value, 100));
                emit_skill_event(SkillTelemetryEvent {
                    session_id: session_id.clone(),
                    timestamp: timestamp.clone(),
                    skill_name: skill_name.clone(),
                    event_type: "expected".to_string(),
                    source: "hook".to_string(),
                    confidence: 1.0,
                    detail: Some("direct slash command expansion".to_string()),
                });
                emit_skill_event(SkillTelemetryEvent {
                    session_id,
                    timestamp,
                    skill_name,
                    event_type: "fired".to_string(),
                    source: "hook".to_string(),
                    confidence: 1.0,
                    detail,
                });
            }
        }
        "PreToolUse" | "PostToolUse" | "PostToolUseFailure" | "PermissionDenied" => {
            let tool_name = payload
                .get("tool_name")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let tool_input = payload.get("tool_input");
            if tool_name.eq_ignore_ascii_case("Skill") {
                let skill_name = tool_input
                    .and_then(skill_name_from_tool_input_value)
                    .unwrap_or_else(|| "unknown".to_string());
                let (event_type, confidence) = match hook_event_name {
                    "PostToolUseFailure" => ("failed", 1.0),
                    "PermissionDenied" => ("failed", 0.9),
                    _ => ("fired", 1.0),
                };
                if hook_event_name != "PostToolUse" {
                    emit_skill_event(SkillTelemetryEvent {
                        session_id,
                        timestamp,
                        skill_name,
                        event_type: event_type.to_string(),
                        source: "hook".to_string(),
                        confidence,
                        detail: summarize_hook_tool_input(tool_input),
                    });
                }
            } else if let Some((server, tool)) = metrics::mcp_tool_labels(tool_name) {
                let event_type = match hook_event_name {
                    "PostToolUse" => "succeeded",
                    "PostToolUseFailure" => "failed",
                    "PermissionDenied" => "denied",
                    _ => "called",
                };
                emit_mcp_event(McpTelemetryEvent {
                    session_id,
                    timestamp,
                    server,
                    tool,
                    event_type: event_type.to_string(),
                    source: "hook".to_string(),
                    detail: summarize_hook_tool_input(tool_input),
                });
            }
        }
        "Stop" | "StopFailure" => {
            finalize_hook_skill_turn(&session_id, &timestamp);
        }
        _ => {}
    }
}

/// Reconstruct the latest turn's tool results by pairing user `tool_result`
/// blocks with prior assistant `tool_use` blocks via `tool_use_id`.
fn parse_latest_tool_results(body: &[u8]) -> Vec<ParsedToolResult> {
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let messages = match v.get("messages").and_then(|m| m.as_array()) {
        Some(msgs) => msgs,
        None => return Vec::new(),
    };

    let Some(last_user_idx) = messages
        .iter()
        .rposition(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))
    else {
        return Vec::new();
    };
    let Some(content) = messages[last_user_idx]
        .get("content")
        .and_then(|c| c.as_array())
    else {
        return Vec::new();
    };

    let mut raw_results = Vec::new();
    let mut referenced_tool_ids = HashSet::new();
    for block in content {
        if block.get("type").and_then(|t| t.as_str()) != Some("tool_result") {
            continue;
        }
        let tool_use_id = block
            .get("tool_use_id")
            .and_then(|id| id.as_str())
            .map(ToString::to_string);
        if let Some(tool_use_id) = tool_use_id.as_ref() {
            referenced_tool_ids.insert(tool_use_id.clone());
        }
        let is_error = block
            .get("is_error")
            .and_then(|e| e.as_bool())
            .unwrap_or(false);
        let outcome = match block.get("is_error").and_then(|e| e.as_bool()) {
            Some(true) => "error".to_string(),
            Some(false) => "success".to_string(),
            None => "unknown".to_string(),
        };
        let duration_ms = block
            .get("duration_ms")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        raw_results.push((tool_use_id, outcome, duration_ms, is_error));
    }

    if raw_results.is_empty() {
        return Vec::new();
    }

    let mut tool_uses_by_id: HashMap<String, (String, String)> = HashMap::new();
    for msg in messages[..last_user_idx].iter().rev() {
        if msg.get("role").and_then(|r| r.as_str()) != Some("assistant") {
            continue;
        }
        let Some(content) = msg.get("content").and_then(|c| c.as_array()) else {
            continue;
        };
        for block in content.iter().rev() {
            if block.get("type").and_then(|t| t.as_str()) != Some("tool_use") {
                continue;
            }
            let Some(tool_use_id) = block.get("id").and_then(|id| id.as_str()) else {
                continue;
            };
            if !referenced_tool_ids.is_empty() && !referenced_tool_ids.contains(tool_use_id) {
                continue;
            }
            if tool_uses_by_id.contains_key(tool_use_id) {
                continue;
            }
            let tool_name = block
                .get("name")
                .and_then(|name| name.as_str())
                .unwrap_or("unknown")
                .to_string();
            let input_json = block
                .get("input")
                .map(|input| serde_json::to_string(input).unwrap_or_default())
                .unwrap_or_default();
            let input_summary = watch::extract_summary(&tool_name, &input_json);
            tool_uses_by_id.insert(tool_use_id.to_string(), (tool_name, input_summary));
        }
        if !referenced_tool_ids.is_empty()
            && referenced_tool_ids
                .iter()
                .all(|tool_use_id| tool_uses_by_id.contains_key(tool_use_id))
        {
            break;
        }
    }

    raw_results
        .into_iter()
        .map(|(tool_use_id, outcome, duration_ms, is_error)| {
            let (tool_name, input_summary) = tool_use_id
                .as_deref()
                .and_then(|id| tool_uses_by_id.get(id))
                .cloned()
                .unwrap_or_else(|| ("unknown".to_string(), String::new()));
            ParsedToolResult {
                tool_name,
                input_summary,
                outcome,
                duration_ms,
                is_error,
            }
        })
        .collect()
}

fn headers_continue() -> HeadersResponse {
    HeadersResponse {
        response: Some(CommonResponse {
            status: ResponseStatus::Continue.into(),
            ..Default::default()
        }),
    }
}
fn body_continue() -> BodyResponse {
    BodyResponse {
        response: Some(CommonResponse {
            status: ResponseStatus::Continue.into(),
            ..Default::default()
        }),
    }
}

fn active_session_count() -> usize {
    diagnosis::SESSIONS
        .iter()
        .filter(|entry| entry.session_inserted)
        .count()
}

// ---------------------------------------------------------------------------
// ext_proc gRPC service
// ---------------------------------------------------------------------------
pub struct ClauditorProcessor;

#[tonic::async_trait]
impl ExternalProcessor for ClauditorProcessor {
    type ProcessStream = ReceiverStream<Result<ProcessingResponse, Status>>;

    async fn process(
        &self,
        request: Request<Streaming<ProcessingRequest>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut request_id = String::new();
            let mut model = String::new();
            let mut started_at = Instant::now();
            let mut resp_acc = ResponseAccumulator::new();
            let mut tool_results: Vec<ParsedToolResult> = Vec::new();
            let mut sys_prompt_hash: u64 = 0;
            let mut working_dir_str = String::new();
            let mut user_prompt_excerpt_buf = String::new();
            let mut request_context_window_hint: Option<u64> = None;
            let mut context_window_tokens = STANDARD_CONTEXT_WINDOW_TOKENS;
            let mut finalized = false;

            loop {
                let msg = match stream.message().await {
                    Ok(Some(r)) => r,
                    Ok(None) => {
                        // Stream closed — finalize if not already done via end_of_stream.
                        if !finalized && !model.is_empty() {
                            finalize_response(
                                &resp_acc,
                                &request_id,
                                &model,
                                &started_at,
                                &tool_results,
                                sys_prompt_hash,
                                &working_dir_str,
                                &user_prompt_excerpt_buf,
                                context_window_tokens,
                            );
                            REQUEST_STATE.remove(&request_id);
                        }
                        break;
                    }
                    Err(e) => {
                        warn!(error = %e, "ext_proc stream error");
                        if !finalized && !model.is_empty() {
                            finalize_response(
                                &resp_acc,
                                &request_id,
                                &model,
                                &started_at,
                                &tool_results,
                                sys_prompt_hash,
                                &working_dir_str,
                                &user_prompt_excerpt_buf,
                                context_window_tokens,
                            );
                            REQUEST_STATE.remove(&request_id);
                        }
                        break;
                    }
                };

                match msg.request {
                    Some(ExtProcRequest::RequestHeaders(ref h)) => {
                        started_at = Instant::now();
                        request_id = extract_header(h, "x-request-id")
                            .unwrap_or_else(|| format!("req_{}", started_at.elapsed().as_nanos()));
                        request_context_window_hint =
                            request_uses_1m_context(h).then_some(EXTENDED_CONTEXT_WINDOW_TOKENS);
                        context_window_tokens = configured_context_window_tokens()
                            .unwrap_or(STANDARD_CONTEXT_WINDOW_TOKENS);

                        // Phase 7: process-wide circuit breaker.
                        if let Some((err_type, msg)) = check_circuit_breaker() {
                            warn!(request_id = %request_id, error_type = err_type, "request blocked");
                            let response = make_block_response(err_type, &msg);
                            if tx.send(Ok(response)).await.is_err() {
                                break;
                            }
                            continue; // stream will close after immediate response
                        }

                        let response = ProcessingResponse {
                            response: Some(ExtProcResponse::RequestHeaders(HeadersResponse {
                                response: Some(CommonResponse {
                                    status: ResponseStatus::Continue.into(),
                                    header_mutation: Some(HeaderMutation {
                                        remove_headers: vec!["accept-encoding".into()],
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                            })),
                            ..Default::default()
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Some(ExtProcRequest::RequestBody(ref b)) => {
                        let parse_start = Instant::now();

                        // Reconstruct tool_result blocks for the latest turn.
                        tool_results = parse_latest_tool_results(&b.body);

                        let mut blocked = false;
                        match parse_request_body(&b.body) {
                            Some((m, mc, ht, sl, et, hash, prefix, user_prompt_excerpt)) => {
                                info!(phase="request_body", request_id=%request_id, model=%m, message_count=mc, has_tools=ht, system_prompt_length=sl, estimated_input_tokens=et, sys_prompt_hash=hash, "ext_proc");
                                if let Some((err_type, msg)) = check_session_budget(
                                    diagnosis::SESSIONS
                                        .get(&hash)
                                        .as_deref()
                                        .map(|state| state.session_id.as_str()),
                                ) {
                                    warn!(request_id = %request_id, error_type = err_type, "request blocked");
                                    let response = make_block_response(err_type, &msg);
                                    if tx.send(Ok(response)).await.is_err() {
                                        break;
                                    }
                                    blocked = true;
                                }
                                sys_prompt_hash = hash;
                                working_dir_str = prefix;
                                // Only first turn carries a meaningful "initial prompt"; for mc>1
                                // messages[0] is still the original, but we only register the
                                // session once (see SESSIONS.get_mut in finalize_response), so
                                // passing the excerpt every time is harmless and first-write-wins.
                                user_prompt_excerpt_buf = user_prompt_excerpt;
                                if !blocked {
                                    context_window_tokens = resolve_context_window_tokens(
                                        request_context_window_hint,
                                        &m,
                                    );
                                    // Session ID resolved later in finalize_response.
                                    REQUEST_STATE.insert(
                                        request_id.clone(),
                                        RequestMeta {
                                            request_id: request_id.clone(),
                                            session_id: String::new(),
                                            model: m.clone(),
                                            message_count: mc,
                                            has_tools: ht,
                                            system_prompt_length: sl,
                                            estimated_input_tokens: et,
                                            started_at,
                                        },
                                    );
                                    model = m;
                                }
                            }
                            None => {
                                warn!(request_id=%request_id, bytes=b.body.len(), "failed to parse request JSON");
                            }
                        }

                        if blocked {
                            continue;
                        }

                        let response = ProcessingResponse {
                            response: Some(ExtProcResponse::RequestBody(body_continue())),
                            ..Default::default()
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }

                        let parse_ms = parse_start.elapsed().as_millis();
                        debug!(request_id=%request_id, parse_ms, "request_body parse time");
                        if parse_ms > 10 {
                            warn!(request_id=%request_id, parse_ms, "request_body parse exceeded 10ms");
                        }
                    }
                    Some(ExtProcRequest::ResponseHeaders(ref h)) => {
                        let status = extract_header(h, ":status")
                            .and_then(|s| s.parse::<u32>().ok())
                            .unwrap_or(0);
                        resp_acc.http_status = status;
                        resp_acc.is_sse = status == 200;

                        // NOTE: Anthropic does not return anthropic-ratelimit-*
                        // headers on Claude Code subscription (OAuth) traffic —
                        // verified empirically. Quota burn is broadcast from a
                        // background task (`quota_burn_monitor`) using our own
                        // token counters instead.
                        let _ = h;

                        // Phase 7: circuit breaker — only track errors from real API requests
                        // (ones where we parsed a model). Ignores envoy DPE/protocol errors.
                        {
                            let mut runtime = RUNTIME_STATE.lock().unwrap();
                            if status >= 400 && !model.is_empty() {
                                runtime.consecutive_errors += 1;
                                let threshold = env_u64("CLAUDITOR_CIRCUIT_BREAKER_THRESHOLD", 5);
                                if runtime.consecutive_errors >= threshold
                                    && runtime.circuit_open_until.is_none()
                                {
                                    runtime.circuit_open_until =
                                        Some(Instant::now() + Duration::from_secs(30));
                                    warn!(
                                        consecutive_errors = runtime.consecutive_errors,
                                        http_status = status,
                                        "circuit breaker tripped — blocking requests for 30s"
                                    );
                                }
                            } else {
                                runtime.consecutive_errors = 0;
                                // Auto-reset circuit breaker on success.
                                if runtime.circuit_open_until.is_some() {
                                    runtime.circuit_open_until = None;
                                    info!("circuit breaker reset after successful response");
                                }
                            }
                        }

                        let response = ProcessingResponse {
                            response: Some(ExtProcResponse::ResponseHeaders(headers_continue())),
                            ..Default::default()
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Some(ExtProcRequest::ResponseBody(ref b)) => {
                        let response = ProcessingResponse {
                            response: Some(ExtProcResponse::ResponseBody(body_continue())),
                            ..Default::default()
                        };
                        let send_ok = tx.send(Ok(response)).await.is_ok();
                        // On end_of_stream, envoy closes the channel — send may fail.
                        // Continue processing to finalize the response regardless.
                        if !send_ok && !b.end_of_stream {
                            break;
                        }
                        if resp_acc.is_sse && !b.body.is_empty() {
                            let chunk_start = Instant::now();
                            resp_acc.process_chunk(&b.body);
                            let chunk_ms = chunk_start.elapsed().as_millis();
                            debug!(request_id=%request_id, chunk_ms, bytes=b.body.len(), "response_body chunk parse time");
                            if chunk_ms > 10 {
                                warn!(request_id=%request_id, chunk_ms, bytes=b.body.len(), "response_body chunk parse exceeded 10ms");
                            }
                        } else if !resp_acc.is_sse && !b.body.is_empty() {
                            // Capture error response body (capped at 1KB).
                            let remaining = 1024usize.saturating_sub(resp_acc.error_body.len());
                            resp_acc
                                .error_body
                                .extend_from_slice(&b.body[..b.body.len().min(remaining)]);
                        }
                        if b.end_of_stream {
                            if !model.is_empty() {
                                finalize_response(
                                    &resp_acc,
                                    &request_id,
                                    &model,
                                    &started_at,
                                    &tool_results,
                                    sys_prompt_hash,
                                    &working_dir_str,
                                    &user_prompt_excerpt_buf,
                                    context_window_tokens,
                                );
                            }
                            REQUEST_STATE.remove(&request_id);
                            finalized = true;
                        }
                    }
                    Some(ExtProcRequest::RequestTrailers(_))
                    | Some(ExtProcRequest::ResponseTrailers(_)) => {
                        continue;
                    }
                    None => {
                        continue;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// ---------------------------------------------------------------------------
// HTTP server (axum): /health, /api/summary, /watch
// ---------------------------------------------------------------------------
use axum::extract::Json;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;

async fn handle_health() -> &'static str {
    "ok"
}

async fn handle_metrics() -> impl IntoResponse {
    metrics::set_active_sessions(active_session_count());
    match metrics::render() {
        Ok((content_type, body)) => {
            let mut headers = axum::http::HeaderMap::new();
            let value = HeaderValue::from_str(&content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("text/plain; version=0.0.4"));
            headers.insert(header::CONTENT_TYPE, value);
            (StatusCode::OK, headers, body).into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn handle_claude_code_hook(Json(payload): Json<Value>) -> impl IntoResponse {
    process_claude_code_hook_payload(&payload, now_iso8601());
    StatusCode::NO_CONTENT
}

async fn handle_summary() -> impl IntoResponse {
    let summary = tokio::task::spawn_blocking(|| {
        let conn = Connection::open(db_path()).ok()?;
        let _ = repair_persisted_session_artifacts(&conn);
        let today = query_summary(&conn, &start_of_today_iso()).ok()?;
        let week = query_summary(&conn, &start_of_week_iso()).ok()?;
        let month = query_summary(&conn, &start_of_month_iso()).ok()?;
        Some(build_summary_response_json(&today, &week, &month))
    })
    .await
    .ok()
    .flatten()
    .unwrap_or(serde_json::json!({"error": "db unavailable"}));

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&summary).unwrap_or_default(),
    )
}

async fn handle_watch(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::sse::Sse<
    impl futures_core::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive, Sse};

    // `?session=X` filters to events for that session AND injects a synthetic
    // SessionStart at stream head so subscribers joining mid-session (e.g. a
    // lazy-discovered tmux pane, or a reattach after the 30s replay window)
    // still see the session header box + initial prompt.
    let session_filter = params.get("session").cloned();

    let (history, mut rx) = watch::BROADCASTER.subscribe_with_history();

    // Look up stored session info synchronously before the stream starts.
    let synthetic_start = session_filter.as_ref().and_then(|sid| {
        diagnosis::SESSIONS.iter().find_map(|entry| {
            let s = entry.value();
            if s.session_id == *sid {
                Some(watch::WatchEvent::SessionStart {
                    session_id: s.session_id.clone(),
                    display_name: s.display_name.clone(),
                    model: s.model.clone(),
                    initial_prompt: s.initial_prompt.clone(),
                })
            } else {
                None
            }
        })
    });

    let stream = async_stream::stream! {
        // Synthetic SessionStart first if we're filtered to a session.
        if let Some(ev) = synthetic_start {
            if let Ok(json) = serde_json::to_string(&ev) {
                yield Ok(Event::default().data(json));
            }
        }

        // Replay recent history, filtered if a session is specified.
        for event in history {
            if !event_matches_session(&event, session_filter.as_deref()) {
                continue;
            }
            if let Ok(json) = serde_json::to_string(&event) {
                yield Ok(Event::default().data(json));
            }
        }
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if !event_matches_session(&event, session_filter.as_deref()) {
                        continue;
                    }
                    if let Ok(json) = serde_json::to_string(&event) {
                        yield Ok(Event::default().data(json));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    let msg = serde_json::json!({"type": "lagged", "missed": n});
                    yield Ok(Event::default().data(msg.to_string()));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

/// True when the event matches the session filter (or no filter is set).
/// Global events (RateLimitStatus, Lagged pseudo-events) are always passed
/// through so clients know about overflow or quota pressure.
fn event_matches_session(ev: &watch::WatchEvent, filter: Option<&str>) -> bool {
    let Some(want) = filter else {
        return true;
    };
    match ev {
        watch::WatchEvent::ToolUse { session_id, .. }
        | watch::WatchEvent::ToolResult { session_id, .. }
        | watch::WatchEvent::SkillEvent { session_id, .. }
        | watch::WatchEvent::McpEvent { session_id, .. }
        | watch::WatchEvent::CacheEvent { session_id, .. }
        | watch::WatchEvent::SessionStart { session_id, .. }
        | watch::WatchEvent::SessionEnd { session_id, .. }
        | watch::WatchEvent::FrustrationSignal { session_id, .. }
        | watch::WatchEvent::CompactionLoop { session_id, .. }
        | watch::WatchEvent::Diagnosis { session_id, .. }
        | watch::WatchEvent::CacheWarning { session_id, .. }
        | watch::WatchEvent::ModelFallback { session_id, .. }
        | watch::WatchEvent::ContextStatus { session_id, .. } => session_id == want,
        watch::WatchEvent::RateLimitStatus { .. } => true,
    }
}

async fn handle_diagnosis(
    axum::extract::Path(session_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(db_path()).ok()?;
        let _ = repair_persisted_session_artifacts(&conn);
        let estimated =
            compute_estimated_costs_for_sessions(&conn, std::slice::from_ref(&session_id))
                .ok()?
                .remove(&session_id)
                .unwrap_or_else(|| CostAccumulator::new().finish());
        let billing = load_latest_billing_reconciliations(&conn, std::slice::from_ref(&session_id))
            .ok()?
            .remove(&session_id);
        let mut stmt = conn
            .prepare(
                "SELECT session_id, completed_at, outcome, total_turns, \
             cache_hit_ratio, degraded, degradation_turn, causes_json, advice_json \
             FROM session_diagnoses WHERE session_id = ?1",
            )
            .ok()?;
        stmt.query_row(rusqlite::params![session_id], |row| {
            let causes_str: String = row.get(7)?;
            let advice_str: String = row.get(8)?;
            Ok(build_diagnosis_response_json(
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                estimated.estimated_cost_dollars,
                estimated.cost_source.clone(),
                estimated.trusted_for_budget_enforcement,
                billing.as_ref().map(|record| record.billed_cost_dollars),
                billing.as_ref().map(|record| record.source.clone()),
                billing.as_ref().map(|record| record.imported_at.clone()),
                row.get::<_, f64>(4)?,
                row.get::<_, i32>(5)? != 0,
                row.get::<_, Option<i64>>(6)?,
                serde_json::from_str::<Value>(&causes_str).unwrap_or(Value::Array(vec![])),
                serde_json::from_str::<Value>(&advice_str).unwrap_or(Value::Array(vec![])),
            ))
        })
        .ok()
    })
    .await
    .ok()
    .flatten();

    match result {
        Some(report) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            serde_json::to_string_pretty(&report).unwrap_or_default(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn handle_sessions(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let limit: i64 = params
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(20);
    let days: i64 = params.get("days").and_then(|d| d.parse().ok()).unwrap_or(7);

    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(db_path()).ok()?;
        let _ = repair_persisted_session_artifacts(&conn);
        let since_secs = now_epoch_secs() - (days as u64 * 86400);
        let since = epoch_to_iso8601(since_secs);

        let mut stmt = conn
            .prepare(
                "SELECT d.session_id, s.started_at, d.outcome, d.degraded, d.total_turns, \
             d.causes_json, d.cache_hit_ratio, s.model \
             FROM session_diagnoses d \
             LEFT JOIN sessions s ON d.session_id = s.session_id \
             WHERE d.completed_at >= ?1 \
             ORDER BY d.completed_at DESC LIMIT ?2",
            )
            .ok()?;

        let session_rows: Vec<(
            String,
            Option<String>,
            String,
            bool,
            i64,
            String,
            f64,
            Option<String>,
        )> = stmt
            .query_map(rusqlite::params![since, limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i32>(3)? != 0,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, f64>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            })
            .ok()?
            .filter_map(|r| r.ok())
            .collect();

        let session_ids = session_rows
            .iter()
            .map(|row| row.0.clone())
            .collect::<Vec<_>>();
        let estimated_costs = compute_estimated_costs_for_sessions(&conn, &session_ids).ok()?;
        let billing = load_latest_billing_reconciliations(&conn, &session_ids).ok()?;

        let sessions: Vec<Value> = session_rows
            .into_iter()
            .map(
                |(
                    session_id,
                    started_at,
                    outcome,
                    degraded,
                    total_turns,
                    causes_str,
                    cache_hit_ratio,
                    model,
                )| {
                    let causes: Vec<Value> = serde_json::from_str(&causes_str).unwrap_or_default();
                    let primary_cause = causes
                        .first()
                        .and_then(|c| c.get("cause_type"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("")
                        .to_string();
                    let estimated = estimated_costs
                        .get(&session_id)
                        .cloned()
                        .unwrap_or_else(|| CostAccumulator::new().finish());
                    let billed = billing.get(&session_id);
                    build_session_summary_json(
                        session_id,
                        started_at,
                        outcome,
                        degraded,
                        total_turns,
                        estimated.estimated_cost_dollars,
                        estimated.cost_source,
                        estimated.trusted_for_budget_enforcement,
                        billed.map(|record| record.billed_cost_dollars),
                        billed.map(|record| record.source.clone()),
                        billed.map(|record| record.imported_at.clone()),
                        primary_cause,
                        cache_hit_ratio,
                        model,
                    )
                },
            )
            .collect();

        Some(build_sessions_response_json(sessions))
    })
    .await
    .ok()
    .flatten()
    .unwrap_or(serde_json::json!({"sessions": []}));

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&result).unwrap_or_default(),
    )
}

#[derive(Debug, Deserialize)]
struct BillingReconciliationInput {
    session_id: String,
    source: String,
    billed_cost_dollars: f64,
    #[serde(default)]
    imported_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BillingReconciliationRequest {
    Single(BillingReconciliationInput),
    Batch {
        reconciliations: Vec<BillingReconciliationInput>,
    },
}

async fn persist_billing_reconciliation(
    tx: &std_mpsc::Sender<DbCommand>,
    record: BillingReconciliationInput,
) -> Result<(), BillingReconciliationWriteError> {
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(DbCommand::WriteBillingReconciliation {
        session_id: record.session_id,
        imported_at: record.imported_at.unwrap_or_else(now_iso8601),
        source: record.source,
        billed_cost_dollars: record.billed_cost_dollars,
        response_tx,
    })
    .map_err(|_| BillingReconciliationWriteError::DbUnavailable)?;

    response_rx
        .await
        .map_err(|_| BillingReconciliationWriteError::DbUnavailable)?
}

async fn handle_billing_reconciliations(
    Json(payload): Json<BillingReconciliationRequest>,
) -> impl IntoResponse {
    let records = match payload {
        BillingReconciliationRequest::Single(record) => vec![record],
        BillingReconciliationRequest::Batch { reconciliations } => reconciliations,
    };

    if records.is_empty() {
        let body = serde_json::json!({
            "inserted": 0,
            "error": "missing reconciliations"
        });
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/json")],
            serde_json::to_string_pretty(&body).unwrap_or_default(),
        )
            .into_response();
    }

    let mut inserted = 0usize;
    for record in records {
        if record.session_id.trim().is_empty()
            || record.source.trim().is_empty()
            || !record.billed_cost_dollars.is_finite()
            || record.billed_cost_dollars < 0.0
        {
            let body = serde_json::json!({
                "inserted": inserted,
                "error": "invalid reconciliation payload"
            });
            return (
                StatusCode::BAD_REQUEST,
                [("content-type", "application/json")],
                serde_json::to_string_pretty(&body).unwrap_or_default(),
            )
                .into_response();
        }

        match persist_billing_reconciliation(&DB_TX, record).await {
            Ok(()) => inserted += 1,
            Err(BillingReconciliationWriteError::DbUnavailable) => {
                let body = serde_json::json!({
                    "inserted": inserted,
                    "error": "db unavailable"
                });
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    [("content-type", "application/json")],
                    serde_json::to_string_pretty(&body).unwrap_or_default(),
                )
                    .into_response();
            }
            Err(BillingReconciliationWriteError::UnknownSession(session_id)) => {
                let body = serde_json::json!({
                    "inserted": inserted,
                    "error": format!("unknown session_id: {session_id}")
                });
                return (
                    StatusCode::NOT_FOUND,
                    [("content-type", "application/json")],
                    serde_json::to_string_pretty(&body).unwrap_or_default(),
                )
                    .into_response();
            }
            Err(BillingReconciliationWriteError::Sqlite(err)) => {
                let body = serde_json::json!({
                    "inserted": inserted,
                    "error": format!("failed to persist reconciliation: {err}")
                });
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [("content-type", "application/json")],
                    serde_json::to_string_pretty(&body).unwrap_or_default(),
                )
                    .into_response();
            }
        }
    }

    let body = serde_json::json!({
        "inserted": inserted,
    });
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&body).unwrap_or_default(),
    )
        .into_response()
}

fn normalize_search_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut last_was_space = true;
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_space = false;
        } else if !last_was_space {
            out.push(' ');
            last_was_space = true;
        }
    }
    out.trim().to_string()
}

fn tokenize_search_text(text: &str) -> Vec<String> {
    normalize_search_text(text)
        .split_whitespace()
        .filter(|term| term.len() >= 2)
        .map(|term| term.to_string())
        .collect()
}

fn search_term_set(text: &str) -> HashSet<String> {
    tokenize_search_text(text).into_iter().collect()
}

fn score_recall_doc(
    query: &str,
    query_terms: &[String],
    initial_prompt: &str,
    final_response_summary: &str,
    model: &str,
) -> Option<i64> {
    let query_norm = normalize_search_text(query);
    let prompt_norm = normalize_search_text(initial_prompt);
    let summary_norm = normalize_search_text(final_response_summary);
    let model_terms = search_term_set(model);
    let prompt_terms = search_term_set(initial_prompt);
    let summary_terms = search_term_set(final_response_summary);

    let mut score = 0i64;
    let mut matched_terms = 0usize;

    if !query_norm.is_empty() {
        if prompt_norm.contains(&query_norm) {
            score += 48;
        }
        if summary_norm.contains(&query_norm) {
            score += 42;
        }
    }

    for term in query_terms {
        let mut hit = false;
        if prompt_terms.contains(term) {
            score += 18;
            hit = true;
        }
        if summary_terms.contains(term) {
            score += 14;
            hit = true;
        }
        if model_terms.contains(term) {
            score += 4;
            hit = true;
        }
        if hit {
            matched_terms += 1;
        }
    }

    if matched_terms == 0 && score == 0 {
        return None;
    }

    if !query_terms.is_empty()
        && query_terms.iter().all(|term| {
            prompt_terms.contains(term)
                || summary_terms.contains(term)
                || model_terms.contains(term)
        })
    {
        score += 20;
    }

    Some(score)
}

async fn handle_recall(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();
    let query_terms = tokenize_search_text(&query);
    let limit: usize = params
        .get("limit")
        .and_then(|l| l.parse().ok())
        .unwrap_or(5usize);
    let days: i64 = params
        .get("days")
        .and_then(|d| d.parse().ok())
        .unwrap_or(30i64);

    if query.trim().is_empty() {
        let body = serde_json::json!({
            "query": query,
            "hits": [],
            "error": "missing query"
        });
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/json")],
            serde_json::to_string_pretty(&body).unwrap_or_default(),
        );
    }

    let query_for_search = query.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<Value, String> {
        let conn = Connection::open(db_path()).map_err(|e| format!("open sqlite: {e}"))?;
        let _ = repair_persisted_session_artifacts(&conn);
        let since_secs = now_epoch_secs().saturating_sub(days.max(0) as u64 * 86400);
        let since = epoch_to_iso8601(since_secs);
        let mut stmt = conn
            .prepare(
            "SELECT r.session_id, s.started_at, s.model, d.completed_at, d.outcome, \
             r.initial_prompt, r.final_response_summary \
             FROM session_recall r \
             LEFT JOIN sessions s ON r.session_id = s.session_id \
             LEFT JOIN session_diagnoses d ON r.session_id = d.session_id \
             WHERE d.completed_at >= ?1 \
                OR (d.completed_at IS NULL AND (s.started_at >= ?1 OR s.started_at IS NULL)) \
             ORDER BY COALESCE(d.completed_at, s.started_at, '') DESC"
            )
            .map_err(|e| format!("prepare recall query: {e}"))?;

        let mut hits: Vec<(i64, String, Value)> = stmt
            .query_map(rusqlite::params![since], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })
            .map_err(|e| format!("query recall rows: {e}"))?
            .filter_map(|row| match row {
                Ok(row) => Some(row),
                Err(err) => {
                    warn!(error = %err, "skipping malformed recall row");
                    None
                }
            })
            .filter_map(|(session_id, started_at, model, completed_at, outcome, initial_prompt, final_response_summary)| {
                let initial_prompt = initial_prompt.unwrap_or_default();
                let final_response_summary = final_response_summary.unwrap_or_default();
                let model = model.unwrap_or_default();
                let score = score_recall_doc(
                    &query_for_search,
                    &query_terms,
                    &initial_prompt,
                    &final_response_summary,
                    &model,
                )?;
                Some((
                    score,
                    completed_at.clone().unwrap_or_default(),
                    serde_json::json!({
                        "session_id": session_id,
                        "started_at": started_at,
                        "completed_at": completed_at,
                        "model": if model.is_empty() { None::<String> } else { Some(model.clone()) },
                        "outcome": outcome,
                        "initial_prompt": initial_prompt,
                        "final_response_summary": final_response_summary,
                        "score": score,
                    }),
                ))
            })
            .collect();

        hits.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        let hits = hits
            .into_iter()
            .take(limit)
            .map(|(_, _, hit)| hit)
            .collect::<Vec<_>>();

        Ok(serde_json::json!({
            "query": query_for_search,
            "hits": hits,
        }))
    })
    .await;

    match result {
        Ok(Ok(body)) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            serde_json::to_string_pretty(&body).unwrap_or_default(),
        ),
        Ok(Err(err)) => {
            warn!(error = %err, "recall search failed");
            let body = serde_json::json!({
                "query": query,
                "hits": [],
                "error": "db unavailable"
            });
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "application/json")],
                serde_json::to_string_pretty(&body).unwrap_or_default(),
            )
        }
        Err(err) => {
            warn!(error = %err, "recall search task failed");
            let body = serde_json::json!({
                "query": query,
                "hits": [],
                "error": "internal error"
            });
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "application/json")],
                serde_json::to_string_pretty(&body).unwrap_or_default(),
            )
        }
    }
}

async fn handle_cache_rebuilds(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let days: u64 = params.get("days").and_then(|d| d.parse().ok()).unwrap_or(7);

    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(db_path()).ok()?;
        let _ = repair_persisted_session_artifacts(&conn);
        let since_secs = now_epoch_secs() - (days * 86400);
        let since = epoch_to_iso8601(since_secs);

        // Find turns where cache was fully rebuilt (creation > 0, read == 0).
        let mut stmt = conn
            .prepare(
                "SELECT turn_number, cache_creation_tokens, cache_read_tokens, gap_from_prev_secs, \
             input_tokens \
             FROM turn_snapshots WHERE timestamp >= ?1",
            )
            .ok()?;

        let mut total_rebuilds: u64 = 0;
        let mut idle_gap_rebuilds: u64 = 0;
        let mut rebuilds_without_idle_gap: u64 = 0;
        let mut cold_start_builds: u64 = 0;
        let mut tokens_wasted: u64 = 0;
        let mut total_tokens: u64 = 0;
        let mut longest_gap: f64 = 0.0;

        let rows = stmt
            .query_map(rusqlite::params![since], |row| {
                Ok((
                    row.get::<_, i64>(0)?, // turn_number
                    row.get::<_, i64>(1)?, // cache_creation_tokens
                    row.get::<_, i64>(2)?, // cache_read_tokens
                    row.get::<_, f64>(3)?, // gap_from_prev_secs
                    row.get::<_, i64>(4)?, // input_tokens
                ))
            })
            .ok()?;

        for row in rows.flatten() {
            let (turn_number, cache_create, cache_read, gap, input) = row;
            total_tokens += (input + cache_read + cache_create) as u64;

            if cache_create > 0 && cache_read == 0 {
                if turn_number <= 1 {
                    cold_start_builds += 1;
                    continue;
                }
                total_rebuilds += 1;
                tokens_wasted += cache_create as u64;
                if gap > 300.0 {
                    idle_gap_rebuilds += 1;
                    if gap > longest_gap {
                        longest_gap = gap;
                    }
                } else {
                    rebuilds_without_idle_gap += 1;
                }
            }
        }

        let wasted_ratio = if total_tokens > 0 {
            tokens_wasted as f64 / total_tokens as f64
        } else {
            0.0
        };

        Some(serde_json::json!({
            "period_days": days,
            "total_rebuilds": total_rebuilds,
            "cold_start_builds": cold_start_builds,
            "rebuilds_from_idle_gaps": idle_gap_rebuilds,
            "rebuilds_without_idle_gap": rebuilds_without_idle_gap,
            "tokens_wasted_on_rebuilds": tokens_wasted,
            "tokens_wasted_ratio": (wasted_ratio * 1000.0).round() / 1000.0,
            "longest_gap_before_rebuild_secs": longest_gap.round() as u64,
        }))
    })
    .await
    .ok()
    .flatten()
    .unwrap_or(serde_json::json!({"error": "db unavailable"}));

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&result).unwrap_or_default(),
    )
}

fn load_degradation_view_from_db(conn: &Connection, session_id: &str) -> Option<Value> {
    let _ = repair_persisted_session_artifacts(conn);

    let diag: Option<(bool, Option<i64>)> = conn
        .prepare("SELECT degraded, degradation_turn FROM session_diagnoses WHERE session_id = ?1")
        .ok()?
        .query_row(rusqlite::params![session_id], |row| {
            Ok((row.get::<_, i32>(0)? != 0, row.get::<_, Option<i64>>(1)?))
        })
        .ok();

    let (degraded, degradation_turn) = diag.unwrap_or((false, None));
    let degradation_turn = if degraded { degradation_turn } else { None };

    let mut stmt = conn
        .prepare(
            "SELECT turn_number, input_tokens, cache_read_tokens, cache_creation_tokens, \
             output_tokens, ttft_ms, gap_from_prev_secs, context_utilization, \
             context_window_tokens, tool_failures, requested_model, actual_model \
             FROM turn_snapshots WHERE session_id = ?1 ORDER BY turn_number",
        )
        .ok()?;

    struct TurnRow {
        turn: i64,
        input: i64,
        cache_read: i64,
        cache_create: i64,
        output: i64,
        ttft_ms: i64,
        gap: f64,
        ctx: f64,
        context_window_tokens: i64,
        failures: i64,
        requested_model: Option<String>,
        actual_model: Option<String>,
    }

    let turns: Vec<TurnRow> = stmt
        .query_map(rusqlite::params![session_id], |row| {
            let input = row.get::<_, i64>(1)?.max(0);
            let cache_read = row.get::<_, i64>(2)?.max(0);
            let cache_create = row.get::<_, i64>(3)?.max(0);
            let requested_model = row.get::<_, Option<String>>(10)?;
            let actual_model = row.get::<_, Option<String>>(11)?;
            let context_window_tokens = row
                .get::<_, Option<i64>>(8)?
                .map(|value| value.max(0))
                .filter(|value| *value > 0)
                .unwrap_or_else(|| {
                    infer_context_window_tokens(
                        requested_model.as_deref(),
                        actual_model.as_deref(),
                        input.max(0) as u64,
                        cache_read.max(0) as u64,
                        cache_create.max(0) as u64,
                    ) as i64
                });
            Ok(TurnRow {
                turn: row.get(0)?,
                input,
                cache_read,
                cache_create,
                output: row.get(4)?,
                ttft_ms: row.get(5)?,
                gap: row.get(6)?,
                ctx: context_fill_ratio(
                    input.max(0) as u64,
                    cache_read.max(0) as u64,
                    cache_create.max(0) as u64,
                    context_window_tokens.max(1) as u64,
                ),
                context_window_tokens,
                failures: row.get(9)?,
                requested_model,
                actual_model,
            })
        })
        .ok()?
        .filter_map(|r| r.ok())
        .collect();

    if turns.is_empty() {
        return Some(serde_json::json!({
            "session_id": session_id,
            "degraded": degraded,
            "degradation_turn": degradation_turn,
            "total_turns": 0,
            "turns": [],
        }));
    }

    let mut ttfts: Vec<i64> = turns.iter().map(|t| t.ttft_ms).collect();
    ttfts.sort();
    let median_ttft = ttfts[ttfts.len() / 2] as f64;

    let turn_data: Vec<Value> = turns
        .iter()
        .map(|t| {
            let mut flags: Vec<&str> = Vec::new();
            if t.cache_create > 0 && t.cache_read == 0 {
                if t.turn <= 1 {
                    flags.push("cold_start");
                } else if t.gap > 300.0 {
                    flags.push("cache_miss_ttl");
                } else {
                    flags.push("cache_miss_thrash");
                }
            }
            if t.ctx > 0.60 {
                flags.push("context_bloat");
            }
            if median_ttft > 0.0 && t.ttft_ms as f64 > median_ttft * 2.0 {
                flags.push("latency_spike");
            }
            if t.failures > 0 {
                flags.push("tool_failures");
            }
            if let (Some(requested), Some(actual)) =
                (t.requested_model.as_deref(), t.actual_model.as_deref())
            {
                if !model_matches(requested, actual) {
                    flags.push("model_fallback");
                }
            }
            serde_json::json!({
                "turn": t.turn,
                "input_tokens": t.input,
                "cache_read_tokens": t.cache_read,
                "cache_creation_tokens": t.cache_create,
                "output_tokens": t.output,
                "turn_duration_ms": t.ttft_ms,
                "gap_from_prev_secs": t.gap,
                "context_utilization": (t.ctx * 1000.0).round() / 1000.0,
                "context_window_tokens": t.context_window_tokens,
                "tool_failures": t.failures,
                "requested_model": t.requested_model.clone(),
                "actual_model": t.actual_model.clone(),
                "flags": flags,
            })
        })
        .collect();

    Some(serde_json::json!({
        "session_id": session_id,
        "degraded": degraded,
        "degradation_turn": degradation_turn,
        "total_turns": turns.len(),
        "turns": turn_data,
    }))
}

async fn handle_degradation(
    axum::extract::Path(session_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(db_path()).ok()?;
        load_degradation_view_from_db(&conn, &session_id)
    })
    .await
    .ok()
    .flatten();

    match result {
        Some(data) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            serde_json::to_string_pretty(&data).unwrap_or_default(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

async fn http_server() {
    let app = Router::new()
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .route("/api/hooks/claude-code", post(handle_claude_code_hook))
        .route("/api/summary", get(handle_summary))
        .route("/api/recall", get(handle_recall))
        .route(
            "/api/billing-reconciliations",
            post(handle_billing_reconciliations),
        )
        .route("/api/diagnosis/:session_id", get(handle_diagnosis))
        .route("/api/degradation/:session_id", get(handle_degradation))
        .route("/api/cache-rebuilds", get(handle_cache_rebuilds))
        .route("/api/sessions", get(handle_sessions))
        .route("/watch", get(handle_watch));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9090")
        .await
        .expect("failed to bind :9090");
    info!("HTTP server listening on 0.0.0.0:9090 (/health, /metrics, /api/hooks/claude-code, /api/summary, /api/recall, /api/billing-reconciliations, /api/sessions, /api/cache-rebuilds, /api/degradation, /watch)");
    axum::serve(listener, app).await.expect("HTTP server error");
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------
async fn cleanup_stale_requests() {
    let ttl = Duration::from_secs(300);
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let before = REQUEST_STATE.len();
        REQUEST_STATE.retain(|_, v| v.started_at.elapsed() < ttl);
        let removed = before - REQUEST_STATE.len();
        if removed > 0 {
            info!(removed, "cleaned up stale request metadata");
        }
    }
}

/// Sample quota burn every 30s so the burn-rate projection and Prometheus
/// gauges stay fresh, but only broadcast the user-facing watch snapshot every
/// five minutes unless the exhaustion alarm flips state. Anthropic does not
/// return rate-limit headers on subscription (OAuth) traffic, so this is
/// computed entirely from our own counters. If the user sets
/// `CLAUDITOR_WEEKLY_TOKEN_BUDGET` we can also surface remaining + projected
/// exhaustion; otherwise we auto-suggest a weekly cap from the last four
/// completed weeks of SQLite history.
async fn quota_burn_monitor() {
    let mut last_broadcast_at: Option<Instant> = None;
    let mut last_alarm: Option<bool> = None;

    loop {
        tokio::time::sleep(QUOTA_SAMPLE_INTERVAL).await;

        // Snapshot the cumulative token counter.
        let total_tokens = {
            let runtime = RUNTIME_STATE.lock().unwrap();
            runtime.total_tokens
        };

        let per_sec = {
            let mut t = BURN_TRACKER.lock().unwrap();
            t.record(total_tokens);
            t.tokens_per_sec()
        };
        let used_this_week = this_week_tokens_used();
        let weekly = env_u64("CLAUDITOR_WEEKLY_TOKEN_BUDGET", 0);
        let (tokens_limit, budget_source) = if weekly > 0 {
            (Some(weekly), Some("env".to_string()))
        } else if let Some(auto) = auto_weekly_budget_suggestion() {
            (Some(auto.tokens_limit), Some("auto_p95_4w".to_string()))
        } else {
            (None, None)
        };
        let remaining = tokens_limit.map(|limit| limit.saturating_sub(used_this_week));
        let projected_exhaustion_secs = match (remaining, per_sec) {
            (Some(r), Some(rate)) if rate > 0.0 && r > 0 => Some((r as f64 / rate).round() as u64),
            _ => None,
        };

        metrics::update_weekly_budget_gauges(
            used_this_week,
            remaining,
            tokens_limit,
            budget_source.as_deref(),
            projected_exhaustion_secs,
        );

        // Skip watch broadcast until we've actually seen traffic — no point
        // telling the orchestrator "0 tokens, 0 burn" before the first turn.
        if total_tokens == 0 {
            continue;
        }

        let _ = per_sec; // currently displayed indirectly via projection
        let seconds_to_reset = Some(seconds_until_weekly_reset());
        let alarm = projected_exhaustion_secs
            .zip(seconds_to_reset)
            .map(|(exhaustion, reset)| exhaustion < reset)
            .unwrap_or(false);
        let now = Instant::now();
        let should_broadcast = should_broadcast_quota_snapshot(
            last_broadcast_at.map(|sent_at| now.duration_since(sent_at)),
            last_alarm,
            alarm,
        );
        last_alarm = Some(alarm);
        if !should_broadcast {
            continue;
        }
        last_broadcast_at = Some(now);

        watch::BROADCASTER.broadcast(watch::WatchEvent::RateLimitStatus {
            seconds_to_reset,
            requests_remaining: None,
            requests_limit: None,
            input_tokens_remaining: None,
            output_tokens_remaining: None,
            tokens_used_this_week: Some(used_this_week),
            tokens_limit,
            tokens_remaining: remaining,
            budget_source,
            projected_exhaustion_secs,
        });
    }
}

async fn historical_metrics_monitor() {
    loop {
        let refreshed_at_epoch = now_epoch_secs();
        let refresh = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(db_path())?;
            let _ = repair_persisted_session_artifacts(&conn);
            query_historical_metrics(&conn, refreshed_at_epoch)
        })
        .await;

        match refresh {
            Ok(Ok(windows)) => {
                metrics::update_historical_gauges(&windows, refreshed_at_epoch);
            }
            Ok(Err(err)) => {
                warn!(error = %err, "failed to refresh historical metrics");
            }
            Err(err) => {
                warn!(error = %err, "historical metrics refresh task failed");
            }
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Sum of tokens spent this week. Reads from SQLite so we don't lose history
/// across core restarts. Weeks start Monday 00:00 UTC — matches the rhythm
/// of Anthropic's publicized weekly caps.
fn this_week_tokens_used() -> u64 {
    let week_start = start_of_week_iso();
    tokio::task::block_in_place(|| {
        let conn = match Connection::open(db_path()) {
            Ok(c) => c,
            Err(_) => return 0u64,
        };
        let mut stmt = match conn.prepare(
            "SELECT COALESCE(SUM(input_tokens + output_tokens + cache_read_tokens + cache_creation_tokens), 0) \
             FROM requests WHERE timestamp >= ?1",
        ) {
            Ok(s) => s,
            Err(_) => return 0u64,
        };
        stmt.query_row(rusqlite::params![week_start], |row| row.get::<_, i64>(0))
            .map(|n| n.max(0) as u64)
            .unwrap_or(0)
    })
}

/// Seconds until Monday 00:00 UTC.
fn seconds_until_weekly_reset() -> u64 {
    let now = now_epoch_secs();
    let next_reset = start_of_week_epoch_at(now) + 7 * 86_400;
    next_reset.saturating_sub(now)
}

async fn data_retention_cleanup() {
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await; // hourly
        let _ = tokio::task::spawn_blocking(|| {
            let conn = match Connection::open(db_path()) {
                Ok(c) => c,
                Err(_) => return,
            };
            let now = now_epoch_secs();
            let thirty_days_ago = epoch_to_iso8601(now - 30 * 86400);
            let ninety_days_ago = epoch_to_iso8601(now - 90 * 86400);

            let _ = conn.execute(
                "DELETE FROM turn_snapshots WHERE timestamp < ?1",
                rusqlite::params![thirty_days_ago],
            );
            let _ = conn.execute(
                "DELETE FROM tool_outcomes WHERE timestamp < ?1",
                rusqlite::params![thirty_days_ago],
            );
            let _ = conn.execute(
                "DELETE FROM session_recall WHERE session_id IN \
                 (SELECT session_id FROM session_diagnoses WHERE completed_at < ?1)",
                rusqlite::params![ninety_days_ago],
            );
            let _ = conn.execute(
                "DELETE FROM session_diagnoses WHERE completed_at < ?1",
                rusqlite::params![ninety_days_ago],
            );
            info!("data retention cleanup complete");
        })
        .await;
    }
}

// ---------------------------------------------------------------------------
// Per-session monitors: inactivity timeout and cache expiry warning.
// Both use periodic scanning of diagnosis::SESSIONS instead of a global Notify.
// ---------------------------------------------------------------------------
async fn cache_expiry_warning_monitor() {
    let warning_secs = env_u64("CLAUDITOR_CACHE_WARNING_SECS", 240);
    let ttl_secs: u64 = 300; // Anthropic prompt cache TTL
    let check_interval = Duration::from_secs(30);

    loop {
        tokio::time::sleep(check_interval).await;
        let now = Instant::now();

        for mut entry in diagnosis::SESSIONS.iter_mut() {
            if !entry.session_inserted || entry.cache_warning_sent {
                continue;
            }
            let idle = now.duration_since(entry.last_activity).as_secs();
            if idle >= warning_secs {
                entry.cache_warning_sent = true;
                let remaining = ttl_secs.saturating_sub(idle);
                watch::BROADCASTER.broadcast(watch::WatchEvent::CacheWarning {
                    session_id: entry.session_id.clone(),
                    idle_secs: idle,
                    ttl_secs: remaining,
                });
                info!(session_id = %entry.session_id, idle_secs = idle, remaining_secs = remaining,
                    "cache expiry warning broadcast");
            }
        }
    }
}

async fn session_inactivity_monitor() {
    let timeout_mins = env_u64("CLAUDITOR_SESSION_TIMEOUT_MINUTES", 5);
    let timeout_secs = timeout_mins * 60;
    let check_interval = Duration::from_secs(30);

    loop {
        tokio::time::sleep(check_interval).await;
        let now = Instant::now();

        // Collect expired session hashes (can't remove while iterating).
        let mut expired: Vec<(u64, String)> = Vec::new();
        for entry in diagnosis::SESSIONS.iter() {
            let idle = now.duration_since(entry.last_activity).as_secs();
            if idle > timeout_secs {
                expired.push((*entry.key(), entry.session_id.clone()));
            }
        }

        for (hash, sid) in expired {
            if let Some((_, state)) = diagnosis::SESSIONS.remove(&hash) {
                metrics::set_active_sessions(active_session_count());
                info!(session_id = %sid, timeout_mins, "session ended (inactivity timeout)");
                end_session(
                    &sid,
                    if state.session_inserted {
                        Some(state.model)
                    } else {
                        None
                    },
                    state.initial_prompt,
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_target(false)
        .json()
        .with_env_filter(filter)
        .init();

    info!("clauditor-core v{}", env!("CARGO_PKG_VERSION"));
    info!("per-terminal session tracking enabled");

    // Ensure /data directory exists for SQLite.
    let _ = std::fs::create_dir_all(
        std::path::Path::new(&db_path())
            .parent()
            .unwrap_or(std::path::Path::new("/data")),
    );

    // Initialize DB writer thread.
    let _ = &*DB_TX;

    // Load pricing catalog once at startup so all pricing decisions share the
    // same resolver state until the next process restart.
    let _ = &*pricing::PRICING_CATALOG;

    // Initialize the event broadcaster.
    let _ = &*watch::BROADCASTER;
    metrics::init();
    metrics::set_active_sessions(active_session_count());

    tokio::spawn(http_server());
    tokio::spawn(cleanup_stale_requests());
    tokio::spawn(session_inactivity_monitor());
    tokio::spawn(cache_expiry_warning_monitor());
    tokio::spawn(data_retention_cleanup());
    tokio::spawn(quota_burn_monitor());
    tokio::spawn(historical_metrics_monitor());

    let addr = "0.0.0.0:50051".parse()?;
    info!(%addr, "gRPC ext_proc server starting");

    Server::builder()
        .add_service(ExternalProcessorServer::new(ClauditorProcessor))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;

    use super::{
        build_diagnosis_response_json, build_session_summary_json, build_sessions_response_json,
        build_summary_response_json, canonical_telemetry_name, compact_response_summary,
        db_writer_loop, ensure_session_columns, epoch_to_iso8601, extract_explicit_skill_refs,
        load_degradation_view_from_db, metrics, now_epoch_secs, parse_latest_tool_results,
        persist_billing_reconciliation, pricing, query_historical_metrics, query_summary,
        repair_persisted_session_artifacts, repair_turn_snapshot_context_windows,
        request_uses_1m_context, resolve_context_window_tokens, seed_live_metric_labels_from_db,
        session_timeout_secs, should_broadcast_quota_snapshot, skill_name_from_tool_input_json,
        BillingReconciliationInput, BillingReconciliationWriteError, DbCommand, HttpHeaders,
        ParsedToolResult, ProtoHeaderValue, SummaryWindowData, ESTIMATED_COST_SOURCE,
        EXTENDED_CONTEXT_WINDOW_TOKENS, QUOTA_WATCH_BROADCAST_INTERVAL, SCHEMA,
        STANDARD_CONTEXT_WINDOW_TOKENS,
    };

    static METRICS_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn create_history_test_db() -> Connection {
        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                model TEXT,
                initial_prompt TEXT
            );
            CREATE TABLE requests (
                request_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                model TEXT NOT NULL,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cache_read_tokens INTEGER DEFAULT 0,
                cache_creation_tokens INTEGER DEFAULT 0,
                cost_dollars REAL,
                cost_source TEXT,
                trusted_for_budget_enforcement INTEGER DEFAULT 0,
                duration_ms INTEGER,
                tool_calls TEXT,
                cache_event TEXT
            );
            CREATE TABLE session_diagnoses (
                session_id TEXT PRIMARY KEY,
                completed_at TEXT NOT NULL,
                outcome TEXT NOT NULL,
                total_turns INTEGER,
                total_cost REAL,
                cache_hit_ratio REAL,
                degraded INTEGER DEFAULT 0,
                degradation_turn INTEGER,
                causes_json TEXT,
                advice_json TEXT
            );
            CREATE TABLE turn_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                turn_number INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                input_tokens INTEGER,
                cache_read_tokens INTEGER DEFAULT 0,
                cache_creation_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER,
                ttft_ms INTEGER,
                tool_calls TEXT,
                tool_failures INTEGER DEFAULT 0,
                gap_from_prev_secs REAL,
                context_utilization REAL,
                context_window_tokens INTEGER,
                frustration_signals INTEGER DEFAULT 0,
                requested_model TEXT,
                actual_model TEXT,
                response_summary TEXT
            );
            CREATE TABLE tool_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                turn_number INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                input_summary TEXT,
                outcome TEXT,
                duration_ms INTEGER
            );
            CREATE TABLE billing_reconciliations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                source TEXT NOT NULL,
                billed_cost_dollars REAL NOT NULL
            );",
        )
        .expect("create test schema");
        conn
    }

    fn create_full_test_db() -> Connection {
        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(SCHEMA).expect("create full test schema");
        conn
    }

    fn metric_line_has(body: &str, metric: &str, labels: &[&str], value: &str) -> bool {
        body.lines().any(|line| {
            line.starts_with(metric)
                && labels.iter().all(|label| line.contains(label))
                && line.ends_with(value)
        })
    }

    #[test]
    fn ensure_session_columns_drops_legacy_cache_hit_ratio() {
        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                total_input_tokens INTEGER DEFAULT 0,
                total_output_tokens INTEGER DEFAULT 0,
                total_cache_read_tokens INTEGER DEFAULT 0,
                total_cache_creation_tokens INTEGER DEFAULT 0,
                total_cost_dollars REAL DEFAULT 0.0,
                cache_hit_ratio REAL,
                cache_waste_dollars REAL DEFAULT 0.0,
                request_count INTEGER DEFAULT 0,
                model TEXT
            );",
        )
        .expect("create legacy sessions table");

        ensure_session_columns(&conn).expect("migrate sessions columns");

        let columns = conn
            .prepare("PRAGMA table_info(sessions)")
            .expect("prepare pragma")
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query pragma")
            .filter_map(|row| row.ok())
            .collect::<Vec<_>>();

        assert!(columns.contains(&"initial_prompt".to_string()));
        assert!(!columns.contains(&"cache_hit_ratio".to_string()));
    }

    fn unique_test_db_path(label: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!(
            "clauditor-{label}-{}-{nanos}.db",
            std::process::id()
        ));
        path.to_string_lossy().into_owned()
    }

    fn cleanup_test_db(path: &str) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{path}-wal"));
        let _ = fs::remove_file(format!("{path}-shm"));
    }

    fn insert_session(
        conn: &Connection,
        session_id: &str,
        started_at: &str,
        ended_at: Option<&str>,
        model: &str,
        initial_prompt: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO sessions (session_id, started_at, ended_at, model, initial_prompt) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![session_id, started_at, ended_at, model, initial_prompt],
        )
        .expect("insert session");
    }

    fn insert_request(
        conn: &Connection,
        request_id: &str,
        session_id: &str,
        timestamp: &str,
        model: &str,
        input_tokens: i64,
        output_tokens: i64,
        cache_read_tokens: i64,
        cache_creation_tokens: i64,
    ) {
        conn.execute(
            "INSERT INTO requests (
                request_id, session_id, timestamp, model, input_tokens, output_tokens,
                cache_read_tokens, cache_creation_tokens, cost_dollars, cost_source,
                trusted_for_budget_enforcement, duration_ms, tool_calls, cache_event
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL, NULL, 0, '[]', NULL)",
            rusqlite::params![
                request_id,
                session_id,
                timestamp,
                model,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_creation_tokens,
            ],
        )
        .expect("insert request");
    }

    fn insert_diagnosis(
        conn: &Connection,
        session_id: &str,
        completed_at: &str,
        degraded: bool,
        causes_json: &str,
    ) {
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Completed', 3, 1.0, 0.5, ?3, NULL, ?4, '[]')",
            rusqlite::params![session_id, completed_at, degraded as i64, causes_json],
        )
        .expect("insert diagnosis");
    }

    fn insert_billing_reconciliation(
        conn: &Connection,
        session_id: &str,
        imported_at: &str,
        source: &str,
        billed_cost_dollars: f64,
    ) {
        conn.execute(
            "INSERT INTO billing_reconciliations (
                session_id, imported_at, source, billed_cost_dollars
            ) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![session_id, imported_at, source, billed_cost_dollars],
        )
        .expect("insert billing reconciliation");
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_turn_snapshot(
        conn: &Connection,
        session_id: &str,
        turn_number: i64,
        timestamp: &str,
        cache_read_tokens: i64,
        cache_creation_tokens: i64,
        ttft_ms: i64,
        gap_from_prev_secs: f64,
        context_utilization: f64,
        response_summary: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO turn_snapshots (
                session_id, turn_number, timestamp, input_tokens, cache_read_tokens,
                cache_creation_tokens, output_tokens, ttft_ms, tool_calls, tool_failures,
                gap_from_prev_secs, context_utilization, frustration_signals,
                requested_model, actual_model, response_summary
            ) VALUES (?1, ?2, ?3, 1000, ?4, ?5, 100, ?6, '[]', 0, ?7, ?8, 0, ?9, ?10, ?11)",
            rusqlite::params![
                session_id,
                turn_number,
                timestamp,
                cache_read_tokens,
                cache_creation_tokens,
                ttft_ms,
                gap_from_prev_secs,
                context_utilization,
                "claude-sonnet",
                "claude-sonnet",
                response_summary,
            ],
        )
        .expect("insert turn snapshot");
    }

    fn make_http_headers(entries: &[(&str, &str)]) -> HttpHeaders {
        HttpHeaders {
            headers: Some(super::envoy::config::core::v3::HeaderMap {
                headers: entries
                    .iter()
                    .map(|(key, value)| ProtoHeaderValue {
                        key: (*key).to_string(),
                        value: (*value).to_string(),
                        raw_value: Vec::new(),
                    })
                    .collect(),
            }),
            ..Default::default()
        }
    }

    fn history_window<'a>(
        windows: &'a [metrics::HistoricalWindowMetrics],
        label: &str,
    ) -> &'a metrics::HistoricalWindowMetrics {
        windows
            .iter()
            .find(|window| window.window == label)
            .expect("window present")
    }

    #[test]
    fn parses_latest_tool_results_with_tool_names() {
        let body = br#"{
          "messages": [
            {
              "role": "assistant",
              "content": [
                {
                  "type": "tool_use",
                  "id": "toolu_read",
                  "name": "Read",
                  "input": { "file_path": "/tmp/demo.txt" }
                },
                {
                  "type": "tool_use",
                  "id": "toolu_bash",
                  "name": "Bash",
                  "input": { "command": "cargo test --lib" }
                }
              ]
            },
            {
              "role": "user",
              "content": [
                {
                  "type": "tool_result",
                  "tool_use_id": "toolu_read",
                  "is_error": false
                },
                {
                  "type": "tool_result",
                  "tool_use_id": "toolu_bash",
                  "is_error": true
                }
              ]
            }
          ]
        }"#;

        let results = parse_latest_tool_results(body);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].tool_name, "Read");
        assert_eq!(results[0].input_summary, "/tmp/demo.txt");
        assert_eq!(results[0].outcome, "success");
        assert!(!results[0].is_error);
        assert_eq!(results[1].tool_name, "Bash");
        assert_eq!(results[1].input_summary, "cargo test --lib");
        assert_eq!(results[1].outcome, "error");
        assert!(results[1].is_error);
    }

    #[test]
    fn unknown_tool_result_stays_unknown_when_mapping_is_missing() {
        let body = br#"{
          "messages": [
            {
              "role": "user",
              "content": [
                {
                  "type": "tool_result",
                  "tool_use_id": "toolu_missing",
                  "is_error": true
                }
              ]
            }
          ]
        }"#;

        let results = parse_latest_tool_results(body);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].tool_name, "unknown");
        assert_eq!(results[0].outcome, "error");
        assert!(results[0].is_error);
    }

    #[test]
    fn compact_response_summary_strips_machine_noise() {
        let raw = r#"
            ```json
            { "tool_use": "Bash", "command": "cargo test auth" }
            ```
            Fixed the auth middleware ordering and reran the targeted tests.
        "#;

        let summary = compact_response_summary(raw, &[]).expect("summary");
        assert_eq!(
            summary,
            "Fixed the auth middleware ordering and reran the targeted tests."
        );
    }

    #[test]
    fn compact_response_summary_falls_back_to_tool_context() {
        let tool_results = vec![ParsedToolResult {
            tool_name: "Bash".to_string(),
            input_summary: "cargo test auth_middleware".to_string(),
            outcome: "success".to_string(),
            duration_ms: 1200,
            is_error: false,
        }];

        let summary =
            compact_response_summary("```json\n{\"tool_use\": \"Bash\"}\n```", &tool_results)
                .expect("summary");
        assert_eq!(summary, "Tools: Bash cargo test auth_middleware");
    }

    #[test]
    fn historical_metrics_empty_db_returns_zeroes() {
        let conn = create_history_test_db();
        let windows = query_historical_metrics(&conn, 1_776_700_000).expect("query history");

        assert_eq!(windows.len(), metrics::HISTORY_WINDOWS.len());
        for window in &windows {
            assert_eq!(window.sessions, 0);
            assert_eq!(window.estimated_spend_dollars, 0.0);
            assert!(window.estimated_spend_dollars_by_model.is_empty());
            assert!(window
                .avg_estimated_session_cost_dollars_by_model
                .is_empty());
            assert_eq!(window.cache_hit_ratio, 0.0);
            assert_eq!(window.degraded_sessions, 0);
            assert_eq!(window.degraded_session_ratio, 0.0);
            assert!(window.degraded_causes.is_empty());
        }
    }

    #[test]
    fn historical_metrics_excludes_rows_outside_window() {
        let conn = create_history_test_db();
        let now = 1_776_700_000;
        let two_days_ago = epoch_to_iso8601(now - 2 * 86_400);

        insert_request(
            &conn,
            "req-old",
            "session-old",
            &two_days_ago,
            "claude-haiku",
            1_000_000,
            0,
            40,
            10,
        );
        insert_session(
            &conn,
            "session-old",
            &two_days_ago,
            Some(&two_days_ago),
            "claude-haiku",
            None,
        );
        insert_diagnosis(
            &conn,
            "session-old",
            &two_days_ago,
            true,
            r#"[{"cause_type":"context_bloat"}]"#,
        );

        let windows = query_historical_metrics(&conn, now).expect("query history");
        let one_day = history_window(&windows, "1d");
        let seven_day = history_window(&windows, "7d");

        assert_eq!(one_day.sessions, 0);
        assert_eq!(one_day.estimated_spend_dollars, 0.0);
        assert_eq!(one_day.degraded_sessions, 0);
        assert!(one_day.degraded_causes.is_empty());

        assert_eq!(seven_day.sessions, 1);
        assert_eq!(seven_day.degraded_sessions, 1);
        let expected =
            pricing::estimate_cost_dollars("claude-haiku", 1_000_000, 0, 40, 10).total_cost_dollars;
        assert!((seven_day.estimated_spend_dollars - expected).abs() < 1e-9);
        assert_eq!(
            seven_day.estimated_spend_dollars_by_model.get("haiku"),
            Some(&expected)
        );
        assert_eq!(seven_day.degraded_causes.get("context_bloat"), Some(&1));
    }

    #[test]
    fn historical_metrics_aggregate_estimated_spend_cache_and_causes() {
        let conn = create_history_test_db();
        let now = 1_776_700_000;
        let one_hour_ago = epoch_to_iso8601(now - 3_600);
        let two_hours_ago = epoch_to_iso8601(now - 7_200);
        let three_hours_ago = epoch_to_iso8601(now - 10_800);

        insert_request(
            &conn,
            "req-a",
            "session-a",
            &one_hour_ago,
            "claude-sonnet",
            500_000,
            0,
            20,
            10,
        );
        insert_session(
            &conn,
            "session-a",
            &one_hour_ago,
            Some(&one_hour_ago),
            "claude-sonnet",
            None,
        );
        insert_request(
            &conn,
            "req-b",
            "session-b",
            &two_hours_ago,
            "claude-haiku",
            625_000,
            0,
            0,
            30,
        );
        insert_session(
            &conn,
            "session-b",
            &two_hours_ago,
            Some(&two_hours_ago),
            "claude-haiku",
            None,
        );
        insert_request(
            &conn,
            "req-c",
            "session-c",
            &three_hours_ago,
            "claude-sonnet",
            666_666,
            0,
            10,
            10,
        );
        insert_session(
            &conn,
            "session-c",
            &three_hours_ago,
            Some(&three_hours_ago),
            "claude-sonnet",
            None,
        );

        insert_diagnosis(
            &conn,
            "session-a",
            &one_hour_ago,
            true,
            r#"[{"cause_type":"cache_miss_thrash"},{"cause_type":"context_bloat"}]"#,
        );
        insert_diagnosis(
            &conn,
            "session-b",
            &two_hours_ago,
            true,
            r#"[{"cause_type":"cache_miss_thrash"}]"#,
        );
        insert_diagnosis(&conn, "session-c", &three_hours_ago, false, "[]");

        let windows = query_historical_metrics(&conn, now).expect("query history");
        let one_day = history_window(&windows, "1d");

        assert_eq!(one_day.sessions, 3);
        assert_eq!(one_day.degraded_sessions, 2);
        let expected = pricing::estimate_cost_dollars("claude-sonnet", 500_000, 0, 20, 10)
            .total_cost_dollars
            + pricing::estimate_cost_dollars("claude-haiku", 625_000, 0, 0, 30).total_cost_dollars
            + pricing::estimate_cost_dollars("claude-sonnet", 666_666, 0, 10, 10)
                .total_cost_dollars;
        assert!((one_day.estimated_spend_dollars - expected).abs() < 1e-9);
        let sonnet_expected = pricing::estimate_cost_dollars("claude-sonnet", 500_000, 0, 20, 10)
            .total_cost_dollars
            + pricing::estimate_cost_dollars("claude-sonnet", 666_666, 0, 10, 10)
                .total_cost_dollars;
        let haiku_expected =
            pricing::estimate_cost_dollars("claude-haiku", 625_000, 0, 0, 30).total_cost_dollars;
        assert_eq!(
            one_day.estimated_spend_dollars_by_model.get("sonnet"),
            Some(&sonnet_expected)
        );
        assert_eq!(
            one_day.estimated_spend_dollars_by_model.get("haiku"),
            Some(&haiku_expected)
        );
        assert_eq!(
            one_day
                .avg_estimated_session_cost_dollars_by_model
                .get("sonnet"),
            Some(&1.0)
        );
        assert_eq!(
            one_day
                .avg_estimated_session_cost_dollars_by_model
                .get("haiku"),
            Some(&1.0)
        );
        assert!((one_day.cache_hit_ratio - 0.375).abs() < 1e-9);
        assert!((one_day.degraded_session_ratio - (2.0 / 3.0)).abs() < 1e-9);
        assert_eq!(one_day.degraded_causes.get("cache_miss_thrash"), Some(&2));
        assert_eq!(one_day.degraded_causes.get("context_bloat"), Some(&1));
    }

    #[test]
    fn historical_metrics_render_exposes_new_metrics() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        metrics::init();

        let mut causes = std::collections::BTreeMap::new();
        causes.insert("context_bloat", 3);
        metrics::update_historical_gauges(
            &[metrics::HistoricalWindowMetrics {
                window: "7d",
                sessions: 5,
                estimated_spend_dollars: 12.5,
                estimated_spend_dollars_by_model: std::collections::BTreeMap::from([(
                    "sonnet", 12.5,
                )]),
                avg_estimated_session_cost_dollars_by_model: std::collections::BTreeMap::from([(
                    "sonnet", 2.5,
                )]),
                estimated_cache_waste_dollars_by_model: std::collections::BTreeMap::from([(
                    "sonnet", 0.75,
                )]),
                cache_hit_ratio: 0.42,
                cache_events: std::collections::BTreeMap::from([("miss_thrash", 2)]),
                degraded_sessions: 2,
                degraded_session_ratio: 0.4,
                degraded_causes: causes,
                model_fallbacks: std::collections::BTreeMap::from([(("opus", "sonnet"), 1)]),
                tool_failures_by_tool: std::collections::BTreeMap::from([("bash".to_string(), 4)]),
            }],
            1_776_700_000,
        );

        let (_, body) = metrics::render().expect("render metrics");
        assert!(body.contains("clauditor_history_sessions"));
        assert!(body.contains("clauditor_history_estimated_spend_dollars"));
        assert!(body.contains("clauditor_history_estimated_spend_dollars_by_model"));
        assert!(body.contains("clauditor_history_avg_estimated_session_cost_dollars"));
        assert!(body.contains("clauditor_history_cache_hit_ratio"));
        assert!(body.contains("clauditor_history_degraded_sessions"));
        assert!(body.contains("clauditor_history_degraded_session_ratio"));
        assert!(body.contains("clauditor_history_degraded_causes"));
        assert!(body.contains("clauditor_history_estimated_cache_waste_dollars"));
        assert!(body.contains("clauditor_history_cache_events"));
        assert!(body.contains("clauditor_history_model_fallbacks"));
        assert!(body.contains("clauditor_history_tool_failures"));
        assert!(body.contains("clauditor_history_refresh_timestamp_seconds 1776700000"));
        assert!(body.contains("window=\"7d\""));
        assert!(body.contains("cause_type=\"context_bloat\""));
        assert!(body.contains("event_type=\"miss_thrash\""));
        assert!(body.contains("requested=\"opus\""));
        assert!(body.contains("actual=\"sonnet\""));
        assert!(body.contains("tool=\"bash\""));
        assert!(body
            .contains("clauditor_cache_events_total{event_type=\"miss_thrash\",model=\"sonnet\"}"));
        assert!(body.contains("clauditor_estimated_cache_waste_dollars_total{model=\"sonnet\"}"));
        assert!(
            body.contains("clauditor_model_fallback_total{actual=\"sonnet\",requested=\"opus\"} 0")
        );
        assert!(body.contains("clauditor_tool_failures_total{tool=\"unknown\"} 0"));
        assert!(
            body.contains("clauditor_mcp_tool_calls_total{server=\"unknown\",tool=\"unknown\"} 0")
        );
        assert!(body.contains("clauditor_mcp_server_calls_total{server=\"unknown\"} 0"));
        assert!(metric_line_has(
            &body,
            "clauditor_mcp_events_total",
            &[
                "server=\"unknown\"",
                "tool=\"unknown\"",
                "event_type=\"called\"",
                "source=\"hook\""
            ],
            " 0"
        ));
        assert!(metric_line_has(
            &body,
            "clauditor_skill_events_total",
            &[
                "skill=\"unknown\"",
                "event_type=\"fired\"",
                "source=\"hook\""
            ],
            " 0"
        ));
    }

    #[test]
    fn seed_live_metric_labels_from_db_precreates_known_tool_series() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        metrics::init();

        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(SCHEMA).expect("create schema");
        conn.execute(
            "INSERT INTO sessions (session_id, started_at, model) VALUES (?1, ?2, ?3)",
            rusqlite::params!["session_1", "2026-01-01T00:00:00Z", "claude-sonnet-4-6"],
        )
        .expect("insert session");
        conn.execute(
            "INSERT INTO requests (request_id, session_id, timestamp, model) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "req_1",
                "session_1",
                "2026-01-01T00:00:00Z",
                "claude-sonnet-4-6"
            ],
        )
        .expect("insert request");
        conn.execute(
            "INSERT INTO tool_calls (request_id, timestamp, tool_name) VALUES (?1, ?2, ?3)",
            rusqlite::params!["req_1", "2026-01-01T00:00:00Z", "Bash"],
        )
        .expect("insert tool call");
        conn.execute(
            "INSERT INTO tool_outcomes (session_id, turn_number, timestamp, tool_name, outcome) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                "session_1",
                1,
                "2026-01-01T00:00:00Z",
                "Read File",
                "success"
            ],
        )
        .expect("insert tool outcome");
        conn.execute(
            "INSERT INTO tool_calls (request_id, timestamp, tool_name) VALUES (?1, ?2, ?3)",
            rusqlite::params!["req_1", "2026-01-01T00:00:01Z", "mcp__github__get_issue"],
        )
        .expect("insert mcp tool call");
        conn.execute(
            "INSERT INTO skill_events (session_id, timestamp, skill_name, event_type, confidence, source) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                "session_1",
                "2026-01-01T00:00:02Z",
                "openai-docs",
                "fired",
                1.0,
                "hook"
            ],
        )
        .expect("insert skill event");
        conn.execute(
            "INSERT INTO mcp_events (session_id, timestamp, server, tool, event_type, source) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                "session_1",
                "2026-01-01T00:00:03Z",
                "github",
                "get_issue",
                "called",
                "hook"
            ],
        )
        .expect("insert mcp event");

        seed_live_metric_labels_from_db(&conn).expect("seed tool labels");

        let (_, body) = metrics::render().expect("render metrics");
        assert!(body.contains("clauditor_tool_calls_total{tool=\"bash\"} 0"));
        assert!(body.contains("clauditor_tool_failures_total{tool=\"bash\"} 0"));
        assert!(body.contains("clauditor_tool_calls_total{tool=\"read_file\"} 0"));
        assert!(body.contains("clauditor_tool_failures_total{tool=\"read_file\"} 0"));
        assert!(
            body.contains("clauditor_mcp_tool_calls_total{server=\"github\",tool=\"get_issue\"} 0")
        );
        assert!(body
            .contains("clauditor_mcp_tool_failures_total{server=\"github\",tool=\"get_issue\"} 0"));
        assert!(body.contains("clauditor_mcp_server_calls_total{server=\"github\"} 0"));
        assert!(body.contains("clauditor_mcp_server_failures_total{server=\"github\"} 0"));
        assert!(metric_line_has(
            &body,
            "clauditor_skill_events_total",
            &[
                "skill=\"openai-docs\"",
                "event_type=\"fired\"",
                "source=\"hook\""
            ],
            " 0"
        ));
        assert!(metric_line_has(
            &body,
            "clauditor_mcp_events_total",
            &[
                "server=\"github\"",
                "tool=\"get_issue\"",
                "event_type=\"called\"",
                "source=\"hook\""
            ],
            " 0"
        ));
    }

    #[test]
    fn tool_metric_path_records_mcp_breakdowns() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        metrics::init();

        let tool_name = "mcp__metricstest_server__lookup_widget";
        metrics::record_tool_call(tool_name);
        metrics::record_tool_failures(tool_name, 2);

        let (_, body) = metrics::render().expect("render metrics");
        assert!(body.contains(
            "clauditor_tool_calls_total{tool=\"mcp__metricstest_server__lookup_widget\"} 1"
        ));
        assert!(body.contains(
            "clauditor_tool_failures_total{tool=\"mcp__metricstest_server__lookup_widget\"} 2"
        ));
        assert!(body.contains(
            "clauditor_mcp_tool_calls_total{server=\"metricstest_server\",tool=\"lookup_widget\"} 1"
        ));
        assert!(body.contains(
            "clauditor_mcp_tool_failures_total{server=\"metricstest_server\",tool=\"lookup_widget\"} 2"
        ));
        assert!(body.contains("clauditor_mcp_server_calls_total{server=\"metricstest_server\"} 1"));
        assert!(
            body.contains("clauditor_mcp_server_failures_total{server=\"metricstest_server\"} 2")
        );
        assert!(metric_line_has(
            &body,
            "clauditor_mcp_events_total",
            &[
                "server=\"metricstest_server\"",
                "tool=\"lookup_widget\"",
                "event_type=\"called\"",
                "source=\"proxy\""
            ],
            " 1"
        ));
        assert!(metric_line_has(
            &body,
            "clauditor_mcp_events_total",
            &[
                "server=\"metricstest_server\"",
                "tool=\"lookup_widget\"",
                "event_type=\"failed\"",
                "source=\"proxy\""
            ],
            " 2"
        ));
    }

    #[test]
    fn historical_gauges_zero_stale_causes_on_refresh() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        metrics::init();

        let mut causes = std::collections::BTreeMap::new();
        causes.insert("cache_miss_thrash", 2);
        metrics::update_historical_gauges(
            &[metrics::HistoricalWindowMetrics {
                window: "7d",
                sessions: 2,
                estimated_spend_dollars: 1.0,
                estimated_spend_dollars_by_model: std::collections::BTreeMap::from([(
                    "sonnet", 1.0,
                )]),
                avg_estimated_session_cost_dollars_by_model: std::collections::BTreeMap::from([(
                    "sonnet", 0.5,
                )]),
                estimated_cache_waste_dollars_by_model: std::collections::BTreeMap::from([(
                    "sonnet", 0.25,
                )]),
                cache_hit_ratio: 0.25,
                cache_events: std::collections::BTreeMap::from([("miss_thrash", 2)]),
                degraded_sessions: 1,
                degraded_session_ratio: 0.5,
                degraded_causes: causes,
                model_fallbacks: std::collections::BTreeMap::from([(("opus", "sonnet"), 1)]),
                tool_failures_by_tool: std::collections::BTreeMap::from([("bash".to_string(), 2)]),
            }],
            1_776_700_000,
        );

        metrics::update_historical_gauges(
            &[metrics::HistoricalWindowMetrics {
                window: "7d",
                sessions: 1,
                estimated_spend_dollars: 0.5,
                estimated_spend_dollars_by_model: std::collections::BTreeMap::new(),
                avg_estimated_session_cost_dollars_by_model: std::collections::BTreeMap::new(),
                estimated_cache_waste_dollars_by_model: std::collections::BTreeMap::new(),
                cache_hit_ratio: 0.0,
                cache_events: std::collections::BTreeMap::new(),
                degraded_sessions: 0,
                degraded_session_ratio: 0.0,
                degraded_causes: std::collections::BTreeMap::new(),
                model_fallbacks: std::collections::BTreeMap::new(),
                tool_failures_by_tool: std::collections::BTreeMap::new(),
            }],
            1_776_700_100,
        );

        let (_, body) = metrics::render().expect("render metrics");
        let stale_line = body
            .lines()
            .find(|line| {
                line.starts_with("clauditor_history_degraded_causes{")
                    && line.contains("window=\"7d\"")
                    && line.contains("cause_type=\"cache_miss_thrash\"")
            })
            .expect("stale cause line");
        assert!(stale_line.ends_with(" 0"));
        let stale_avg_line = body
            .lines()
            .find(|line| {
                line.starts_with("clauditor_history_avg_estimated_session_cost_dollars{")
                    && line.contains("window=\"7d\"")
                    && line.contains("model=\"sonnet\"")
            })
            .expect("stale avg line");
        assert!(stale_avg_line.ends_with(" 0"));
        let stale_spend_line = body
            .lines()
            .find(|line| {
                line.starts_with("clauditor_history_estimated_spend_dollars_by_model{")
                    && line.contains("window=\"7d\"")
                    && line.contains("model=\"sonnet\"")
            })
            .expect("stale spend line");
        assert!(stale_spend_line.ends_with(" 0"));
    }

    #[test]
    fn summary_response_uses_estimated_cost_fields_only() {
        let today = SummaryWindowData {
            sessions: 2,
            estimated_cost_dollars: 12.345,
            cost_source: "pricing_file:test-contract".to_string(),
            trusted_for_budget_enforcement: true,
            billed_cost_dollars: Some(10.25),
            billed_sessions: 1,
            cache_hit_ratio: 0.5,
        };
        let week = SummaryWindowData {
            sessions: 3,
            estimated_cost_dollars: 20.0,
            cost_source: pricing::MIXED_COST_SOURCE.to_string(),
            trusted_for_budget_enforcement: false,
            billed_cost_dollars: None,
            billed_sessions: 0,
            cache_hit_ratio: 0.25,
        };
        let month = SummaryWindowData {
            sessions: 4,
            estimated_cost_dollars: 30.0,
            cost_source: ESTIMATED_COST_SOURCE.to_string(),
            trusted_for_budget_enforcement: false,
            billed_cost_dollars: Some(18.0),
            billed_sessions: 2,
            cache_hit_ratio: 0.75,
        };
        let expected_source = pricing::active_catalog_source();
        let json = build_summary_response_json(&today, &week, &month);
        assert_eq!(
            json.get("cost_source").and_then(|v| v.as_str()),
            Some(expected_source.as_str())
        );
        assert_eq!(
            json.get("trusted_for_budget_enforcement")
                .and_then(|v| v.as_bool()),
            Some(pricing::trusted_for_budget_enforcement())
        );
        let today = json.get("today").expect("today");
        assert_eq!(
            today.get("estimated_cost_dollars").and_then(|v| v.as_f64()),
            Some(12.35)
        );
        assert_eq!(
            today.get("billed_cost_dollars").and_then(|v| v.as_f64()),
            Some(10.25)
        );
        assert_eq!(
            today.get("billed_sessions").and_then(|v| v.as_u64()),
            Some(1)
        );
        assert_eq!(
            today.get("cost_source").and_then(|v| v.as_str()),
            Some("pricing_file:test-contract")
        );
        assert!(today.get("cost").is_none());
    }

    #[test]
    fn diagnosis_response_uses_estimated_cost_fields_only() {
        let json = build_diagnosis_response_json(
            "session_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            "Completed".to_string(),
            4,
            1.25,
            "pricing_file:test-contract".to_string(),
            true,
            Some(0.98),
            Some("invoice_2026q2".to_string()),
            Some("2026-01-02T00:00:00Z".to_string()),
            0.4,
            true,
            Some(2),
            serde_json::json!([]),
            serde_json::json!(["Retry less"]),
        );
        assert_eq!(
            json.get("estimated_total_cost_dollars")
                .and_then(|v| v.as_f64()),
            Some(1.25)
        );
        assert_eq!(
            json.get("cost_source").and_then(|v| v.as_str()),
            Some("pricing_file:test-contract")
        );
        assert_eq!(
            json.get("trusted_for_budget_enforcement")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            json.get("billed_cost_dollars").and_then(|v| v.as_f64()),
            Some(0.98)
        );
        assert_eq!(
            json.get("billing_source").and_then(|v| v.as_str()),
            Some("invoice_2026q2")
        );
        assert!(json.get("total_cost_dollars").is_none());
    }

    #[test]
    fn session_summary_uses_estimated_cost_fields_only() {
        let json = build_session_summary_json(
            "session_1".to_string(),
            Some("2026-01-01T00:00:00Z".to_string()),
            "Completed".to_string(),
            false,
            4,
            1.25,
            "pricing_file:test-contract".to_string(),
            true,
            Some(0.98),
            Some("invoice_2026q2".to_string()),
            Some("2026-01-02T00:00:00Z".to_string()),
            "cache_miss_ttl".to_string(),
            0.4,
            Some("claude-sonnet".to_string()),
        );
        assert_eq!(
            json.get("estimated_total_cost_dollars")
                .and_then(|v| v.as_f64()),
            Some(1.25)
        );
        assert_eq!(
            json.get("cost_source").and_then(|v| v.as_str()),
            Some("pricing_file:test-contract")
        );
        assert_eq!(
            json.get("billed_cost_dollars").and_then(|v| v.as_f64()),
            Some(0.98)
        );
        assert!(json.get("total_cost_dollars").is_none());
    }

    #[test]
    fn sessions_response_exposes_cost_source_once_at_root() {
        let sessions = vec![build_session_summary_json(
            "session_1".to_string(),
            Some("2026-01-01T00:00:00Z".to_string()),
            "Completed".to_string(),
            false,
            4,
            1.25,
            ESTIMATED_COST_SOURCE.to_string(),
            false,
            Some(1.10),
            Some("invoice_2026q2".to_string()),
            Some("2026-01-02T00:00:00Z".to_string()),
            "cache_miss_ttl".to_string(),
            0.4,
            Some("claude-sonnet".to_string()),
        )];
        let json = build_sessions_response_json(sessions);

        let expected_source = pricing::active_catalog_source();
        assert_eq!(
            json.get("cost_source").and_then(|v| v.as_str()),
            Some(expected_source.as_str())
        );
        assert_eq!(
            json.get("trusted_for_budget_enforcement")
                .and_then(|v| v.as_bool()),
            Some(pricing::trusted_for_budget_enforcement())
        );
        assert_eq!(
            json.get("sessions")
                .and_then(|v| v.as_array())
                .map(|v| v.len()),
            Some(1)
        );
        assert!(json.get("cost").is_none());
    }

    #[test]
    fn query_summary_uses_latest_billing_reconciliation_per_session() {
        let conn = create_history_test_db();
        let now = 1_776_700_000;
        let one_hour_ago = epoch_to_iso8601(now - 3_600);
        insert_session(
            &conn,
            "session-a",
            &one_hour_ago,
            Some(&one_hour_ago),
            "claude-sonnet",
            None,
        );
        insert_session(
            &conn,
            "session-b",
            &one_hour_ago,
            Some(&one_hour_ago),
            "claude-haiku",
            None,
        );
        insert_request(
            &conn,
            "req-a",
            "session-a",
            &one_hour_ago,
            "claude-sonnet",
            1_000_000,
            0,
            0,
            0,
        );
        insert_request(
            &conn,
            "req-b",
            "session-b",
            &one_hour_ago,
            "claude-haiku",
            500_000,
            0,
            0,
            0,
        );
        insert_billing_reconciliation(
            &conn,
            "session-a",
            "2026-01-01T00:00:00Z",
            "invoice_old",
            2.0,
        );
        insert_billing_reconciliation(
            &conn,
            "session-a",
            "2026-01-03T00:00:00Z",
            "invoice_new",
            2.5,
        );
        insert_billing_reconciliation(
            &conn,
            "session-b",
            "2026-01-02T00:00:00Z",
            "invoice_b",
            1.25,
        );

        let summary = query_summary(&conn, &epoch_to_iso8601(now - 86_400)).expect("summary");
        assert_eq!(summary.sessions, 2);
        assert_eq!(summary.billed_sessions, 2);
        assert_eq!(summary.billed_cost_dollars, Some(3.75));
        assert!((summary.estimated_cost_dollars - 3.4).abs() < 1e-9);
    }

    #[test]
    fn query_summary_excludes_internal_request_groups_from_session_count() {
        let conn = create_history_test_db();
        let now = 1_776_700_000;
        let one_hour_ago = epoch_to_iso8601(now - 3_600);
        insert_session(
            &conn,
            "session-real",
            &one_hour_ago,
            Some(&one_hour_ago),
            "claude-sonnet",
            None,
        );
        insert_request(
            &conn,
            "req-real",
            "session-real",
            &one_hour_ago,
            "claude-sonnet",
            1_000_000,
            0,
            0,
            0,
        );
        insert_request(
            &conn,
            "req-title",
            "session-internal",
            &one_hour_ago,
            "claude-haiku",
            500_000,
            0,
            0,
            0,
        );

        let summary = query_summary(&conn, &epoch_to_iso8601(now - 86_400)).expect("summary");
        assert_eq!(summary.sessions, 1);
        assert!(summary.estimated_cost_dollars > 0.0);
    }

    #[test]
    fn repair_persisted_session_artifacts_reaches_older_missing_rows() {
        let conn = create_full_test_db();
        let cutoff_base = now_epoch_secs().saturating_sub(session_timeout_secs() + 3_600);

        for idx in 0..205u64 {
            let ts = epoch_to_iso8601(cutoff_base.saturating_sub(205 - idx));
            let session_id = format!("session-{idx:03}");
            insert_session(&conn, &session_id, &ts, Some(&ts), "claude-sonnet", None);
            insert_turn_snapshot(&conn, &session_id, 1, &ts, 0, 1000, 1000, 0.0, 0.10, None);
        }

        repair_persisted_session_artifacts(&conn).expect("first repair");
        let repaired_after_first: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_diagnoses", [], |row| {
                row.get(0)
            })
            .expect("count diagnoses after first repair");
        assert_eq!(repaired_after_first, 200);

        repair_persisted_session_artifacts(&conn).expect("second repair");
        let repaired_after_second: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_diagnoses", [], |row| {
                row.get(0)
            })
            .expect("count diagnoses after second repair");
        assert_eq!(repaired_after_second, 205);
    }

    #[test]
    fn request_uses_1m_context_detects_anthropic_beta_header() {
        let headers = make_http_headers(&[(
            "anthropic-beta",
            "prompt-tools-2025-04-02, context-1m-2025-08-07",
        )]);

        assert!(request_uses_1m_context(&headers));
        assert_eq!(
            resolve_context_window_tokens(
                Some(EXTENDED_CONTEXT_WINDOW_TOKENS),
                "claude-sonnet-4-6"
            ),
            EXTENDED_CONTEXT_WINDOW_TOKENS
        );
    }

    #[test]
    fn resolve_context_window_tokens_accepts_model_suffix() {
        assert_eq!(
            resolve_context_window_tokens(None, "claude-sonnet-4-6[1m]"),
            EXTENDED_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(None, "claude-sonnet-4-6"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
    }

    #[test]
    fn model_matches_ignores_1m_suffix() {
        assert!(super::model_matches(
            "claude-sonnet-4-6[1m]",
            "claude-sonnet-4-6"
        ));
        assert!(super::model_matches(
            "claude-opus-4-7",
            "claude-opus-4-7[1m]"
        ));
    }

    #[test]
    fn canonical_skill_names_are_metric_friendly() {
        assert_eq!(
            canonical_telemetry_name("/OpenAI Docs"),
            Some("openai-docs".to_string())
        );
        assert_eq!(
            canonical_telemetry_name("/tmp/project/.claude/skills/review/SKILL.md"),
            Some("review".to_string())
        );
        assert_eq!(canonical_telemetry_name("   "), None);
    }

    #[test]
    fn skill_tool_input_extracts_name_variants() {
        assert_eq!(
            skill_name_from_tool_input_json(r#"{"skill_name":"openai-docs"}"#),
            Some("openai-docs".to_string())
        );
        assert_eq!(
            skill_name_from_tool_input_json(
                r#"{"file_path":"/tmp/.claude/skills/browser-use/SKILL.md"}"#
            ),
            Some("browser-use".to_string())
        );
    }

    #[test]
    fn explicit_skill_refs_detect_slash_and_dollar_mentions() {
        let refs = extract_explicit_skill_refs("Use /openai-docs and $browser-use:browser here");
        assert!(refs.contains("openai-docs"));
        assert!(refs.contains("browser-use:browser"));
    }

    #[test]
    fn explicit_skill_refs_ignore_inline_slashes_and_absolute_paths() {
        let refs = extract_explicit_skill_refs(
            "Use read/list/search tools in /Users/pradeep/code, then /base-gke-readonly",
        );
        assert!(refs.contains("base-gke-readonly"));
        assert!(!refs.contains("list"));
        assert!(!refs.contains("search"));
        assert!(!refs.contains("users"));
    }

    #[test]
    fn repair_turn_snapshot_context_windows_backfills_oversized_turns_as_1m() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-long",
            "2026-04-23T06:00:00Z",
            Some("2026-04-23T06:00:00Z"),
            "claude-sonnet-4-6",
            None,
        );
        insert_turn_snapshot(
            &conn,
            "session-long",
            1,
            "2026-04-23T06:00:00Z",
            250_000,
            0,
            1000,
            0.0,
            1.255,
            None,
        );

        repair_turn_snapshot_context_windows(&conn).expect("repair turn snapshots");

        let (context_window_tokens, context_utilization): (i64, f64) = conn
            .query_row(
                "SELECT context_window_tokens, context_utilization \
                 FROM turn_snapshots WHERE session_id = 'session-long'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("load repaired row");

        assert_eq!(context_window_tokens, EXTENDED_CONTEXT_WINDOW_TOKENS as i64);
        assert!((context_utilization - 0.251).abs() < 0.0001);
    }

    #[test]
    fn degradation_view_repairs_missing_diagnosis_before_rendering() {
        let conn = create_full_test_db();
        let ended_at =
            epoch_to_iso8601(now_epoch_secs().saturating_sub(session_timeout_secs() + 3_600));
        insert_session(
            &conn,
            "session-ttl",
            &ended_at,
            Some(&ended_at),
            "claude-sonnet",
            Some("keep auth cache warm"),
        );
        insert_turn_snapshot(
            &conn,
            "session-ttl",
            1,
            &ended_at,
            0,
            1000,
            1000,
            0.0,
            0.10,
            None,
        );
        insert_turn_snapshot(
            &conn,
            "session-ttl",
            2,
            &ended_at,
            0,
            1000,
            3500,
            600.0,
            0.15,
            Some("Retried after cache expiry"),
        );
        insert_turn_snapshot(
            &conn,
            "session-ttl",
            3,
            &ended_at,
            900,
            0,
            1200,
            30.0,
            0.18,
            Some("Finished request"),
        );

        let json = load_degradation_view_from_db(&conn, "session-ttl").expect("degradation view");
        assert_eq!(json.get("degraded").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            json.get("degradation_turn").and_then(|v| v.as_i64()),
            Some(2)
        );
        assert_eq!(json.get("total_turns").and_then(|v| v.as_u64()), Some(3));
    }

    #[test]
    fn repair_clears_degradation_turn_for_non_degraded_sessions() {
        let conn = create_full_test_db();
        let ended_at =
            epoch_to_iso8601(now_epoch_secs().saturating_sub(session_timeout_secs() + 3_600));
        insert_session(
            &conn,
            "session-ok",
            &ended_at,
            Some(&ended_at),
            "claude-sonnet",
            Some("ship the fix"),
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Likely Completed', 4, 1.5, 0.98, 0, 8, '[]', '[]')",
            rusqlite::params!["session-ok", ended_at],
        )
        .expect("insert inconsistent diagnosis");

        repair_persisted_session_artifacts(&conn).expect("repair persisted artifacts");

        let stored_turn = conn
            .query_row(
                "SELECT degradation_turn FROM session_diagnoses WHERE session_id = 'session-ok'",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .expect("load repaired diagnosis turn");
        assert_eq!(stored_turn, None);

        let json = load_degradation_view_from_db(&conn, "session-ok")
            .expect("degradation view after repair");
        assert_eq!(json.get("degraded").and_then(|v| v.as_bool()), Some(false));
        assert!(json.get("degradation_turn").is_some());
        assert!(json.get("degradation_turn").unwrap().is_null());
    }

    #[test]
    fn cache_event_serializes_estimated_rebuild_cost_field_only() {
        let json = serde_json::to_value(crate::watch::WatchEvent::CacheEvent {
            session_id: "session_1".to_string(),
            event_type: "miss_ttl".to_string(),
            cache_expires_at_epoch: Some(1_776_700_000),
            estimated_rebuild_cost_dollars: Some(0.24),
        })
        .expect("serialize cache event");

        assert_eq!(
            json.get("estimated_rebuild_cost_dollars")
                .and_then(|v| v.as_f64()),
            Some(0.24)
        );
        assert!(json.get("rebuild_cost_dollars").is_none());
    }

    #[tokio::test]
    async fn persist_billing_reconciliation_waits_for_db_ack() {
        let path = unique_test_db_path("billing-ack");
        let (tx, rx) = std::sync::mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));

        tx.send(DbCommand::InsertSession {
            session_id: "session-test".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            model: "claude-sonnet".to_string(),
            initial_prompt: None,
        })
        .expect("queue session insert");

        persist_billing_reconciliation(
            &tx,
            BillingReconciliationInput {
                session_id: "session-test".to_string(),
                source: "invoice_q1".to_string(),
                billed_cost_dollars: 1.23,
                imported_at: Some("2026-01-02T00:00:00Z".to_string()),
            },
        )
        .await
        .expect("persist billing reconciliation");

        drop(tx);
        handle.join().expect("join db writer");

        let conn = Connection::open(&path).expect("open test db");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM billing_reconciliations WHERE session_id = ?1",
                rusqlite::params!["session-test"],
                |row| row.get(0),
            )
            .expect("count billing rows");
        assert_eq!(count, 1);

        cleanup_test_db(&path);
    }

    #[tokio::test]
    async fn persist_billing_reconciliation_rejects_unknown_sessions() {
        let path = unique_test_db_path("billing-missing-session");
        let (tx, rx) = std::sync::mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));

        let err = persist_billing_reconciliation(
            &tx,
            BillingReconciliationInput {
                session_id: "session-missing".to_string(),
                source: "invoice_q1".to_string(),
                billed_cost_dollars: 4.56,
                imported_at: Some("2026-01-02T00:00:00Z".to_string()),
            },
        )
        .await
        .expect_err("missing session should fail");

        assert_eq!(
            err,
            BillingReconciliationWriteError::UnknownSession("session-missing".to_string())
        );

        drop(tx);
        handle.join().expect("join db writer");

        let conn = Connection::open(&path).expect("open test db");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM billing_reconciliations", [], |row| {
                row.get(0)
            })
            .expect("count billing rows");
        assert_eq!(count, 0);

        cleanup_test_db(&path);
    }

    #[test]
    fn quota_snapshot_broadcasts_immediately_before_any_prior_send() {
        assert!(should_broadcast_quota_snapshot(None, None, false));
    }

    #[test]
    fn quota_snapshot_stays_quiet_inside_broadcast_window_when_alarm_is_stable() {
        assert!(!should_broadcast_quota_snapshot(
            Some(Duration::from_secs(120)),
            Some(false),
            false,
        ));
    }

    #[test]
    fn quota_snapshot_broadcasts_when_window_elapses() {
        assert!(should_broadcast_quota_snapshot(
            Some(QUOTA_WATCH_BROADCAST_INTERVAL),
            Some(false),
            false,
        ));
    }

    #[test]
    fn quota_snapshot_broadcasts_when_alarm_flips_before_window_elapses() {
        assert!(should_broadcast_quota_snapshot(
            Some(Duration::from_secs(120)),
            Some(false),
            true,
        ));
    }
}
