use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use regex::Regex;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
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

pub mod correlation;
pub mod diagnosis;
pub mod guard;
pub mod jsonl;
pub mod metrics;
pub mod pricing;
pub mod watch;

use envoy::config::core::v3::{
    header_value_option::HeaderAppendAction, HeaderValue as ProtoHeaderValue,
    HeaderValueOption as ProtoHeaderValueOption,
};
use envoy::service::ext_proc::v3::{
    body_mutation,
    common_response::ResponseStatus,
    external_processor_server::{ExternalProcessor, ExternalProcessorServer},
    processing_request::Request as ExtProcRequest,
    processing_response::Response as ExtProcResponse,
    BodyMutation, BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, HttpHeaders,
    ImmediateResponse, ProcessingRequest, ProcessingResponse,
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
    (secs - (secs % 86400)).saturating_sub(dow * 86400)
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
    counted_api_error_request_ids: HashSet<String>,
    counted_api_error_request_order: VecDeque<String>,
}

impl RuntimeState {
    fn new() -> Self {
        Self {
            total_spend: 0.0,
            total_tokens: 0,
            request_count: 0,
            consecutive_errors: 0,
            circuit_open_until: None,
            counted_api_error_request_ids: HashSet::new(),
            counted_api_error_request_order: VecDeque::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SessionBudgetState {
    total_spend: f64,
    trusted_spend: f64,
    untrusted_spend: f64,
    total_tokens: u64,
    request_count: u64,
    estimated_cache_waste_dollars: f64,
}

static RUNTIME_STATE: LazyLock<Mutex<RuntimeState>> =
    LazyLock::new(|| Mutex::new(RuntimeState::new()));
static SESSION_BUDGETS: LazyLock<DashMap<String, SessionBudgetState>> = LazyLock::new(DashMap::new);
static FALLBACK_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);
const GUARD_POLICY_PATH_ENV: &str = "CC_BLACKBOX_GUARD_POLICY_PATH";
const JSONL_DIR_ENV: &str = "CC_BLACKBOX_JSONL_DIR";
const GLOBAL_GUARD_SESSION_ID: &str = "guard_api_error_cooldown";
const API_ERROR_REQUEST_DEDUP_LIMIT: usize = 2048;

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(lock = name, "recovering poisoned mutex");
            poisoned.into_inner()
        }
    }
}

fn fallback_request_id() -> String {
    let counter = FALLBACK_REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("req_fallback_{nanos}_{counter}")
}

fn session_budget_observation(
    session_id: Option<&str>,
) -> Result<Option<guard::SessionBudgetObservation>, String> {
    let Some(session_id) = session_id else {
        return Ok(None);
    };
    let Some(state) = SESSION_BUDGETS.get(session_id) else {
        return Ok(None);
    };

    Ok(Some(guard::SessionBudgetObservation {
        session_id: session_id.to_string(),
        total_tokens: state.total_tokens,
        trusted_spend_dollars: state.trusted_spend,
        untrusted_spend_dollars: state.untrusted_spend,
        total_spend_dollars: state.total_spend,
        request_count: state.request_count,
        estimated_cache_waste_dollars: state.estimated_cache_waste_dollars,
    }))
}

fn api_cooldown_observation(
    session_id: Option<&str>,
) -> Result<Option<guard::ApiCooldownObservation>, String> {
    let mut runtime = lock_or_recover(&RUNTIME_STATE, "runtime_state");
    let Some(until) = runtime.circuit_open_until else {
        return Ok(None);
    };
    let now = Instant::now();
    if now >= until {
        runtime.circuit_open_until = None;
        return Ok(None);
    }

    let remaining = until.duration_since(now);
    let remaining_secs = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
    Ok(Some(guard::ApiCooldownObservation {
        session_id: Some(session_id.unwrap_or(GLOBAL_GUARD_SESSION_ID).to_string()),
        consecutive_errors: runtime.consecutive_errors,
        remaining_cooldown_secs: remaining_secs,
    }))
}

fn record_api_response_for_guard(
    request_id: Option<&str>,
    status: u32,
    has_parsed_model: bool,
    policy: Option<&guard::GuardPolicy>,
) -> Option<guard::GuardFinding> {
    if !has_parsed_model {
        return None;
    }

    let mut runtime = lock_or_recover(&RUNTIME_STATE, "runtime_state");
    if status >= 400 {
        if let Some(request_id) = request_id.map(str::trim).filter(|id| !id.is_empty()) {
            if !runtime
                .counted_api_error_request_ids
                .insert(request_id.to_string())
            {
                return None;
            }
            runtime
                .counted_api_error_request_order
                .push_back(request_id.to_string());
            while runtime.counted_api_error_request_order.len() > API_ERROR_REQUEST_DEDUP_LIMIT {
                if let Some(old_request_id) = runtime.counted_api_error_request_order.pop_front() {
                    runtime
                        .counted_api_error_request_ids
                        .remove(&old_request_id);
                }
            }
        }
        runtime.consecutive_errors += 1;
        let policy = policy?;
        let rule = policy
            .rules
            .get(&guard::RuleId::ApiErrorCircuitBreakerCooldown)?;
        if !matches!(
            rule.action,
            guard::GuardAction::Cooldown | guard::GuardAction::Block
        ) {
            return None;
        }
        let threshold = rule.threshold_count.unwrap_or(5);
        let cooldown_secs = rule.cooldown_secs.unwrap_or(30);
        if threshold > 0
            && cooldown_secs > 0
            && runtime.consecutive_errors >= threshold
            && runtime.circuit_open_until.is_none()
        {
            runtime.circuit_open_until = Some(Instant::now() + Duration::from_secs(cooldown_secs));
            warn!(
                consecutive_errors = runtime.consecutive_errors,
                http_status = status,
                cooldown_secs,
                "guard API error cooldown tripped"
            );
            let detail = format!(
                "{} consecutive API errors opened a {}s cooldown; {}s remain.",
                runtime.consecutive_errors, cooldown_secs, cooldown_secs
            );
            let suggested =
                "Wait for the cooldown to expire, fix the API error cause, or raise the cooldown policy."
                    .to_string();
            let draft = guard::FindingDraft::new(
                guard::RuleId::ApiErrorCircuitBreakerCooldown,
                guard::FindingSeverity::Critical,
                guard::EvidenceLevel::DirectProxy,
                guard::FindingSource::Proxy,
                1.0,
                GLOBAL_GUARD_SESSION_ID,
                now_iso8601(),
            )
            .with_detail(detail)
            .with_suggested_action(suggested);
            return Some(policy.resolve_finding(draft));
        }
    } else {
        runtime.consecutive_errors = 0;
        runtime.counted_api_error_request_ids.clear();
        runtime.counted_api_error_request_order.clear();
        if runtime.circuit_open_until.is_some() {
            runtime.circuit_open_until = None;
            info!("guard API error cooldown reset after successful response");
        }
    }
    None
}

fn apply_legacy_env_limits(policy: &mut guard::GuardPolicy) {
    let token_budget = env_u64("CC_BLACKBOX_SESSION_BUDGET_TOKENS", 0);
    if token_budget > 0 {
        if let Some(rule) = policy
            .rules
            .get_mut(&guard::RuleId::PerSessionTokenBudgetExceeded)
        {
            rule.limit_tokens = Some(token_budget);
        }
    }

    let dollar_budget = env_f64("CC_BLACKBOX_SESSION_BUDGET_DOLLARS", 0.0);
    if dollar_budget > 0.0 {
        if let Some(rule) = policy
            .rules
            .get_mut(&guard::RuleId::PerSessionTrustedDollarBudgetExceeded)
        {
            rule.limit_dollars = Some(dollar_budget);
        }
    }

    let api_threshold = env_u64("CC_BLACKBOX_CIRCUIT_BREAKER_THRESHOLD", 0);
    if api_threshold > 0 {
        if let Some(rule) = policy
            .rules
            .get_mut(&guard::RuleId::ApiErrorCircuitBreakerCooldown)
        {
            rule.threshold_count = Some(api_threshold);
        }
    }

    let cooldown_secs = env_u64("CC_BLACKBOX_CIRCUIT_BREAKER_COOLDOWN_SECS", 0);
    if cooldown_secs > 0 {
        if let Some(rule) = policy
            .rules
            .get_mut(&guard::RuleId::ApiErrorCircuitBreakerCooldown)
        {
            rule.cooldown_secs = Some(cooldown_secs);
        }
    }
}

fn request_guard_policy() -> Option<guard::GuardPolicy> {
    let mut policy = match std::env::var(GUARD_POLICY_PATH_ENV) {
        Ok(path) if !path.trim().is_empty() => {
            match guard::GuardPolicy::load_effective_from_path(Path::new(&path)) {
                Ok(loaded) => loaded.policy,
                Err(err) => {
                    warn!(error = %err, "guard policy failed to load; allowing request");
                    return None;
                }
            }
        }
        _ => guard::GuardPolicy::default(),
    };
    apply_legacy_env_limits(&mut policy);
    Some(policy)
}

fn build_guard_policy_report_json() -> Value {
    let defaults = guard::GuardPolicy::default();
    let mut warnings = Vec::new();
    let (mut policy, source) = match std::env::var(GUARD_POLICY_PATH_ENV) {
        Ok(path) if !path.trim().is_empty() => {
            match guard::GuardPolicy::load_effective_from_path(Path::new(&path)) {
                Ok(loaded) => {
                    warnings.extend(loaded.warnings);
                    (loaded.policy, loaded.source)
                }
                Err(err) => {
                    warnings.push(format!("{err}; guard request enforcement will fail open"));
                    (guard::GuardPolicy::default(), path)
                }
            }
        }
        _ => (guard::GuardPolicy::default(), "defaults".to_string()),
    };

    let before_legacy_env = policy.clone();
    apply_legacy_env_limits(&mut policy);
    if policy != before_legacy_env {
        warnings.push("legacy guard budget environment overrides applied".to_string());
    }

    serde_json::json!({
        "source": source,
        "warnings": warnings,
        "defaults": defaults,
        "policy": policy,
    })
}

fn guard_state_rank(state: &str) -> u8 {
    match state {
        "cooldown" => 6,
        "blocked" => 5,
        "critical" => 4,
        "warning" => 3,
        "watching" => 2,
        "healthy" => 1,
        "ended" => 0,
        _ => 1,
    }
}

fn merge_guard_state(current: &str, candidate: &str) -> String {
    if guard_state_rank(candidate) > guard_state_rank(current) {
        candidate.to_string()
    } else {
        current.to_string()
    }
}

fn guard_state_from_findings(base_state: &str, findings: &[Value]) -> String {
    if base_state == "ended" {
        return "ended".to_string();
    }

    let mut state = base_state.to_string();
    for finding in findings {
        let action = finding
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let severity = finding
            .get("severity")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let candidate = match action {
            "cooldown" => "cooldown",
            "block" => "blocked",
            "critical" => "critical",
            "warn" => "warning",
            "allow" | "diagnose_only" => base_state,
            _ if severity == "critical" => "critical",
            _ if severity == "warning" => "warning",
            _ => base_state,
        };
        state = merge_guard_state(&state, candidate);
    }
    state
}

fn guard_finding_title(rule_id: &str) -> String {
    match rule_id {
        "per_session_token_budget_exceeded" => "Session token budget exceeded",
        "per_session_trusted_dollar_budget_exceeded" => "Session dollar budget exceeded",
        "api_error_circuit_breaker_cooldown" => "API error cooldown",
        "repeated_cache_rebuilds" => "Repeated cache rebuilds",
        "context_near_warning_threshold" => "Context near limit",
        "jsonl_compaction_boundary" => "JSONL compaction boundary",
        "model_mismatch" => "Model route mismatch",
        "suspected_compaction_loop" => "Suspected compaction loop",
        "tool_failure_streak" => "Tool failure streak",
        "jsonl_only_tool_failure_streak" => "JSONL tool failure streak",
        "high_weekly_project_quota_burn" => "High quota burn",
        other => other,
    }
    .to_string()
}

const ACTIVE_GUARD_FINDING_TTL_SECS: u64 = 30 * 60;

fn status_action_for_rule(policy: Option<&guard::GuardPolicy>, rule_id: &str) -> Option<String> {
    guard::RuleId::from_policy_key(rule_id).map(|rule_id| {
        serialized_label(policy.map_or(guard::GuardAction::Allow, |policy| {
            policy.rule_action(rule_id)
        }))
    })
}

fn load_guard_findings_for_session(
    conn: Option<&Connection>,
    session_id: &str,
    limit: usize,
    policy: Option<&guard::GuardPolicy>,
) -> Vec<Value> {
    let Some(conn) = conn else {
        return Vec::new();
    };
    let mut stmt = match conn.prepare(
        "SELECT rule_id, severity, action, source, evidence_level, confidence, \
                timestamp, detail, suggested_action \
         FROM guard_findings WHERE session_id = ?1 \
           AND timestamp >= ?2 \
         ORDER BY timestamp DESC, id DESC LIMIT ?3",
    ) {
        Ok(stmt) => stmt,
        Err(_) => return Vec::new(),
    };
    let cutoff = epoch_to_iso8601(now_epoch_secs().saturating_sub(ACTIVE_GUARD_FINDING_TTL_SECS));

    let rows = match stmt.query_map(rusqlite::params![session_id, cutoff, limit as i64], |row| {
        let rule_id: String = row.get(0)?;
        let stored_action: String = row.get(2)?;
        let action = status_action_for_rule(policy, &rule_id).unwrap_or(stored_action);
        Ok(serde_json::json!({
            "rule_id": rule_id,
            "title": guard_finding_title(&rule_id),
            "severity": row.get::<_, String>(1)?,
            "action": action,
            "source": row.get::<_, String>(3)?,
            "evidence_level": row.get::<_, String>(4)?,
            "confidence": row.get::<_, f64>(5)?,
            "timestamp": row.get::<_, String>(6)?,
            "detail": row.get::<_, String>(7)?,
            "suggested_action": row.get::<_, Option<String>>(8)?,
        }))
    }) {
        Ok(rows) => rows,
        Err(_) => return Vec::new(),
    };

    rows.filter_map(Result::ok).collect()
}

fn guard_finding_status_json(
    finding: &guard::GuardFinding,
    policy: Option<&guard::GuardPolicy>,
) -> Value {
    let rule_id = finding.rule_id.to_string();
    let action = status_action_for_rule(policy, &rule_id)
        .unwrap_or_else(|| serialized_label(finding.action));
    serde_json::json!({
        "rule_id": &rule_id,
        "title": guard_finding_title(&rule_id),
        "severity": serialized_label(finding.severity),
        "action": action,
        "source": serialized_label(finding.source),
        "evidence_level": serialized_label(finding.evidence_level),
        "confidence": finding.confidence,
        "timestamp": &finding.timestamp,
        "detail": &finding.detail,
        "suggested_action": &finding.suggested_action,
    })
}

fn guard_session_status_json(
    session_id: String,
    display_name: String,
    model: String,
    base_state: &str,
    findings: Vec<Value>,
    signals: Option<Value>,
) -> Value {
    let state = guard_state_from_findings(base_state, &findings);
    let mut value = serde_json::json!({
        "session_id": session_id,
        "display_name": display_name,
        "model": model,
        "state": state,
        "findings": findings,
    });
    if let Some(signals) = signals {
        if let Some(object) = value.as_object_mut() {
            object.insert("signals".to_string(), signals);
        }
    }
    value
}

fn active_session_signals_json(session_id: &str, session: &diagnosis::SessionState) -> Value {
    let idle_secs = session.last_activity.elapsed().as_secs();
    let context = diagnosis::SESSION_TURNS.get(session_id).and_then(|turns| {
        let current = turns.iter().rev().find(|turn| {
            turn_has_context_usage(
                turn.input_tokens as u64,
                turn.cache_read_tokens as u64,
                turn.cache_creation_tokens as u64,
            )
        })?;
        let fill_percent = context_fill_percent(
            current.input_tokens as u64,
            current.cache_read_tokens as u64,
            current.cache_creation_tokens as u64,
            current.context_window_tokens,
        );
        Some(serde_json::json!({
            "fill_percent": (fill_percent * 10.0).round() / 10.0,
            "context_window_tokens": current.context_window_tokens,
            "turns_to_compact": project_turns_to_compact(session_id, fill_percent),
            "heuristic": true,
        }))
    });
    let cache = CACHE_TRACKERS.get(session_id).map(|tracker| {
        let ttl = tracker.last_ttl_min_secs;
        serde_json::json!({
            "idle_secs": idle_secs,
            "ttl_min_secs": tracker.last_ttl_min_secs,
            "ttl_max_secs": tracker.last_ttl_max_secs,
            "cache_expires_in_secs": ttl.saturating_sub(idle_secs),
            "event_type": tracker.last_event_type,
            "event_age_secs": tracker.last_event_time.map(|time| time.elapsed().as_secs()),
            "estimated_rebuild_cost_dollars": tracker.last_rebuild_cost_dollars,
            "heuristic": true,
        })
    });
    let totals = in_memory_postmortem_totals(session_id);
    serde_json::json!({
        "idle_secs": idle_secs,
        "postmortem_ready": session.idle_postmortem_sent,
        "total_tokens": totals.map(|(tokens, _)| tokens),
        "total_turns": totals.map(|(_, turns)| turns),
        "context": context,
        "cache": cache,
    })
}

fn ended_guard_session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, String, String)> {
    Ok((
        row.get::<_, String>(0)?,
        row.get::<_, String>(1)?,
        row.get::<_, String>(2)?,
    ))
}

fn load_ended_guard_sessions(
    conn: Option<&Connection>,
    session_filter: Option<&str>,
    limit: usize,
    policy: Option<&guard::GuardPolicy>,
) -> Vec<Value> {
    let Some(conn) = conn else {
        return Vec::new();
    };
    let sql = if session_filter.is_some() {
        "SELECT s.session_id, COALESCE(s.model, ''), COALESCE(s.ended_at, d.completed_at, '') \
         FROM sessions s LEFT JOIN session_diagnoses d ON s.session_id = d.session_id \
         WHERE s.session_id = ?1 AND (s.ended_at IS NOT NULL OR d.completed_at IS NOT NULL) \
         ORDER BY COALESCE(s.ended_at, d.completed_at, '') DESC LIMIT ?2"
    } else {
        "SELECT s.session_id, COALESCE(s.model, ''), COALESCE(s.ended_at, d.completed_at, '') \
         FROM sessions s LEFT JOIN session_diagnoses d ON s.session_id = d.session_id \
         WHERE s.ended_at IS NOT NULL OR d.completed_at IS NOT NULL \
         ORDER BY COALESCE(s.ended_at, d.completed_at, '') DESC LIMIT ?2"
    };
    let mut stmt = match conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(_) => return Vec::new(),
    };

    let rows: Vec<(String, String, String)> = if let Some(session_id) = session_filter {
        match stmt.query_map(
            rusqlite::params![session_id, limit as i64],
            ended_guard_session_row,
        ) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(_) => Vec::new(),
        }
    } else {
        match stmt.query_map(rusqlite::params![limit as i64], ended_guard_session_row) {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(_) => Vec::new(),
        }
    };

    rows.into_iter()
        .map(|(session_id, model, _ended_at)| {
            let findings = load_guard_findings_for_session(Some(conn), &session_id, 3, policy);
            guard_session_status_json(
                session_id.clone(),
                session_id,
                model,
                "ended",
                findings,
                None,
            )
        })
        .collect()
}

fn active_global_cooldown_status_json(policy: Option<&guard::GuardPolicy>) -> Option<Value> {
    let timestamp = now_iso8601();
    let decision = guard::evaluate_api_cooldown_for_request(
        policy,
        api_cooldown_observation(None),
        &timestamp,
    );
    let finding = decision.finding?;
    Some(guard_session_status_json(
        GLOBAL_GUARD_SESSION_ID.to_string(),
        "API error cooldown".to_string(),
        "guard".to_string(),
        "cooldown",
        vec![guard_finding_status_json(&finding, policy)],
        None,
    ))
}

fn build_guard_status_report_json(session_filter: Option<&str>) -> Value {
    let conn = Connection::open(db_path()).ok();
    let policy = request_guard_policy();
    let mut sessions = diagnosis::SESSIONS
        .iter()
        .filter_map(|entry| {
            let session = entry.value();
            if !session.session_inserted {
                return None;
            }
            if session_filter
                .map(|wanted| wanted != session.session_id)
                .unwrap_or(false)
            {
                return None;
            }
            let findings = load_guard_findings_for_session(
                conn.as_ref(),
                &session.session_id,
                3,
                policy.as_ref(),
            );
            Some(guard_session_status_json(
                session.session_id.clone(),
                session.display_name.clone(),
                session.model.clone(),
                "watching",
                findings,
                Some(active_session_signals_json(&session.session_id, session)),
            ))
        })
        .collect::<Vec<_>>();

    if session_filter
        .map(|wanted| wanted == GLOBAL_GUARD_SESSION_ID)
        .unwrap_or(true)
    {
        if let Some(global_cooldown) = active_global_cooldown_status_json(policy.as_ref()) {
            sessions.push(global_cooldown);
        }
    }

    sessions.sort_by(|a, b| {
        let a_id = a
            .get("session_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let b_id = b
            .get("session_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        a_id.cmp(b_id)
    });

    if sessions.is_empty() {
        sessions.extend(load_ended_guard_sessions(
            conn.as_ref(),
            session_filter,
            5,
            policy.as_ref(),
        ));
    }

    let overall_state = sessions
        .iter()
        .filter_map(|session| session.get("state").and_then(Value::as_str))
        .max_by_key(|state| guard_state_rank(state))
        .unwrap_or("healthy");

    let quota_burn_status = current_quota_burn_watch_event()
        .and_then(|event| serde_json::to_value(event).ok())
        .map(|mut value| {
            if let Some(object) = value.as_object_mut() {
                object.remove("type");
            }
            value
        });

    serde_json::json!({
        "overall_state": overall_state,
        "sessions": sessions,
        "quota_burn_status": quota_burn_status,
    })
}

fn evaluate_request_guard_for_session(
    session_id: Option<&str>,
    policy: Option<&guard::GuardPolicy>,
) -> Option<guard::RequestGuardDecision> {
    let timestamp = now_iso8601();
    let cooldown_decision = guard::evaluate_api_cooldown_for_request(
        policy,
        api_cooldown_observation(session_id),
        &timestamp,
    );
    if cooldown_decision.block.is_some() {
        return Some(cooldown_decision);
    }

    let budget_decision = guard::evaluate_session_budget_for_request(
        policy,
        session_budget_observation(session_id),
        &timestamp,
    );
    if budget_decision.block.is_some() {
        return Some(budget_decision);
    }

    None
}

fn guard_block_user_message(block: &guard::GuardBlock) -> String {
    let mut parts = vec![
        format!("cc-blackbox blocked this request: {}", block.reason),
        format!("rule: {}", block.rule_id),
        format!("limit/threshold: {}", block.configured_threshold_or_limit),
        format!("current: {}", block.current_observed_value),
    ];
    if let Some(session_id) = block.session_id.as_deref() {
        parts.push(format!("session: {session_id}"));
    }
    if let Some(remaining) = block.cooldown_remaining_secs {
        parts.push(format!("cooldown remaining: {remaining}s"));
    }
    parts.push(format!("recovery: {}", block.suggested_next_action));
    parts.join("; ")
}

fn make_guard_block_response(block: &guard::GuardBlock) -> ProcessingResponse {
    let message = guard_block_user_message(block);
    let body = serde_json::json!({
        "type": "error",
        "error": {
            "type": block.error_type,
            "message": message,
            "reason": block.reason,
            "rule_id": block.rule_id,
            "configured_threshold_or_limit": block.configured_threshold_or_limit,
            "configured_limit": block.configured_threshold_or_limit,
            "configured_threshold": block.configured_threshold_or_limit,
            "current_observed_value": block.current_observed_value,
            "session_id": block.session_id,
            "suggested_next_action": block.suggested_next_action,
            "cooldown_remaining_secs": block.cooldown_remaining_secs,
        }
    })
    .to_string();

    ProcessingResponse {
        response: Some(ExtProcResponse::ImmediateResponse(ImmediateResponse {
            status: Some(envoy::r#type::v3::HttpStatus {
                code: envoy::r#type::v3::StatusCode::PaymentRequired.into(),
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

fn guard_finding_watch_event(finding: guard::GuardFinding) -> watch::WatchEvent {
    watch::WatchEvent::GuardFinding {
        session_id: finding.session_id,
        rule_id: finding.rule_id,
        severity: finding.severity,
        action: finding.action,
        evidence_level: finding.evidence_level,
        source: finding.source,
        confidence: finding.confidence,
        timestamp: finding.timestamp,
        detail: finding.detail,
        suggested_action: finding.suggested_action,
    }
}

fn broadcast_guard_finding(finding: guard::GuardFinding) {
    metrics::record_guard_finding(
        &finding.rule_id.to_string(),
        &serialized_label(finding.severity),
        &serialized_label(finding.action),
        &serialized_label(finding.source),
    );
    let _ = DB_TX.send(DbCommand::WriteGuardFinding {
        finding: finding.clone(),
    });
    watch::BROADCASTER.broadcast(guard_finding_watch_event(finding));
}

// ---------------------------------------------------------------------------
// SQLite persistence (Phase 6)
// ---------------------------------------------------------------------------
fn db_path() -> String {
    std::env::var("CC_BLACKBOX_DB_PATH").unwrap_or_else(|_| "/data/cc-blackbox.db".to_string())
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    last_activity_at TEXT,
    ended_at TEXT,
    total_input_tokens INTEGER DEFAULT 0,
    total_output_tokens INTEGER DEFAULT 0,
    total_cache_read_tokens INTEGER DEFAULT 0,
    total_cache_creation_tokens INTEGER DEFAULT 0,
    total_cost_dollars REAL DEFAULT 0.0,
    cache_waste_dollars REAL DEFAULT 0.0,
    request_count INTEGER DEFAULT 0,
    model TEXT,
    working_dir TEXT,
    first_message_hash TEXT,
    prompt_correlation_hash TEXT,
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
    cache_creation_5m_tokens INTEGER DEFAULT 0,
    cache_creation_1h_tokens INTEGER DEFAULT 0,
    cache_ttl_min_secs INTEGER DEFAULT 300,
    cache_ttl_max_secs INTEGER DEFAULT 300,
    cache_ttl_source TEXT DEFAULT 'default_5m',
    outcome TEXT DEFAULT 'success',
    error_type TEXT,
    error_message TEXT,
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
    cache_ttl_min_secs    INTEGER DEFAULT 300,
    cache_ttl_max_secs    INTEGER DEFAULT 300,
    cache_ttl_source      TEXT DEFAULT 'default_5m',
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
    response_error_type   TEXT,
    response_error_message TEXT,
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

CREATE TABLE IF NOT EXISTS session_jsonl_correlations (
    proxy_session_id           TEXT PRIMARY KEY,
    jsonl_session_id           TEXT NOT NULL,
    status                     TEXT NOT NULL,
    confidence                 REAL NOT NULL,
    matched_at                 TEXT NOT NULL,
    jsonl_started_at           TEXT,
    jsonl_last_activity_at     TEXT,
    jsonl_request_count        INTEGER,
    jsonl_turn_count           INTEGER,
    FOREIGN KEY (proxy_session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS billing_reconciliations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT NOT NULL,
    imported_at         TEXT NOT NULL,
    source              TEXT NOT NULL,
    billed_cost_dollars REAL NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS guard_findings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id       TEXT NOT NULL,
    request_id       TEXT,
    turn_number      INTEGER,
    timestamp        TEXT NOT NULL,
    rule_id          TEXT NOT NULL,
    severity         TEXT NOT NULL,
    action           TEXT NOT NULL,
    source           TEXT NOT NULL,
    evidence_level   TEXT NOT NULL,
    confidence       REAL NOT NULL,
    detail           TEXT NOT NULL,
    suggested_action TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_turns_session ON turn_snapshots(session_id, turn_number);
CREATE INDEX IF NOT EXISTS idx_diagnoses_completed ON session_diagnoses(completed_at);
CREATE INDEX IF NOT EXISTS idx_tool_outcomes_session ON tool_outcomes(session_id);
CREATE INDEX IF NOT EXISTS idx_skill_events_session ON skill_events(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_mcp_events_session ON mcp_events(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_session_recall_session ON session_recall(session_id);
CREATE INDEX IF NOT EXISTS idx_session_jsonl_correlations_jsonl
    ON session_jsonl_correlations(jsonl_session_id);
CREATE INDEX IF NOT EXISTS idx_guard_findings_session ON guard_findings(session_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_billing_reconciliations_session_imported
    ON billing_reconciliations(session_id, imported_at DESC);
";

enum DbCommand {
    InsertSession {
        session_id: String,
        started_at: String,
        model: String,
        initial_prompt: Option<String>,
        working_dir: String,
        first_message_hash: String,
        prompt_correlation_hash: String,
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
        cache_creation_5m_tokens: u64,
        cache_creation_1h_tokens: u64,
        cache_ttl_min_secs: u64,
        cache_ttl_max_secs: u64,
        cache_ttl_source: String,
        outcome: String,
        error_type: Option<String>,
        error_message: Option<String>,
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
        cache_ttl_min_secs: u64,
        cache_ttl_max_secs: u64,
        cache_ttl_source: String,
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
        response_error_type: Option<String>,
        response_error_message: Option<String>,
    },
    FinalizeSession {
        session_id: String,
        ended_at: String,
        diagnosis: PersistedDiagnosis,
        recall: Option<PersistedRecall>,
        response_tx: std_mpsc::Sender<Result<(), String>>,
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
    WriteGuardFinding {
        finding: guard::GuardFinding,
    },
    WriteBillingReconciliation {
        session_id: String,
        imported_at: String,
        source: String,
        billed_cost_dollars: f64,
        response_tx: oneshot::Sender<Result<(), BillingReconciliationWriteError>>,
    },
}

#[derive(Clone, Debug)]
struct PersistedDiagnosis {
    completed_at: String,
    outcome: String,
    total_turns: u32,
    total_cost: f64,
    cache_hit_ratio: f64,
    degraded: bool,
    degradation_turn: Option<u32>,
    causes_json: String,
    advice_json: String,
}

#[derive(Clone, Debug)]
struct PersistedRecall {
    initial_prompt: String,
    final_response_summary: String,
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
    if !columns.contains("cache_ttl_min_secs") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN cache_ttl_min_secs INTEGER DEFAULT 300",
            [],
        )?;
    }
    if !columns.contains("cache_ttl_max_secs") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN cache_ttl_max_secs INTEGER DEFAULT 300",
            [],
        )?;
    }
    if !columns.contains("cache_ttl_source") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN cache_ttl_source TEXT DEFAULT 'default_5m'",
            [],
        )?;
    }
    if !columns.contains("response_error_type") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN response_error_type TEXT",
            [],
        )?;
    }
    if !columns.contains("response_error_message") {
        conn.execute(
            "ALTER TABLE turn_snapshots ADD COLUMN response_error_message TEXT",
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
    if !columns.contains("working_dir") {
        conn.execute("ALTER TABLE sessions ADD COLUMN working_dir TEXT", [])?;
    }
    if !columns.contains("first_message_hash") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN first_message_hash TEXT",
            [],
        )?;
    }
    if !columns.contains("prompt_correlation_hash") {
        conn.execute(
            "ALTER TABLE sessions ADD COLUMN prompt_correlation_hash TEXT",
            [],
        )?;
    }
    let added_last_activity_at = !columns.contains("last_activity_at");
    if added_last_activity_at {
        conn.execute("ALTER TABLE sessions ADD COLUMN last_activity_at TEXT", [])?;
        conn.execute(
            "UPDATE sessions SET last_activity_at = COALESCE(ended_at, started_at) \
             WHERE last_activity_at IS NULL",
            [],
        )?;

        let has_diagnosis_table = table_exists(conn, "session_diagnoses")?;
        let has_recall_table = table_exists(conn, "session_recall")?;
        if has_diagnosis_table {
            let recall_clause = if has_recall_table {
                " OR NOT EXISTS (
                    SELECT 1 FROM session_recall r
                    WHERE r.session_id = sessions.session_id
                  )"
            } else {
                ""
            };
            let sql = format!(
                "UPDATE sessions \
                 SET ended_at = NULL \
                 WHERE ended_at IS NOT NULL \
                   AND (
                     NOT EXISTS (
                       SELECT 1 FROM session_diagnoses d \
                       WHERE d.session_id = sessions.session_id
                     ){recall_clause}
                   )",
            );
            conn.execute(&sql, [])?;
        }
    }
    if columns.contains("cache_hit_ratio") {
        conn.execute("ALTER TABLE sessions DROP COLUMN cache_hit_ratio", [])?;
    }

    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
        rusqlite::params![table],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
}

fn derived_content_metadata(label: &str, value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let words = trimmed.split_whitespace().count();
    let chars = trimmed.chars().count();
    Some(format!(
        "{label} captured (redacted, {words} words, {chars} chars)."
    ))
}

fn is_derived_content_metadata(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.contains(" captured (redacted, ") && trimmed.ends_with(").")
}

fn content_metadata_for_storage(label: &str, value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if is_derived_content_metadata(trimmed) {
        return Some(trimmed.to_string());
    }
    derived_content_metadata(label, trimmed)
}

fn derived_optional_content_metadata(label: &str, value: Option<String>) -> Option<String> {
    value.and_then(|value| content_metadata_for_storage(label, &value))
}

fn serialized_label<T: serde::Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default()
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
    if !columns.contains("cache_creation_5m_tokens") {
        conn.execute(
            "ALTER TABLE requests ADD COLUMN cache_creation_5m_tokens INTEGER DEFAULT 0",
            [],
        )?;
    }
    if !columns.contains("cache_creation_1h_tokens") {
        conn.execute(
            "ALTER TABLE requests ADD COLUMN cache_creation_1h_tokens INTEGER DEFAULT 0",
            [],
        )?;
    }
    if !columns.contains("cache_ttl_min_secs") {
        conn.execute(
            "ALTER TABLE requests ADD COLUMN cache_ttl_min_secs INTEGER DEFAULT 300",
            [],
        )?;
    }
    if !columns.contains("cache_ttl_max_secs") {
        conn.execute(
            "ALTER TABLE requests ADD COLUMN cache_ttl_max_secs INTEGER DEFAULT 300",
            [],
        )?;
    }
    if !columns.contains("cache_ttl_source") {
        conn.execute(
            "ALTER TABLE requests ADD COLUMN cache_ttl_source TEXT DEFAULT 'default_5m'",
            [],
        )?;
    }
    if !columns.contains("outcome") {
        conn.execute(
            "ALTER TABLE requests ADD COLUMN outcome TEXT DEFAULT 'success'",
            [],
        )?;
    }
    if !columns.contains("error_type") {
        conn.execute("ALTER TABLE requests ADD COLUMN error_type TEXT", [])?;
    }
    if !columns.contains("error_message") {
        conn.execute("ALTER TABLE requests ADD COLUMN error_message TEXT", [])?;
    }

    Ok(())
}

fn repair_turn_snapshot_context_windows(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(
        "SELECT id, input_tokens, cache_read_tokens, cache_creation_tokens, \
         context_window_tokens, requested_model, actual_model \
         FROM turn_snapshots \
         WHERE context_window_tokens IS NULL \
            OR context_window_tokens <= 0 \
            OR context_window_tokens = ?1",
    )?;

    let rows = stmt
        .query_map(
            rusqlite::params![EXTENDED_CONTEXT_WINDOW_TOKENS as i64],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?.max(0) as u64,
                    row.get::<_, i64>(2)?.max(0) as u64,
                    row.get::<_, i64>(3)?.max(0) as u64,
                    row.get::<_, Option<i64>>(4)?
                        .map(|value| value.max(0) as u64),
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    for (
        id,
        input_tokens,
        cache_read_tokens,
        cache_creation_tokens,
        existing_context_window_tokens,
        requested_model,
        actual_model,
    ) in rows
    {
        if !turn_snapshot_context_window_needs_repair(
            existing_context_window_tokens,
            requested_model.as_deref(),
            actual_model.as_deref(),
            input_tokens,
            cache_read_tokens,
            cache_creation_tokens,
        ) {
            continue;
        }

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

fn persist_final_session_artifacts(
    conn: &Connection,
    session_id: &str,
    ended_at: &str,
    diagnosis: &PersistedDiagnosis,
    recall: Option<&PersistedRecall>,
) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;

    let updated = tx.execute(
        "UPDATE sessions SET ended_at = ?2 WHERE session_id = ?1",
        rusqlite::params![session_id, ended_at],
    )?;
    if updated == 0 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }

    if let Some(recall) = recall {
        let initial_prompt = content_metadata_for_storage("initial prompt", &recall.initial_prompt);
        let final_response_summary =
            content_metadata_for_storage("final response summary", &recall.final_response_summary);
        tx.execute(
            "INSERT INTO session_recall (session_id, initial_prompt, final_response_summary) \
             VALUES (?1,?2,?3) \
             ON CONFLICT(session_id) DO UPDATE SET \
                initial_prompt = excluded.initial_prompt, \
                final_response_summary = excluded.final_response_summary",
            rusqlite::params![session_id, initial_prompt, final_response_summary],
        )?;
    }

    tx.execute(
        "INSERT OR REPLACE INTO session_diagnoses (session_id, completed_at, \
         outcome, total_turns, total_cost, cache_hit_ratio, degraded, \
         degradation_turn, causes_json, advice_json) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        rusqlite::params![
            session_id,
            &diagnosis.completed_at,
            &diagnosis.outcome,
            diagnosis.total_turns,
            diagnosis.total_cost,
            diagnosis.cache_hit_ratio,
            diagnosis.degraded as i32,
            diagnosis.degradation_turn,
            &diagnosis.causes_json,
            &diagnosis.advice_json,
        ],
    )?;

    tx.commit()?;

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
    if let Err(e) = configure_sqlite_storage_privacy(&conn) {
        eprintln!("Failed to configure SQLite privacy pragmas: {e}");
    }
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
    if let Err(e) = repair_legacy_raw_content_storage(&conn) {
        eprintln!("Failed to repair legacy raw content storage: {e}");
        return;
    }
    if let Err(e) = truncate_sqlite_wal(&conn) {
        eprintln!("Failed to truncate SQLite WAL after privacy repair: {e}");
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
                working_dir,
                first_message_hash,
                prompt_correlation_hash,
            } => {
                let initial_prompt =
                    derived_optional_content_metadata("initial prompt", initial_prompt);
                let working_dir = normalize_working_dir(&working_dir);
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO sessions \
                     (session_id, started_at, last_activity_at, model, working_dir, \
                      first_message_hash, prompt_correlation_hash, initial_prompt) \
                     VALUES (?1, ?2, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        session_id,
                        started_at,
                        model,
                        working_dir,
                        first_message_hash,
                        prompt_correlation_hash,
                        initial_prompt
                    ],
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
                cache_creation_5m_tokens,
                cache_creation_1h_tokens,
                cache_ttl_min_secs,
                cache_ttl_max_secs,
                cache_ttl_source,
                outcome,
                error_type,
                error_message,
                cost_dollars,
                cost_source,
                trusted_for_budget_enforcement,
                duration_ms,
                tool_calls_json,
                tool_calls_list,
                cache_event,
            } => {
                let error_message =
                    derived_optional_content_metadata("error message", error_message);
                let inserted_rows = conn
                    .execute(
                    "INSERT OR IGNORE INTO requests (request_id, session_id, timestamp, model, \
                     input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, \
                     cache_creation_5m_tokens, cache_creation_1h_tokens, cache_ttl_min_secs, \
                     cache_ttl_max_secs, cache_ttl_source, outcome, error_type, error_message, \
                     cost_dollars, cost_source, trusted_for_budget_enforcement, duration_ms, \
                     tool_calls, cache_event) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
                    rusqlite::params![
                        &request_id,
                        &session_id,
                        timestamp,
                        model,
                        input_tokens,
                        output_tokens,
                        cache_read_tokens,
                        cache_creation_tokens,
                        cache_creation_5m_tokens,
                        cache_creation_1h_tokens,
                        cache_ttl_min_secs,
                        cache_ttl_max_secs,
                        cache_ttl_source,
                        outcome,
                        error_type,
                        error_message,
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
                     last_activity_at = ?7 \
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
                cache_ttl_min_secs,
                cache_ttl_max_secs,
                cache_ttl_source,
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
                response_error_type,
                response_error_message,
            } => {
                let response_summary =
                    derived_optional_content_metadata("assistant response", response_summary);
                let response_error_message = derived_optional_content_metadata(
                    "response error message",
                    response_error_message,
                );
                let _ = conn.execute(
                    "INSERT INTO turn_snapshots (session_id, turn_number, timestamp, \
                     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, \
                     ttft_ms, tool_calls, tool_failures, gap_from_prev_secs, \
                     context_utilization, context_window_tokens, frustration_signals, \
                     requested_model, actual_model, response_summary, cache_ttl_min_secs, \
                     cache_ttl_max_secs, cache_ttl_source, response_error_type, response_error_message) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
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
                        cache_ttl_min_secs,
                        cache_ttl_max_secs,
                        cache_ttl_source,
                        response_error_type,
                        response_error_message,
                    ],
                );
            }
            DbCommand::FinalizeSession {
                session_id,
                ended_at,
                diagnosis,
                recall,
                response_tx,
            } => {
                let result = persist_final_session_artifacts(
                    &conn,
                    &session_id,
                    &ended_at,
                    &diagnosis,
                    recall.as_ref(),
                )
                .map_err(|err| err.to_string());

                if let Err(err) = &result {
                    warn!(
                        session_id,
                        error = %err,
                        "failed to persist final session artifacts"
                    );
                }

                let _ = response_tx.send(result);
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
                let input_summary = content_metadata_for_storage("tool input", &input_summary);
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
                let detail = derived_optional_content_metadata("event detail", detail);
                let _ = conn.execute(
                    "INSERT INTO skill_events (session_id, timestamp, skill_name, event_type, \
                     confidence, source, detail) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    rusqlite::params![
                        session_id, timestamp, skill_name, event_type, confidence, source, detail,
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
                let detail = derived_optional_content_metadata("event detail", detail);
                let _ = conn.execute(
                    "INSERT INTO mcp_events (session_id, timestamp, server, tool, event_type, \
                     source, detail) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    rusqlite::params![
                        session_id, timestamp, server, tool, event_type, source, detail,
                    ],
                );
            }
            DbCommand::WriteGuardFinding { finding } => {
                let session_id =
                    if finding.session_id.trim().is_empty() || finding.session_id == "unknown" {
                        GLOBAL_GUARD_SESSION_ID.to_string()
                    } else {
                        finding.session_id.clone()
                    };
                if session_id == GLOBAL_GUARD_SESSION_ID {
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO sessions (
                            session_id, started_at, model, working_dir, first_message_hash,
                            prompt_correlation_hash, initial_prompt
                        ) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                        rusqlite::params![
                            &session_id,
                            &finding.timestamp,
                            "guard",
                            "",
                            "guard_global",
                            correlation::prompt_correlation_hash(
                                "guard-level finding (no prompt content)."
                            ),
                            "guard-level finding (no prompt content).",
                        ],
                    );
                }
                let rule_id = finding.rule_id.to_string();
                let severity = serialized_label(finding.severity);
                let action = serialized_label(finding.action);
                let source = serialized_label(finding.source);
                let evidence_level = serialized_label(finding.evidence_level);
                let _ = conn.execute(
                    "INSERT INTO guard_findings (
                        session_id, request_id, turn_number, timestamp, rule_id, severity,
                        action, source, evidence_level, confidence, detail, suggested_action
                    ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
                    rusqlite::params![
                        session_id,
                        finding.request_id,
                        finding.turn_number,
                        finding.timestamp,
                        rule_id,
                        severity,
                        action,
                        source,
                        evidence_level,
                        finding.confidence,
                        finding.detail,
                        finding.suggested_action,
                    ],
                );
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
    total_input_cache_rate: f64,
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
    let rounded = (amount.max(0.0) * 100.0).round() / 100.0;
    if rounded == -0.0 {
        0.0
    } else {
        rounded
    }
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
         COALESCE(cache_creation_5m_tokens, 0), COALESCE(cache_creation_1h_tokens, 0), \
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
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, Option<f64>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<i64>>(10)?,
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
            cache_create_5m_raw,
            cache_create_1h_raw,
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
            let mut cache_create_5m = cache_create_5m_raw.max(0) as u64;
            let cache_create_1h = cache_create_1h_raw.max(0) as u64;
            if cache_create.max(0) > 0 && cache_create_5m == 0 && cache_create_1h == 0 {
                cache_create_5m = cache_create.max(0) as u64;
            }
            accumulator.record(pricing::estimate_cost_dollars_with_cache_buckets(
                &model,
                input.max(0) as u64,
                output.max(0) as u64,
                cache_read.max(0) as u64,
                cache_create_5m,
                cache_create_1h,
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
                r.cache_creation_tokens, COALESCE(r.cache_creation_5m_tokens, 0), \
                COALESCE(r.cache_creation_1h_tokens, 0), r.cost_dollars, r.cost_source, \
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
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, Option<f64>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<i64>>(10)?,
            row.get::<_, i64>(11)? != 0,
        ))
    })?;

    let mut session_ids = HashSet::new();
    let mut cost_accumulator = CostAccumulator::new();
    let mut cache_read_tokens: i64 = 0;
    let mut cache_total_tokens: i64 = 0;
    let mut total_input_side_tokens: i64 = 0;

    for row in rows {
        let (
            session_id,
            model,
            input,
            output,
            cache_read,
            cache_create,
            cache_create_5m_raw,
            cache_create_1h_raw,
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
        total_input_side_tokens += (input + cache_read + cache_create).max(0);
        if let Some(cost) = stored_cost {
            cost_accumulator.record_persisted(cost, stored_source, stored_trusted.map(|n| n != 0));
        } else {
            let mut cache_create_5m = cache_create_5m_raw.max(0) as u64;
            let cache_create_1h = cache_create_1h_raw.max(0) as u64;
            if cache_create.max(0) > 0 && cache_create_5m == 0 && cache_create_1h == 0 {
                cache_create_5m = cache_create.max(0) as u64;
            }
            cost_accumulator.record(pricing::estimate_cost_dollars_with_cache_buckets(
                &model,
                input.max(0) as u64,
                output.max(0) as u64,
                cache_read.max(0) as u64,
                cache_create_5m,
                cache_create_1h,
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
    let total_input_cache_rate = if total_input_side_tokens > 0 {
        cache_read_tokens.max(0) as f64 / total_input_side_tokens.max(0) as f64
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
        total_input_cache_rate,
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
        "cache_reusable_prefix_ratio": (summary.cache_hit_ratio * 100.0).round() / 100.0,
        "cache_hit_ratio": (summary.cache_hit_ratio * 100.0).round() / 100.0,
        "total_input_cache_rate": (summary.total_input_cache_rate * 100.0).round() / 100.0,
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
        "pricing_caveats": pricing::pricing_caveats(),
        "today": summary_window_json(today),
        "this_week": summary_window_json(week),
        "this_month": summary_window_json(month),
    })
}

#[allow(clippy::too_many_arguments)]
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
    let causes = mark_heuristic_causes_json(causes);
    serde_json::json!({
        "session_id": session_id,
        "completed_at": completed_at,
        "outcome": outcome,
        "total_turns": total_turns,
        "estimated_total_cost_dollars": estimated_total_cost_dollars,
        "cost_source": cost_source,
        "trusted_for_budget_enforcement": trusted_for_budget_enforcement,
        "pricing_caveats": pricing::pricing_caveats(),
        "billed_cost_dollars": rounded_billed_cost_dollars(billed_cost_dollars),
        "billing_source": billing_source,
        "billing_imported_at": billing_imported_at,
        "cache_reusable_prefix_ratio": cache_hit_ratio,
        "cache_hit_ratio": cache_hit_ratio,
        "cache_ratio_label": "reusable_prefix_ratio",
        "degraded": degraded,
        "degradation_turn": if degraded { degradation_turn } else { None },
        "causes": causes,
        "advice": advice,
    })
}

#[allow(clippy::too_many_arguments)]
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
        "pricing_caveats": pricing::pricing_caveats(),
        "billed_cost_dollars": rounded_billed_cost_dollars(billed_cost_dollars),
        "billing_source": billing_source,
        "billing_imported_at": billing_imported_at,
        "primary_cause": primary_cause,
        "cache_reusable_prefix_ratio": cache_hit_ratio,
        "cache_hit_ratio": cache_hit_ratio,
        "cache_ratio_label": "reusable_prefix_ratio",
        "model": model,
    })
}

fn build_sessions_response_json(sessions: Vec<Value>) -> Value {
    serde_json::json!({
        "cost_source": pricing::active_catalog_source(),
        "trusted_for_budget_enforcement": pricing::trusted_for_budget_enforcement(),
        "pricing_caveats": pricing::pricing_caveats(),
        "sessions": sessions,
    })
}

fn build_recent_sessions_response_from_db(
    conn: &Connection,
    limit: i64,
    days: i64,
) -> Result<Value, String> {
    let since_secs = now_epoch_secs().saturating_sub(days.max(0) as u64 * 86400);
    let since = epoch_to_iso8601(since_secs);

    let mut stmt = conn
        .prepare(
            "SELECT ids.session_id, s.started_at, s.last_activity_at, s.ended_at, \
                    s.model, s.working_dir, s.request_count, s.total_input_tokens, \
                    s.total_output_tokens, s.total_cache_read_tokens, \
                    s.total_cache_creation_tokens, d.completed_at, d.outcome, \
                    d.degraded, d.total_turns, d.causes_json, d.cache_hit_ratio \
             FROM (
                SELECT session_id FROM sessions
                UNION
                SELECT session_id FROM session_diagnoses
             ) ids \
             LEFT JOIN sessions s ON s.session_id = ids.session_id \
             LEFT JOIN session_diagnoses d ON d.session_id = ids.session_id \
             WHERE COALESCE(d.completed_at, s.ended_at, s.last_activity_at, s.started_at) >= ?1 \
             ORDER BY COALESCE(d.completed_at, s.ended_at, s.last_activity_at, s.started_at) DESC \
             LIMIT ?2",
        )
        .map_err(|err| format!("prepare sessions query: {err}"))?;

    type RecentSessionRow = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<String>,
        Option<String>,
        Option<i32>,
        Option<i64>,
        Option<String>,
        Option<f64>,
    );

    let rows: Vec<RecentSessionRow> = stmt
        .query_map(rusqlite::params![since, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<u64>>(7)?,
                row.get::<_, Option<u64>>(8)?,
                row.get::<_, Option<u64>>(9)?,
                row.get::<_, Option<u64>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<i32>>(13)?,
                row.get::<_, Option<i64>>(14)?,
                row.get::<_, Option<String>>(15)?,
                row.get::<_, Option<f64>>(16)?,
            ))
        })
        .map_err(|err| format!("query sessions: {err}"))?
        .filter_map(|row| match row {
            Ok(row) => Some(row),
            Err(err) => {
                warn!(error = %err, "skipping malformed session row");
                None
            }
        })
        .collect();

    let session_ids = rows.iter().map(|row| row.0.clone()).collect::<Vec<_>>();
    let estimated_costs = compute_estimated_costs_for_sessions(conn, &session_ids)
        .map_err(|err| format!("load session cost estimates: {err}"))?;
    let billing = load_latest_billing_reconciliations(conn, &session_ids)
        .map_err(|err| format!("load billing reconciliations: {err}"))?;

    let sessions = rows
        .into_iter()
        .map(
            |(
                session_id,
                started_at,
                last_activity_at,
                ended_at,
                model,
                working_dir,
                request_count,
                total_input_tokens,
                total_output_tokens,
                total_cache_read_tokens,
                total_cache_creation_tokens,
                completed_at,
                outcome,
                degraded,
                total_turns,
                causes_str,
                diagnosis_cache_hit_ratio,
            )| {
                let causes: Vec<Value> = causes_str
                    .as_deref()
                    .and_then(|causes| serde_json::from_str(causes).ok())
                    .unwrap_or_default();
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
                let total_input_tokens = total_input_tokens.unwrap_or(0);
                let total_output_tokens = total_output_tokens.unwrap_or(0);
                let total_cache_read_tokens = total_cache_read_tokens.unwrap_or(0);
                let total_cache_creation_tokens = total_cache_creation_tokens.unwrap_or(0);
                let request_count = request_count.unwrap_or(0);
                let cache_total = total_cache_read_tokens + total_cache_creation_tokens;
                let cache_hit_ratio = diagnosis_cache_hit_ratio.unwrap_or_else(|| {
                    if cache_total == 0 {
                        0.0
                    } else {
                        total_cache_read_tokens as f64 / cache_total as f64
                    }
                });
                let outcome = outcome.unwrap_or_else(|| {
                    if ended_at.is_some() {
                        "Ended".to_string()
                    } else {
                        "In Progress".to_string()
                    }
                });
                let mut summary = build_session_summary_json(
                    session_id.clone(),
                    started_at,
                    outcome,
                    degraded.unwrap_or(0) != 0,
                    total_turns.unwrap_or(request_count),
                    estimated.estimated_cost_dollars,
                    estimated.cost_source,
                    estimated.trusted_for_budget_enforcement,
                    billed.map(|record| record.billed_cost_dollars),
                    billed.map(|record| record.source.clone()),
                    billed.map(|record| record.imported_at.clone()),
                    primary_cause,
                    cache_hit_ratio,
                    model,
                );
                if let Value::Object(ref mut object) = summary {
                    object.insert(
                        "last_activity_at".to_string(),
                        serde_json::json!(last_activity_at.clone()),
                    );
                    object.insert("ended_at".to_string(), serde_json::json!(ended_at.clone()));
                    object.insert(
                        "completed_at".to_string(),
                        serde_json::json!(completed_at.clone()),
                    );
                    object.insert("working_dir".to_string(), serde_json::json!(working_dir));
                    object.insert(
                        "request_count".to_string(),
                        serde_json::json!(request_count),
                    );
                    object.insert(
                        "total_tokens".to_string(),
                        serde_json::json!(
                            total_input_tokens
                                + total_output_tokens
                                + total_cache_read_tokens
                                + total_cache_creation_tokens
                        ),
                    );
                    object.insert(
                        "active".to_string(),
                        serde_json::json!(ended_at.is_none() && completed_at.is_none()),
                    );
                }
                summary
            },
        )
        .collect();

    Ok(build_sessions_response_json(sessions))
}

#[derive(Clone, Debug)]
struct PostmortemSessionRow {
    session_id: String,
    started_at: String,
    started_at_epoch_secs: u64,
    last_activity_at: Option<String>,
    last_activity_at_epoch_secs: u64,
    ended_at: Option<String>,
    model: Option<String>,
    working_dir: String,
    first_message_hash: String,
    prompt_correlation_hash: String,
    initial_prompt: Option<String>,
    total_input_tokens: u64,
    total_output_tokens: u64,
    total_cache_read_tokens: u64,
    total_cache_creation_tokens: u64,
    request_count: u64,
    duration_secs: Option<u64>,
}

#[derive(Clone, Debug)]
struct PostmortemTokenTotals {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
}

impl PostmortemTokenTotals {
    fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_creation_tokens
    }
}

#[derive(Clone, Debug)]
struct PostmortemDiagnosisRow {
    completed_at: String,
    outcome: String,
    total_turns: u64,
    total_cost: f64,
    cache_hit_ratio: f64,
    degraded: bool,
    degradation_turn: Option<u64>,
    causes: Value,
    advice: Value,
}

#[derive(Clone, Debug)]
struct PostmortemTurnRow {
    turn_number: u64,
    timestamp: String,
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    gap_from_prev_secs: f64,
    context_fill_percent: f64,
    context_window_tokens: u64,
    tool_failures: u64,
    requested_model: Option<String>,
    actual_model: Option<String>,
    response_summary: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct ToolAggregate {
    tool_name: String,
    calls: u64,
    failures: u64,
    total_duration_ms: u64,
    first_turn: u64,
    last_turn: u64,
    sample: Option<String>,
}

#[derive(Debug)]
enum PostmortemError {
    DbUnavailable(String),
    NoSessions,
    UnknownSession(String),
}

static URL_QUERY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(https?://[^\s<>"'\)\]]+)\?[^\s<>"'\)\]]*"#)
        .expect("valid URL query redaction regex")
});
static UNIX_PATH_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?P<prefix>^|[\s\(\["'=])/(Users|home|private|tmp|var|etc|opt|Volumes|data)(/[^\s,;\)"'\]]+)+"#)
        .expect("valid unix path redaction regex")
});
static WINDOWS_PATH_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\b[A-Z]:\\[^\s,;\)"'\]]+"#).expect("valid windows path redaction regex")
});
static SECRET_ASSIGNMENT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\b(api[_-]?key|token|secret|password|authorization|bearer)\s*[:=]\s*["']?[^"'\s,;\)]+["']?"#)
        .expect("valid secret assignment redaction regex")
});
static SECRET_TOKEN_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\b(sk-ant-[A-Za-z0-9_-]+|sk-[A-Za-z0-9_-]{12,}|xox[baprs]-[A-Za-z0-9_-]+|gh[pousr]_[A-Za-z0-9_]{12,})\b"#)
        .expect("valid secret token redaction regex")
});
static LONG_SECRET_LIKE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\b[A-Za-z0-9+/=_-]{40,}\b"#).expect("valid long secret-like redaction regex")
});

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn redact_operational_text(value: &str, redact: bool) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if redact
        && ((trimmed.starts_with('{') && trimmed.ends_with('}'))
            || (trimmed.starts_with('[') && trimmed.ends_with(']')))
    {
        return "<structured payload redacted>".to_string();
    }
    if !redact {
        return truncate_chars(trimmed, 240);
    }

    let mut out = trimmed.to_string();
    out = URL_QUERY_RE
        .replace_all(&out, "${1}?<redacted>")
        .to_string();
    out = UNIX_PATH_RE
        .replace_all(&out, "${prefix}<path>")
        .to_string();
    out = WINDOWS_PATH_RE.replace_all(&out, "<path>").to_string();
    out = SECRET_ASSIGNMENT_RE
        .replace_all(&out, "$1=<redacted>")
        .to_string();
    out = SECRET_TOKEN_RE.replace_all(&out, "<secret>").to_string();
    out = LONG_SECRET_LIKE_RE
        .replace_all(&out, "<secret-like>")
        .to_string();
    truncate_chars(out.trim(), 240)
}

fn redacted_human_summary(label: &str, value: &str, redact: bool) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if redact {
        let words = trimmed.split_whitespace().count();
        return Some(format!("{label} captured (redacted, {words} words)."));
    }
    Some(redact_operational_text(trimmed, false))
}

fn query_latest_postmortem_session_id(conn: &Connection) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT s.session_id \
         FROM sessions s \
         LEFT JOIN session_diagnoses d ON d.session_id = s.session_id \
         ORDER BY COALESCE(d.completed_at, s.ended_at, s.last_activity_at, s.started_at) DESC \
         LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    )
    .optional()
}

fn load_postmortem_session(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<Option<PostmortemSessionRow>> {
    conn.query_row(
        "SELECT session_id, started_at, \
                CAST(strftime('%s', started_at) AS INTEGER), \
                last_activity_at, \
                CAST(strftime('%s', COALESCE(last_activity_at, ended_at, started_at)) AS INTEGER), \
                ended_at, model, COALESCE(working_dir, ''), COALESCE(first_message_hash, ''), \
                COALESCE(prompt_correlation_hash, ''), initial_prompt, \
                total_input_tokens, total_output_tokens, total_cache_read_tokens, \
                total_cache_creation_tokens, request_count, \
                CASE WHEN ended_at IS NOT NULL \
                     THEN CAST(strftime('%s', ended_at) AS INTEGER) - CAST(strftime('%s', started_at) AS INTEGER) \
                     WHEN last_activity_at IS NOT NULL \
                     THEN CAST(strftime('%s', last_activity_at) AS INTEGER) - CAST(strftime('%s', started_at) AS INTEGER) \
                END \
         FROM sessions WHERE session_id = ?1",
        rusqlite::params![session_id],
        |row| {
            Ok(PostmortemSessionRow {
                session_id: row.get(0)?,
                started_at: row.get(1)?,
                started_at_epoch_secs: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
                last_activity_at: row.get(3)?,
                last_activity_at_epoch_secs: row.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0)
                    as u64,
                ended_at: row.get(5)?,
                model: row.get(6)?,
                working_dir: row.get(7)?,
                first_message_hash: row.get(8)?,
                prompt_correlation_hash: row.get(9)?,
                initial_prompt: row.get(10)?,
                total_input_tokens: row.get::<_, Option<i64>>(11)?.unwrap_or(0).max(0) as u64,
                total_output_tokens: row.get::<_, Option<i64>>(12)?.unwrap_or(0).max(0) as u64,
                total_cache_read_tokens: row.get::<_, Option<i64>>(13)?.unwrap_or(0).max(0)
                    as u64,
                total_cache_creation_tokens: row.get::<_, Option<i64>>(14)?.unwrap_or(0).max(0)
                    as u64,
                request_count: row.get::<_, Option<i64>>(15)?.unwrap_or(0).max(0) as u64,
                duration_secs: row
                    .get::<_, Option<i64>>(16)?
                    .map(|value| value.max(0) as u64),
            })
        },
    )
    .optional()
}

fn session_is_live(session_id: &str) -> bool {
    diagnosis::SESSION_TURNS.get(session_id).is_some()
        || diagnosis::SESSIONS
            .iter()
            .any(|entry| entry.session_id == session_id)
}

fn remove_session_state_by_id(session_id: &str) -> Option<diagnosis::SessionState> {
    let hash = diagnosis::SESSIONS
        .iter()
        .find_map(|entry| (entry.session_id == session_id).then_some(*entry.key()))?;
    diagnosis::SESSIONS.remove(&hash).map(|(_, state)| state)
}

#[derive(Clone, Debug)]
struct ExpiredSessionState {
    session_id: String,
    session_inserted: bool,
    model: String,
    initial_prompt: Option<String>,
}

fn session_state_if_still_expired(
    hash: u64,
    timeout_secs: u64,
    now: Instant,
) -> Option<ExpiredSessionState> {
    let state = diagnosis::SESSIONS.get(&hash)?;
    let idle_secs = now.duration_since(state.last_activity).as_secs();
    if idle_secs <= timeout_secs {
        return None;
    }

    Some(ExpiredSessionState {
        session_id: state.session_id.clone(),
        session_inserted: state.session_inserted,
        model: state.model.clone(),
        initial_prompt: state.initial_prompt.clone(),
    })
}

#[derive(Clone, Debug)]
struct LiveSessionSnapshot {
    session_inserted: bool,
    model: String,
    initial_prompt: Option<String>,
}

fn live_session_snapshot_by_id(session_id: &str) -> Option<LiveSessionSnapshot> {
    diagnosis::SESSIONS.iter().find_map(|entry| {
        let state = entry.value();
        (state.session_id == session_id).then(|| LiveSessionSnapshot {
            session_inserted: state.session_inserted,
            model: state.model.clone(),
            initial_prompt: state.initial_prompt.clone(),
        })
    })
}

fn load_postmortem_token_totals(
    conn: &Connection,
    session: &PostmortemSessionRow,
) -> rusqlite::Result<PostmortemTokenTotals> {
    let mut totals = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), \
                COALESCE(SUM(cache_read_tokens), 0), COALESCE(SUM(cache_creation_tokens), 0) \
         FROM requests WHERE session_id = ?1",
        rusqlite::params![&session.session_id],
        |row| {
            Ok(PostmortemTokenTotals {
                request_count: row.get::<_, i64>(0)?.max(0) as u64,
                input_tokens: row.get::<_, i64>(1)?.max(0) as u64,
                output_tokens: row.get::<_, i64>(2)?.max(0) as u64,
                cache_read_tokens: row.get::<_, i64>(3)?.max(0) as u64,
                cache_creation_tokens: row.get::<_, i64>(4)?.max(0) as u64,
            })
        },
    )?;

    if totals.total_tokens() == 0 {
        totals = PostmortemTokenTotals {
            request_count: session.request_count,
            input_tokens: session.total_input_tokens,
            output_tokens: session.total_output_tokens,
            cache_read_tokens: session.total_cache_read_tokens,
            cache_creation_tokens: session.total_cache_creation_tokens,
        };
    }

    Ok(totals)
}

fn load_postmortem_diagnosis(
    conn: &Connection,
    session: &PostmortemSessionRow,
) -> rusqlite::Result<Option<PostmortemDiagnosisRow>> {
    if session.ended_at.is_none() {
        return Ok(None);
    }

    let stored = conn
        .query_row(
            "SELECT completed_at, outcome, total_turns, total_cost, cache_hit_ratio, degraded, \
                    degradation_turn, causes_json, advice_json \
             FROM session_diagnoses WHERE session_id = ?1",
            rusqlite::params![&session.session_id],
            |row| {
                let causes_json: String = row.get(7)?;
                let advice_json: String = row.get(8)?;
                Ok(PostmortemDiagnosisRow {
                    completed_at: row.get(0)?,
                    outcome: row.get(1)?,
                    total_turns: row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64,
                    total_cost: row.get::<_, Option<f64>>(3)?.unwrap_or(0.0).max(0.0),
                    cache_hit_ratio: row.get::<_, Option<f64>>(4)?.unwrap_or(0.0).clamp(0.0, 1.0),
                    degraded: row.get::<_, i32>(5)? != 0,
                    degradation_turn: row
                        .get::<_, Option<i64>>(6)?
                        .map(|value| value.max(0) as u64),
                    causes: serde_json::from_str::<Value>(&causes_json)
                        .unwrap_or_else(|_| Value::Array(vec![])),
                    advice: serde_json::from_str::<Value>(&advice_json)
                        .unwrap_or_else(|_| Value::Array(vec![])),
                })
            },
        )
        .optional()?;

    if stored.is_some() {
        return Ok(stored);
    }

    let turns = load_turn_snapshots_from_db(conn, &session.session_id)?;
    if turns.is_empty() {
        return Ok(None);
    }

    let report = diagnosis::analyze_session(&session.session_id, &turns);
    let completed_at = session
        .ended_at
        .clone()
        .unwrap_or_else(|| session.started_at.clone());
    if session.ended_at.is_some() {
        let _ = conn.execute(
            "INSERT OR REPLACE INTO session_diagnoses (session_id, completed_at, \
             outcome, total_turns, total_cost, cache_hit_ratio, degraded, degradation_turn, \
             causes_json, advice_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            rusqlite::params![
                &session.session_id,
                &completed_at,
                &report.outcome,
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

    Ok(Some(PostmortemDiagnosisRow {
        completed_at,
        outcome: report.outcome,
        total_turns: report.total_turns as u64,
        total_cost: report.estimated_total_cost_dollars,
        cache_hit_ratio: report.cache_hit_ratio,
        degraded: report.degraded,
        degradation_turn: report.degradation_turn.map(u64::from),
        causes: serde_json::to_value(&report.causes).unwrap_or_else(|_| Value::Array(vec![])),
        advice: serde_json::to_value(&report.advice).unwrap_or_else(|_| Value::Array(vec![])),
    }))
}

fn load_postmortem_turns(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<Vec<PostmortemTurnRow>> {
    let mut stmt = conn.prepare(
        "SELECT turn_number, timestamp, input_tokens, cache_read_tokens, cache_creation_tokens, \
                gap_from_prev_secs, context_window_tokens, tool_failures, \
                requested_model, actual_model, response_summary \
         FROM turn_snapshots WHERE session_id = ?1 ORDER BY turn_number ASC",
    )?;

    let rows = stmt.query_map(rusqlite::params![session_id], |row| {
        let input_tokens = row.get::<_, Option<i64>>(2)?.unwrap_or(0).max(0) as u64;
        let cache_read_tokens = row.get::<_, Option<i64>>(3)?.unwrap_or(0).max(0) as u64;
        let cache_creation_tokens = row.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64;
        let requested_model = row.get::<_, Option<String>>(8)?;
        let actual_model = row.get::<_, Option<String>>(9)?;
        let context_window_tokens = row
            .get::<_, Option<i64>>(6)?
            .map(|value| value.max(0) as u64)
            .filter(|value| *value > 0)
            .unwrap_or_else(|| {
                infer_context_window_tokens(
                    requested_model.as_deref(),
                    actual_model.as_deref(),
                    input_tokens,
                    cache_read_tokens,
                    cache_creation_tokens,
                )
            });
        Ok(PostmortemTurnRow {
            turn_number: row.get::<_, i64>(0)?.max(0) as u64,
            timestamp: row.get(1)?,
            input_tokens,
            cache_read_tokens,
            cache_creation_tokens,
            gap_from_prev_secs: row.get::<_, Option<f64>>(5)?.unwrap_or(0.0).max(0.0),
            context_fill_percent: context_fill_percent(
                input_tokens,
                cache_read_tokens,
                cache_creation_tokens,
                context_window_tokens,
            ),
            context_window_tokens,
            tool_failures: row.get::<_, Option<i64>>(7)?.unwrap_or(0).max(0) as u64,
            requested_model,
            actual_model,
            response_summary: row
                .get::<_, Option<String>>(10)?
                .filter(|value| !value.trim().is_empty()),
        })
    })?;

    Ok(rows.filter_map(|row| row.ok()).collect())
}

fn load_postmortem_recall(
    conn: &Connection,
    session_id: &str,
) -> rusqlite::Result<(Option<String>, Option<String>)> {
    conn.query_row(
        "SELECT initial_prompt, final_response_summary FROM session_recall WHERE session_id = ?1",
        rusqlite::params![session_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map(|row| row.unwrap_or((None, None)))
}

fn load_postmortem_tool_summaries(
    conn: &Connection,
    session_id: &str,
    redact: bool,
) -> rusqlite::Result<Vec<ToolAggregate>> {
    let mut stmt = conn.prepare(
        "SELECT turn_number, tool_name, input_summary, outcome, duration_ms \
         FROM tool_outcomes WHERE session_id = ?1 ORDER BY turn_number ASC, id ASC",
    )?;
    let rows = stmt.query_map(rusqlite::params![session_id], |row| {
        Ok((
            row.get::<_, i64>(0)?.max(0) as u64,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64,
        ))
    })?;

    let mut by_tool: BTreeMap<String, ToolAggregate> = BTreeMap::new();
    for row in rows.flatten() {
        let (turn, tool_name, input_summary, outcome, duration_ms) = row;
        let entry = by_tool
            .entry(tool_name.clone())
            .or_insert_with(|| ToolAggregate {
                tool_name,
                first_turn: turn,
                last_turn: turn,
                ..ToolAggregate::default()
            });
        entry.calls += 1;
        entry.total_duration_ms += duration_ms;
        entry.first_turn = entry.first_turn.min(turn);
        entry.last_turn = entry.last_turn.max(turn);
        if matches!(outcome.as_deref(), Some("error" | "failed")) {
            entry.failures += 1;
        }
        if entry.sample.is_none() {
            entry.sample = input_summary
                .as_deref()
                .map(|summary| redact_operational_text(summary, redact))
                .filter(|summary| !summary.is_empty());
        }
    }

    let mut tools = by_tool.into_values().collect::<Vec<_>>();
    tools.sort_by(|a, b| {
        b.failures
            .cmp(&a.failures)
            .then_with(|| b.calls.cmp(&a.calls))
            .then_with(|| a.tool_name.cmp(&b.tool_name))
    });
    tools.truncate(8);
    Ok(tools)
}

fn load_grouped_event_summaries(
    conn: &Connection,
    session_id: &str,
    table: &str,
) -> rusqlite::Result<Vec<Value>> {
    let sql = match table {
        "skill_events" => {
            "SELECT skill_name, event_type, source, COUNT(*) \
             FROM skill_events WHERE session_id = ?1 \
             GROUP BY skill_name, event_type, source \
             ORDER BY COUNT(*) DESC, skill_name ASC, event_type ASC, source ASC LIMIT 8"
        }
        "mcp_events" => {
            "SELECT server, tool, event_type, source, COUNT(*) \
             FROM mcp_events WHERE session_id = ?1 \
             GROUP BY server, tool, event_type, source \
             ORDER BY COUNT(*) DESC, server ASC, tool ASC, event_type ASC, source ASC LIMIT 8"
        }
        _ => return Ok(Vec::new()),
    };

    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(rusqlite::params![session_id], |row| {
        if table == "skill_events" {
            Ok(serde_json::json!({
                "skill_name": row.get::<_, String>(0)?,
                "event_type": row.get::<_, String>(1)?,
                "source": row.get::<_, String>(2)?,
                "count": row.get::<_, i64>(3)?.max(0) as u64,
            }))
        } else {
            Ok(serde_json::json!({
                "server": row.get::<_, String>(0)?,
                "tool": row.get::<_, String>(1)?,
                "event_type": row.get::<_, String>(2)?,
                "source": row.get::<_, String>(3)?,
                "count": row.get::<_, i64>(4)?.max(0) as u64,
            }))
        }
    })?;

    Ok(rows.filter_map(|row| row.ok()).collect())
}

fn evidence_json(
    evidence_type: &str,
    label: &str,
    detail: String,
    turn: Option<u64>,
    timestamp: Option<String>,
) -> Value {
    serde_json::json!({
        "type": evidence_type,
        "label": label,
        "detail": detail,
        "turn": turn,
        "timestamp": timestamp,
    })
}

fn first_cause(causes: &Value) -> Option<&Value> {
    causes.as_array().and_then(|items| items.first())
}

fn cause_string(cause: &Value, key: &str) -> Option<String> {
    cause
        .get(key)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

fn cause_u64(cause: &Value, key: &str) -> Option<u64> {
    cause.get(key).and_then(|value| value.as_u64())
}

fn cause_f64(cause: &Value, key: &str) -> Option<f64> {
    cause.get(key).and_then(|value| value.as_f64())
}

fn cause_is_heuristic(cause: &Value) -> bool {
    cause
        .get("is_heuristic")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

fn heuristic_detail_has_marker(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("[heuristic]")
        || lower.contains("heuristic")
        || lower.contains("inferred")
        || lower.contains("likely")
        || lower.contains("suspected")
        || lower.contains("appears")
        || lower.contains("suggests")
        || lower.contains("may indicate")
}

fn mark_heuristic_detail(detail: String, is_heuristic: bool) -> String {
    if is_heuristic && !heuristic_detail_has_marker(&detail) {
        format!("[heuristic] {detail}")
    } else {
        detail
    }
}

fn mark_heuristic_causes_json(causes: Value) -> Value {
    let Some(items) = causes.as_array() else {
        return causes;
    };

    Value::Array(
        items
            .iter()
            .map(|cause| {
                let mut cause = cause.clone();
                let is_heuristic = cause_is_heuristic(&cause);
                if is_heuristic {
                    if let Some(detail) = cause.get("detail").and_then(|value| value.as_str()) {
                        let marked = mark_heuristic_detail(detail.to_string(), true);
                        if let Some(object) = cause.as_object_mut() {
                            object.insert("detail".to_string(), Value::String(marked));
                        }
                    }
                }
                cause
            })
            .collect(),
    )
}

fn sanitize_causes(causes: &Value, redact: bool) -> Vec<Value> {
    causes
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|cause| {
                    let cause_type = cause_string(cause, "cause_type")?;
                    let is_heuristic = cause_is_heuristic(cause);
                    let detail = cause_string(cause, "detail")
                        .map(|value| redact_operational_text(&value, redact))
                        .map(|value| mark_heuristic_detail(value, is_heuristic))
                        .unwrap_or_else(|| cause_type.clone());
                    Some(serde_json::json!({
                        "cause_type": cause_type,
                        "detail": detail,
                        "turn_first_noticed": cause_u64(cause, "turn_first_noticed"),
                        "turn_last_noticed": cause_u64(cause, "turn_last_noticed"),
                        "estimated_cost": cause_f64(cause, "estimated_cost").unwrap_or(0.0),
                        "estimated_wasted_tokens": cause_u64(cause, "estimated_wasted_tokens"),
                        "is_heuristic": is_heuristic,
                        "requested_model": cause_string(cause, "requested_model"),
                        "actual_model": cause_string(cause, "actual_model"),
                    }))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn sanitize_advice(advice: &Value, redact: bool) -> Vec<String> {
    advice
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(|item| redact_operational_text(item, redact))
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn postmortem_confidence(
    diagnosis: Option<&PostmortemDiagnosisRow>,
    evidence_count: usize,
) -> &'static str {
    let Some(diagnosis) = diagnosis else {
        return "low";
    };
    if diagnosis.total_turns < 3 && !diagnosis.degraded {
        return "low";
    }
    if let Some(cause) = first_cause(&diagnosis.causes) {
        if cause_is_heuristic(cause) {
            "medium"
        } else if diagnosis.degraded {
            "high"
        } else {
            "medium"
        }
    } else if evidence_count >= 3 {
        "medium"
    } else {
        "low"
    }
}

fn recommended_action(primary_cause: &str, advice: &[String], partial: bool) -> String {
    if partial {
        return "Session is still partial; use this as a progress snapshot, not a final diagnosis."
            .to_string();
    }
    if let Some(first) = advice.first() {
        return first.clone();
    }
    match primary_cause {
        "cache_miss_ttl" => "Restart with a smaller prompt or keep the cache warm before expensive follow-up turns.".to_string(),
        "cache_miss_thrash" => "Reduce prompt churn before continuing; repeated cache rebuilds are likely wasting tokens.".to_string(),
        "context_bloat" | "near_compaction" | "compaction_suspected" => {
            "Compact or start a fresh session with a short state summary before asking for more work.".to_string()
        }
        "model_fallback" => "Treat model-routing changes as part of the failure; retry later or explicitly choose the model you need.".to_string(),
        "tool_failure_streak" => "Stop the loop, fix the failing command or tool precondition manually, then restart Claude with the result.".to_string(),
        "" | "none" => "No major degradation signal was found; continue unless the session behavior felt wrong.".to_string(),
        _ => "Review the evidence below, then restart with a shorter prompt if Claude was slow, costly, or repetitive.".to_string(),
    }
}

fn cache_hit_ratio_from_tokens(totals: &PostmortemTokenTotals) -> f64 {
    let cache_total = totals.cache_read_tokens + totals.cache_creation_tokens;
    if cache_total == 0 {
        0.0
    } else {
        totals.cache_read_tokens as f64 / cache_total as f64
    }
}

fn total_input_cache_rate_from_tokens(totals: &PostmortemTokenTotals) -> f64 {
    let input_side_total =
        totals.input_tokens + totals.cache_read_tokens + totals.cache_creation_tokens;
    if input_side_total == 0 {
        0.0
    } else {
        totals.cache_read_tokens as f64 / input_side_total as f64
    }
}

fn proxy_only_jsonl_enrichment_status(reason: &str) -> Value {
    serde_json::json!({
        "source": "jsonl",
        "status": "proxy_only",
        "reason": reason,
        "confidence": 0.0,
    })
}

struct PostmortemJsonlEnrichment {
    status: Value,
    summary: Option<jsonl::JsonlDerivedSession>,
    findings: Vec<guard::GuardFinding>,
}

#[derive(Clone, Debug)]
struct StoredJsonlCorrelation {
    jsonl_session_id: String,
}

fn postmortem_session_allows_jsonl_enrichment(session: &PostmortemSessionRow) -> bool {
    if session.ended_at.is_some() {
        return true;
    }
    let last_activity = session.last_activity_at_epoch_secs;
    if last_activity == 0 {
        return false;
    }
    now_epoch_secs().saturating_sub(last_activity) >= postmortem_idle_secs()
}

fn jsonl_search_roots() -> Vec<PathBuf> {
    if let Ok(raw) = std::env::var(JSONL_DIR_ENV) {
        return std::env::split_paths(&raw).collect();
    }
    #[cfg(test)]
    {
        Vec::new()
    }
    #[cfg(not(test))]
    {
        std::env::var("HOME")
            .ok()
            .map(|home| PathBuf::from(home).join(".claude").join("projects"))
            .into_iter()
            .collect()
    }
}

fn collect_jsonl_paths(root: &Path, paths: &mut Vec<PathBuf>) {
    const MAX_JSONL_FILES: usize = 512;
    if paths.len() >= MAX_JSONL_FILES {
        return;
    }
    let Ok(metadata) = std::fs::metadata(root) else {
        return;
    };
    if metadata.is_file() {
        if root.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            paths.push(root.to_path_buf());
        }
        return;
    }
    if !metadata.is_dir() {
        return;
    }

    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        collect_jsonl_paths(&entry.path(), paths);
        if paths.len() >= MAX_JSONL_FILES {
            break;
        }
    }
}

fn load_jsonl_candidate_sessions() -> Vec<jsonl::JsonlDerivedSession> {
    let mut paths = Vec::new();
    for root in jsonl_search_roots() {
        collect_jsonl_paths(&root, &mut paths);
    }
    paths.sort();
    paths.dedup();
    paths
        .iter()
        .filter_map(|path| match jsonl::parse_jsonl_file(path) {
            Ok(session) => session,
            Err(err) => {
                debug!(path = %path.display(), error = %err, "failed to parse JSONL candidate");
                None
            }
        })
        .collect()
}

fn load_jsonl_candidate_session_by_id(
    jsonl_session_id: &str,
) -> Option<jsonl::JsonlDerivedSession> {
    load_jsonl_candidate_sessions()
        .into_iter()
        .find(|session| session.jsonl_session_id == jsonl_session_id)
}

fn stored_proxy_session_for_jsonl(
    conn: &Connection,
    jsonl_session_id: &str,
) -> rusqlite::Result<Option<String>> {
    if !table_exists(conn, "session_jsonl_correlations")? {
        return Ok(None);
    }

    conn.query_row(
        "SELECT proxy_session_id \
         FROM session_jsonl_correlations \
         WHERE jsonl_session_id = ?1 AND status = 'matched' \
         ORDER BY matched_at DESC \
         LIMIT 1",
        rusqlite::params![jsonl_session_id],
        |row| row.get(0),
    )
    .optional()
}

fn active_proxy_session_correlations(
    conn: &Connection,
) -> rusqlite::Result<Vec<correlation::ProxySessionCorrelation>> {
    let mut stmt = conn.prepare(
        "SELECT s.session_id, s.working_dir, s.started_at, \
                COALESCE(s.last_activity_at, s.started_at), \
                COALESCE(s.first_message_hash, ''), \
                COALESCE(s.prompt_correlation_hash, ''), \
                COALESCE(s.request_count, 0) \
         FROM sessions s \
         LEFT JOIN session_diagnoses d ON d.session_id = s.session_id \
         WHERE s.ended_at IS NULL AND d.completed_at IS NULL",
    )?;

    let rows = stmt.query_map([], |row| {
        let started_at: String = row.get(2)?;
        let last_activity_at: String = row.get(3)?;
        let request_count = row.get::<_, Option<i64>>(6)?.unwrap_or(0).max(0) as u64;
        Ok(correlation::ProxySessionCorrelation {
            session_id: row.get(0)?,
            cwd: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            started_at_epoch_secs: jsonl::parse_epoch_secs(&started_at).unwrap_or(0),
            last_activity_at_epoch_secs: jsonl::parse_epoch_secs(&last_activity_at).unwrap_or(0),
            first_message_hash: row.get(4)?,
            prompt_correlation_hash: row.get(5)?,
            request_count,
            turn_count: request_count,
        })
    })?;

    Ok(rows.filter_map(Result::ok).collect())
}

fn build_jsonl_session_resolution_response_from_db(
    conn: &Connection,
    jsonl_session_id: &str,
) -> Result<Value, String> {
    let jsonl_session_id = jsonl_session_id.trim();
    if jsonl_session_id.is_empty() {
        return Ok(serde_json::json!({
            "status": "missing_jsonl_session_id",
            "matched": false,
        }));
    }

    if let Some(proxy_session_id) = stored_proxy_session_for_jsonl(conn, jsonl_session_id)
        .map_err(|err| format!("load stored jsonl correlation: {err}"))?
    {
        return Ok(serde_json::json!({
            "status": "matched",
            "matched": true,
            "source": "stored_jsonl_correlation",
            "jsonl_session_id": jsonl_session_id,
            "proxy_session_id": proxy_session_id,
        }));
    }

    let Some(summary) = load_jsonl_candidate_session_by_id(jsonl_session_id) else {
        return Ok(serde_json::json!({
            "status": "missing_jsonl",
            "matched": false,
            "jsonl_session_id": jsonl_session_id,
        }));
    };

    let candidate = summary.correlation();
    let proxies = active_proxy_session_correlations(conn)
        .map_err(|err| format!("load active proxy sessions: {err}"))?;
    let matches = proxies
        .iter()
        .filter(|proxy| correlation::candidate_matches_proxy(proxy, &candidate))
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [proxy] => {
            persist_jsonl_correlation(conn, &proxy.session_id, &summary)
                .map_err(|err| format!("persist jsonl correlation: {err}"))?;
            Ok(serde_json::json!({
                "status": "matched",
                "matched": true,
                "source": "live_jsonl_correlation",
                "jsonl_session_id": jsonl_session_id,
                "proxy_session_id": proxy.session_id,
            }))
        }
        [] => Ok(serde_json::json!({
            "status": "no_match",
            "matched": false,
            "jsonl_session_id": jsonl_session_id,
        })),
        many => Ok(serde_json::json!({
            "status": "ambiguous",
            "matched": false,
            "jsonl_session_id": jsonl_session_id,
            "candidate_count": many.len(),
        })),
    }
}

fn proxy_correlation_for_postmortem_session(
    session: &PostmortemSessionRow,
    turn_count: u64,
) -> correlation::ProxySessionCorrelation {
    correlation::ProxySessionCorrelation {
        session_id: session.session_id.clone(),
        cwd: session.working_dir.clone(),
        started_at_epoch_secs: session.started_at_epoch_secs,
        last_activity_at_epoch_secs: session.last_activity_at_epoch_secs,
        first_message_hash: session.first_message_hash.clone(),
        prompt_correlation_hash: session.prompt_correlation_hash.clone(),
        request_count: session.request_count,
        turn_count,
    }
}

fn jsonl_matched_status(proxy_session_id: &str, jsonl_session_id: &str) -> Value {
    serde_json::json!({
        "source": "jsonl",
        "status": "matched",
        "proxy_session_id": proxy_session_id,
        "jsonl_session_id": jsonl_session_id,
        "confidence": 0.95,
    })
}

fn load_stored_jsonl_correlation(
    conn: &Connection,
    proxy_session_id: &str,
) -> rusqlite::Result<Option<StoredJsonlCorrelation>> {
    if !table_exists(conn, "session_jsonl_correlations")? {
        return Ok(None);
    }

    conn.query_row(
        "SELECT jsonl_session_id \
         FROM session_jsonl_correlations \
         WHERE proxy_session_id = ?1 AND status = 'matched'",
        rusqlite::params![proxy_session_id],
        |row| {
            Ok(StoredJsonlCorrelation {
                jsonl_session_id: row.get(0)?,
            })
        },
    )
    .optional()
}

fn persist_jsonl_correlation(
    conn: &Connection,
    proxy_session_id: &str,
    summary: &jsonl::JsonlDerivedSession,
) -> rusqlite::Result<()> {
    let jsonl_started_at = (summary.started_at_epoch_secs > 0)
        .then(|| epoch_to_iso8601(summary.started_at_epoch_secs));
    let jsonl_last_activity_at = (summary.last_activity_at_epoch_secs > 0)
        .then(|| epoch_to_iso8601(summary.last_activity_at_epoch_secs));
    conn.execute(
        "INSERT INTO session_jsonl_correlations (
            proxy_session_id, jsonl_session_id, status, confidence, matched_at,
            jsonl_started_at, jsonl_last_activity_at, jsonl_request_count, jsonl_turn_count
        ) VALUES (?1, ?2, 'matched', 0.95, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(proxy_session_id) DO UPDATE SET
            jsonl_session_id = excluded.jsonl_session_id,
            status = excluded.status,
            confidence = excluded.confidence,
            matched_at = excluded.matched_at,
            jsonl_started_at = excluded.jsonl_started_at,
            jsonl_last_activity_at = excluded.jsonl_last_activity_at,
            jsonl_request_count = excluded.jsonl_request_count,
            jsonl_turn_count = excluded.jsonl_turn_count",
        rusqlite::params![
            proxy_session_id,
            &summary.jsonl_session_id,
            now_iso8601(),
            jsonl_started_at,
            jsonl_last_activity_at,
            summary.request_count,
            summary.assistant_turn_count,
        ],
    )?;
    Ok(())
}

fn stored_jsonl_match(
    conn: &Connection,
    proxy: &correlation::ProxySessionCorrelation,
    candidates: &[jsonl::JsonlDerivedSession],
) -> Option<jsonl::JsonlDerivedSession> {
    let stored = match load_stored_jsonl_correlation(conn, &proxy.session_id) {
        Ok(stored) => stored?,
        Err(err) => {
            debug!(
                session_id = %proxy.session_id,
                error = %err,
                "failed to load stored JSONL correlation"
            );
            return None;
        }
    };

    candidates
        .iter()
        .find(|candidate| {
            candidate.jsonl_session_id == stored.jsonl_session_id
                && correlation::candidate_matches_proxy(proxy, &candidate.correlation())
        })
        .cloned()
}

fn jsonl_ambiguous_status(proxy_session_id: &str, candidate_count: usize) -> Value {
    serde_json::json!({
        "source": "jsonl",
        "status": "ambiguous",
        "proxy_session_id": proxy_session_id,
        "candidate_count": candidate_count,
        "reason": "ambiguous_match",
        "confidence": 0.0,
    })
}

fn jsonl_tool_failure_finding(
    session_id: &str,
    summary: &jsonl::JsonlDerivedSession,
) -> Option<guard::GuardFinding> {
    let consecutive_failure_turns = summary.longest_tool_failure_streak();
    let failed_tool_calls = summary.total_tool_failures();
    let policy = guard::GuardPolicy::default();
    guard::detect_tool_failure_finding(
        Some(&policy),
        guard::ToolFailureObservation {
            session_id: session_id.to_string(),
            request_id: None,
            turn_number: summary.last_failed_tool_turn().map(|turn| turn as u32),
            timestamp: epoch_to_iso8601(summary.last_activity_at_epoch_secs),
            consecutive_failure_turns: consecutive_failure_turns.min(u32::MAX as u64) as u32,
            failed_tool_calls: failed_tool_calls.min(u32::MAX as u64) as u32,
            source: guard::FindingSource::Jsonl,
        },
    )
}

fn load_postmortem_jsonl_enrichment(
    conn: &Connection,
    session: &PostmortemSessionRow,
    turn_count: u64,
) -> PostmortemJsonlEnrichment {
    if !postmortem_session_allows_jsonl_enrichment(session) {
        return PostmortemJsonlEnrichment {
            status: proxy_only_jsonl_enrichment_status("session_active"),
            summary: None,
            findings: Vec::new(),
        };
    }

    let candidates = load_jsonl_candidate_sessions();
    let proxy = proxy_correlation_for_postmortem_session(session, turn_count);
    let correlations = candidates
        .iter()
        .map(jsonl::JsonlDerivedSession::correlation)
        .collect::<Vec<_>>();

    if let Some(summary) = stored_jsonl_match(conn, &proxy, &candidates) {
        let findings = jsonl_tool_failure_finding(&session.session_id, &summary)
            .into_iter()
            .collect::<Vec<_>>();
        return PostmortemJsonlEnrichment {
            status: jsonl_matched_status(&proxy.session_id, &summary.jsonl_session_id),
            summary: Some(summary),
            findings,
        };
    }

    match correlation::correlate_jsonl_session(&proxy, &correlations) {
        correlation::JsonlEnrichmentStatus::Matched {
            proxy_session_id,
            jsonl_session_id,
        } => {
            let summary = candidates
                .into_iter()
                .find(|candidate| candidate.jsonl_session_id == jsonl_session_id);
            let findings = summary
                .as_ref()
                .and_then(|summary| jsonl_tool_failure_finding(&session.session_id, summary))
                .into_iter()
                .collect::<Vec<_>>();
            if let Some(summary) = &summary {
                if let Err(err) = persist_jsonl_correlation(conn, &session.session_id, summary) {
                    debug!(
                        session_id = %session.session_id,
                        jsonl_session_id = %summary.jsonl_session_id,
                        error = %err,
                        "failed to persist JSONL correlation"
                    );
                }
            }
            PostmortemJsonlEnrichment {
                status: jsonl_matched_status(&proxy_session_id, &jsonl_session_id),
                summary,
                findings,
            }
        }
        correlation::JsonlEnrichmentStatus::ProxyOnly { reason, .. } => {
            let reason = match reason {
                correlation::ProxyOnlyReason::MissingJsonl => "missing_jsonl",
                correlation::ProxyOnlyReason::NoMatch => "no_confident_match",
            };
            PostmortemJsonlEnrichment {
                status: proxy_only_jsonl_enrichment_status(reason),
                summary: None,
                findings: Vec::new(),
            }
        }
        correlation::JsonlEnrichmentStatus::Ambiguous {
            proxy_session_id,
            candidate_count,
        } => PostmortemJsonlEnrichment {
            status: jsonl_ambiguous_status(&proxy_session_id, candidate_count),
            summary: None,
            findings: Vec::new(),
        },
    }
}

fn persist_guard_finding_if_absent(
    conn: &Connection,
    finding: &guard::GuardFinding,
) -> rusqlite::Result<()> {
    let rule_id = finding.rule_id.to_string();
    let source = serialized_label(finding.source);
    let evidence_level = serialized_label(finding.evidence_level);
    let exists = conn
        .query_row(
            "SELECT 1 FROM guard_findings \
             WHERE session_id = ?1 AND rule_id = ?2 AND source = ?3 AND evidence_level = ?4 \
             LIMIT 1",
            rusqlite::params![&finding.session_id, &rule_id, &source, &evidence_level],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if exists {
        return Ok(());
    }

    let severity = serialized_label(finding.severity);
    let action = serialized_label(finding.action);
    conn.execute(
        "INSERT INTO guard_findings (
            session_id, request_id, turn_number, timestamp, rule_id, severity,
            action, source, evidence_level, confidence, detail, suggested_action
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        rusqlite::params![
            &finding.session_id,
            &finding.request_id,
            finding.turn_number,
            &finding.timestamp,
            rule_id,
            severity,
            action,
            source,
            evidence_level,
            finding.confidence,
            &finding.detail,
            &finding.suggested_action,
        ],
    )?;
    Ok(())
}

fn load_postmortem_findings(conn: &Connection, session_id: &str) -> rusqlite::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT rule_id, severity, action, source, evidence_level, confidence, \
                timestamp, detail, suggested_action, turn_number, request_id \
         FROM guard_findings WHERE session_id = ?1 \
         ORDER BY timestamp ASC, id ASC",
    )?;
    let rows = stmt.query_map(rusqlite::params![session_id], |row| {
        let rule_id: String = row.get(0)?;
        Ok(serde_json::json!({
            "rule_id": rule_id,
            "title": guard_finding_title(&rule_id),
            "severity": row.get::<_, String>(1)?,
            "action": row.get::<_, String>(2)?,
            "source": row.get::<_, String>(3)?,
            "evidence_level": row.get::<_, String>(4)?,
            "confidence": row.get::<_, f64>(5)?,
            "timestamp": row.get::<_, String>(6)?,
            "detail": row.get::<_, String>(7)?,
            "suggested_action": row.get::<_, Option<String>>(8)?,
            "turn_number": row.get::<_, Option<i64>>(9)?.map(|turn| turn.max(0) as u64),
            "request_id": row.get::<_, Option<String>>(10)?,
        }))
    })?;
    Ok(rows.filter_map(Result::ok).collect())
}

fn diagnosis_cause_finding(session_id: &str, completed_at: &str, cause: &Value) -> Option<Value> {
    let cause_type = cause.get("cause_type").and_then(Value::as_str)?;
    let is_heuristic = cause
        .get("is_heuristic")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let (rule_id, source, evidence_level, confidence) = match cause_type {
        "api_error" => (
            "api_error_circuit_breaker_cooldown",
            "proxy",
            "direct_proxy",
            0.95,
        ),
        "cache_miss_thrash" => ("repeated_cache_rebuilds", "proxy", "direct_proxy", 0.9),
        "cache_miss_ttl" => ("repeated_cache_rebuilds", "proxy", "inferred", 0.78),
        "context_bloat" | "near_compaction" => {
            ("context_near_warning_threshold", "proxy", "derived", 0.82)
        }
        "model_fallback" => ("model_mismatch", "proxy", "direct_proxy", 0.98),
        "tool_failure_streak" => ("tool_failure_streak", "proxy", "direct_proxy", 0.92),
        "compaction_suspected" => ("suspected_compaction_loop", "heuristic", "inferred", 0.72),
        "no_progress_turns" => ("no_progress_turns", "heuristic", "inferred", 0.65),
        _ if is_heuristic => (cause_type, "heuristic", "inferred", 0.6),
        _ => (cause_type, "proxy", "derived", 0.75),
    };
    let detail = cause
        .get("detail")
        .and_then(Value::as_str)
        .unwrap_or(cause_type);
    Some(serde_json::json!({
        "rule_id": rule_id,
        "title": guard_finding_title(rule_id),
        "severity": "warning",
        "action": "diagnose_only",
        "source": source,
        "evidence_level": evidence_level,
        "confidence": confidence,
        "session_id": session_id,
        "timestamp": completed_at,
        "detail": detail,
        "turn_number": cause.get("turn_first_noticed").and_then(Value::as_u64),
        "request_id": null,
        "suggested_action": null,
    }))
}

fn diagnosis_findings_from_causes(
    session_id: &str,
    completed_at: Option<&str>,
    causes: &[Value],
) -> Vec<Value> {
    let completed_at = completed_at
        .map(Cow::Borrowed)
        .unwrap_or_else(|| Cow::Owned(now_iso8601()));
    causes
        .iter()
        .filter_map(|cause| diagnosis_cause_finding(session_id, completed_at.as_ref(), cause))
        .collect()
}

fn finding_identity(value: &Value) -> String {
    format!(
        "{}|{}|{}|{}",
        value
            .get("rule_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        value
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        value
            .get("evidence_level")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        value
            .get("detail")
            .and_then(Value::as_str)
            .unwrap_or_default()
    )
}

fn merge_postmortem_findings(mut primary: Vec<Value>, secondary: Vec<Value>) -> Vec<Value> {
    let mut seen = primary.iter().map(finding_identity).collect::<HashSet<_>>();
    for finding in secondary {
        if seen.insert(finding_identity(&finding)) {
            primary.push(finding);
        }
    }
    primary
}

fn evidence_labels_json(findings: &[Value]) -> Value {
    let mut seen = HashSet::new();
    let labels = findings
        .iter()
        .filter_map(|finding| {
            let source = finding.get("source").and_then(Value::as_str)?;
            let evidence_level = finding.get("evidence_level").and_then(Value::as_str)?;
            let rule_id = finding
                .get("rule_id")
                .and_then(Value::as_str)
                .unwrap_or("finding");
            let confidence = finding
                .get("confidence")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            let key = format!("{rule_id}|{source}|{evidence_level}");
            seen.insert(key).then(|| {
                serde_json::json!({
                    "rule_id": rule_id,
                    "source": source,
                    "evidence_level": evidence_level,
                    "confidence": confidence,
                })
            })
        })
        .collect::<Vec<_>>();
    Value::Array(labels)
}

fn count_map_json(map: &BTreeMap<String, u64>, key_name: &str) -> Value {
    Value::Array(
        map.iter()
            .map(|(key, count)| {
                serde_json::json!({
                    key_name: key,
                    "count": count,
                })
            })
            .collect(),
    )
}

fn string_set_json(values: &std::collections::BTreeSet<String>) -> Value {
    Value::Array(
        values
            .iter()
            .map(|value| Value::String(value.clone()))
            .collect(),
    )
}

fn top_count_labels(map: &BTreeMap<String, u64>, max_items: usize) -> Vec<String> {
    let mut items = map
        .iter()
        .filter(|(_, count)| **count > 0)
        .collect::<Vec<_>>();
    items.sort_by(|(left_name, left_count), (right_name, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_name.cmp(right_name))
    });
    items
        .into_iter()
        .take(max_items)
        .map(|(name, count)| format!("{name} ({count})"))
        .collect()
}

fn push_unique_limited(values: &mut Vec<String>, value: String, limit: usize) {
    let value = value.trim();
    if value.is_empty() || values.iter().any(|existing| existing == value) || values.len() >= limit
    {
        return;
    }
    values.push(value.to_string());
}

fn handoff_summary_line(label: &str, value: &str, redact: bool) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if is_derived_content_metadata(trimmed) {
        return Some(format!("{label}: {trimmed}"));
    }
    if redact {
        return redacted_human_summary(label, trimmed, true)
            .map(|summary| format!("{label}: {summary}"));
    }
    Some(format!(
        "{label}: {}",
        redact_operational_text(trimmed, false)
    ))
}

fn handoff_tool_activity(
    tools: &[ToolAggregate],
    jsonl_summary: Option<&jsonl::JsonlDerivedSession>,
) -> Vec<String> {
    let mut activity = Vec::new();
    for tool in tools.iter().take(4) {
        let detail = if tool.failures > 0 {
            format!(
                "{}: {} calls, {} failures",
                tool.tool_name, tool.calls, tool.failures
            )
        } else {
            format!("{}: {} calls, 0 failures", tool.tool_name, tool.calls)
        };
        push_unique_limited(&mut activity, detail, 6);
    }

    if let Some(summary) = jsonl_summary {
        if activity.is_empty() {
            let mut jsonl_tools = summary
                .tool_calls
                .iter()
                .map(|(tool_name, calls)| {
                    (
                        tool_name.clone(),
                        *calls,
                        summary.tool_failures.get(tool_name).copied().unwrap_or(0),
                    )
                })
                .collect::<Vec<_>>();
            jsonl_tools.sort_by(
                |(left_name, left_calls, left_failures),
                 (right_name, right_calls, right_failures)| {
                    right_failures
                        .cmp(left_failures)
                        .then_with(|| right_calls.cmp(left_calls))
                        .then_with(|| left_name.cmp(right_name))
                },
            );
            for (tool_name, calls, failures) in jsonl_tools.into_iter().take(4) {
                push_unique_limited(
                    &mut activity,
                    format!("{tool_name}: {calls} JSONL calls, {failures} failures"),
                    6,
                );
            }
        }

        let bash_categories = top_count_labels(&summary.bash_command_categories, 3);
        if !bash_categories.is_empty() {
            push_unique_limited(
                &mut activity,
                format!("Bash categories: {}", bash_categories.join(", ")),
                6,
            );
        }
        let file_categories = top_count_labels(&summary.file_operation_categories, 3);
        if !file_categories.is_empty() {
            push_unique_limited(
                &mut activity,
                format!("File operations: {}", file_categories.join(", ")),
                6,
            );
        }
    }

    activity
}

fn jsonl_signal_json(summary: Option<&jsonl::JsonlDerivedSession>) -> Value {
    let Some(summary) = summary else {
        return Value::Null;
    };
    serde_json::json!({
        "session_id": summary.jsonl_session_id,
        "started_at": (summary.started_at_epoch_secs > 0).then(|| epoch_to_iso8601(summary.started_at_epoch_secs)),
        "last_activity_at": (summary.last_activity_at_epoch_secs > 0).then(|| epoch_to_iso8601(summary.last_activity_at_epoch_secs)),
        "user_turn_count": summary.user_turn_count,
        "assistant_turn_count": summary.assistant_turn_count,
        "request_count": summary.request_count,
        "tool_calls": count_map_json(&summary.tool_calls, "tool_name"),
        "tool_failures": count_map_json(&summary.tool_failures, "tool_name"),
        "mcp_tool_names": string_set_json(&summary.mcp_tool_names),
        "skill_names": string_set_json(&summary.skill_names),
        "bash_command_categories": count_map_json(&summary.bash_command_categories, "category"),
        "file_operation_categories": count_map_json(&summary.file_operation_categories, "category"),
        "usage_fallback": {
            "input_tokens": summary.usage.input_tokens,
            "output_tokens": summary.usage.output_tokens,
            "cache_read_tokens": summary.usage.cache_read_tokens,
            "cache_creation_tokens": summary.usage.cache_creation_tokens,
        },
        "system_event_categories": count_map_json(&summary.system_event_categories, "category"),
    })
}

#[allow(clippy::too_many_arguments)]
fn handoff_context_json(
    session: &PostmortemSessionRow,
    partial: bool,
    display_outcome: &str,
    primary_cause: &str,
    primary_detail: &str,
    next_action: &str,
    prompt_source: &str,
    final_summary_source: &str,
    tools: &[ToolAggregate],
    jsonl_summary: Option<&jsonl::JsonlDerivedSession>,
    max_context_fill_percent: f64,
    turns_to_compact: Option<u64>,
    jsonl_compaction_boundary_epoch: Option<u64>,
    jsonl_continued_after_proxy_secs: Option<u64>,
    redact: bool,
) -> Value {
    let mut continue_from = Vec::new();
    push_unique_limited(&mut continue_from, format!("Outcome: {display_outcome}"), 6);
    push_unique_limited(
        &mut continue_from,
        if partial {
            "Session is still running; treat this as a progress snapshot.".to_string()
        } else {
            "Session has ended; use this as the final local report.".to_string()
        },
        6,
    );
    if primary_cause != "none" {
        push_unique_limited(
            &mut continue_from,
            format!("Main issue: {primary_detail}"),
            6,
        );
    }
    if let Some(line) = handoff_summary_line("Initial prompt", prompt_source, redact) {
        push_unique_limited(&mut continue_from, line, 6);
    }
    if let Some(line) = handoff_summary_line("Last response", final_summary_source, redact) {
        push_unique_limited(&mut continue_from, line, 6);
    }

    let mut recent_activity = handoff_tool_activity(tools, jsonl_summary);
    if max_context_fill_percent > 0.0 {
        let context = match turns_to_compact {
            Some(turns) => format!(
                "Context reached {:.0}% full; about {turns} turns before auto-compaction.",
                max_context_fill_percent
            ),
            None => format!("Context reached {:.0}% full.", max_context_fill_percent),
        };
        push_unique_limited(&mut recent_activity, context, 6);
    }

    let total_tool_failures = tools.iter().map(|tool| tool.failures).sum::<u64>()
        + jsonl_summary
            .map(jsonl::JsonlDerivedSession::total_tool_failures)
            .unwrap_or(0);
    let mut avoid_repeating = Vec::new();
    if total_tool_failures > 0 || primary_cause == "tool_failure_streak" {
        push_unique_limited(
            &mut avoid_repeating,
            "Fix failing tool preconditions before rerunning the same tool loop.".to_string(),
            4,
        );
    }
    match primary_cause {
        "cache_miss_ttl" | "cache_miss_thrash" => push_unique_limited(
            &mut avoid_repeating,
            "Avoid resubmitting a large unchanged prompt until cache behavior is fixed."
                .to_string(),
            4,
        ),
        "context_bloat" | "near_compaction" | "compaction_suspected" => push_unique_limited(
            &mut avoid_repeating,
            "Start with a compact state summary and avoid broad scans.".to_string(),
            4,
        ),
        "model_fallback" => push_unique_limited(
            &mut avoid_repeating,
            "Do not assume the requested model was used; verify the model route.".to_string(),
            4,
        ),
        _ => {}
    }
    if jsonl_compaction_boundary_epoch.is_some() {
        push_unique_limited(
            &mut avoid_repeating,
            "Do not treat the compaction boundary as task abandonment.".to_string(),
            4,
        );
    }

    serde_json::json!({
        "status": if partial { "running" } else { "ended" },
        "outcome": display_outcome,
        "action": next_action,
        "provenance": {
            "cc_blackbox_session_id": session.session_id,
            "claude_jsonl_session_id": jsonl_summary.map(|summary| summary.jsonl_session_id.clone()),
        },
        "continue_from": continue_from,
        "recent_activity": recent_activity,
        "avoid_repeating": avoid_repeating,
        "jsonl_continued_after_proxy_secs": jsonl_continued_after_proxy_secs,
        "privacy": if redact {
            "redacted: raw prompts, assistant text, tool inputs, tool outputs, and file contents are omitted"
        } else {
            "local: raw transcripts and file contents are still not included unless already stored as derived metadata"
        },
    })
}

fn jsonl_compaction_boundary_near_session(
    summary: &jsonl::JsonlDerivedSession,
    session: &PostmortemSessionRow,
) -> Option<u64> {
    const TOLERANCE_SECS: u64 = 120;
    let anchor = session.last_activity_at_epoch_secs;
    if anchor == 0 {
        return None;
    }
    summary
        .system_events
        .iter()
        .filter(|event| event.category == "compaction")
        .filter(|event| event.timestamp_epoch_secs.abs_diff(anchor) <= TOLERANCE_SECS)
        .min_by_key(|event| event.timestamp_epoch_secs.abs_diff(anchor))
        .map(|event| event.timestamp_epoch_secs)
}

fn jsonl_continuation_after_session_secs(
    summary: &jsonl::JsonlDerivedSession,
    session: &PostmortemSessionRow,
) -> Option<u64> {
    const CONTINUATION_SECS: u64 = 60;
    let anchor = session.last_activity_at_epoch_secs;
    if anchor == 0 {
        return None;
    }
    let continued_for = summary.last_activity_at_epoch_secs.saturating_sub(anchor);
    (continued_for >= CONTINUATION_SECS).then_some(continued_for)
}

fn jsonl_compaction_boundary_finding(
    session_id: &str,
    boundary_epoch_secs: u64,
    continued_for_secs: Option<u64>,
) -> Value {
    let mut detail = format!(
        "Claude Code JSONL recorded a compact_boundary at {}; this proxy segment ended at the compaction boundary, not at task abandonment.",
        epoch_to_iso8601(boundary_epoch_secs)
    );
    if let Some(secs) = continued_for_secs {
        detail.push_str(&format!(
            " The same JSONL session continued for about {} min afterward.",
            (secs + 30) / 60
        ));
    }
    serde_json::json!({
        "rule_id": "jsonl_compaction_boundary",
        "title": guard_finding_title("jsonl_compaction_boundary"),
        "severity": "info",
        "action": "diagnose_only",
        "source": "jsonl",
        "evidence_level": "direct_jsonl",
        "confidence": 0.95,
        "timestamp": epoch_to_iso8601(boundary_epoch_secs),
        "detail": detail,
        "suggested_action": "Continue from the compacted session segment; do not treat this proxy segment as abandoned.",
        "turn_number": null,
        "request_id": null,
        "session_id": session_id,
    })
}

fn build_postmortem_response_from_db(
    conn: &Connection,
    target: &str,
    redact: bool,
) -> Result<Value, PostmortemError> {
    let _ = repair_persisted_session_artifacts(conn);
    let session_id = if target == "last" {
        query_latest_postmortem_session_id(conn)
            .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?
            .ok_or(PostmortemError::NoSessions)?
    } else {
        target.to_string()
    };

    let mut session = load_postmortem_session(conn, &session_id)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?
        .ok_or_else(|| PostmortemError::UnknownSession(session_id.clone()))?;
    if session_is_live(&session.session_id) {
        session.ended_at = None;
    }
    let diagnosis = load_postmortem_diagnosis(conn, &session)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;
    let token_totals = load_postmortem_token_totals(conn, &session)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;
    let turns = load_postmortem_turns(conn, &session.session_id)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;
    let tools = load_postmortem_tool_summaries(conn, &session.session_id, redact)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;
    let skill_events = load_grouped_event_summaries(conn, &session.session_id, "skill_events")
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;
    let mcp_events = load_grouped_event_summaries(conn, &session.session_id, "mcp_events")
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;
    let (recall_prompt, recall_summary) = if session.ended_at.is_some() {
        load_postmortem_recall(conn, &session.session_id)
            .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?
    } else {
        (None, None)
    };

    let session_ids = vec![session.session_id.clone()];
    let estimated = compute_estimated_costs_for_sessions(conn, &session_ids)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?
        .remove(&session.session_id)
        .unwrap_or_else(|| CostAccumulator::new().finish());
    let billing = load_latest_billing_reconciliations(conn, &session_ids)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?
        .remove(&session.session_id);
    let jsonl_enrichment = load_postmortem_jsonl_enrichment(conn, &session, turns.len() as u64);
    let jsonl_compaction_boundary_epoch = jsonl_enrichment
        .summary
        .as_ref()
        .and_then(|summary| jsonl_compaction_boundary_near_session(summary, &session));
    let jsonl_continued_after_proxy_secs = jsonl_enrichment
        .summary
        .as_ref()
        .and_then(|summary| jsonl_continuation_after_session_secs(summary, &session));
    for finding in &jsonl_enrichment.findings {
        let _ = persist_guard_finding_if_absent(conn, finding);
    }
    let stored_findings = load_postmortem_findings(conn, &session.session_id)
        .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;

    let sanitized_causes = diagnosis
        .as_ref()
        .map(|diag| sanitize_causes(&diag.causes, redact))
        .unwrap_or_default();
    let sanitized_advice = diagnosis
        .as_ref()
        .map(|diag| sanitize_advice(&diag.advice, redact))
        .unwrap_or_default();
    let diagnosis_findings = diagnosis_findings_from_causes(
        &session.session_id,
        diagnosis.as_ref().map(|diag| diag.completed_at.as_str()),
        &sanitized_causes,
    );
    let mut findings = merge_postmortem_findings(diagnosis_findings, stored_findings);
    if let Some(boundary_epoch_secs) = jsonl_compaction_boundary_epoch {
        findings.insert(
            0,
            jsonl_compaction_boundary_finding(
                &session.session_id,
                boundary_epoch_secs,
                jsonl_continued_after_proxy_secs,
            ),
        );
    }
    let partial = session.ended_at.is_none();
    let primary_cause = sanitized_causes
        .first()
        .and_then(|cause| cause.get("cause_type"))
        .and_then(|value| value.as_str())
        .unwrap_or("none")
        .to_string();
    let primary_cause_is_heuristic = sanitized_causes
        .first()
        .and_then(|cause| cause.get("is_heuristic"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let default_primary_detail = if partial {
        "Session is still active; inactivity alone is not a degradation signal."
    } else {
        "No primary degradation cause was recorded."
    };
    let primary_detail = sanitized_causes
        .first()
        .and_then(|cause| cause.get("detail"))
        .and_then(|value| value.as_str())
        .unwrap_or(default_primary_detail)
        .to_string();
    let mut next_action = recommended_action(&primary_cause, &sanitized_advice, partial);
    if jsonl_compaction_boundary_epoch.is_some() {
        next_action = "Continue from the compacted session segment; the JSONL shows this proxy segment ended at compaction, not task abandonment.".to_string();
    }
    let diagnosis_outcome = diagnosis
        .as_ref()
        .map(|diag| diag.outcome.clone())
        .unwrap_or_else(|| {
            if partial {
                "In Progress".to_string()
            } else {
                "Completed state unknown".to_string()
            }
        });
    let display_outcome = if jsonl_compaction_boundary_epoch.is_some() {
        "Compaction Boundary".to_string()
    } else if jsonl_continued_after_proxy_secs.is_some() && !partial {
        "Continued In Same Claude Session".to_string()
    } else {
        diagnosis_outcome.clone()
    };

    let max_context_fill_percent = turns
        .iter()
        .map(|turn| turn.context_fill_percent)
        .fold(0.0, f64::max);
    let context_turns = turns
        .iter()
        .filter(|turn| {
            turn_has_context_usage(
                turn.input_tokens,
                turn.cache_read_tokens,
                turn.cache_creation_tokens,
            )
        })
        .collect::<Vec<_>>();
    let latest_context_fill_percent = context_turns
        .last()
        .map(|turn| turn.context_fill_percent)
        .unwrap_or(0.0);
    let turns_to_compact = if context_turns.len() >= 2 {
        let prev = context_turns[context_turns.len() - 2];
        let current = context_turns[context_turns.len() - 1];
        project_turns_until_compaction(prev.context_fill_percent, current.context_fill_percent)
    } else {
        None
    };

    let cache_rebuild_turns = turns
        .iter()
        .filter(|turn| turn.cache_creation_tokens > 0 && turn.cache_read_tokens == 0)
        .collect::<Vec<_>>();
    let model_fallbacks = turns
        .iter()
        .filter_map(|turn| {
            let requested = turn.requested_model.as_ref()?;
            let actual = turn.actual_model.as_ref()?;
            (!model_matches(requested, actual)).then(|| {
                serde_json::json!({
                    "turn": turn.turn_number,
                    "requested": requested,
                    "actual": actual,
                })
            })
        })
        .collect::<Vec<_>>();
    let tool_failures = tools.iter().map(|tool| tool.failures).sum::<u64>();
    let repeated_tools = tools
        .iter()
        .filter(|tool| tool.calls >= 3)
        .map(|tool| tool.tool_name.clone())
        .collect::<Vec<_>>();
    let estimated_wasted_cost = sanitized_causes
        .iter()
        .filter_map(|cause| cause.get("estimated_cost").and_then(|value| value.as_f64()))
        .sum::<f64>();
    let mut compaction_waste_ranges = Vec::new();
    let mut has_unranged_compaction_waste = false;
    let mut compaction_waste_tokens = 0u64;
    for cause in sanitized_causes.iter().filter(|cause| {
        cause.get("cause_type").and_then(|value| value.as_str()) == Some("compaction_suspected")
    }) {
        let Some(wasted_tokens) = cause
            .get("estimated_wasted_tokens")
            .and_then(|value| value.as_u64())
        else {
            continue;
        };
        compaction_waste_tokens += wasted_tokens;
        if wasted_tokens == 0 {
            continue;
        }
        match (
            cause
                .get("turn_first_noticed")
                .and_then(|value| value.as_u64()),
            cause
                .get("turn_last_noticed")
                .and_then(|value| value.as_u64()),
        ) {
            (Some(first), Some(last)) => compaction_waste_ranges.push((first, last.max(first))),
            _ => has_unranged_compaction_waste = true,
        }
    }
    let non_cold_cache_rebuild_tokens = if has_unranged_compaction_waste {
        0
    } else {
        cache_rebuild_turns
            .iter()
            .filter(|turn| turn.turn_number > 1)
            .filter(|turn| {
                !compaction_waste_ranges
                    .iter()
                    .any(|(first, last)| turn.turn_number > *first && turn.turn_number <= *last)
            })
            .map(|turn| turn.cache_creation_tokens)
            .sum::<u64>()
    };
    let estimated_wasted_tokens = non_cold_cache_rebuild_tokens + compaction_waste_tokens;

    let prompt_source = recall_prompt
        .as_deref()
        .or(session.initial_prompt.as_deref())
        .unwrap_or("");
    let final_summary_source = recall_summary
        .as_deref()
        .or_else(|| {
            turns
                .iter()
                .rev()
                .find_map(|turn| turn.response_summary.as_deref())
        })
        .unwrap_or("");

    let mut evidence = Vec::new();
    if let Some(diag) = &diagnosis {
        evidence.push(evidence_json(
            if sanitized_causes
                .first()
                .and_then(|cause| cause.get("is_heuristic"))
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
            {
                "heuristic"
            } else {
                "derived"
            },
            "diagnosis",
            format!("{} with outcome {}", primary_detail, display_outcome),
            diag.degradation_turn,
            Some(diag.completed_at.clone()),
        ));
    }
    evidence.push(evidence_json(
        "direct",
        "tokens",
        format!(
            "{} total tokens across {} requests",
            token_totals.total_tokens(),
            token_totals.request_count
        ),
        None,
        session.ended_at.clone(),
    ));
    if !cache_rebuild_turns.is_empty() {
        evidence.push(evidence_json(
            "derived",
            "cache",
            format!(
                "{} cache rebuild turns, {} non-cold-start cache creation tokens",
                cache_rebuild_turns.len(),
                non_cold_cache_rebuild_tokens
            ),
            cache_rebuild_turns.first().map(|turn| turn.turn_number),
            cache_rebuild_turns
                .first()
                .map(|turn| turn.timestamp.clone()),
        ));
    }
    if max_context_fill_percent >= 60.0 {
        evidence.push(evidence_json(
            "heuristic",
            "context",
            format!(
                "context reached {:.0}% full; turns to compact: {}",
                max_context_fill_percent,
                turns_to_compact
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ),
            turns
                .iter()
                .max_by(|a, b| a.context_fill_percent.total_cmp(&b.context_fill_percent))
                .map(|turn| turn.turn_number),
            None,
        ));
    }
    if !model_fallbacks.is_empty() {
        evidence.push(evidence_json(
            "direct",
            "model",
            format!(
                "{} requested/actual model mismatches",
                model_fallbacks.len()
            ),
            model_fallbacks
                .first()
                .and_then(|value| value.get("turn"))
                .and_then(|value| value.as_u64()),
            None,
        ));
    }
    if tool_failures > 0 || !repeated_tools.is_empty() {
        evidence.push(evidence_json(
            "direct",
            "tools",
            format!(
                "{} tool failures; repeated tools: {}",
                tool_failures,
                if repeated_tools.is_empty() {
                    "none".to_string()
                } else {
                    repeated_tools.join(", ")
                }
            ),
            tools.first().map(|tool| tool.first_turn),
            None,
        ));
    }
    if !skill_events.is_empty() {
        evidence.push(evidence_json(
            "hook",
            "skills",
            format!("{} grouped skill event signals", skill_events.len()),
            None,
            None,
        ));
    }
    if !mcp_events.is_empty() {
        evidence.push(evidence_json(
            "hook",
            "mcp",
            format!("{} grouped MCP event signals", mcp_events.len()),
            None,
            None,
        ));
    }
    for finding in &jsonl_enrichment.findings {
        evidence.push(evidence_json(
            "direct_jsonl",
            "tools",
            finding.detail.clone(),
            finding.turn_number.map(u64::from),
            Some(finding.timestamp.clone()),
        ));
    }
    if let Some(boundary_epoch_secs) = jsonl_compaction_boundary_epoch {
        evidence.push(evidence_json(
            "direct_jsonl",
            "jsonl",
            "JSONL compact_boundary aligned with this proxy segment's last activity".to_string(),
            None,
            Some(epoch_to_iso8601(boundary_epoch_secs)),
        ));
    }
    if let Some(continued_for_secs) = jsonl_continued_after_proxy_secs {
        evidence.push(evidence_json(
            "direct_jsonl",
            "jsonl",
            format!(
                "same JSONL session continued for about {} min after this proxy segment",
                (continued_for_secs + 30) / 60
            ),
            None,
            jsonl_enrichment
                .summary
                .as_ref()
                .map(|summary| epoch_to_iso8601(summary.last_activity_at_epoch_secs)),
        ));
    }

    let confidence = postmortem_confidence(diagnosis.as_ref(), evidence.len()).to_string();

    let mut timeline = Vec::new();
    timeline.push(serde_json::json!({
        "timestamp": session.started_at,
        "turn": null,
        "label": "session_started",
        "detail": "Session row created.",
        "evidence_type": "direct",
    }));
    for turn in &turns {
        let mut details = Vec::new();
        if turn.cache_creation_tokens > 0 && turn.cache_read_tokens == 0 {
            let cache_kind = if turn.turn_number <= 1 {
                "cold cache build"
            } else if turn.gap_from_prev_secs > 300.0 {
                "cache rebuild after idle gap"
            } else {
                "cache rebuild without long idle gap"
            };
            details.push(format!(
                "{cache_kind}: {} creation tokens",
                turn.cache_creation_tokens
            ));
        }
        if turn.context_fill_percent >= 80.0 {
            details.push(format!("context {:.0}% full", turn.context_fill_percent));
        }
        if turn.tool_failures > 0 {
            details.push(format!("{} tool failures", turn.tool_failures));
        }
        if let (Some(requested), Some(actual)) = (
            turn.requested_model.as_deref(),
            turn.actual_model.as_deref(),
        ) {
            if !model_matches(requested, actual) {
                details.push(format!(
                    "model routed differently: requested {requested}, got {actual}"
                ));
            }
        }
        if !details.is_empty() {
            timeline.push(serde_json::json!({
                "timestamp": turn.timestamp,
                "turn": turn.turn_number,
                "label": "turn_signal",
                "detail": redact_operational_text(&details.join("; "), redact),
                "evidence_type": "direct",
            }));
        }
        if timeline.len() >= 9 {
            break;
        }
    }
    if let Some(ended_at) = &session.ended_at {
        timeline.push(serde_json::json!({
            "timestamp": ended_at,
            "turn": null,
            "label": "session_ended",
            "detail": display_outcome.clone(),
            "evidence_type": "direct",
        }));
    }

    let tool_values = tools
        .iter()
        .map(|tool| {
            serde_json::json!({
                "tool_name": tool.tool_name,
                "calls": tool.calls,
                "failures": tool.failures,
                "first_turn": tool.first_turn,
                "last_turn": tool.last_turn,
                "avg_duration_ms": tool.total_duration_ms.checked_div(tool.calls).unwrap_or(0),
                "sample": tool.sample,
            })
        })
        .collect::<Vec<_>>();
    let mut recommendations = sanitized_advice.clone();
    if !recommendations.iter().any(|item| item == &next_action) {
        recommendations.insert(0, next_action.clone());
    }
    if recommendations.is_empty() {
        recommendations.push(next_action.clone());
    }
    let enrichment_status = jsonl_enrichment.status.clone();
    let evidence_labels = evidence_labels_json(&findings);
    let handoff_context = handoff_context_json(
        &session,
        partial,
        &display_outcome,
        &primary_cause,
        &primary_detail,
        &next_action,
        prompt_source,
        final_summary_source,
        &tools,
        jsonl_enrichment.summary.as_ref(),
        max_context_fill_percent,
        turns_to_compact.map(u64::from),
        jsonl_compaction_boundary_epoch,
        jsonl_continued_after_proxy_secs,
        redact,
    );

    Ok(serde_json::json!({
        "session_id": session.session_id,
        "generated_at": now_iso8601(),
        "redacted": redact,
        "redaction_status": if redact { "redacted" } else { "local_unredacted" },
        "partial": partial,
        "proxy_summary": {
            "outcome": display_outcome.clone(),
            "estimated_total_cost_dollars": rounded_estimated_cost_dollars(estimated.estimated_cost_dollars),
            "total_turns": diagnosis
                .as_ref()
                .map(|diag| diag.total_turns)
                .filter(|turns| *turns > 0)
                .unwrap_or(turns.len() as u64),
            "main_degradation": primary_cause,
            "request_count": token_totals.request_count,
            "total_tokens": token_totals.total_tokens(),
        },
        "summary": {
            "started_at": session.started_at,
            "last_activity_at": session.last_activity_at,
            "ended_at": session.ended_at,
            "duration_secs": session.duration_secs,
            "model": session.model,
            "outcome": display_outcome.clone(),
            "stored_diagnosis_outcome": diagnosis_outcome,
            "total_turns": diagnosis
                .as_ref()
                .map(|diag| diag.total_turns)
                .filter(|turns| *turns > 0)
                .unwrap_or(turns.len() as u64),
            "total_tokens": token_totals.total_tokens(),
            "initial_prompt_summary": redacted_human_summary("Initial prompt", prompt_source, redact),
            "final_response_summary": redacted_human_summary("Final response summary", final_summary_source, redact),
        },
        "diagnosis": {
            "degraded": diagnosis.as_ref().map(|diag| diag.degraded).unwrap_or(false),
            "degradation_turn": diagnosis.as_ref().and_then(|diag| diag.degradation_turn),
            "likely_cause": primary_cause,
            "likely_cause_is_heuristic": primary_cause_is_heuristic,
            "detail": primary_detail,
            "confidence": confidence,
            "causes": sanitized_causes,
            "next_action": next_action,
        },
        "impact": {
            "request_count": token_totals.request_count,
            "input_tokens": token_totals.input_tokens,
            "output_tokens": token_totals.output_tokens,
            "cache_read_tokens": token_totals.cache_read_tokens,
            "cache_creation_tokens": token_totals.cache_creation_tokens,
            "total_tokens": token_totals.total_tokens(),
            "estimated_total_cost_dollars": rounded_estimated_cost_dollars(estimated.estimated_cost_dollars),
            "diagnosis_estimated_total_cost_dollars": diagnosis.as_ref().map(|diag| rounded_estimated_cost_dollars(diag.total_cost)),
            "estimated_likely_wasted_tokens": estimated_wasted_tokens,
            "estimated_likely_wasted_cost_dollars": rounded_estimated_cost_dollars(estimated_wasted_cost),
            "cost_source": estimated.cost_source,
            "trusted_for_budget_enforcement": estimated.trusted_for_budget_enforcement,
            "billed_cost_dollars": billing.as_ref().map(|record| rounded_estimated_cost_dollars(record.billed_cost_dollars)),
            "billing_source": billing.as_ref().map(|record| record.source.clone()),
            "billing_imported_at": billing.as_ref().map(|record| record.imported_at.clone()),
            "cost_caveat": pricing::pricing_caveats().join(" "),
            "pricing_caveats": pricing::pricing_caveats(),
        },
        "signals": {
            "cache": {
                "cache_reusable_prefix_ratio": diagnosis
                    .as_ref()
                    .map(|diag| diag.cache_hit_ratio)
                    .unwrap_or_else(|| cache_hit_ratio_from_tokens(&token_totals)),
                "cache_hit_ratio": diagnosis
                    .as_ref()
                    .map(|diag| diag.cache_hit_ratio)
                    .unwrap_or_else(|| cache_hit_ratio_from_tokens(&token_totals)),
                "cache_ratio_label": "reusable_prefix_ratio",
                "total_input_cache_rate": total_input_cache_rate_from_tokens(&token_totals),
                "rebuild_turns": cache_rebuild_turns.len(),
                "non_cold_rebuild_tokens": non_cold_cache_rebuild_tokens,
            },
            "context": {
                "latest_fill_percent": (latest_context_fill_percent * 10.0).round() / 10.0,
                "max_fill_percent": (max_context_fill_percent * 10.0).round() / 10.0,
                "turns_to_compact": turns_to_compact,
                "context_window_tokens": context_turns
                    .last()
                    .copied()
                    .or_else(|| turns.last())
                    .map(|turn| turn.context_window_tokens),
                "heuristic": true,
            },
            "model": {
                "fallbacks": model_fallbacks,
            },
            "tools": tool_values,
            "skills": skill_events,
            "mcp": mcp_events,
            "jsonl": jsonl_signal_json(jsonl_enrichment.summary.as_ref()),
        },
        "findings": findings,
        "evidence_labels": evidence_labels,
        "evidence": evidence,
        "timeline": timeline,
        "enrichment_status": enrichment_status,
        "handoff_context": handoff_context,
        "recommendations": recommendations,
        "caveats": [
            "This report is deterministic and generated from local cc-blackbox SQLite data.",
            "Estimated costs are not billing truth unless a billed reconciliation is shown.",
            "Built-in pricing does not include contract discounts, data residency, fast-mode modifiers, or server-tool charges unless separately reconciled.",
            "Context runway and some degradation causes are heuristics.",
            if redact {
                "Redacted output omits raw prompts, absolute paths, query strings, secret-like values, and structured tool payloads."
            } else {
                "Unredacted output may contain local prompt summaries and tool summaries from your machine."
            }
        ],
    }))
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
         COALESCE(cache_creation_5m_tokens, 0), COALESCE(cache_creation_1h_tokens, 0), \
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
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, Option<f64>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<i64>>(9)?,
            row.get::<_, Option<String>>(10)?,
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
            cache_create_5m_raw,
            cache_create_1h_raw,
            stored_cost,
            stored_source,
            stored_trusted,
            cache_event,
        ) = row?;
        let mut cache_create_5m = cache_create_5m_raw.max(0) as u64;
        let cache_create_1h = cache_create_1h_raw.max(0) as u64;
        if cache_create.max(0) > 0 && cache_create_5m == 0 && cache_create_1h == 0 {
            cache_create_5m = cache_create.max(0) as u64;
        }
        let row_cost = if let Some(cost) = stored_cost {
            cost_accumulator.record_persisted(cost, stored_source, stored_trusted.map(|n| n != 0));
            cost
        } else {
            let estimated = pricing::estimate_cost_dollars_with_cache_buckets(
                &model,
                input.max(0) as u64,
                output.max(0) as u64,
                cache_read.max(0) as u64,
                cache_create_5m,
                cache_create_1h,
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
            let waste = pricing::estimate_cache_rebuild_waste_dollars_with_cache_buckets(
                &model,
                cache_create_5m,
                cache_create_1h,
            )
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
#[derive(Clone, Debug, PartialEq, Eq)]
struct CacheTtlEvidence {
    min_secs: u64,
    max_secs: u64,
    source: &'static str,
}

impl CacheTtlEvidence {
    fn default_5m() -> Self {
        Self {
            min_secs: CACHE_TTL_SECS,
            max_secs: CACHE_TTL_SECS,
            source: "default_5m",
        }
    }

    fn from_ttls(ttls: &[u64], source: &'static str) -> Option<Self> {
        let min_secs = ttls.iter().copied().min()?;
        let max_secs = ttls.iter().copied().max().unwrap_or(min_secs);
        Some(Self {
            min_secs,
            max_secs,
            source,
        })
    }

    fn from_response_buckets(acc: &ResponseAccumulator) -> Option<Self> {
        let has_5m = acc.cache_creation_5m_tokens > 0;
        let has_1h = acc.cache_creation_1h_tokens > 0;
        match (has_5m, has_1h) {
            (true, true) => Some(Self {
                min_secs: 300,
                max_secs: 3600,
                source: "response_cache_creation_mixed",
            }),
            (true, false) => Some(Self {
                min_secs: 300,
                max_secs: 300,
                source: "response_cache_creation_5m",
            }),
            (false, true) => Some(Self {
                min_secs: 3600,
                max_secs: 3600,
                source: "response_cache_creation_1h",
            }),
            (false, false) => None,
        }
    }

    fn is_mixed(&self) -> bool {
        self.min_secs != self.max_secs
    }
}

struct CacheTracker {
    consecutive_misses: u64,
    last_request_time: Option<Instant>,
    last_ttl_min_secs: u64,
    last_ttl_max_secs: u64,
    last_event_type: &'static str,
    last_event_time: Option<Instant>,
    last_rebuild_cost_dollars: Option<f64>,
}

impl CacheTracker {
    fn new() -> Self {
        Self {
            consecutive_misses: 0,
            last_request_time: None,
            last_ttl_min_secs: CACHE_TTL_SECS,
            last_ttl_max_secs: CACHE_TTL_SECS,
            last_event_type: "none",
            last_event_time: None,
            last_rebuild_cost_dollars: None,
        }
    }
}

static CACHE_TRACKERS: LazyLock<DashMap<String, CacheTracker>> = LazyLock::new(DashMap::new);
const CACHE_TTL_SECS: u64 = 300;
const THRASH_THRESHOLD: u64 = 3;

/// Returns cache event type: "hit", "partial", "cold_start", "miss_ttl",
/// "miss_rebuild", "miss_thrash", or "none".
fn update_cache_intelligence(
    session_id: &str,
    cache_read: u64,
    cache_create: u64,
    ttl: &CacheTtlEvidence,
) -> &'static str {
    let mut t = CACHE_TRACKERS
        .entry(session_id.to_string())
        .or_insert_with(CacheTracker::new);
    let now = Instant::now();
    classify_cache_event(&mut t, now, cache_read, cache_create, ttl)
}

fn classify_cache_event(
    tracker: &mut CacheTracker,
    now: Instant,
    cache_read: u64,
    cache_create: u64,
    ttl: &CacheTtlEvidence,
) -> &'static str {
    tracker.last_ttl_min_secs = ttl.min_secs;
    tracker.last_ttl_max_secs = ttl.max_secs;
    let is_full_miss = cache_create > 0 && cache_read == 0;
    let is_first = tracker.last_request_time.is_none();
    let gap = tracker
        .last_request_time
        .map(|p| now.duration_since(p).as_secs())
        .unwrap_or(0);
    let is_ttl = is_full_miss && gap > ttl.max_secs;

    let event = if cache_read == 0 && cache_create == 0 {
        "none"
    } else if is_full_miss {
        if is_first {
            tracker.consecutive_misses = 0;
            "cold_start"
        } else if is_ttl {
            tracker.consecutive_misses = 0;
            info!(
                gap,
                cache_create,
                ttl_max_secs = ttl.max_secs,
                "cache miss inferred from TTL expiry evidence"
            );
            "miss_ttl"
        } else {
            tracker.consecutive_misses += 1;
            if tracker.consecutive_misses >= THRASH_THRESHOLD {
                warn!(
                    consecutive_misses = tracker.consecutive_misses,
                    "cache thrashing detected within a single session"
                );
                "miss_thrash"
            } else {
                "miss_rebuild"
            }
        }
    } else if cache_create > 0 {
        tracker.consecutive_misses = 0;
        "partial"
    } else {
        tracker.consecutive_misses = 0;
        "hit"
    };
    tracker.last_request_time = Some(now);
    tracker.last_event_type = event;
    tracker.last_event_time = Some(now);
    tracker.last_rebuild_cost_dollars = None;
    event
}

fn record_cache_event_cost(session_id: &str, event_type: &'static str, rebuild_cost: f64) {
    if let Some(mut tracker) = CACHE_TRACKERS.get_mut(session_id) {
        tracker.last_event_type = event_type;
        tracker.last_event_time = Some(Instant::now());
        tracker.last_rebuild_cost_dollars = Some(rebuild_cost);
    }
}

fn response_cache_ttl_evidence(
    acc: &ResponseAccumulator,
    request_cache_ttl: Option<CacheTtlEvidence>,
) -> CacheTtlEvidence {
    CacheTtlEvidence::from_response_buckets(acc)
        .or(request_cache_ttl)
        .unwrap_or_else(CacheTtlEvidence::default_5m)
}

#[cfg(test)]
fn response_cache_ttl_secs(acc: &ResponseAccumulator, request_cache_ttl_secs: Option<u64>) -> u64 {
    response_cache_ttl_evidence(
        acc,
        request_cache_ttl_secs.map(|ttl| CacheTtlEvidence {
            min_secs: ttl,
            max_secs: ttl,
            source: "request_cache_control",
        }),
    )
    .min_secs
}

// ---------------------------------------------------------------------------
// Estimated pricing
// ---------------------------------------------------------------------------
pub fn token_cost(tokens: u64, price_per_mtok: f64) -> f64 {
    pricing::token_cost(tokens, price_per_mtok)
}

fn estimated_rebuild_cost_for_cache_event(
    acc: &ResponseAccumulator,
    model: &str,
    ttl: &CacheTtlEvidence,
) -> f64 {
    let resolved = pricing::resolve_pricing(model);
    if acc.cache_creation_tokens > 0 {
        return token_cost(
            acc.cache_creation_5m_tokens,
            resolved.pricing.cache_write_5m,
        ) + token_cost(
            acc.cache_creation_1h_tokens,
            resolved.pricing.cache_write_1h,
        );
    }

    let prompt_size_tokens = acc.input_tokens + acc.cache_read_tokens + acc.cache_creation_tokens;
    let write_price = if ttl.min_secs >= 3600 && ttl.max_secs >= 3600 {
        resolved.pricing.cache_write_1h
    } else if ttl.is_mixed() {
        // A cache-hit turn does not expose bucket sizes. Use the 1h write
        // rate as a conservative upper-bound estimate when request evidence
        // says the reusable prefix is mixed 5m/1h.
        resolved.pricing.cache_write_1h
    } else {
        resolved.pricing.cache_write_5m
    };
    token_cost(prompt_size_tokens, write_price)
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
    json_body: Vec<u8>,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    cache_creation_5m_tokens: u64,
    cache_creation_1h_tokens: u64,
    tool_calls: Vec<String>,
    is_sse: bool,
    is_json: bool,
    http_status: u32,
    error_body: Vec<u8>,
    stream_error_type: Option<String>,
    stream_error_message: Option<String>,
    current_sse_event: Option<String>,
    /// Model Anthropic reported in `message_start` or non-streaming JSON. This
    /// may differ from what the client requested when Anthropic routes the
    /// request to a different model.
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
            json_body: Vec::with_capacity(4096),
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            cache_creation_5m_tokens: 0,
            cache_creation_1h_tokens: 0,
            tool_calls: Vec::new(),
            is_sse: false,
            is_json: false,
            http_status: 0,
            error_body: Vec::new(),
            stream_error_type: None,
            stream_error_message: None,
            current_sse_event: None,
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
        while let Some(pos) = self.sse_buffer.iter().position(|&b| b == b'\n') {
            let Ok(line) = std::str::from_utf8(&self.sse_buffer[..pos]) else {
                self.sse_buffer.drain(..=pos);
                continue;
            };
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                self.current_sse_event = None;
            } else if let Some(event) = line.strip_prefix("event: ") {
                self.current_sse_event = Some(event.trim().to_string());
            } else if let Some(data) = line.strip_prefix("data: ") {
                if data != "[DONE]" {
                    if let Ok(v) = serde_json::from_str::<Value>(data) {
                        if self.current_sse_event.as_deref() == Some("error") {
                            self.process_sse_error(&v);
                        }
                        self.process_event(&v);
                    }
                }
            }
            self.sse_buffer.drain(..=pos);
        }
    }

    fn drain_deferred_watch_events(&mut self) -> Vec<watch::WatchEvent> {
        std::mem::take(&mut self.deferred_watch_events)
    }

    fn process_json_body(&mut self) {
        if self.json_body.is_empty() {
            return;
        }
        match serde_json::from_slice::<Value>(&self.json_body) {
            Ok(value) => self.process_message_json(&value),
            Err(err) => {
                warn!(error = %err, bytes = self.json_body.len(), "failed to parse Claude JSON response");
            }
        }
    }

    fn process_message_json(&mut self, value: &Value) {
        if value.get("type").and_then(|kind| kind.as_str()) == Some("error")
            || value.get("error").is_some()
        {
            self.process_sse_error(value);
        }
        if let Some(usage) = value.get("usage") {
            self.apply_usage(usage);
        }
        if let Some(model) = value.get("model").and_then(|model| model.as_str()) {
            self.response_model = Some(model.to_string());
        }
        if let Some(content) = value.get("content").and_then(|content| content.as_array()) {
            for block in content {
                match block.get("type").and_then(|kind| kind.as_str()) {
                    Some("text") => {
                        if let Some(text) = block.get("text").and_then(|text| text.as_str()) {
                            self.finish_text_block(text);
                        }
                    }
                    Some("tool_use") => {
                        let Some(tool_name) = block.get("name").and_then(|name| name.as_str())
                        else {
                            continue;
                        };
                        let input_json = block
                            .get("input")
                            .map(|input| serde_json::to_string(input).unwrap_or_default())
                            .unwrap_or_default();
                        self.finish_tool_block(tool_name, &input_json);
                    }
                    _ => {}
                }
            }
        }
    }

    fn apply_usage(&mut self, usage: &Value) {
        if let Some(input) = usage.get("input_tokens").and_then(|value| value.as_u64()) {
            self.input_tokens = input;
        }
        if let Some(output) = usage.get("output_tokens").and_then(|value| value.as_u64()) {
            self.output_tokens = output;
        }
        if let Some(cache_read) = usage
            .get("cache_read_input_tokens")
            .and_then(|value| value.as_u64())
        {
            self.cache_read_tokens = cache_read;
        }

        let total_creation = usage
            .get("cache_creation_input_tokens")
            .and_then(|value| value.as_u64());
        let creation_5m = usage
            .pointer("/cache_creation/ephemeral_5m_input_tokens")
            .and_then(|value| value.as_u64());
        let creation_1h = usage
            .pointer("/cache_creation/ephemeral_1h_input_tokens")
            .and_then(|value| value.as_u64());

        if total_creation.is_some() || creation_5m.is_some() || creation_1h.is_some() {
            let mut five_minute = creation_5m.unwrap_or(0);
            let one_hour = creation_1h.unwrap_or(0);
            let total = total_creation.unwrap_or(five_minute + one_hour);
            if creation_5m.is_none() && creation_1h.is_none() {
                five_minute = total;
            } else if total > five_minute + one_hour {
                five_minute += total - five_minute - one_hour;
            }
            self.cache_creation_5m_tokens = five_minute;
            self.cache_creation_1h_tokens = one_hour;
            self.cache_creation_tokens = total.max(five_minute + one_hour);
        }
    }

    fn finish_text_block(&mut self, text: &str) {
        for pattern in FRUSTRATION_PATTERNS.iter() {
            if pattern.regex.is_match(text) {
                self.frustration_signal_count += 1;
                self.deferred_watch_events
                    .push(watch::WatchEvent::FrustrationSignal {
                        session_id: String::new(),
                        signal_type: pattern.signal_type.to_string(),
                    });
            }
        }
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            if !self.response_text.is_empty() {
                self.response_text.push('\n');
            }
            self.response_text.push_str(trimmed);
        }
    }

    fn finish_tool_block(&mut self, tool_name: &str, input_json: &str) {
        self.tool_calls.push(tool_name.to_string());
        let summary = watch::extract_summary(tool_name, input_json);
        self.deferred_watch_events.push(watch::WatchEvent::ToolUse {
            session_id: String::new(),
            timestamp: now_iso8601(),
            tool_name: tool_name.to_string(),
            summary,
        });
        if tool_name.eq_ignore_ascii_case("Skill") {
            if let Some(skill_name) = skill_name_from_tool_input_json(input_json) {
                self.deferred_watch_events
                    .push(watch::WatchEvent::SkillEvent {
                        session_id: String::new(),
                        timestamp: now_iso8601(),
                        skill_name,
                        event_type: "fired".to_string(),
                        source: "proxy".to_string(),
                        confidence: 0.65,
                        detail: Some("inferred from Skill tool_use".to_string()),
                    });
            }
        }
    }

    fn process_sse_error(&mut self, v: &Value) {
        let error = v.get("error").unwrap_or(v);
        let error_type = error
            .get("type")
            .and_then(|value| value.as_str())
            .or_else(|| v.get("type").and_then(|value| value.as_str()))
            .unwrap_or("stream_error");
        let message = error
            .get("message")
            .and_then(|value| value.as_str())
            .or_else(|| v.get("message").and_then(|value| value.as_str()))
            .unwrap_or("Claude API stream returned an error event");
        self.stream_error_type = Some(error_type.to_string());
        self.stream_error_message = Some(message.to_string());
    }

    fn has_response_error(&self) -> bool {
        self.http_status >= 400 || self.stream_error_type.is_some()
    }

    fn process_event(&mut self, v: &Value) {
        match v.get("type").and_then(|t| t.as_str()) {
            Some("error") => {
                self.process_sse_error(v);
            }
            Some("message_start") => {
                if let Some(u) = v.pointer("/message/usage") {
                    self.apply_usage(u);
                }
                // Capture the model Anthropic actually routed to. If it differs
                // from what the user requested we surface a model-route mismatch
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
                        let text = std::mem::take(&mut self.text_buffer);
                        self.finish_text_block(&text);
                    }
                    ContentBlockType::ToolUse => {
                        // Defer ToolUse event — broadcast after session start in finalize_response.
                        if let Some(tool_name) = self.tool_calls.last() {
                            let tool_name = tool_name.clone();
                            let input_json = std::mem::take(&mut self.tool_input_buffer);
                            let summary = watch::extract_summary(&tool_name, &input_json);
                            self.deferred_watch_events.push(watch::WatchEvent::ToolUse {
                                session_id: String::new(), // Filled in finalize_response.
                                timestamp: now_iso8601(),
                                tool_name: tool_name.clone(),
                                summary,
                            });
                            if tool_name.eq_ignore_ascii_case("Skill") {
                                if let Some(skill_name) =
                                    skill_name_from_tool_input_json(&input_json)
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
                    }
                    ContentBlockType::Other => {}
                }
                self.current_block_type = ContentBlockType::Other;
            }
            Some("message_delta") => {
                if let Some(u) = v.get("usage") {
                    self.apply_usage(u);
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Finalize: metrics + DB persistence
// ---------------------------------------------------------------------------
/// Claude Code can expose 1M context as a UI model suffix. Current native-1M
/// models use the normal upstream model id; the suffix is compatibility input.
fn strip_model_1m_alias(model: &str) -> Option<&str> {
    let model = model.trim();
    let lower = model.to_ascii_lowercase();
    lower
        .ends_with("[1m]")
        .then(|| &model[..model.len().saturating_sub("[1m]".len())])
        .map(str::trim)
        .filter(|model| !model.is_empty())
}

fn canonical_known_model_alias(model: &str) -> Option<&'static str> {
    match model.to_ascii_lowercase().as_str() {
        "claude-opus-4-7" | "claude-opus-4-7-20260410" => Some("claude-opus-4-7"),
        "claude-sonnet-4-6" | "claude-sonnet-4-6-20250514" | "claude-sonnet-4-6-20260217" => {
            Some("claude-sonnet-4-6")
        }
        "claude-sonnet-4-5" | "claude-sonnet-4-5-20250929" => Some("claude-sonnet-4-5"),
        "claude-haiku-4-5" | "claude-haiku-4-5-20250929" | "claude-haiku-4-5-20251001" => {
            Some("claude-haiku-4-5")
        }
        "claude-3-7-sonnet" | "claude-3-7-sonnet-20250219" => Some("claude-3-7-sonnet"),
        "claude-3-haiku" | "claude-3-haiku-20240307" => Some("claude-3-haiku"),
        _ => None,
    }
}

/// Explicit model equivalence for known-safe aliases only. Unknown model names
/// intentionally fall back to exact string matching so arbitrary prefix overlap
/// cannot hide a real requested/actual model route mismatch.
fn normalize_model_for_match(model: &str) -> Cow<'_, str> {
    let trimmed = model.trim();
    let without_1m = strip_model_1m_alias(trimmed).unwrap_or(trimmed);
    canonical_known_model_alias(without_1m)
        .map(Cow::Borrowed)
        .unwrap_or_else(|| Cow::Borrowed(without_1m))
}

pub(crate) fn model_matches(requested: &str, actual: &str) -> bool {
    let requested = normalize_model_for_match(requested);
    let actual = normalize_model_for_match(actual);
    requested == actual
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
    let prev = turns.iter().rev().find(|turn| {
        turn_has_context_usage(
            turn.input_tokens as u64,
            turn.cache_read_tokens as u64,
            turn.cache_creation_tokens as u64,
        )
    })?;
    let prev_fill = context_fill_percent(
        prev.input_tokens as u64,
        prev.cache_read_tokens as u64,
        prev.cache_creation_tokens as u64,
        prev.context_window_tokens,
    );
    project_turns_until_compaction(prev_fill, current_fill_percent)
}

fn proxy_visible_tool_failure_streak(turns: &[diagnosis::TurnSnapshot]) -> Option<(u32, u32)> {
    let mut consecutive = 0u32;
    let mut failed_tool_calls = 0u32;
    for turn in turns.iter().rev() {
        let total_tools = turn.tool_calls.len() as u32;
        let failure_ratio = if total_tools > 0 {
            turn.tool_results_failed as f64 / total_tools as f64
        } else {
            0.0
        };
        if turn.tool_results_failed > 0 && failure_ratio > 0.5 {
            consecutive += 1;
            failed_tool_calls += turn.tool_results_failed;
        } else {
            break;
        }
    }

    (consecutive > 0).then_some((consecutive, failed_tool_calls))
}

fn ensure_session_state(
    sys_prompt_hash: u64,
    working_dir_str: &str,
    model: &str,
    user_prompt_excerpt: &str,
) -> String {
    if let Some(mut existing) = diagnosis::SESSIONS.get_mut(&sys_prompt_hash) {
        existing.last_activity = Instant::now();
        existing.cache_warning_sent = false;
        existing.idle_postmortem_sent = false;
        return existing.session_id.clone();
    }

    // Derive display name BEFORE inserting to avoid deadlock: the display-name
    // helper iterates SESSIONS to check collisions.
    let name = derive_display_name(working_dir_str, model, sys_prompt_hash);
    let ts = now_epoch_secs();
    let hash_suffix = &format!("{:x}", sys_prompt_hash)[..4];
    let sid = format!("session_{ts}_{hash_suffix}");
    diagnosis::SESSIONS.insert(
        sys_prompt_hash,
        diagnosis::SessionState {
            session_id: sid.clone(),
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
            idle_postmortem_sent: false,
        },
    );
    sid
}

fn ensure_session_inserted(
    sys_prompt_hash: u64,
    session_id: &str,
    model: &str,
    working_dir: &str,
    first_message_hash: &str,
    user_prompt_excerpt: &str,
) {
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
        let initial_prompt = if user_prompt_excerpt.is_empty() {
            None
        } else {
            Some(user_prompt_excerpt.to_string())
        };
        let _ = DB_TX.send(DbCommand::InsertSession {
            session_id: session_id.to_string(),
            started_at: now_iso8601(),
            model: model.to_string(),
            initial_prompt: initial_prompt.clone(),
            working_dir: working_dir.to_string(),
            first_message_hash: first_message_hash.to_string(),
            prompt_correlation_hash: if user_prompt_excerpt.is_empty() {
                String::new()
            } else {
                correlation::prompt_correlation_hash(user_prompt_excerpt)
            },
        });
        watch::BROADCASTER.broadcast(watch::WatchEvent::SessionStart {
            session_id: session_id.to_string(),
            display_name,
            model: model.to_string(),
            initial_prompt,
        });
        metrics::increment_active_sessions();
    }
}

fn flush_deferred_watch_events(session_id: &str, events: Vec<watch::WatchEvent>) {
    for event in events {
        let mut event = event;
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
                *sid = session_id.to_string();
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
}

#[allow(clippy::too_many_arguments)]
fn finalize_response(
    acc: &mut ResponseAccumulator,
    request_id: &str,
    model: &str,
    started_at: &Instant,
    tool_results: &[ParsedToolResult],
    sys_prompt_hash: u64,
    working_dir_str: &str,
    first_message_hash: &str,
    user_prompt_excerpt: &str,
    context_window_tokens: u64,
    is_internal_request: bool,
    request_cache_ttl: Option<CacheTtlEvidence>,
) {
    let duration = started_at.elapsed();
    let duration_secs = duration.as_secs_f64();
    let duration_ms = duration.as_millis() as u64;
    let tool_failures = tool_results.iter().filter(|result| result.is_error).count() as u32;

    let session_id =
        ensure_session_state(sys_prompt_hash, working_dir_str, model, user_prompt_excerpt);

    let billing_model = acc
        .response_model
        .clone()
        .unwrap_or_else(|| model.to_string());
    let estimated_cost = pricing::estimate_cost_dollars_with_cache_buckets(
        &billing_model,
        acc.input_tokens,
        acc.output_tokens,
        acc.cache_read_tokens,
        acc.cache_creation_5m_tokens,
        acc.cache_creation_1h_tokens,
    );
    let total_cost = estimated_cost.total_cost_dollars;

    metrics::record_request(
        &billing_model,
        acc.input_tokens,
        acc.output_tokens,
        acc.cache_read_tokens,
        acc.cache_creation_tokens,
        total_cost,
        duration_secs,
    );

    let cache_ttl = response_cache_ttl_evidence(acc, request_cache_ttl);
    let response_error_type = acc
        .stream_error_type
        .clone()
        .or_else(|| (acc.http_status >= 400).then(|| format!("http_{}", acc.http_status)));
    let response_error_message = acc.stream_error_message.clone().or_else(|| {
        (acc.http_status >= 400 && !acc.error_body.is_empty())
            .then(|| String::from_utf8_lossy(&acc.error_body).to_string())
    });
    let guard_policy = request_guard_policy();
    let api_cooldown_started = if acc.stream_error_type.is_some() {
        record_api_response_for_guard(
            Some(request_id),
            500,
            !model.is_empty(),
            guard_policy.as_ref(),
        )
    } else if acc.http_status > 0 {
        record_api_response_for_guard(
            Some(request_id),
            acc.http_status,
            !model.is_empty(),
            guard_policy.as_ref(),
        )
    } else {
        None
    };
    if let Some(finding) = api_cooldown_started {
        broadcast_guard_finding(finding);
    }
    let cache_event = if is_internal_request || acc.has_response_error() {
        "none"
    } else {
        update_cache_intelligence(
            &session_id,
            acc.cache_read_tokens,
            acc.cache_creation_tokens,
            &cache_ttl,
        )
    };

    // Phase 7: update budget state.
    {
        let mut runtime = lock_or_recover(&RUNTIME_STATE, "runtime_state");
        runtime.total_spend += total_cost;
        runtime.total_tokens += acc.input_tokens
            + acc.output_tokens
            + acc.cache_read_tokens
            + acc.cache_creation_tokens;
        runtime.request_count += 1;
    }

    let estimated_cache_waste_dollars = if matches!(cache_event, "miss_ttl" | "miss_thrash") {
        Some(
            pricing::estimate_cache_rebuild_waste_dollars_with_cache_buckets(
                &billing_model,
                acc.cache_creation_5m_tokens,
                acc.cache_creation_1h_tokens,
            )
            .total_cost_dollars,
        )
    } else {
        None
    };
    {
        let mut entry = SESSION_BUDGETS.entry(session_id.clone()).or_default();
        entry.total_spend += total_cost;
        if estimated_cost.trusted_for_budget_enforcement {
            entry.trusted_spend += total_cost;
        } else {
            entry.untrusted_spend += total_cost;
        }
        entry.total_tokens += acc.input_tokens
            + acc.output_tokens
            + acc.cache_read_tokens
            + acc.cache_creation_tokens;
        entry.request_count += 1;
        if let Some(waste) = estimated_cache_waste_dollars {
            entry.estimated_cache_waste_dollars += waste;
        }
    }

    if !is_internal_request {
        ensure_session_inserted(
            sys_prompt_hash,
            &session_id,
            model,
            working_dir_str,
            first_message_hash,
            user_prompt_excerpt,
        );

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
        if let Some(error_type) = acc.stream_error_type.as_ref() {
            watch::BROADCASTER.broadcast(watch::WatchEvent::RequestError {
                session_id: session_id.clone(),
                error_type: error_type.clone(),
                message: acc
                    .stream_error_message
                    .clone()
                    .unwrap_or_else(|| "Claude API stream returned an error event".to_string()),
            });
        }

        if cache_event != "none" && !acc.has_response_error() {
            let rebuild_cost =
                estimated_rebuild_cost_for_cache_event(acc, &billing_model, &cache_ttl);
            record_cache_event_cost(&session_id, cache_event, rebuild_cost);
            let now_epoch = now_epoch_secs();
            let expires_at = now_epoch + cache_ttl.min_secs;
            let latest_expires_at = (cache_ttl.max_secs != cache_ttl.min_secs)
                .then_some(now_epoch + cache_ttl.max_secs);
            metrics::record_cache_event(&billing_model, cache_event, estimated_cache_waste_dollars);
            watch::BROADCASTER.broadcast(watch::WatchEvent::CacheEvent {
                session_id: session_id.clone(),
                event_type: cache_event.to_string(),
                cache_expires_at_epoch: Some(expires_at),
                cache_expires_at_latest_epoch: latest_expires_at,
                cache_ttl_source: Some(cache_ttl.source.to_string()),
                cache_ttl_mixed: cache_ttl.is_mixed().then_some(true),
                estimated_rebuild_cost_dollars: Some(rebuild_cost),
            });
        }

        // Flush any SSE events not already emitted during streaming, primarily
        // events that arrived in the final chunk.
        flush_deferred_watch_events(&session_id, acc.drain_deferred_watch_events());

        // Model-route mismatch detector: Anthropic can route a request to a
        // different model than the client asked for. We emit a one-shot alert whenever the
        // `message.model` we saw in the SSE doesn't match the requested model.
        // Matching uses explicit known-safe aliases only; unknown model strings
        // must match exactly so prefix overlap cannot suppress a real warning.
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
        let (turn_number, compaction_loop_signal, tool_failure_streak) = {
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
                cache_ttl_min_secs: cache_ttl.min_secs,
                cache_ttl_max_secs: cache_ttl.max_secs,
                cache_ttl_source: cache_ttl.source.to_string(),
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
                response_error_type: response_error_type.clone(),
                response_error_message: response_error_message.clone(),
            };
            entry.push(snapshot);
            let signal = diagnosis::compaction_loop_signal_ending_at(
                entry.as_slice(),
                entry.len().saturating_sub(1),
            );
            let tool_failure_streak = proxy_visible_tool_failure_streak(entry.as_slice());
            (n, signal, tool_failure_streak)
        };

        if cache_event != "none" && !acc.has_response_error() {
            let rebuild_cost =
                estimated_rebuild_cost_for_cache_event(acc, &billing_model, &cache_ttl);
            if let Some(finding) = guard::detect_cache_rebuild_finding(
                guard_policy.as_ref(),
                guard::CacheRebuildObservation {
                    session_id: session_id.clone(),
                    request_id: Some(request_id.to_string()),
                    turn_number: Some(turn_number),
                    timestamp: now_iso8601(),
                    event_type: cache_event.to_string(),
                    estimated_rebuild_cost_dollars: Some(rebuild_cost),
                    cache_creation_tokens: acc.cache_creation_tokens,
                },
            ) {
                broadcast_guard_finding(finding);
            }
        }

        if let Some(actual) = acc.response_model.as_ref() {
            if let Some(finding) = guard::detect_model_mismatch_finding(
                guard_policy.as_ref(),
                guard::ModelMismatchObservation {
                    session_id: session_id.clone(),
                    request_id: Some(request_id.to_string()),
                    turn_number: Some(turn_number),
                    timestamp: now_iso8601(),
                    requested_model: model.to_string(),
                    reported_model: actual.clone(),
                },
            ) {
                broadcast_guard_finding(finding);
            }
        }

        if let Some(finding) = guard::detect_context_finding(
            guard_policy.as_ref(),
            guard::ContextObservation {
                session_id: session_id.clone(),
                request_id: Some(request_id.to_string()),
                turn_number: Some(turn_number),
                timestamp: now_iso8601(),
                fill_percent,
                context_window_tokens: Some(context_window_tokens),
                turns_to_compact,
            },
        ) {
            broadcast_guard_finding(finding);
        }

        if let Some((consecutive_failure_turns, failed_tool_calls)) = tool_failure_streak {
            if let Some(finding) = guard::detect_tool_failure_finding(
                guard_policy.as_ref(),
                guard::ToolFailureObservation {
                    session_id: session_id.clone(),
                    request_id: Some(request_id.to_string()),
                    turn_number: Some(turn_number),
                    timestamp: now_iso8601(),
                    consecutive_failure_turns,
                    failed_tool_calls,
                    source: guard::FindingSource::Proxy,
                },
            ) {
                broadcast_guard_finding(finding);
            }
        }

        if let Some(signal) = compaction_loop_signal {
            // Emit once when the heuristic first crosses the detection threshold.
            if signal.consecutive_turns == 3 {
                watch::BROADCASTER.broadcast(watch::WatchEvent::CompactionLoop {
                    session_id: session_id.clone(),
                    consecutive: signal.consecutive_turns,
                    wasted_tokens: signal.wasted_tokens,
                });
                if let Some(finding) = guard::detect_compaction_loop_finding(
                    guard_policy.as_ref(),
                    guard::CompactionLoopObservation {
                        session_id: session_id.clone(),
                        request_id: Some(request_id.to_string()),
                        turn_number: Some(turn_number),
                        timestamp: now_iso8601(),
                        consecutive_turns: signal.consecutive_turns,
                        wasted_tokens: signal.wasted_tokens,
                    },
                ) {
                    broadcast_guard_finding(finding);
                }
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
        let snap_cache_ttl_min_secs = cache_ttl.min_secs;
        let snap_cache_ttl_max_secs = cache_ttl.max_secs;
        let snap_cache_ttl_source = cache_ttl.source.to_string();
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
        let snap_response_error_type = response_error_type.clone();
        let snap_response_error_message = response_error_message.clone();
        let _ = DB_TX.send(DbCommand::WriteTurnSnapshot {
            session_id: snap_session,
            turn_number,
            timestamp: snap_ts.clone(),
            input_tokens: snap_input,
            cache_read_tokens: snap_cache_read,
            cache_creation_tokens: snap_cache_create,
            cache_ttl_min_secs: snap_cache_ttl_min_secs,
            cache_ttl_max_secs: snap_cache_ttl_max_secs,
            cache_ttl_source: snap_cache_ttl_source,
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
            response_error_type: snap_response_error_type,
            response_error_message: snap_response_error_message,
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
        model: billing_model.clone(),
        input_tokens: acc.input_tokens,
        output_tokens: acc.output_tokens,
        cache_read_tokens: acc.cache_read_tokens,
        cache_creation_tokens: acc.cache_creation_tokens,
        cache_creation_5m_tokens: acc.cache_creation_5m_tokens,
        cache_creation_1h_tokens: acc.cache_creation_1h_tokens,
        cache_ttl_min_secs: cache_ttl.min_secs,
        cache_ttl_max_secs: cache_ttl.max_secs,
        cache_ttl_source: cache_ttl.source.to_string(),
        outcome: if acc.has_response_error() {
            "failed".to_string()
        } else {
            "success".to_string()
        },
        error_type: response_error_type,
        error_message: response_error_message,
        cost_dollars: total_cost,
        cost_source: estimated_cost.cost_source.clone(),
        trusted_for_budget_enforcement: estimated_cost.trusted_for_budget_enforcement,
        duration_ms,
        tool_calls_json,
        tool_calls_list,
        cache_event: cache_event.to_string(),
    });

    if let Some(error_type) = acc.stream_error_type.as_deref() {
        warn!(
            request_id,
            model,
            http_status = acc.http_status,
            error_type,
            error = %acc.stream_error_message.as_deref().unwrap_or("Claude API stream returned an error event"),
            duration_s = format!("{:.2}", duration_secs),
            "request failed"
        );
    } else if acc.http_status >= 400 {
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
            billed_as = %billing_model,
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
    env_u64("CC_BLACKBOX_SESSION_TIMEOUT_MINUTES", 60) * 60
}

fn postmortem_idle_secs() -> u64 {
    env_u64("CC_BLACKBOX_POSTMORTEM_IDLE_SECS", 90)
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
         context_utilization, context_window_tokens, frustration_signals, requested_model, actual_model, response_summary, \
         COALESCE(cache_ttl_min_secs, 300), COALESCE(cache_ttl_max_secs, 300), \
         COALESCE(cache_ttl_source, 'default_5m'), response_error_type, response_error_message \
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
            let cache_ttl_min_secs = row.get::<_, Option<i64>>(15)?.unwrap_or(300).max(1) as u64;
            let cache_ttl_max_secs = row
                .get::<_, Option<i64>>(16)?
                .unwrap_or(cache_ttl_min_secs as i64)
                .max(cache_ttl_min_secs as i64) as u64;
            let cache_ttl_source = row
                .get::<_, Option<String>>(17)?
                .filter(|source| !source.trim().is_empty())
                .unwrap_or_else(|| "default_5m".to_string());
            Ok(diagnosis::TurnSnapshot {
                turn_number: row.get::<_, i64>(0)?.max(0) as u32,
                timestamp: Instant::now(),
                input_tokens,
                cache_read_tokens,
                cache_creation_tokens,
                cache_ttl_min_secs,
                cache_ttl_max_secs,
                cache_ttl_source,
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
                response_error_type: row
                    .get::<_, Option<String>>(18)?
                    .filter(|s| !s.trim().is_empty()),
                response_error_message: row
                    .get::<_, Option<String>>(19)?
                    .filter(|s| !s.trim().is_empty()),
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

fn repair_legacy_content_metadata_column(
    conn: &Connection,
    table: &str,
    column: &str,
    label: &str,
) -> rusqlite::Result<usize> {
    let select_sql = format!(
        "SELECT rowid, {column} FROM {table} \
         WHERE {column} IS NOT NULL AND trim({column}) != ''"
    );
    let mut stmt = conn.prepare(&select_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    let update_sql = format!("UPDATE {table} SET {column} = ?1 WHERE rowid = ?2");
    let mut repaired = 0usize;
    for (rowid, current) in rows {
        let Some(derived) = content_metadata_for_storage(label, &current) else {
            continue;
        };
        if derived == current.trim() {
            continue;
        }
        conn.execute(&update_sql, rusqlite::params![derived, rowid])?;
        repaired += 1;
    }
    Ok(repaired)
}

fn configure_sqlite_storage_privacy(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA secure_delete=ON;
         PRAGMA journal_size_limit=0;",
    )
}

fn truncate_sqlite_wal(conn: &Connection) -> rusqlite::Result<()> {
    let (busy, _log_frames, _checkpointed_frames): (i64, i64, i64) =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if busy != 0 {
        warn!(busy, "SQLite WAL checkpoint could not fully truncate");
    }
    Ok(())
}

fn legacy_api_error_detail_replacement(cause: &Value) -> String {
    let turn = cause
        .get("turn_first_noticed")
        .and_then(|value| value.as_u64())
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    format!(
        "Claude API streaming error at turn {turn}; response error message captured as derived metadata."
    )
}

fn repair_legacy_api_error_causes_value(causes: &mut Value) -> bool {
    let Some(items) = causes.as_array_mut() else {
        return false;
    };

    let mut changed = false;
    for cause in items {
        if cause.get("cause_type").and_then(|value| value.as_str()) != Some("api_error") {
            continue;
        }
        let detail_is_already_derived = cause
            .get("detail")
            .and_then(|value| value.as_str())
            .is_some_and(|detail| {
                detail.contains("response error message captured (redacted")
                    || detail.contains("response error message captured as derived metadata")
                    || detail.contains("no response error message was captured")
            });
        if detail_is_already_derived {
            continue;
        }
        let replacement = legacy_api_error_detail_replacement(cause);
        if let Some(object) = cause.as_object_mut() {
            object.insert("detail".to_string(), Value::String(replacement));
            changed = true;
        }
    }
    changed
}

fn repair_legacy_api_error_diagnosis_details(conn: &Connection) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare(
        "SELECT rowid, causes_json FROM session_diagnoses \
         WHERE causes_json IS NOT NULL AND trim(causes_json) != ''",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|row| row.ok())
        .collect::<Vec<_>>();

    let mut repaired = 0usize;
    for (rowid, causes_json) in rows {
        let Ok(mut causes) = serde_json::from_str::<Value>(&causes_json) else {
            continue;
        };
        if !repair_legacy_api_error_causes_value(&mut causes) {
            continue;
        }
        let repaired_json = serde_json::to_string(&causes).unwrap_or_else(|_| "[]".to_string());
        conn.execute(
            "UPDATE session_diagnoses SET causes_json = ?1 WHERE rowid = ?2",
            rusqlite::params![repaired_json, rowid],
        )?;
        repaired += 1;
    }
    Ok(repaired)
}

fn repair_legacy_raw_content_storage(conn: &Connection) -> rusqlite::Result<()> {
    for (table, column, label) in [
        ("sessions", "initial_prompt", "initial prompt"),
        ("requests", "error_message", "error message"),
        ("turn_snapshots", "response_summary", "assistant response"),
        (
            "turn_snapshots",
            "response_error_message",
            "response error message",
        ),
        ("tool_outcomes", "input_summary", "tool input"),
        ("skill_events", "detail", "event detail"),
        ("mcp_events", "detail", "event detail"),
        ("session_recall", "initial_prompt", "initial prompt"),
        (
            "session_recall",
            "final_response_summary",
            "final response summary",
        ),
    ] {
        let _ = repair_legacy_content_metadata_column(conn, table, column, label)?;
    }
    let _ = repair_legacy_api_error_diagnosis_details(conn)?;
    Ok(())
}

fn repair_persisted_session_artifacts(conn: &Connection) -> rusqlite::Result<()> {
    let _ = configure_sqlite_storage_privacy(conn);
    let _ = ensure_turn_snapshot_model_columns(conn);
    let _ = ensure_session_columns(conn);
    let _ = ensure_request_cost_columns(conn);
    let _ = repair_turn_snapshot_context_windows(conn);
    let _ = repair_session_diagnosis_degradation_turns(conn);
    let _ = repair_legacy_raw_content_storage(conn);
    let _ = truncate_sqlite_wal(conn);
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
                let prompt = content_metadata_for_storage("initial prompt", &prompt);
                let summary = content_metadata_for_storage("final response summary", &summary);
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

/// End a session: run diagnosis, persist final artifacts, broadcast SessionEnd, clean up.
fn end_session(
    session_id: &str,
    session_model: Option<String>,
    initial_prompt: Option<String>,
) -> bool {
    let finalized = end_session_with_db_tx(session_id, session_model, initial_prompt, &DB_TX);
    metrics::record_session_finalization("timeout", if finalized { "finalized" } else { "failed" });
    finalized
}

fn end_session_with_db_tx(
    session_id: &str,
    session_model: Option<String>,
    initial_prompt: Option<String>,
    db_tx: &std_mpsc::Sender<DbCommand>,
) -> bool {
    end_session_with_db_tx_and_outcome(session_id, session_model, initial_prompt, db_tx, None)
}

fn end_session_with_db_tx_and_outcome(
    session_id: &str,
    session_model: Option<String>,
    initial_prompt: Option<String>,
    db_tx: &std_mpsc::Sender<DbCommand>,
    forced_outcome: Option<&str>,
) -> bool {
    let live_snapshot = live_session_snapshot_by_id(session_id);
    let turns = diagnosis::SESSION_TURNS
        .get(session_id)
        .map(|entry| entry.clone())
        .unwrap_or_default();
    if turns.is_empty()
        && !live_snapshot
            .as_ref()
            .is_some_and(|state| state.session_inserted)
    {
        let removed_state = remove_session_state_by_id(session_id);
        if removed_state
            .as_ref()
            .is_some_and(|state| state.session_inserted)
        {
            metrics::decrement_active_sessions();
        }
        SESSION_BUDGETS.remove(session_id);
        CACHE_TRACKERS.remove(session_id);
        return true;
    }

    let mut report = diagnosis::analyze_session(session_id, &turns);
    if let Some(outcome) = forced_outcome {
        report.outcome = outcome.to_string();
    }
    let recall_initial_prompt = initial_prompt
        .or_else(|| {
            live_snapshot
                .as_ref()
                .and_then(|state| state.initial_prompt.clone())
        })
        .unwrap_or_default();
    let recall_summary = last_session_response_summary(&turns);
    let recall = PersistedRecall {
        initial_prompt: recall_initial_prompt,
        final_response_summary: recall_summary,
    };

    let ended_at = now_iso8601();
    let diagnosis = PersistedDiagnosis {
        completed_at: ended_at.clone(),
        outcome: report.outcome.clone(),
        total_turns: report.total_turns,
        total_cost: report.estimated_total_cost_dollars,
        cache_hit_ratio: report.cache_hit_ratio,
        degraded: report.degraded,
        degradation_turn: report.degradation_turn,
        causes_json: serde_json::to_string(&report.causes).unwrap_or_default(),
        advice_json: serde_json::to_string(&report.advice).unwrap_or_default(),
    };

    let (response_tx, response_rx) = std_mpsc::channel();
    let persisted_final_artifacts = match db_tx.send(DbCommand::FinalizeSession {
        session_id: session_id.to_string(),
        ended_at,
        diagnosis,
        recall: Some(recall),
        response_tx,
    }) {
        Ok(()) => match response_rx.recv() {
            Ok(Ok(())) => true,
            Ok(Err(err)) => {
                warn!(
                    session_id,
                    error = %err,
                    "final session artifacts were not persisted before SessionEnd"
                );
                false
            }
            Err(err) => {
                warn!(
                    session_id,
                    error = %err,
                    "final session artifact persistence channel closed before SessionEnd"
                );
                false
            }
        },
        Err(err) => {
            warn!(
                session_id,
                error = %err,
                "failed to queue final session artifact persistence"
            );
            false
        }
    };

    if !persisted_final_artifacts {
        return false;
    }

    let removed_state = remove_session_state_by_id(session_id);
    if removed_state
        .as_ref()
        .is_some_and(|state| state.session_inserted)
    {
        metrics::decrement_active_sessions();
    }
    let _ = diagnosis::SESSION_TURNS.remove(session_id);
    SESSION_BUDGETS.remove(session_id);
    CACHE_TRACKERS.remove(session_id);

    watch::BROADCASTER.broadcast(watch::WatchEvent::SessionEnd {
        session_id: session_id.to_string(),
        outcome: report.outcome.clone(),
        total_tokens: report.total_tokens,
        total_turns: report.total_turns,
    });
    let model_for_metrics = session_model
        .as_deref()
        .or(live_snapshot.as_ref().map(|state| state.model.as_str()));
    metrics::record_session_end(
        &report.outcome,
        model_for_metrics,
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
            report,
        });
    }

    true
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct FinalizeSessionResult {
    session_id: String,
    outcome: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct FinalizeSessionsResponse {
    reason: String,
    results: Vec<FinalizeSessionResult>,
}

#[derive(Clone, Debug, Deserialize)]
struct FinalizeSessionsRequest {
    session_ids: Vec<String>,
    reason: String,
    child_exit_code: Option<i32>,
    rule_id: Option<String>,
    metadata: Option<Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoredFinalizationState {
    Active,
    Ended,
}

fn stored_finalization_state(
    db_path: &str,
    session_id: &str,
) -> Result<Option<StoredFinalizationState>, String> {
    let conn = Connection::open(db_path).map_err(|err| err.to_string())?;
    let ended_at = conn
        .query_row(
            "SELECT ended_at FROM sessions WHERE session_id = ?1",
            rusqlite::params![session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(|err| err.to_string())?;
    Ok(ended_at.map(|ended_at| {
        if ended_at
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
        {
            StoredFinalizationState::Ended
        } else {
            StoredFinalizationState::Active
        }
    }))
}

fn normalize_finalization_reason(reason: &str) -> &'static str {
    match reason {
        "child_exit" => "child_exit",
        "budget_block" => "budget_block",
        "timeout" => "timeout",
        "explicit_api" => "explicit_api",
        _ => "other",
    }
}

fn finalize_one_session_with_db_tx(
    session_id: &str,
    reason: &str,
    db_tx: &std_mpsc::Sender<DbCommand>,
    db_path: &str,
) -> FinalizeSessionResult {
    let session_id = session_id.trim();
    if session_id.is_empty() {
        return FinalizeSessionResult {
            session_id: String::new(),
            outcome: "not_found".to_string(),
        };
    }

    if session_is_live(session_id) {
        let snapshot = live_session_snapshot_by_id(session_id);
        let forced_outcome =
            (normalize_finalization_reason(reason) == "budget_block").then_some("Budget Exceeded");
        let finalized = end_session_with_db_tx_and_outcome(
            session_id,
            snapshot.as_ref().map(|state| state.model.clone()),
            snapshot
                .as_ref()
                .and_then(|state| state.initial_prompt.clone()),
            db_tx,
            forced_outcome,
        );
        return FinalizeSessionResult {
            session_id: session_id.to_string(),
            outcome: if finalized {
                "finalized".to_string()
            } else {
                "failed".to_string()
            },
        };
    }

    let outcome = match stored_finalization_state(db_path, session_id) {
        Ok(Some(StoredFinalizationState::Ended)) => "already_finalized",
        Ok(Some(StoredFinalizationState::Active)) => "failed",
        Ok(None) => "not_found",
        Err(_) => "failed",
    };
    FinalizeSessionResult {
        session_id: session_id.to_string(),
        outcome: outcome.to_string(),
    }
}

fn finalize_sessions_with_db_tx(
    session_ids: &[String],
    reason: &str,
    db_tx: &std_mpsc::Sender<DbCommand>,
    db_path: &str,
) -> FinalizeSessionsResponse {
    let reason = normalize_finalization_reason(reason).to_string();
    let mut results = Vec::with_capacity(session_ids.len());
    for session_id in session_ids {
        let result = finalize_one_session_with_db_tx(session_id, &reason, db_tx, db_path);
        metrics::record_session_finalization(&reason, &result.outcome);
        results.push(result);
    }
    FinalizeSessionsResponse { reason, results }
}

fn is_terminal_budget_rule(rule_id: guard::RuleId) -> bool {
    matches!(
        rule_id,
        guard::RuleId::PerSessionTokenBudgetExceeded
            | guard::RuleId::PerSessionTrustedDollarBudgetExceeded
    )
}

fn finalize_terminal_budget_block_with_db_tx(
    block: &guard::GuardBlock,
    db_tx: &std_mpsc::Sender<DbCommand>,
    db_path: &str,
) -> Option<FinalizeSessionResult> {
    if !is_terminal_budget_rule(block.rule_id) {
        return None;
    }
    let session_id = block.session_id.as_deref()?;
    let response =
        finalize_sessions_with_db_tx(&[session_id.to_string()], "budget_block", db_tx, db_path);
    response.results.into_iter().next()
}

fn finalize_terminal_budget_block(block: &guard::GuardBlock) -> Option<FinalizeSessionResult> {
    let result = finalize_terminal_budget_block_with_db_tx(block, &DB_TX, &db_path());
    if let Some(result) = result.as_ref() {
        if result.outcome == "failed" {
            warn!(
                session_id = %result.session_id,
                rule_id = %block.rule_id,
                "failed to finalize terminal budget-blocked session"
            );
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive a short display name from the working directory path.
/// Falls back to model+time if working_dir is empty.
/// Appends a 3-char hash suffix if the name collides with an existing session.
fn derive_display_name(working_dir: &str, model: &str, sys_prompt_hash: u64) -> String {
    let base = if !working_dir.is_empty() {
        // Extract the last path component: /Users/pradeep/code/idea/cc-blackbox → cc-blackbox
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
    let value = std::env::var("CC_BLACKBOX_CONTEXT_WINDOW_TOKENS").ok()?;
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

fn any_model_requests_1m_context(
    requested_model: Option<&str>,
    actual_model: Option<&str>,
) -> bool {
    requested_model.is_some_and(model_requests_1m_context)
        || actual_model.is_some_and(model_requests_1m_context)
}

fn observed_input_tokens(
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
) -> u64 {
    input_tokens
        .saturating_add(cache_read_tokens)
        .saturating_add(cache_creation_tokens)
}

fn legacy_native_1m_model_match(model: &str, family: &str) -> Option<(u32, u32)> {
    let lower = normalize_model_for_match(model).to_ascii_lowercase();
    let tokens = lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let family_index = tokens.iter().position(|token| *token == family)?;

    let parse_version_part = |token: &str| -> Option<u32> {
        let value = token.parse::<u32>().ok()?;
        (value <= 99).then_some(value)
    };

    if let Some(major) = tokens
        .get(family_index + 1)
        .and_then(|token| parse_version_part(token))
    {
        let minor = tokens
            .get(family_index + 2)
            .and_then(|token| parse_version_part(token))
            .unwrap_or(0);
        return Some((major, minor));
    }

    if let Some(minor) = family_index
        .checked_sub(1)
        .and_then(|idx| tokens.get(idx))
        .and_then(|token| parse_version_part(token))
    {
        let major = family_index
            .checked_sub(2)
            .and_then(|idx| tokens.get(idx))
            .and_then(|token| parse_version_part(token))?;
        return Some((major, minor));
    }

    None
}

// Older builds inferred 1M context from plain Sonnet/Opus/Mythos model ids.
// Keep that matcher only to repair rows written by that retired assumption.
fn model_matches_legacy_native_1m_rule(model: &str) -> bool {
    let lower = normalize_model_for_match(model).to_ascii_lowercase();
    lower.contains("mythos")
        || legacy_native_1m_model_match(&lower, "opus")
            .is_some_and(|(major, minor)| major > 4 || (major == 4 && minor >= 6))
        || legacy_native_1m_model_match(&lower, "sonnet")
            .is_some_and(|(major, minor)| major > 4 || (major == 4 && minor >= 6))
}

fn turn_snapshot_context_window_needs_repair(
    existing_context_window_tokens: Option<u64>,
    requested_model: Option<&str>,
    actual_model: Option<&str>,
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
) -> bool {
    let Some(existing_context_window_tokens) = existing_context_window_tokens else {
        return true;
    };
    if existing_context_window_tokens == 0 {
        return true;
    }
    if existing_context_window_tokens != EXTENDED_CONTEXT_WINDOW_TOKENS {
        return false;
    }
    if configured_context_window_tokens().is_some()
        || any_model_requests_1m_context(requested_model, actual_model)
        || observed_input_tokens(input_tokens, cache_read_tokens, cache_creation_tokens)
            > STANDARD_CONTEXT_WINDOW_TOKENS
    {
        return false;
    }

    requested_model
        .into_iter()
        .chain(actual_model)
        .any(model_matches_legacy_native_1m_rule)
}

fn request_uses_1m_context(headers: &HttpHeaders) -> bool {
    let _ = headers;
    false
}

#[derive(Debug, Default, PartialEq, Eq)]
struct UpstreamRequestAdjustment {
    body: Option<Vec<u8>>,
    anthropic_beta: Option<String>,
    remove_anthropic_beta: bool,
}

fn clean_anthropic_beta(beta_values: &[String]) -> (String, bool) {
    let mut removed_any = false;
    let kept = beta_values
        .iter()
        .flat_map(|value| value.split(','))
        .filter_map(|value| {
            let value = value.trim();
            if value.is_empty() {
                removed_any = true;
                return None;
            }
            if value
                .to_ascii_lowercase()
                .starts_with(CONTEXT_1M_BETA_PREFIX)
            {
                removed_any = true;
                return None;
            }
            Some(value)
        })
        .collect::<Vec<_>>();
    (kept.join(", "), removed_any)
}

fn upstream_anthropic_beta_adjustment(beta_values: &[String]) -> (Option<String>, bool) {
    let (cleaned_beta, should_sanitize_beta) = clean_anthropic_beta(beta_values);
    (
        (should_sanitize_beta && !cleaned_beta.is_empty()).then_some(cleaned_beta),
        should_sanitize_beta,
    )
}

fn upstream_request_adjustment_for_body(
    beta_values: &[String],
    body: &[u8],
) -> UpstreamRequestAdjustment {
    let (anthropic_beta, remove_anthropic_beta) = upstream_anthropic_beta_adjustment(beta_values);
    let mut adjustment = UpstreamRequestAdjustment {
        body: None,
        anthropic_beta,
        remove_anthropic_beta,
    };

    let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
        return adjustment;
    };

    let Some(model) = value.get("model").and_then(|model| model.as_str()) else {
        return adjustment;
    };
    let Some(upstream_model) = strip_model_1m_alias(model) else {
        return adjustment;
    };

    value["model"] = Value::String(upstream_model.to_string());
    adjustment.body = serde_json::to_vec(&value).ok();
    adjustment
}

fn request_context_window_hint_from_headers(headers: &HttpHeaders) -> Option<u64> {
    if request_uses_1m_context(headers) {
        Some(EXTENDED_CONTEXT_WINDOW_TOKENS)
    } else {
        None
    }
}

fn resolve_context_window_tokens(header_hint: Option<u64>, model: &str) -> u64 {
    resolve_context_window_tokens_with_config(
        configured_context_window_tokens(),
        header_hint,
        model,
    )
}

fn resolve_context_window_tokens_with_config(
    configured: Option<u64>,
    header_hint: Option<u64>,
    model: &str,
) -> u64 {
    configured
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

    if any_model_requests_1m_context(requested_model, actual_model) {
        return EXTENDED_CONTEXT_WINDOW_TOKENS;
    }

    let total_input_tokens =
        observed_input_tokens(input_tokens, cache_read_tokens, cache_creation_tokens);
    if total_input_tokens > STANDARD_CONTEXT_WINDOW_TOKENS {
        return EXTENDED_CONTEXT_WINDOW_TOKENS;
    }

    STANDARD_CONTEXT_WINDOW_TOKENS
}

fn turn_has_context_usage(
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
) -> bool {
    observed_input_tokens(input_tokens, cache_read_tokens, cache_creation_tokens) > 0
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
/// their subscription-tier weekly budget via `CC_BLACKBOX_WEEKLY_TOKEN_BUDGET`
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

#[derive(Clone)]
struct QuotaBurnSnapshot {
    seconds_to_reset: Option<u64>,
    tokens_used_this_week: u64,
    tokens_limit: Option<u64>,
    tokens_remaining: Option<u64>,
    budget_source: Option<String>,
    projected_exhaustion_secs: Option<u64>,
}

fn quota_burn_snapshot(per_sec: Option<f64>) -> QuotaBurnSnapshot {
    let used_this_week = this_week_tokens_used();
    let weekly = env_u64("CC_BLACKBOX_WEEKLY_TOKEN_BUDGET", 0);
    let (tokens_limit, budget_source) = if weekly > 0 {
        (Some(weekly), Some("env".to_string()))
    } else if let Some(auto) = auto_weekly_budget_suggestion() {
        (Some(auto.tokens_limit), Some("auto_p95_4w".to_string()))
    } else {
        (None, None)
    };
    let tokens_remaining = tokens_limit.map(|limit| limit.saturating_sub(used_this_week));
    let projected_exhaustion_secs = match (tokens_remaining, per_sec) {
        (Some(remaining), Some(rate)) if rate > 0.0 && remaining > 0 => {
            Some((remaining as f64 / rate).round() as u64)
        }
        _ => None,
    };

    QuotaBurnSnapshot {
        seconds_to_reset: Some(seconds_until_weekly_reset()),
        tokens_used_this_week: used_this_week,
        tokens_limit,
        tokens_remaining,
        budget_source,
        projected_exhaustion_secs,
    }
}

fn quota_burn_watch_event(snapshot: QuotaBurnSnapshot) -> watch::WatchEvent {
    watch::WatchEvent::RateLimitStatus {
        seconds_to_reset: snapshot.seconds_to_reset,
        requests_remaining: None,
        requests_limit: None,
        input_tokens_remaining: None,
        output_tokens_remaining: None,
        tokens_used_this_week: Some(snapshot.tokens_used_this_week),
        tokens_limit: snapshot.tokens_limit,
        tokens_remaining: snapshot.tokens_remaining,
        budget_source: snapshot.budget_source,
        projected_exhaustion_secs: snapshot.projected_exhaustion_secs,
    }
}

fn current_quota_burn_watch_event() -> Option<watch::WatchEvent> {
    let total_tokens = {
        let runtime = lock_or_recover(&RUNTIME_STATE, "runtime_state");
        runtime.total_tokens
    };
    let per_sec = {
        let t = lock_or_recover(&BURN_TRACKER, "burn_tracker");
        t.tokens_per_sec()
    };
    let snapshot = quota_burn_snapshot(per_sec);
    if total_tokens == 0 && snapshot.tokens_used_this_week == 0 {
        return None;
    }
    Some(quota_burn_watch_event(snapshot))
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
        let cache = lock_or_recover(&AUTO_WEEKLY_BUDGET_CACHE, "auto_weekly_budget_cache");
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

    let mut cache = lock_or_recover(&AUTO_WEEKLY_BUDGET_CACHE, "auto_weekly_budget_cache");
    cache.refreshed_at = Some(now);
    cache.week_start_epoch = Some(current_week_start);
    cache.suggestion = suggestion.clone();
    suggestion
}

#[derive(Clone, Debug)]
struct ParsedRequestBody {
    model: String,
    message_count: usize,
    has_tools: bool,
    system_prompt_length: usize,
    estimated_input_tokens: usize,
    sys_prompt_hash: u64,
    working_dir: String,
    first_message_hash: String,
    user_prompt_excerpt: String,
    is_internal_request: bool,
    cache_ttl_evidence: Option<CacheTtlEvidence>,
}

fn cache_control_ttl_secs(cache_control: &Value) -> Option<u64> {
    if cache_control.get("type").and_then(|value| value.as_str()) != Some("ephemeral") {
        return None;
    }
    match cache_control.get("ttl").and_then(|value| value.as_str()) {
        Some("1h") => Some(3600),
        Some("5m") | None => Some(300),
        Some(_) => Some(300),
    }
}

fn collect_cache_control_ttls(value: &Value, ttls: &mut Vec<u64>) {
    match value {
        Value::Object(map) => {
            if let Some(cache_control) = map.get("cache_control") {
                if let Some(ttl) = cache_control_ttl_secs(cache_control) {
                    ttls.push(ttl);
                }
            }
            for child in map.values() {
                collect_cache_control_ttls(child, ttls);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_cache_control_ttls(child, ttls);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
fn request_cache_ttl_secs(value: &Value) -> Option<u64> {
    request_cache_ttl_evidence(value).map(|evidence| evidence.min_secs)
}

fn request_cache_ttl_evidence(value: &Value) -> Option<CacheTtlEvidence> {
    let mut ttls = Vec::new();
    collect_cache_control_ttls(value, &mut ttls);
    CacheTtlEvidence::from_ttls(&ttls, "request_cache_control")
}

fn looks_like_title_request(prompt: &str, message_count: usize, has_tools: bool) -> bool {
    if has_tools || message_count > 2 {
        return false;
    }
    let normalized = normalize_search_text(prompt);
    normalized.contains("generate a short title")
        || normalized.contains("conversation title")
        || normalized.contains("title for this conversation")
        || normalized.contains("summarize this conversation in")
}

fn is_internal_request_shape(
    working_dir: &str,
    system_prompt_length: usize,
    message_count: usize,
    has_tools: bool,
    user_prompt_excerpt: &str,
) -> bool {
    working_dir.trim().is_empty()
        || (system_prompt_length == 0
            && looks_like_title_request(user_prompt_excerpt, message_count, has_tools))
        || looks_like_title_request(user_prompt_excerpt, message_count, has_tools)
}

/// Returns (model, message_count, has_tools, system_prompt_length, estimated_input_tokens, sys_prompt_hash, working_dir).
/// sys_prompt_hash: stable per-terminal session key derived from working directory.
/// working_dir: extracted working directory path — used for session naming.
/// user_prompt_excerpt: cleaned text of the first user message (no Claude Code preamble), for display.
fn parse_request_body(body: &[u8]) -> Option<ParsedRequestBody> {
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
    let first_message_hash = hash_text_hex(&first_user_message);
    let cache_ttl_evidence = request_cache_ttl_evidence(&v);
    let is_internal_request =
        is_internal_request_shape(&working_dir, sl, mc, ht, &user_prompt_excerpt);

    Some(ParsedRequestBody {
        model,
        message_count: mc,
        has_tools: ht,
        system_prompt_length: sl,
        estimated_input_tokens: body.len() / 4,
        sys_prompt_hash,
        working_dir: normalize_working_dir(&working_dir),
        first_message_hash,
        user_prompt_excerpt,
        is_internal_request,
        cache_ttl_evidence,
    })
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

fn normalize_working_dir(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed == "/" {
        return trimmed.to_string();
    }
    trimmed.trim_end_matches('/').to_string()
}

fn hash_text_hex(raw: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    raw.hash(&mut hasher);
    format!("firstmsg_{:016x}", hasher.finish())
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
    if raw.chars().count() <= max {
        raw.to_string()
    } else {
        format!("{}...", raw.chars().take(max).collect::<String>())
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
    let mut states = lock_or_recover(&HOOK_SKILL_TURNS, "hook_skill_turns");
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
            || prompt_norm.contains(&format!("use the {skill_phrase}"))
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
        let mut states = lock_or_recover(&HOOK_SKILL_TURNS, "hook_skill_turns");
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
        "UserPromptExpansion"
            if payload
                .get("expansion_type")
                .and_then(|value| value.as_str())
                == Some("slash_command") =>
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

fn request_header_continue_with_sanitized_headers(_beta_values: &[String]) -> HeadersResponse {
    HeadersResponse {
        response: Some(CommonResponse {
            status: ResponseStatus::Continue.into(),
            header_mutation: Some(HeaderMutation {
                set_headers: Vec::new(),
                remove_headers: vec!["accept-encoding".into()],
            }),
            ..Default::default()
        }),
    }
}

fn body_continue_with_upstream_adjustment(adjustment: UpstreamRequestAdjustment) -> BodyResponse {
    let mut set_headers = Vec::new();
    let mut remove_headers = Vec::new();
    let body_len = adjustment.body.as_ref().map(Vec::len);
    if let Some(value) = adjustment.anthropic_beta {
        if adjustment.remove_anthropic_beta {
            remove_headers.push("anthropic-beta".to_string());
        }
        set_headers.push(ProtoHeaderValueOption {
            header: Some(ProtoHeaderValue {
                key: "anthropic-beta".to_string(),
                value,
                raw_value: Vec::new(),
            }),
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd.into(),
            ..Default::default()
        });
    } else if adjustment.remove_anthropic_beta {
        remove_headers.push("anthropic-beta".to_string());
    }
    if let Some(len) = body_len {
        set_headers.push(ProtoHeaderValueOption {
            header: Some(ProtoHeaderValue {
                key: "content-length".to_string(),
                value: len.to_string(),
                raw_value: Vec::new(),
            }),
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd.into(),
            ..Default::default()
        });
    }
    BodyResponse {
        response: Some(CommonResponse {
            status: ResponseStatus::Continue.into(),
            header_mutation: (!set_headers.is_empty() || !remove_headers.is_empty()).then_some(
                HeaderMutation {
                    set_headers,
                    remove_headers,
                },
            ),
            body_mutation: adjustment.body.map(|body| BodyMutation {
                mutation: Some(body_mutation::Mutation::Body(body)),
            }),
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
pub struct CcBlackboxProcessor;

#[tonic::async_trait]
impl ExternalProcessor for CcBlackboxProcessor {
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
            let mut first_message_hash_buf = String::new();
            let mut user_prompt_excerpt_buf = String::new();
            let mut is_internal_request = false;
            let mut request_cache_ttl: Option<CacheTtlEvidence> = None;
            let mut request_context_window_hint: Option<u64> = None;
            let mut request_anthropic_beta_values: Vec<String> = Vec::new();
            let mut context_window_tokens = STANDARD_CONTEXT_WINDOW_TOKENS;
            let mut finalized = false;

            loop {
                let msg = match stream.message().await {
                    Ok(Some(r)) => r,
                    Ok(None) => {
                        // Stream closed — finalize if not already done via end_of_stream.
                        if !finalized && !model.is_empty() {
                            finalize_response(
                                &mut resp_acc,
                                &request_id,
                                &model,
                                &started_at,
                                &tool_results,
                                sys_prompt_hash,
                                &working_dir_str,
                                &first_message_hash_buf,
                                &user_prompt_excerpt_buf,
                                context_window_tokens,
                                is_internal_request,
                                request_cache_ttl.clone(),
                            );
                            REQUEST_STATE.remove(&request_id);
                        }
                        break;
                    }
                    Err(e) => {
                        warn!(error = %e, "ext_proc stream error");
                        if !finalized && !model.is_empty() {
                            finalize_response(
                                &mut resp_acc,
                                &request_id,
                                &model,
                                &started_at,
                                &tool_results,
                                sys_prompt_hash,
                                &working_dir_str,
                                &first_message_hash_buf,
                                &user_prompt_excerpt_buf,
                                context_window_tokens,
                                is_internal_request,
                                request_cache_ttl.clone(),
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
                            .filter(|value| !value.trim().is_empty())
                            .unwrap_or_else(fallback_request_id);
                        request_anthropic_beta_values = extract_headers(h, "anthropic-beta");
                        request_context_window_hint = request_context_window_hint_from_headers(h);
                        context_window_tokens = configured_context_window_tokens()
                            .unwrap_or(STANDARD_CONTEXT_WINDOW_TOKENS);

                        let response = ProcessingResponse {
                            response: Some(ExtProcResponse::RequestHeaders(
                                request_header_continue_with_sanitized_headers(
                                    &request_anthropic_beta_values,
                                ),
                            )),
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
                            Some(parsed) => {
                                info!(phase="request_body", request_id=%request_id, model=%parsed.model, message_count=parsed.message_count, has_tools=parsed.has_tools, system_prompt_length=parsed.system_prompt_length, estimated_input_tokens=parsed.estimated_input_tokens, sys_prompt_hash=parsed.sys_prompt_hash, "ext_proc");
                                let session_id = ensure_session_state(
                                    parsed.sys_prompt_hash,
                                    &parsed.working_dir,
                                    &parsed.model,
                                    &parsed.user_prompt_excerpt,
                                );
                                let policy = request_guard_policy();
                                if let Some(decision) = evaluate_request_guard_for_session(
                                    Some(&session_id),
                                    policy.as_ref(),
                                ) {
                                    let finding = decision.finding.clone();
                                    if let Some(block) = decision.block.as_ref() {
                                        warn!(request_id = %request_id, error_type = %block.error_type, rule_id = %block.rule_id, "request blocked");
                                        metrics::record_guard_block(&block.rule_id.to_string());
                                        ensure_session_inserted(
                                            parsed.sys_prompt_hash,
                                            &session_id,
                                            &parsed.model,
                                            &parsed.working_dir,
                                            &parsed.first_message_hash,
                                            &parsed.user_prompt_excerpt,
                                        );
                                        let response = make_guard_block_response(block);
                                        if tx.send(Ok(response)).await.is_err() {
                                            break;
                                        }
                                        if let Some(finding) = finding {
                                            broadcast_guard_finding(finding);
                                        }
                                        let _ = finalize_terminal_budget_block(block);
                                        blocked = true;
                                    }
                                }
                                sys_prompt_hash = parsed.sys_prompt_hash;
                                working_dir_str = parsed.working_dir;
                                first_message_hash_buf = parsed.first_message_hash;
                                // Only first turn carries a meaningful "initial prompt"; for mc>1
                                // messages[0] is still the original, but we only register the
                                // session once (see SESSIONS.get_mut in finalize_response), so
                                // passing the excerpt every time is harmless and first-write-wins.
                                user_prompt_excerpt_buf = parsed.user_prompt_excerpt;
                                is_internal_request = parsed.is_internal_request;
                                request_cache_ttl = parsed.cache_ttl_evidence.clone();
                                if !blocked {
                                    context_window_tokens = resolve_context_window_tokens(
                                        request_context_window_hint,
                                        &parsed.model,
                                    );
                                    // Session ID resolved later in finalize_response.
                                    REQUEST_STATE.insert(
                                        request_id.clone(),
                                        RequestMeta {
                                            request_id: request_id.clone(),
                                            session_id: String::new(),
                                            model: parsed.model.clone(),
                                            message_count: parsed.message_count,
                                            has_tools: parsed.has_tools,
                                            system_prompt_length: parsed.system_prompt_length,
                                            estimated_input_tokens: parsed.estimated_input_tokens,
                                            started_at,
                                        },
                                    );
                                    model = parsed.model;
                                }
                            }
                            None => {
                                warn!(request_id=%request_id, bytes=b.body.len(), "failed to parse request JSON");
                            }
                        }

                        if blocked {
                            continue;
                        }

                        let upstream_adjustment = upstream_request_adjustment_for_body(
                            &request_anthropic_beta_values,
                            &b.body,
                        );
                        let response = ProcessingResponse {
                            response: Some(ExtProcResponse::RequestBody(
                                body_continue_with_upstream_adjustment(upstream_adjustment),
                            )),
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
                        let content_type = extract_header(h, "content-type")
                            .unwrap_or_default()
                            .to_ascii_lowercase();
                        resp_acc.is_sse =
                            status == 200 && content_type.contains("text/event-stream");
                        resp_acc.is_json =
                            status == 200 && content_type.contains("application/json");

                        // NOTE: Anthropic does not return anthropic-ratelimit-*
                        // headers on Claude Code subscription (OAuth) traffic —
                        // verified empirically. Quota burn is broadcast from a
                        // background task (`quota_burn_monitor`) using our own
                        // token counters instead.
                        let _ = h;

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
                            if !is_internal_request
                                && !model.is_empty()
                                && !resp_acc.deferred_watch_events.is_empty()
                            {
                                let session_id = ensure_session_state(
                                    sys_prompt_hash,
                                    &working_dir_str,
                                    &model,
                                    &user_prompt_excerpt_buf,
                                );
                                ensure_session_inserted(
                                    sys_prompt_hash,
                                    &session_id,
                                    &model,
                                    &working_dir_str,
                                    &first_message_hash_buf,
                                    &user_prompt_excerpt_buf,
                                );
                                flush_deferred_watch_events(
                                    &session_id,
                                    resp_acc.drain_deferred_watch_events(),
                                );
                            }
                            let chunk_ms = chunk_start.elapsed().as_millis();
                            debug!(request_id=%request_id, chunk_ms, bytes=b.body.len(), "response_body chunk parse time");
                            if chunk_ms > 10 {
                                warn!(request_id=%request_id, chunk_ms, bytes=b.body.len(), "response_body chunk parse exceeded 10ms");
                            }
                        } else if resp_acc.is_json && !b.body.is_empty() {
                            resp_acc.json_body.extend_from_slice(&b.body);
                        } else if !resp_acc.is_sse && !b.body.is_empty() {
                            // Capture error response body (capped at 1KB).
                            let remaining = 1024usize.saturating_sub(resp_acc.error_body.len());
                            resp_acc
                                .error_body
                                .extend_from_slice(&b.body[..b.body.len().min(remaining)]);
                        }
                        if b.end_of_stream {
                            if resp_acc.is_json {
                                resp_acc.process_json_body();
                            }
                            if !model.is_empty() {
                                finalize_response(
                                    &mut resp_acc,
                                    &request_id,
                                    &model,
                                    &started_at,
                                    &tool_results,
                                    sys_prompt_hash,
                                    &working_dir_str,
                                    &first_message_hash_buf,
                                    &user_prompt_excerpt_buf,
                                    context_window_tokens,
                                    is_internal_request,
                                    request_cache_ttl.clone(),
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

async fn handle_guard_policy() -> impl IntoResponse {
    let report = build_guard_policy_report_json();
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&report).unwrap_or_default(),
    )
}

async fn handle_guard_status(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let session = params.get("session").cloned();
    let report =
        tokio::task::spawn_blocking(move || build_guard_status_report_json(session.as_deref()))
            .await
            .unwrap_or_else(|_| serde_json::json!({"overall_state": "healthy", "sessions": []}));
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&report).unwrap_or_default(),
    )
}

async fn handle_finalize_sessions(
    Json(payload): Json<FinalizeSessionsRequest>,
) -> impl IntoResponse {
    if payload.session_ids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/json")],
            serde_json::json!({"error": "session_ids must not be empty"}).to_string(),
        )
            .into_response();
    }
    if payload.reason.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/json")],
            serde_json::json!({"error": "reason must not be empty"}).to_string(),
        )
            .into_response();
    }

    let session_ids = payload.session_ids;
    let reason = payload.reason;
    let _metadata = (payload.child_exit_code, payload.rule_id, payload.metadata);
    let db_tx = DB_TX.clone();
    let path = db_path();
    let response = tokio::task::spawn_blocking(move || {
        finalize_sessions_with_db_tx(&session_ids, &reason, &db_tx, &path)
    })
    .await
    .unwrap_or_else(|_err| FinalizeSessionsResponse {
        reason: "other".to_string(),
        results: vec![FinalizeSessionResult {
            session_id: String::new(),
            outcome: "failed".to_string(),
        }],
    });

    (StatusCode::OK, Json(response)).into_response()
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
    let replay_history = !matches!(
        params.get("replay").map(|value| value.as_str()),
        Some("false" | "0" | "no")
    );

    let (history, mut rx) = watch::BROADCASTER.subscribe_with_history();
    let quota_start_snapshot = current_quota_burn_watch_event();

    // Look up stored session info synchronously before the stream starts.
    let synthetic_start = replay_history
        .then_some(())
        .and(session_filter.as_ref())
        .and_then(|sid| {
            diagnosis::SESSIONS.iter().find_map(|entry| {
                let s = entry.value();
                if s.session_id == *sid && s.session_inserted {
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
    let synthetic_session_id = synthetic_start.as_ref().map(|event| match event {
        watch::WatchEvent::SessionStart { session_id, .. } => session_id.clone(),
        _ => String::new(),
    });

    let stream = async_stream::stream! {
        // Show the local weekly burn once when a watcher connects. The
        // background monitor keeps Prometheus gauges fresh without repeatedly
        // printing the same quota line to the terminal.
        if let Some(ev) = quota_start_snapshot {
            if let Ok(json) = serde_json::to_string(&ev) {
                yield Ok(Event::default().data(json));
            }
        }

        // Synthetic SessionStart first if we're filtered to a session.
        if let Some(ev) = synthetic_start {
            if let Ok(json) = serde_json::to_string(&ev) {
                yield Ok(Event::default().data(json));
            }
        }

        // Replay recent history, filtered if a session is specified.
        if replay_history {
            for event in history {
                if matches!(event, watch::WatchEvent::RateLimitStatus { .. }) {
                    continue;
                }
                if !event_matches_session(&event, session_filter.as_deref()) {
                    continue;
                }
                if let (
                    Some(injected_session_id),
                    watch::WatchEvent::SessionStart { session_id, .. },
                ) = (&synthetic_session_id, &event)
                {
                    if session_id == injected_session_id {
                        continue;
                    }
                }
                if let Ok(json) = serde_json::to_string(&event) {
                    yield Ok(Event::default().data(json));
                }
            }
        }
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if matches!(event, watch::WatchEvent::RateLimitStatus { .. }) {
                        continue;
                    }
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
        | watch::WatchEvent::PostmortemReady { session_id, .. }
        | watch::WatchEvent::FrustrationSignal { session_id, .. }
        | watch::WatchEvent::CompactionLoop { session_id, .. }
        | watch::WatchEvent::Diagnosis { session_id, .. }
        | watch::WatchEvent::CacheWarning { session_id, .. }
        | watch::WatchEvent::RequestError { session_id, .. }
        | watch::WatchEvent::GuardFinding { session_id, .. }
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

fn query_redact_enabled(params: &std::collections::HashMap<String, String>) -> bool {
    match params.get("redact").map(|value| value.as_str()) {
        Some("0" | "false" | "no" | "off") => false,
        Some("1" | "true" | "yes" | "on") => true,
        Some(_) | None => true,
    }
}

async fn handle_postmortem(
    axum::extract::Path(session_id): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    handle_postmortem_target(session_id, query_redact_enabled(&params)).await
}

async fn handle_postmortem_last(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    handle_postmortem_target("last".to_string(), query_redact_enabled(&params)).await
}

async fn handle_postmortem_target(target: String, redact: bool) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(db_path())
            .map_err(|err| PostmortemError::DbUnavailable(err.to_string()))?;
        build_postmortem_response_from_db(&conn, &target, redact)
    })
    .await;

    match result {
        Ok(Ok(report)) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            serde_json::to_string_pretty(&report).unwrap_or_default(),
        )
            .into_response(),
        Ok(Err(PostmortemError::NoSessions)) => {
            (StatusCode::NOT_FOUND, "no sessions found").into_response()
        }
        Ok(Err(PostmortemError::UnknownSession(session_id))) => (
            StatusCode::NOT_FOUND,
            format!("unknown session_id: {session_id}"),
        )
            .into_response(),
        Ok(Err(PostmortemError::DbUnavailable(err))) => {
            warn!(error = %err, "postmortem query failed");
            (StatusCode::SERVICE_UNAVAILABLE, "db unavailable").into_response()
        }
        Err(err) => {
            warn!(error = %err, "postmortem task failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
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
        build_recent_sessions_response_from_db(&conn, limit, days).ok()
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

async fn handle_resolve_jsonl_session(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let jsonl_session_id = params
        .get("jsonl_session_id")
        .or_else(|| params.get("session_id"))
        .cloned()
        .unwrap_or_default();

    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(db_path()).map_err(|err| err.to_string())?;
        let _ = repair_persisted_session_artifacts(&conn);
        build_jsonl_session_resolution_response_from_db(&conn, &jsonl_session_id)
    })
    .await;

    let (status, body) = match result {
        Ok(Ok(body)) => (StatusCode::OK, body),
        Ok(Err(err)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "status": "error",
                "matched": false,
                "error": err,
            }),
        ),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({
                "status": "error",
                "matched": false,
                "error": err.to_string(),
            }),
        ),
    };

    (
        status,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&body).unwrap_or_default(),
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

fn build_cache_rebuilds_response_from_db(
    conn: &Connection,
    days: u64,
    now_epoch: u64,
) -> Option<Value> {
    let _ = repair_persisted_session_artifacts(conn);
    let since_secs = now_epoch.saturating_sub(days * 86400);
    let since = epoch_to_iso8601(since_secs);

    // Find turns where cache was fully rebuilt (creation > 0, read == 0).
    let mut stmt = conn
        .prepare(
            "SELECT turn_number, cache_creation_tokens, cache_read_tokens, gap_from_prev_secs, \
         input_tokens, COALESCE(cache_ttl_min_secs, 300), COALESCE(cache_ttl_max_secs, 300) \
         FROM turn_snapshots WHERE timestamp >= ?1",
        )
        .ok()?;

    let mut total_rebuilds: u64 = 0;
    let mut idle_gap_rebuilds: u64 = 0;
    let mut rebuilds_without_idle_gap: u64 = 0;
    let mut partial_ttl_gap_rebuilds: u64 = 0;
    let mut cold_start_builds: u64 = 0;
    let mut tokens_wasted: u64 = 0;
    let mut total_tokens: u64 = 0;
    let mut longest_gap: f64 = 0.0;

    let rows = stmt
        .query_map(rusqlite::params![since], |row| {
            Ok((
                row.get::<_, i64>(0)?,                               // turn_number
                row.get::<_, i64>(1)?,                               // cache_creation_tokens
                row.get::<_, i64>(2)?,                               // cache_read_tokens
                row.get::<_, f64>(3)?,                               // gap_from_prev_secs
                row.get::<_, i64>(4)?,                               // input_tokens
                row.get::<_, Option<i64>>(5)?.unwrap_or(300).max(1), // cache_ttl_min_secs
                row.get::<_, Option<i64>>(6)?.unwrap_or(300).max(1), // cache_ttl_max_secs
            ))
        })
        .ok()?;

    for row in rows.flatten() {
        let (turn_number, cache_create, cache_read, gap, input, ttl_min, ttl_max_raw) = row;
        let ttl_max = ttl_max_raw.max(ttl_min);
        total_tokens += (input + cache_read + cache_create).max(0) as u64;

        if cache_create > 0 && cache_read == 0 {
            if turn_number <= 1 {
                cold_start_builds += 1;
                continue;
            }
            total_rebuilds += 1;
            tokens_wasted += cache_create.max(0) as u64;
            if gap > ttl_max as f64 {
                idle_gap_rebuilds += 1;
                if gap > longest_gap {
                    longest_gap = gap;
                }
            } else if ttl_max > ttl_min && gap > ttl_min as f64 {
                partial_ttl_gap_rebuilds += 1;
                rebuilds_without_idle_gap += 1;
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
        "rebuilds_from_inferred_idle_gaps": idle_gap_rebuilds,
        "mixed_ttl_partial_gap_rebuilds": partial_ttl_gap_rebuilds,
        "rebuilds_without_idle_gap": rebuilds_without_idle_gap,
        "tokens_wasted_on_rebuilds": tokens_wasted,
        "tokens_wasted_ratio": (wasted_ratio * 1000.0).round() / 1000.0,
        "longest_gap_before_rebuild_secs": longest_gap.round() as u64,
        "miss_cause_caveat": "Cache miss causes are inferred from local TTL and token evidence; Anthropic does not expose provider-confirmed miss causes.",
    }))
}

async fn handle_cache_rebuilds(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let days: u64 = params.get("days").and_then(|d| d.parse().ok()).unwrap_or(7);

    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::open(db_path()).ok()?;
        build_cache_rebuilds_response_from_db(&conn, days, now_epoch_secs())
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
             context_window_tokens, tool_failures, requested_model, actual_model, \
             COALESCE(cache_ttl_min_secs, 300), COALESCE(cache_ttl_max_secs, 300), \
             COALESCE(cache_ttl_source, 'default_5m'), response_error_type, response_error_message \
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
        cache_ttl_min_secs: i64,
        cache_ttl_max_secs: i64,
        cache_ttl_source: String,
        response_error_type: Option<String>,
        response_error_message: Option<String>,
    }

    let turns: Vec<TurnRow> = stmt
        .query_map(rusqlite::params![session_id], |row| {
            let input = row.get::<_, i64>(1)?.max(0);
            let cache_read = row.get::<_, i64>(2)?.max(0);
            let cache_create = row.get::<_, i64>(3)?.max(0);
            let requested_model = row.get::<_, Option<String>>(10)?;
            let actual_model = row.get::<_, Option<String>>(11)?;
            let cache_ttl_min_secs = row.get::<_, Option<i64>>(12)?.unwrap_or(300).max(1);
            let cache_ttl_max_secs = row
                .get::<_, Option<i64>>(13)?
                .unwrap_or(cache_ttl_min_secs)
                .max(cache_ttl_min_secs);
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
                cache_ttl_min_secs,
                cache_ttl_max_secs,
                cache_ttl_source: row
                    .get::<_, Option<String>>(14)?
                    .filter(|source| !source.trim().is_empty())
                    .unwrap_or_else(|| "default_5m".to_string()),
                response_error_type: row
                    .get::<_, Option<String>>(15)?
                    .filter(|value| !value.trim().is_empty()),
                response_error_message: row
                    .get::<_, Option<String>>(16)?
                    .filter(|value| !value.trim().is_empty()),
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
                } else if t.gap > t.cache_ttl_max_secs as f64 {
                    flags.push("cache_miss_ttl");
                } else if t.cache_ttl_max_secs > t.cache_ttl_min_secs
                    && t.gap > t.cache_ttl_min_secs as f64
                {
                    flags.push("cache_miss_partial_ttl");
                } else {
                    flags.push("cache_miss_inferred_rebuild");
                }
            }
            if t.response_error_type.is_some() {
                flags.push("api_error");
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
                "cache_ttl_min_secs": t.cache_ttl_min_secs,
                "cache_ttl_max_secs": t.cache_ttl_max_secs,
                "cache_ttl_source": t.cache_ttl_source,
                "output_tokens": t.output,
                "turn_duration_ms": t.ttft_ms,
                "gap_from_prev_secs": t.gap,
                "context_utilization": (t.ctx * 1000.0).round() / 1000.0,
                "context_window_tokens": t.context_window_tokens,
                "tool_failures": t.failures,
                "requested_model": t.requested_model.clone(),
                "actual_model": t.actual_model.clone(),
                "response_error_type": t.response_error_type.clone(),
                "response_error_message": t.response_error_message.clone(),
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
        .route("/api/guard/policy", get(handle_guard_policy))
        .route("/api/guard/status", get(handle_guard_status))
        .route("/api/recall", get(handle_recall))
        .route(
            "/api/billing-reconciliations",
            post(handle_billing_reconciliations),
        )
        .route("/api/diagnosis/:session_id", get(handle_diagnosis))
        .route("/api/postmortem/last", get(handle_postmortem_last))
        .route("/api/postmortem/:session_id", get(handle_postmortem))
        .route("/api/degradation/:session_id", get(handle_degradation))
        .route("/api/cache-rebuilds", get(handle_cache_rebuilds))
        .route("/api/sessions", get(handle_sessions))
        .route(
            "/api/sessions/resolve-jsonl",
            get(handle_resolve_jsonl_session),
        )
        .route("/api/sessions/finalize", post(handle_finalize_sessions))
        .route("/watch", get(handle_watch));

    let addr =
        std::env::var("CC_BLACKBOX_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|err| panic!("failed to bind HTTP server at {addr}: {err}"));
    let bound_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| addr.clone());
    info!("HTTP server listening on {bound_addr} (/health, /metrics, /api/hooks/claude-code, /api/summary, /api/recall, /api/billing-reconciliations, /api/sessions, /api/postmortem, /api/cache-rebuilds, /api/degradation, /watch)");
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
/// gauges stay fresh. The terminal watch renders quota once on connection
/// instead of receiving recurring quota broadcasts. Anthropic does not return
/// rate-limit headers on subscription (OAuth) traffic, so this is computed
/// entirely from our own counters. If the user sets
/// `CC_BLACKBOX_WEEKLY_TOKEN_BUDGET` we can also surface remaining + projected
/// exhaustion; otherwise we auto-suggest a weekly cap from the last four
/// completed weeks of SQLite history.
async fn quota_burn_monitor() {
    loop {
        tokio::time::sleep(QUOTA_SAMPLE_INTERVAL).await;

        // Snapshot the cumulative token counter.
        let total_tokens = {
            let runtime = lock_or_recover(&RUNTIME_STATE, "runtime_state");
            runtime.total_tokens
        };

        let per_sec = {
            let mut t = lock_or_recover(&BURN_TRACKER, "burn_tracker");
            t.record(total_tokens);
            t.tokens_per_sec()
        };
        let snapshot = quota_burn_snapshot(per_sec);

        metrics::update_weekly_budget_gauges(
            snapshot.tokens_used_this_week,
            snapshot.tokens_remaining,
            snapshot.tokens_limit,
            snapshot.budget_source.as_deref(),
            snapshot.projected_exhaustion_secs,
        );
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
                "DELETE FROM guard_findings WHERE timestamp < ?1",
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
// Per-session monitors: idle postmortem, inactivity timeout, and cache expiry warning.
// Both use periodic scanning of diagnosis::SESSIONS instead of a global Notify.
// ---------------------------------------------------------------------------
async fn cache_expiry_warning_monitor() {
    let warning_secs = env_u64("CC_BLACKBOX_CACHE_WARNING_SECS", 240);
    let check_interval = Duration::from_secs(30);

    loop {
        tokio::time::sleep(check_interval).await;
        let now = Instant::now();

        for mut entry in diagnosis::SESSIONS.iter_mut() {
            if !entry.session_inserted || entry.cache_warning_sent {
                continue;
            }
            let idle = now.duration_since(entry.last_activity).as_secs();
            let ttl_secs = CACHE_TRACKERS
                .get(&entry.session_id)
                .map(|tracker| tracker.last_ttl_min_secs)
                .unwrap_or(CACHE_TTL_SECS);
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

fn in_memory_postmortem_totals(session_id: &str) -> Option<(u64, u32)> {
    let turns = diagnosis::SESSION_TURNS.get(session_id)?;
    if turns.is_empty() {
        return None;
    }

    let total_tokens = turns
        .iter()
        .map(|turn| {
            turn.input_tokens as u64
                + turn.cache_read_tokens as u64
                + turn.cache_creation_tokens as u64
                + turn.output_tokens as u64
        })
        .sum::<u64>();
    Some((total_tokens, turns.len() as u32))
}

async fn idle_postmortem_monitor() {
    let check_interval = Duration::from_secs(30);

    loop {
        tokio::time::sleep(check_interval).await;

        let threshold_secs = postmortem_idle_secs();
        if threshold_secs == 0 {
            continue;
        }

        let now = Instant::now();
        let mut ready = Vec::new();

        for mut entry in diagnosis::SESSIONS.iter_mut() {
            if !entry.session_inserted || entry.idle_postmortem_sent {
                continue;
            }

            let idle_secs = now.duration_since(entry.last_activity).as_secs();
            if idle_secs < threshold_secs {
                continue;
            }

            let Some((total_tokens, total_turns)) = in_memory_postmortem_totals(&entry.session_id)
            else {
                continue;
            };
            entry.idle_postmortem_sent = true;
            ready.push((
                entry.session_id.clone(),
                idle_secs,
                total_tokens,
                total_turns,
            ));
        }

        for (session_id, idle_secs, total_tokens, total_turns) in ready {
            watch::BROADCASTER.broadcast(watch::WatchEvent::PostmortemReady {
                session_id: session_id.clone(),
                idle_secs,
                total_tokens,
                total_turns,
            });
            info!(
                session_id = %session_id,
                idle_secs,
                total_turns,
                "idle postmortem ready"
            );
        }
    }
}

async fn session_inactivity_monitor() {
    let timeout_mins = env_u64("CC_BLACKBOX_SESSION_TIMEOUT_MINUTES", 60);
    let timeout_secs = timeout_mins * 60;
    let check_interval = Duration::from_secs(30);

    loop {
        tokio::time::sleep(check_interval).await;
        let now = Instant::now();

        // Collect expired session hashes (can't remove while iterating).
        let mut expired: Vec<u64> = Vec::new();
        for entry in diagnosis::SESSIONS.iter() {
            let idle = now.duration_since(entry.last_activity).as_secs();
            if idle > timeout_secs {
                expired.push(*entry.key());
            }
        }

        for hash in expired {
            let Some(state) = session_state_if_still_expired(hash, timeout_secs, Instant::now())
            else {
                continue;
            };

            info!(session_id = %state.session_id, timeout_mins, "session ended (inactivity timeout)");
            let _ = end_session(
                &state.session_id,
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

    info!("cc-blackbox-core v{}", env!("CARGO_PKG_VERSION"));
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
    tokio::spawn(idle_postmortem_monitor());
    tokio::spawn(session_inactivity_monitor());
    tokio::spawn(cache_expiry_warning_monitor());
    tokio::spawn(data_retention_cleanup());
    tokio::spawn(quota_burn_monitor());
    tokio::spawn(historical_metrics_monitor());

    let addr = std::env::var("CC_BLACKBOX_GRPC_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()?;
    info!(%addr, "gRPC ext_proc server starting");

    Server::builder()
        .add_service(ExternalProcessorServer::new(CcBlackboxProcessor))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{mpsc as std_mpsc, LazyLock, Mutex};
    use std::time::Duration;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;
    use serde_json::Value;

    use super::{
        build_cache_rebuilds_response_from_db, build_diagnosis_response_json,
        build_guard_policy_report_json, build_guard_status_report_json,
        build_postmortem_response_from_db, build_recent_sessions_response_from_db,
        build_session_summary_json, build_sessions_response_json, build_summary_response_json,
        canonical_telemetry_name, classify_cache_event, clean_user_prompt,
        compact_response_summary, content_metadata_for_storage, context_fill_percent,
        context_fill_ratio, correlation, db_writer_loop, derive_display_name, diagnosis,
        end_session_with_db_tx, ensure_session_columns, epoch_to_iso8601,
        estimated_rebuild_cost_for_cache_event, evaluate_request_guard_for_session,
        extract_explicit_skill_refs, extract_header, extract_headers, extract_working_dir,
        fallback_request_id, guard, guard_finding_watch_event, in_memory_postmortem_totals,
        infer_context_window_tokens, is_internal_request_shape, load_degradation_view_from_db,
        lock_or_recover, looks_like_machine_recall_line, looks_like_title_request,
        make_guard_block_response, metrics, model_requests_1m_context, normalize_search_text,
        now_epoch_secs, now_iso8601, parse_latest_tool_results, parse_request_body,
        persist_billing_reconciliation, persist_final_session_artifacts, pricing,
        query_historical_metrics, query_redact_enabled, query_summary,
        record_api_response_for_guard, redact_operational_text, repair_persisted_session_artifacts,
        repair_turn_snapshot_context_windows, request_cache_ttl_evidence, request_cache_ttl_secs,
        request_context_window_hint_from_headers, request_guard_policy, request_uses_1m_context,
        resolve_context_window_tokens, resolve_context_window_tokens_with_config,
        response_cache_ttl_secs, score_recall_doc, seed_live_metric_labels_from_db,
        session_state_if_still_expired, session_timeout_secs, skill_name_from_skill_file,
        skill_name_from_tool_input_json, strip_model_1m_alias, summarize_hook_tool_input,
        tokenize_search_text, tool_recall_context, truncate_detail,
        upstream_request_adjustment_for_body, BillingReconciliationInput,
        BillingReconciliationWriteError, CacheTracker, CacheTtlEvidence, DbCommand,
        ExtProcResponse, HttpHeaders, ParsedToolResult, PersistedDiagnosis, PersistedRecall,
        PostmortemError, ProtoHeaderValue, ResponseAccumulator, SessionBudgetState,
        SummaryWindowData, ESTIMATED_COST_SOURCE, EXTENDED_CONTEXT_WINDOW_TOKENS, SCHEMA,
        SESSION_BUDGETS, STANDARD_CONTEXT_WINDOW_TOKENS,
    };

    static METRICS_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    static ENV_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn reset_runtime_state_for_test() {
        let mut runtime = lock_or_recover(&super::RUNTIME_STATE, "runtime_state_test");
        *runtime = super::RuntimeState::new();
    }

    fn unique_temp_path(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("cc-blackbox-{label}-{nanos}.toml"))
    }

    fn create_history_test_db() -> Connection {
        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                last_activity_at TEXT,
                ended_at TEXT,
                model TEXT,
                working_dir TEXT,
                first_message_hash TEXT,
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
                cache_creation_5m_tokens INTEGER DEFAULT 0,
                cache_creation_1h_tokens INTEGER DEFAULT 0,
                cache_ttl_min_secs INTEGER DEFAULT 300,
                cache_ttl_max_secs INTEGER DEFAULT 300,
                cache_ttl_source TEXT DEFAULT 'default_5m',
                outcome TEXT DEFAULT 'success',
                error_type TEXT,
                error_message TEXT,
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
                cache_ttl_min_secs INTEGER DEFAULT 300,
                cache_ttl_max_secs INTEGER DEFAULT 300,
                cache_ttl_source TEXT DEFAULT 'default_5m',
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
                response_summary TEXT,
                response_error_type TEXT,
                response_error_message TEXT
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

    fn metric_scalar_line<'a>(body: &'a str, metric: &str) -> Option<&'a str> {
        body.lines()
            .find(|line| line.starts_with(metric) && line[metric.len()..].starts_with(' '))
    }

    #[test]
    fn epoch_and_week_formatting_are_utc_stable() {
        assert_eq!(epoch_to_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(epoch_to_iso8601(1_582_934_400), "2020-02-29T00:00:00Z");
        assert_eq!(super::start_of_week_iso_at(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            super::start_of_week_iso_at(1_619_827_200),
            "2021-04-26T00:00:00Z"
        );
    }

    #[test]
    fn request_body_parser_extracts_session_identity_inputs() {
        let body = br#"{
          "model": "claude-sonnet-4-5[1m]",
          "system": [
            { "type": "text", "text": "irrelevant" },
            { "type": "text", "text": "- Primary working directory: /tmp/cc-blackbox-demo" }
          ],
          "tools": [{ "name": "Read" }],
          "messages": [
            {
              "role": "user",
              "content": [
                { "type": "text", "text": "<system-reminder>noise</system-reminder>" },
                { "type": "text", "text": "Please inspect the auth cache." }
              ]
            },
            { "role": "assistant", "content": "ok" }
          ]
        }"#;

        let parsed = parse_request_body(body).expect("parse body");

        assert_eq!(parsed.model, "claude-sonnet-4-5[1m]");
        assert_eq!(parsed.message_count, 2);
        assert!(parsed.has_tools);
        assert!(parsed.system_prompt_length > 0);
        assert_eq!(parsed.estimated_input_tokens, body.len() / 4);
        assert_ne!(parsed.sys_prompt_hash, 0);
        assert_eq!(parsed.working_dir, "/tmp/cc-blackbox-demo");
        assert_eq!(parsed.user_prompt_excerpt, "Please inspect the auth cache.");
        assert!(!parsed.is_internal_request);
        assert_eq!(
            extract_working_dir("Primary working directory: /tmp/demo").as_deref(),
            Some("/tmp/demo")
        );
    }

    #[test]
    fn request_body_parser_keeps_haiku_coding_sessions_but_excludes_title_requests() {
        let coding = br#"{
          "model": "claude-haiku-4-5-20251001",
          "system": "Primary working directory: /tmp/cc-blackbox-haiku",
          "tools": [{ "name": "Read" }],
          "messages": [
            { "role": "user", "content": "Use Haiku to inspect the cache module." }
          ]
        }"#;
        let parsed = parse_request_body(coding).expect("parse coding request");
        assert_eq!(parsed.model, "claude-haiku-4-5-20251001");
        assert_eq!(parsed.working_dir, "/tmp/cc-blackbox-haiku");
        assert!(!parsed.is_internal_request);

        let title = br#"{
          "model": "claude-haiku-4-5-20251001",
          "system": "",
          "messages": [
            { "role": "user", "content": "Generate a short title for this conversation." }
          ]
        }"#;
        let parsed = parse_request_body(title).expect("parse title request");
        assert!(parsed.is_internal_request);
        assert!(is_internal_request_shape(
            "",
            0,
            1,
            false,
            &parsed.user_prompt_excerpt
        ));

        assert!(!looks_like_title_request(
            "Use Haiku to generate code for this session",
            1,
            true
        ));
    }

    #[test]
    fn request_cache_ttl_tracks_5m_and_1h_cache_controls() {
        let one_hour = serde_json::json!({
            "system": [
                {
                    "type": "text",
                    "text": "Primary working directory: /tmp/cache",
                    "cache_control": {"type": "ephemeral", "ttl": "1h"}
                }
            ],
            "messages": []
        });
        assert_eq!(request_cache_ttl_secs(&one_hour), Some(3600));
        assert_eq!(
            request_cache_ttl_evidence(&one_hour),
            Some(CacheTtlEvidence {
                min_secs: 3600,
                max_secs: 3600,
                source: "request_cache_control",
            })
        );

        let mixed = serde_json::json!({
            "system": [
                {
                    "type": "text",
                    "text": "Primary working directory: /tmp/cache",
                    "cache_control": {"type": "ephemeral", "ttl": "1h"}
                }
            ],
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "short-lived",
                            "cache_control": {"type": "ephemeral", "ttl": "5m"}
                        }
                    ]
                }
            ]
        });
        assert_eq!(request_cache_ttl_secs(&mixed), Some(300));
        assert_eq!(
            request_cache_ttl_evidence(&mixed),
            Some(CacheTtlEvidence {
                min_secs: 300,
                max_secs: 3600,
                source: "request_cache_control",
            })
        );
    }

    #[test]
    fn response_accumulator_parses_streaming_sse_usage_model_text_and_tools() {
        let mut acc = ResponseAccumulator::new();
        let events = vec![
            serde_json::json!({
                "type": "message_start",
                "message": {
                    "model": "claude-sonnet-4-6-20260217",
                    "usage": {
                        "input_tokens": 1200,
                        "cache_read_input_tokens": 80,
                        "cache_creation_input_tokens": 420,
                        "cache_creation": {
                            "ephemeral_5m_input_tokens": 320,
                            "ephemeral_1h_input_tokens": 100
                        }
                    }
                }
            }),
            serde_json::json!({
                "type": "content_block_start",
                "content_block": {"type": "tool_use", "name": "Read", "input": {}}
            }),
            serde_json::json!({
                "type": "content_block_delta",
                "delta": {"type": "input_json_delta", "partial_json": "{\"file_path\":\"src/lib.rs\"}"}
            }),
            serde_json::json!({"type": "content_block_stop"}),
            serde_json::json!({
                "type": "content_block_start",
                "content_block": {"type": "text", "text": ""}
            }),
            serde_json::json!({
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": "héllo"}
            }),
            serde_json::json!({"type": "content_block_stop"}),
            serde_json::json!({
                "type": "message_delta",
                "usage": {"output_tokens": 90}
            }),
        ];
        let sse = events
            .into_iter()
            .map(|event| format!("data: {}\n\n", serde_json::to_string(&event).unwrap()))
            .collect::<String>();
        let bytes = sse.as_bytes();
        let split = bytes
            .iter()
            .position(|byte| *byte == 0xc3)
            .expect("contains multibyte char")
            + 1;

        acc.process_chunk(&bytes[..split]);
        acc.process_chunk(&bytes[split..]);

        assert_eq!(
            acc.response_model.as_deref(),
            Some("claude-sonnet-4-6-20260217")
        );
        assert_eq!(acc.input_tokens, 1200);
        assert_eq!(acc.output_tokens, 90);
        assert_eq!(acc.cache_read_tokens, 80);
        assert_eq!(acc.cache_creation_tokens, 420);
        assert_eq!(acc.cache_creation_5m_tokens, 320);
        assert_eq!(acc.cache_creation_1h_tokens, 100);
        assert_eq!(acc.tool_calls, vec!["Read"]);
        assert_eq!(acc.response_text, "héllo");
        assert!(acc.deferred_watch_events.iter().any(|event| {
            matches!(
                event,
                crate::watch::WatchEvent::ToolUse { tool_name, summary, .. }
                    if tool_name == "Read" && summary == "src/lib.rs"
            )
        }));
    }

    #[test]
    fn response_accumulator_parses_non_streaming_json_like_sse_contract() {
        let mut acc = ResponseAccumulator::new();
        acc.json_body = serde_json::to_vec(&serde_json::json!({
            "id": "msg_fake",
            "type": "message",
            "role": "assistant",
            "model": "claude-haiku-4-5-20251001",
            "content": [
                {"type": "tool_use", "id": "toolu_1", "name": "Skill", "input": {"skill_name": "qa"}},
                {"type": "text", "text": "Done without streaming."}
            ],
            "usage": {
                "input_tokens": 100,
                "cache_read_input_tokens": 20,
                "cache_creation_input_tokens": 30,
                "cache_creation": {
                    "ephemeral_5m_input_tokens": 0,
                    "ephemeral_1h_input_tokens": 30
                },
                "output_tokens": 40
            }
        }))
        .expect("json body");

        acc.process_json_body();

        assert_eq!(
            acc.response_model.as_deref(),
            Some("claude-haiku-4-5-20251001")
        );
        assert_eq!(acc.input_tokens, 100);
        assert_eq!(acc.output_tokens, 40);
        assert_eq!(acc.cache_read_tokens, 20);
        assert_eq!(acc.cache_creation_tokens, 30);
        assert_eq!(acc.cache_creation_5m_tokens, 0);
        assert_eq!(acc.cache_creation_1h_tokens, 30);
        assert_eq!(acc.tool_calls, vec!["Skill"]);
        assert_eq!(acc.response_text, "Done without streaming.");
        assert_eq!(response_cache_ttl_secs(&acc, Some(300)), 3600);
        assert!(acc.deferred_watch_events.iter().any(|event| {
            matches!(
                event,
                crate::watch::WatchEvent::SkillEvent { skill_name, .. } if skill_name == "qa"
            )
        }));
    }

    #[test]
    fn cache_hit_rebuild_estimate_uses_one_hour_ttl_evidence() {
        let mut acc = ResponseAccumulator::new();
        acc.input_tokens = 1_000_000;
        acc.cache_read_tokens = 1_000_000;
        let five_minute = CacheTtlEvidence::default_5m();
        let one_hour = CacheTtlEvidence {
            min_secs: 3600,
            max_secs: 3600,
            source: "request_cache_control",
        };

        let estimate_5m =
            estimated_rebuild_cost_for_cache_event(&acc, "claude-sonnet-4-6", &five_minute);
        let estimate_1h =
            estimated_rebuild_cost_for_cache_event(&acc, "claude-sonnet-4-6", &one_hour);

        assert!((estimate_5m - 7.5).abs() < 1e-9);
        assert!((estimate_1h - 12.0).abs() < 1e-9);
    }

    #[test]
    fn response_accumulator_records_streaming_error_events() {
        let mut acc = ResponseAccumulator::new();
        let sse = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"server overloaded\"}}\n\n";

        acc.process_chunk(sse.as_bytes());

        assert_eq!(acc.stream_error_type.as_deref(), Some("overloaded_error"));
        assert_eq!(
            acc.stream_error_message.as_deref(),
            Some("server overloaded")
        );
        assert!(acc.has_response_error());
    }

    #[test]
    fn cache_classification_uses_ttl_and_thresholded_thrash() {
        let now = Instant::now();

        let mut tracker = CacheTracker::new();
        assert_eq!(
            classify_cache_event(&mut tracker, now, 0, 100, &CacheTtlEvidence::default_5m()),
            "cold_start"
        );
        assert_eq!(
            classify_cache_event(
                &mut tracker,
                now + Duration::from_secs(20),
                80,
                0,
                &CacheTtlEvidence::default_5m()
            ),
            "hit"
        );

        let mut tracker = CacheTracker::new();
        assert_eq!(
            classify_cache_event(&mut tracker, now, 0, 100, &CacheTtlEvidence::default_5m()),
            "cold_start"
        );
        assert_eq!(
            classify_cache_event(
                &mut tracker,
                now + Duration::from_secs(10),
                0,
                100,
                &CacheTtlEvidence::default_5m()
            ),
            "miss_rebuild"
        );
        assert_eq!(
            classify_cache_event(
                &mut tracker,
                now + Duration::from_secs(20),
                0,
                100,
                &CacheTtlEvidence::default_5m()
            ),
            "miss_rebuild"
        );
        assert_eq!(
            classify_cache_event(
                &mut tracker,
                now + Duration::from_secs(30),
                0,
                100,
                &CacheTtlEvidence::default_5m()
            ),
            "miss_thrash"
        );

        let mut tracker = CacheTracker::new();
        assert_eq!(
            classify_cache_event(&mut tracker, now, 0, 100, &CacheTtlEvidence::default_5m()),
            "cold_start"
        );
        assert_eq!(
            classify_cache_event(
                &mut tracker,
                now + Duration::from_secs(301),
                0,
                100,
                &CacheTtlEvidence::default_5m()
            ),
            "miss_ttl"
        );

        let one_hour = CacheTtlEvidence {
            min_secs: 3600,
            max_secs: 3600,
            source: "request_cache_control",
        };
        let mut tracker = CacheTracker::new();
        assert_eq!(
            classify_cache_event(&mut tracker, now, 0, 100, &one_hour),
            "cold_start"
        );
        assert_eq!(
            classify_cache_event(
                &mut tracker,
                now + Duration::from_secs(600),
                0,
                100,
                &one_hour
            ),
            "miss_rebuild"
        );
    }

    #[test]
    fn upstream_adjustment_strips_1m_model_alias_without_adding_retired_beta_header() {
        let body = br#"{
          "model": "claude-opus-4-7[1m]",
          "max_tokens": 10,
          "messages": [{ "role": "user", "content": "hello" }]
        }"#;

        let adjustment = upstream_request_adjustment_for_body(&[], body);
        let rewritten: serde_json::Value =
            serde_json::from_slice(&adjustment.body.expect("rewritten body"))
                .expect("valid rewritten json");

        assert_eq!(rewritten["model"], "claude-opus-4-7");
        assert_eq!(adjustment.anthropic_beta, None);
        assert!(!adjustment.remove_anthropic_beta);
        assert_eq!(
            strip_model_1m_alias("claude-opus-4-7[1M]"),
            Some("claude-opus-4-7")
        );
    }

    #[test]
    fn upstream_body_rewrite_sets_matching_content_length() {
        let body = br#"{"model":"claude-opus-4-7[1m]","messages":[]}"#;
        let adjustment = upstream_request_adjustment_for_body(&[], body);
        let rewritten_len = adjustment.body.as_ref().expect("rewritten body").len();

        let response = super::body_continue_with_upstream_adjustment(adjustment);
        let common = response.response.expect("common response");
        assert!(common.body_mutation.is_some());

        let content_length = common
            .header_mutation
            .and_then(|mutation| {
                mutation.set_headers.into_iter().find_map(|header| {
                    header
                        .header
                        .filter(|header| header.key.eq_ignore_ascii_case("content-length"))
                        .map(|header| header.value)
                })
            })
            .expect("content-length header");
        assert_eq!(content_length, rewritten_len.to_string());
    }

    #[test]
    fn upstream_adjustment_removes_retired_context_beta_when_normalizing_alias() {
        let body = br#"{"model":"claude-sonnet-4-6[1m]","messages":[]}"#;
        let existing = vec!["prompt-tools-2025-04-02".to_string()];

        let adjustment = upstream_request_adjustment_for_body(&existing, body);

        assert_eq!(adjustment.anthropic_beta, None);
        assert!(!adjustment.remove_anthropic_beta);
        assert!(adjustment.body.is_some());

        let already_has_context =
            vec!["prompt-tools-2025-04-02, context-1m-2025-08-07".to_string()];
        let adjustment = upstream_request_adjustment_for_body(&already_has_context, body);

        assert_eq!(
            adjustment.anthropic_beta.as_deref(),
            Some("prompt-tools-2025-04-02")
        );
        assert!(adjustment.remove_anthropic_beta);
        assert!(adjustment.body.is_some());
    }

    #[test]
    fn upstream_adjustment_leaves_normal_models_untouched() {
        let body = br#"{"model":"claude-sonnet-4-6","messages":[]}"#;
        let adjustment = upstream_request_adjustment_for_body(&[], body);

        assert_eq!(adjustment, super::UpstreamRequestAdjustment::default());
    }

    #[test]
    fn upstream_adjustment_leaves_clean_beta_header_untouched() {
        let body = br#"{"model":"claude-sonnet-4-6","messages":[]}"#;
        let adjustment = upstream_request_adjustment_for_body(
            &["fine-grained-tool-streaming-2025-05-14".into()],
            body,
        );

        assert_eq!(adjustment, super::UpstreamRequestAdjustment::default());
    }

    #[test]
    fn upstream_adjustment_removes_retired_context_beta_without_model_alias() {
        let body = br#"{"model":"claude-sonnet-4-6","messages":[]}"#;
        let existing =
            vec!["prompt-tools-2025-04-02, context-1m-2025-08-07, fine-grained-tool-streaming-2025-05-14".to_string()];

        let adjustment = upstream_request_adjustment_for_body(&existing, body);

        assert_eq!(
            adjustment.anthropic_beta.as_deref(),
            Some("prompt-tools-2025-04-02, fine-grained-tool-streaming-2025-05-14")
        );
        assert!(adjustment.remove_anthropic_beta);
        assert!(adjustment.body.is_none());
    }

    #[test]
    fn upstream_adjustment_removes_empty_anthropic_beta_header() {
        let body = br#"{"model":"claude-opus-4-7","messages":[]}"#;

        let adjustment = upstream_request_adjustment_for_body(&[String::new()], body);

        assert_eq!(adjustment.anthropic_beta, None);
        assert!(adjustment.remove_anthropic_beta);
        assert!(adjustment.body.is_none());

        let response = super::body_continue_with_upstream_adjustment(adjustment);
        let mutation = response
            .response
            .expect("common response")
            .header_mutation
            .expect("header mutation");
        assert_eq!(mutation.remove_headers, vec!["anthropic-beta"]);
        assert!(mutation.set_headers.is_empty());
    }

    #[test]
    fn upstream_adjustment_preserves_valid_beta_when_removing_empty_entries() {
        let body = br#"{"model":"claude-opus-4-7","messages":[]}"#;
        let existing = vec!["prompt-tools-2025-04-02, ".to_string()];

        let adjustment = upstream_request_adjustment_for_body(&existing, body);

        assert_eq!(
            adjustment.anthropic_beta.as_deref(),
            Some("prompt-tools-2025-04-02")
        );
        assert!(adjustment.remove_anthropic_beta);
        assert!(adjustment.body.is_none());

        let response = super::body_continue_with_upstream_adjustment(adjustment);
        let mutation = response
            .response
            .expect("common response")
            .header_mutation
            .expect("header mutation");
        assert_eq!(mutation.remove_headers, vec!["anthropic-beta"]);
        let header = mutation.set_headers[0]
            .header
            .as_ref()
            .expect("anthropic beta header");
        assert_eq!(header.key, "anthropic-beta");
        assert_eq!(header.value, "prompt-tools-2025-04-02");
    }

    #[test]
    fn request_header_mutation_strips_accept_encoding_without_touching_beta() {
        let headers = super::request_header_continue_with_sanitized_headers(&[String::new()]);
        let mutation = headers
            .response
            .expect("common response")
            .header_mutation
            .expect("header mutation");

        assert!(mutation.set_headers.is_empty());
        assert_eq!(mutation.remove_headers, vec!["accept-encoding".to_string()]);
    }

    #[test]
    fn request_header_mutation_keeps_beta_for_lua_and_body_phase_rewrite() {
        let headers = super::request_header_continue_with_sanitized_headers(&[
            "prompt-tools-2025-04-02, ".into(),
        ]);
        let mutation = headers
            .response
            .expect("common response")
            .header_mutation
            .expect("header mutation");

        assert!(mutation.set_headers.is_empty());
        assert_eq!(mutation.remove_headers, vec!["accept-encoding".to_string()]);
    }

    #[test]
    fn clean_user_prompt_strips_preamble_and_truncates_long_text() {
        let cleaned = clean_user_prompt(
            r#"
            <system-reminder>Ignore this</system-reminder>
            <command-name>review</command-name>
            Please review the cache module.
            "#,
        );
        assert_eq!(cleaned, "Please review the cache module.");

        let long = clean_user_prompt(&"x".repeat(400));
        assert_eq!(long.chars().count(), 321);
        assert!(long.ends_with('\u{2026}'));
    }

    #[test]
    fn headers_and_context_helpers_handle_common_variants() {
        let mut headers = make_http_headers(&[
            ("Anthropic-Beta", "context-1m-2025-08-07, other"),
            ("X-Context-Window-Tokens", "123456"),
        ]);
        headers
            .headers
            .as_mut()
            .expect("headers")
            .headers
            .push(ProtoHeaderValue {
                key: "x-raw".to_string(),
                value: String::new(),
                raw_value: b"raw-value".to_vec(),
            });

        assert_eq!(
            extract_header(&headers, "anthropic-beta").as_deref(),
            Some("context-1m-2025-08-07, other")
        );
        assert_eq!(
            extract_header(&headers, "X-RAW").as_deref(),
            Some("raw-value")
        );
        assert_eq!(extract_headers(&headers, "missing"), Vec::<String>::new());
        assert!(!request_uses_1m_context(&headers));
        assert!(model_requests_1m_context("claude-sonnet[1m]"));
        assert_eq!(
            infer_context_window_tokens(Some("sonnet"), None, 200_001, 0, 0),
            EXTENDED_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            infer_context_window_tokens(Some("sonnet"), None, 100, 50, 25),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(context_fill_ratio(100, 50, 50, 200), 1.0);
        assert_eq!(context_fill_percent(100, 50, 50, 200), 100.0);
    }

    #[test]
    fn display_names_use_workdir_and_collision_suffix() {
        assert_eq!(
            derive_display_name("/Users/pradeep/code/cc-blackbox", "claude-sonnet", 0xabc),
            "cc-blackbox"
        );

        let hash = 0xabc_u64;
        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: "session_existing".to_string(),
                display_name: "cc-blackbox".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        let display = derive_display_name("/tmp/cc-blackbox", "claude-sonnet", hash);
        let _ = diagnosis::SESSIONS.remove(&hash);
        assert_eq!(display, "cc-blackbox-abc");
    }

    #[test]
    fn recall_text_helpers_score_human_content_not_machine_noise() {
        assert!(looks_like_machine_recall_line("```json"));
        assert!(looks_like_machine_recall_line(r#""tool_use": "Bash","#));
        assert!(!looks_like_machine_recall_line(
            "Fixed the auth cache warm path."
        ));

        let results = vec![
            ParsedToolResult {
                tool_name: "Read".to_string(),
                input_summary: "src/main.rs".to_string(),
                outcome: "success".to_string(),
                is_error: false,
                duration_ms: 12,
            },
            ParsedToolResult {
                tool_name: "Bash".to_string(),
                input_summary: "cargo test".to_string(),
                outcome: "error".to_string(),
                is_error: true,
                duration_ms: 34,
            },
        ];
        assert_eq!(
            tool_recall_context(&results).as_deref(),
            Some("Tools: Read src/main.rs; Bash cargo test (error)")
        );

        assert_eq!(normalize_search_text("Auth-cache!!"), "auth cache");
        let terms = tokenize_search_text("auth cache x");
        assert_eq!(terms, vec!["auth", "cache"]);
        let score = score_recall_doc(
            "auth cache",
            &terms,
            "Investigate auth cache",
            "Fixed cache warm path",
            "claude-sonnet",
        )
        .expect("score");
        assert!(score > 80);
        assert!(score_recall_doc("billing", &["billing".to_string()], "", "", "sonnet").is_none());
    }

    #[test]
    fn skill_discovery_helpers_accept_frontmatter_and_paths() {
        let dir = std::env::temp_dir().join(format!(
            "cc-blackbox-skill-test-{}-{}",
            std::process::id(),
            now_epoch_secs()
        ));
        let skill_dir = dir.join(".claude/skills/test-skill");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        let skill_file = skill_dir.join("SKILL.md");
        fs::write(
            &skill_file,
            "---\nname: Fancy Skill\n---\nUse this for fancy work.\n",
        )
        .expect("write skill");

        assert_eq!(
            skill_name_from_skill_file(&skill_file, &fs::read_to_string(&skill_file).unwrap())
                .as_deref(),
            Some("fancy-skill")
        );
        let expected = super::expected_skill_names_from_prompt(
            "Please use the fancy skill for this.",
            Some(dir.to_str().expect("utf8 temp dir")),
        );
        assert!(expected.contains("fancy-skill"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn hook_tool_input_summaries_cover_common_shapes() {
        assert_eq!(
            summarize_hook_tool_input(Some(&serde_json::json!({"command": "cargo test"})))
                .as_deref(),
            Some("cargo test")
        );
        assert_eq!(
            summarize_hook_tool_input(Some(&serde_json::json!({"file_path": "/tmp/demo.rs"})))
                .as_deref(),
            Some("/tmp/demo.rs")
        );
        assert_eq!(
            summarize_hook_tool_input(Some(&serde_json::json!({"query": "auth cache"}))).as_deref(),
            Some("auth cache")
        );
        assert!(
            summarize_hook_tool_input(Some(&serde_json::json!({"other": "value"})))
                .expect("json summary")
                .contains("\"other\"")
        );
        assert_eq!(summarize_hook_tool_input(None), None);
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
        assert!(columns.contains(&"last_activity_at".to_string()));
        assert!(!columns.contains(&"cache_hit_ratio".to_string()));
    }

    #[test]
    fn ensure_session_columns_migrates_legacy_activity_without_marking_active_final() {
        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                model TEXT
            );
            CREATE TABLE session_diagnoses (
                session_id TEXT PRIMARY KEY
            );
            CREATE TABLE session_recall (
                session_id TEXT PRIMARY KEY
            );
            INSERT INTO sessions (session_id, started_at, ended_at, model)
            VALUES
                ('session-active-legacy', '2026-01-01T00:00:00Z', '2026-01-01T00:05:00Z', 'claude-sonnet'),
                ('session-final-legacy', '2026-01-01T00:00:00Z', '2026-01-01T00:10:00Z', 'claude-sonnet');
            INSERT INTO session_diagnoses (session_id)
            VALUES ('session-active-legacy'), ('session-final-legacy');
            INSERT INTO session_recall (session_id) VALUES ('session-final-legacy');",
        )
        .expect("create legacy lifecycle schema");

        ensure_session_columns(&conn).expect("migrate sessions columns");

        let active: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT last_activity_at, ended_at FROM sessions WHERE session_id = 'session-active-legacy'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query active legacy row");
        assert_eq!(active.0.as_deref(), Some("2026-01-01T00:05:00Z"));
        assert_eq!(active.1, None);

        let final_row: (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT last_activity_at, ended_at FROM sessions WHERE session_id = 'session-final-legacy'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query final legacy row");
        assert_eq!(final_row.0.as_deref(), Some("2026-01-01T00:10:00Z"));
        assert_eq!(final_row.1.as_deref(), Some("2026-01-01T00:10:00Z"));
    }

    fn unique_test_db_path(label: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!(
            "cc-blackbox-{label}-{}-{nanos}.db",
            std::process::id()
        ));
        path.to_string_lossy().into_owned()
    }

    fn cleanup_test_db(path: &str) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{path}-wal"));
        let _ = fs::remove_file(format!("{path}-shm"));
    }

    fn sqlite_artifact_bytes(path: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        for artifact in [
            path.to_string(),
            format!("{path}-wal"),
            format!("{path}-shm"),
        ] {
            if let Ok(mut artifact_bytes) = fs::read(&artifact) {
                bytes.append(&mut artifact_bytes);
            }
        }
        bytes
    }

    fn bytes_contain(haystack: &[u8], needle: &str) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle.as_bytes())
    }

    fn insert_session(
        conn: &Connection,
        session_id: &str,
        started_at: &str,
        ended_at: Option<&str>,
        model: &str,
        initial_prompt: Option<&str>,
    ) {
        let last_activity_at = ended_at.unwrap_or(started_at);
        conn.execute(
            "INSERT INTO sessions \
             (session_id, started_at, last_activity_at, ended_at, model, working_dir, \
              first_message_hash, initial_prompt) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                session_id,
                started_at,
                last_activity_at,
                ended_at,
                model,
                "/tmp/cc-blackbox-test",
                "firstmsg_test",
                initial_prompt
            ],
        )
        .expect("insert session");
    }

    fn insert_live_session_for_finalization(
        db_path: &str,
        session_id: &str,
        hash: u64,
        prompt: &str,
    ) {
        {
            let conn = Connection::open(db_path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            insert_session(
                &conn,
                session_id,
                "2026-01-01T00:00:00Z",
                None,
                "claude-sonnet",
                Some(prompt),
            );
        }
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        let _ = diagnosis::SESSIONS.remove(&hash);
        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: session_id.to_string(),
                display_name: "finalization-test".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: Some(prompt.to_string()),
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        diagnosis::SESSION_TURNS.insert(
            session_id.to_string(),
            vec![test_live_turn(1, Some("Budgeted response."))],
        );
    }

    #[allow(clippy::too_many_arguments)]
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

    fn test_record_request_command(
        request_id: &str,
        session_id: &str,
        timestamp: &str,
    ) -> DbCommand {
        DbCommand::RecordRequest {
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            timestamp: timestamp.to_string(),
            model: "claude-sonnet".to_string(),
            input_tokens: 1_000,
            output_tokens: 100,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            cache_creation_5m_tokens: 0,
            cache_creation_1h_tokens: 0,
            cache_ttl_min_secs: 300,
            cache_ttl_max_secs: 300,
            cache_ttl_source: "default_5m".to_string(),
            outcome: "success".to_string(),
            error_type: None,
            error_message: None,
            cost_dollars: 0.01,
            cost_source: "pricing_file:test".to_string(),
            trusted_for_budget_enforcement: true,
            duration_ms: 10,
            tool_calls_json: "[]".to_string(),
            tool_calls_list: Vec::new(),
            cache_event: "none".to_string(),
        }
    }

    fn test_turn_snapshot_command(
        session_id: &str,
        turn_number: u32,
        timestamp: &str,
        response_summary: &str,
    ) -> DbCommand {
        DbCommand::WriteTurnSnapshot {
            session_id: session_id.to_string(),
            turn_number,
            timestamp: timestamp.to_string(),
            input_tokens: 1_000,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            cache_ttl_min_secs: 300,
            cache_ttl_max_secs: 300,
            cache_ttl_source: "default_5m".to_string(),
            output_tokens: 100,
            ttft_ms: 10,
            tool_calls_json: "[]".to_string(),
            tool_failures: 0,
            gap_from_prev_secs: 0.0,
            context_utilization: 0.01,
            context_window_tokens: STANDARD_CONTEXT_WINDOW_TOKENS,
            frustration_signals: 0,
            requested_model: Some("claude-sonnet".to_string()),
            actual_model: Some("claude-sonnet".to_string()),
            response_summary: Some(response_summary.to_string()),
            response_error_type: None,
            response_error_message: None,
        }
    }

    #[test]
    fn db_writer_persists_only_derived_metadata_not_raw_content() {
        let path = unique_test_db_path("privacy-derived-metadata");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));

        let raw_prompt = "RAW_PROMPT_SECRET_41";
        let raw_assistant = "RAW_ASSISTANT_TEXT_42";
        let raw_tool_input = "/tmp/RAW_FILE_CONTENT_SECRET_43.rs";
        let raw_error = "RAW_TOOL_OUTPUT_44";
        let raw_skill_detail = "RAW_SKILL_DETAIL_45";
        let raw_mcp_detail = "RAW_MCP_DETAIL_46";

        tx.send(DbCommand::InsertSession {
            session_id: "session-privacy-db".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            model: "claude-sonnet".to_string(),
            initial_prompt: Some(raw_prompt.to_string()),
            working_dir: "/tmp/privacy".to_string(),
            first_message_hash: "firstmsg_privacy".to_string(),
            prompt_correlation_hash: correlation::prompt_correlation_hash(raw_prompt),
        })
        .expect("queue session insert");
        let mut request = test_record_request_command(
            "req-privacy-db",
            "session-privacy-db",
            "2026-01-01T00:01:00Z",
        );
        if let DbCommand::RecordRequest { error_message, .. } = &mut request {
            *error_message = Some(raw_error.to_string());
        }
        tx.send(request).expect("queue request");

        tx.send(DbCommand::WriteTurnSnapshot {
            session_id: "session-privacy-db".to_string(),
            turn_number: 1,
            timestamp: "2026-01-01T00:01:00Z".to_string(),
            input_tokens: 1_000,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            cache_ttl_min_secs: 300,
            cache_ttl_max_secs: 300,
            cache_ttl_source: "default_5m".to_string(),
            output_tokens: 100,
            ttft_ms: 10,
            tool_calls_json: r#"["Read"]"#.to_string(),
            tool_failures: 1,
            gap_from_prev_secs: 0.0,
            context_utilization: 0.01,
            context_window_tokens: STANDARD_CONTEXT_WINDOW_TOKENS,
            frustration_signals: 0,
            requested_model: Some("claude-sonnet".to_string()),
            actual_model: Some("claude-sonnet".to_string()),
            response_summary: Some(raw_assistant.to_string()),
            response_error_type: Some("tool_error".to_string()),
            response_error_message: Some(raw_error.to_string()),
        })
        .expect("queue turn snapshot");
        tx.send(DbCommand::WriteToolOutcome {
            session_id: "session-privacy-db".to_string(),
            turn_number: 1,
            timestamp: "2026-01-01T00:01:00Z".to_string(),
            tool_name: "Read".to_string(),
            input_summary: raw_tool_input.to_string(),
            outcome: "error".to_string(),
            duration_ms: 11,
        })
        .expect("queue tool outcome");
        tx.send(DbCommand::WriteSkillEvent {
            session_id: "session-privacy-db".to_string(),
            timestamp: "2026-01-01T00:01:00Z".to_string(),
            skill_name: "qa".to_string(),
            event_type: "failed".to_string(),
            confidence: 1.0,
            source: "hook".to_string(),
            detail: Some(raw_skill_detail.to_string()),
        })
        .expect("queue skill event");
        tx.send(DbCommand::WriteMcpEvent {
            session_id: "session-privacy-db".to_string(),
            timestamp: "2026-01-01T00:01:00Z".to_string(),
            server: "filesystem".to_string(),
            tool: "read_file".to_string(),
            event_type: "failed".to_string(),
            source: "hook".to_string(),
            detail: Some(raw_mcp_detail.to_string()),
        })
        .expect("queue mcp event");

        let (response_tx, response_rx) = std_mpsc::channel();
        tx.send(DbCommand::FinalizeSession {
            session_id: "session-privacy-db".to_string(),
            ended_at: "2026-01-01T00:02:00Z".to_string(),
            diagnosis: PersistedDiagnosis {
                completed_at: "2026-01-01T00:02:00Z".to_string(),
                outcome: "Likely Completed".to_string(),
                total_turns: 1,
                total_cost: 0.01,
                cache_hit_ratio: 0.0,
                degraded: false,
                degradation_turn: None,
                causes_json: "[]".to_string(),
                advice_json: "[]".to_string(),
            },
            recall: Some(PersistedRecall {
                initial_prompt: raw_prompt.to_string(),
                final_response_summary: raw_assistant.to_string(),
            }),
            response_tx,
        })
        .expect("queue finalization");
        response_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("finalization ack")
            .expect("finalization ok");
        drop(tx);
        handle.join().expect("db writer exits");

        let conn = Connection::open(&path).expect("open test db");
        let mut persisted_text = String::new();
        for sql in [
            "SELECT COALESCE(initial_prompt, '') FROM sessions",
            "SELECT COALESCE(error_message, '') || ' ' || COALESCE(tool_calls, '') FROM requests",
            "SELECT COALESCE(response_summary, '') || ' ' || COALESCE(response_error_message, '') || ' ' || COALESCE(tool_calls, '') FROM turn_snapshots",
            "SELECT COALESCE(input_summary, '') || ' ' || COALESCE(outcome, '') FROM tool_outcomes",
            "SELECT COALESCE(detail, '') FROM skill_events",
            "SELECT COALESCE(detail, '') FROM mcp_events",
            "SELECT COALESCE(initial_prompt, '') || ' ' || COALESCE(final_response_summary, '') FROM session_recall",
        ] {
            let mut stmt = conn.prepare(sql).expect("prepare privacy query");
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .expect("query privacy rows");
            for row in rows {
                persisted_text.push_str(&row.expect("text row"));
                persisted_text.push('\n');
            }
        }

        for forbidden in [
            raw_prompt,
            raw_assistant,
            raw_tool_input,
            raw_error,
            raw_skill_detail,
            raw_mcp_detail,
        ] {
            assert!(
                !persisted_text.contains(forbidden),
                "persisted raw content sample {forbidden}: {persisted_text}"
            );
        }

        cleanup_test_db(&path);
    }

    #[test]
    fn db_writer_startup_repair_removes_legacy_raw_content_from_sqlite_artifacts() {
        let path = unique_test_db_path("legacy-privacy-artifacts");
        let raw_prompt = "RAW_WAL_PROMPT_SECRET_1001";
        let raw_error = "RAW_WAL_ERROR_SECRET_1002";
        let raw_recall = "RAW_WAL_RECALL_SECRET_1003";
        let before_repair = {
            let conn = Connection::open(&path).expect("open legacy sqlite");
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA secure_delete=OFF;",
            )
            .expect("enable legacy wal");
            conn.execute_batch(SCHEMA).expect("create schema");
            let reader = Connection::open(&path).expect("open checkpoint-blocking reader");
            reader
                .execute_batch("PRAGMA journal_mode=WAL; BEGIN;")
                .expect("start reader transaction");
            let _: i64 = reader
                .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
                .expect("establish reader snapshot");
            conn.execute(
                "INSERT INTO sessions (
                    session_id, started_at, model, working_dir, first_message_hash,
                    initial_prompt
                ) VALUES (?1,?2,?3,?4,?5,?6)",
                rusqlite::params![
                    "session-legacy-privacy-artifacts",
                    "2026-01-01T00:00:00Z",
                    "claude-sonnet",
                    "/tmp/cc-blackbox-legacy",
                    "firstmsg_legacy_privacy",
                    raw_prompt,
                ],
            )
            .expect("insert legacy session");
            conn.execute(
                "INSERT INTO requests (
                    request_id, session_id, timestamp, model, outcome, error_message
                ) VALUES (?1,?2,?3,?4,?5,?6)",
                rusqlite::params![
                    "req-legacy-privacy-artifacts",
                    "session-legacy-privacy-artifacts",
                    "2026-01-01T00:01:00Z",
                    "claude-sonnet",
                    "error",
                    raw_error,
                ],
            )
            .expect("insert legacy request");
            conn.execute(
                "INSERT INTO session_recall (
                    session_id, initial_prompt, final_response_summary
                ) VALUES (?1,?2,?3)",
                rusqlite::params!["session-legacy-privacy-artifacts", raw_prompt, raw_recall,],
            )
            .expect("insert legacy recall");
            let derived_prompt =
                content_metadata_for_storage("initial prompt", raw_prompt).expect("derive prompt");
            let derived_error =
                content_metadata_for_storage("error message", raw_error).expect("derive error");
            let derived_recall = content_metadata_for_storage("final response summary", raw_recall)
                .expect("derive recall");
            conn.execute(
                "UPDATE sessions SET initial_prompt = ?1 WHERE session_id = ?2",
                rusqlite::params![derived_prompt, "session-legacy-privacy-artifacts"],
            )
            .expect("derive legacy session");
            conn.execute(
                "UPDATE requests SET error_message = ?1 WHERE request_id = ?2",
                rusqlite::params![derived_error, "req-legacy-privacy-artifacts"],
            )
            .expect("derive legacy request");
            conn.execute(
                "UPDATE session_recall SET initial_prompt = ?1, final_response_summary = ?2
                 WHERE session_id = ?3",
                rusqlite::params![
                    content_metadata_for_storage("initial prompt", raw_prompt)
                        .expect("derive recall prompt"),
                    derived_recall,
                    "session-legacy-privacy-artifacts",
                ],
            )
            .expect("derive legacy recall");

            drop(conn);
            let bytes = sqlite_artifact_bytes(&path);
            drop(reader);
            bytes
        };
        assert!(
            [raw_prompt, raw_error, raw_recall]
                .iter()
                .any(|needle| bytes_contain(&before_repair, needle)),
            "legacy fixture did not write raw sentinel bytes"
        );

        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));
        let (response_tx, response_rx) = std_mpsc::channel();
        tx.send(DbCommand::FinalizeSession {
            session_id: "session-legacy-privacy-artifacts".to_string(),
            ended_at: "2026-01-01T00:02:00Z".to_string(),
            diagnosis: PersistedDiagnosis {
                completed_at: "2026-01-01T00:02:00Z".to_string(),
                outcome: "Likely Completed".to_string(),
                total_turns: 1,
                total_cost: 0.0,
                cache_hit_ratio: 0.0,
                degraded: false,
                degradation_turn: None,
                causes_json: "[]".to_string(),
                advice_json: "[]".to_string(),
            },
            recall: None,
            response_tx,
        })
        .expect("queue startup barrier");
        response_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("startup barrier response")
            .expect("startup barrier finalization");

        let conn = Connection::open(&path).expect("open repaired sqlite");
        let persisted: String = conn
            .query_row(
                "SELECT COALESCE(s.initial_prompt, '') || ' ' ||
                        COALESCE(req.error_message, '') || ' ' ||
                        COALESCE(r.initial_prompt, '') || ' ' ||
                        COALESCE(r.final_response_summary, '')
                 FROM sessions s
                 LEFT JOIN requests req ON req.session_id = s.session_id
                 LEFT JOIN session_recall r ON r.session_id = s.session_id
                 WHERE s.session_id = ?1",
                rusqlite::params!["session-legacy-privacy-artifacts"],
                |row| row.get(0),
            )
            .expect("query repaired content");
        let artifacts = sqlite_artifact_bytes(&path);
        for forbidden in [raw_prompt, raw_error, raw_recall] {
            assert!(
                !persisted.contains(forbidden),
                "SQL-visible raw content remained: {persisted}"
            );
            assert!(
                !bytes_contain(&artifacts, forbidden),
                "SQLite artifact retained raw sentinel {forbidden}"
            );
        }
        drop(conn);
        drop(tx);
        handle.join().expect("db writer exits");

        cleanup_test_db(&path);
    }

    #[test]
    fn session_rows_keep_correlation_metadata_without_hash_input() {
        let path = unique_test_db_path("session-correlation-metadata");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));

        tx.send(DbCommand::InsertSession {
            session_id: "session-correlation-db".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            model: "claude-sonnet".to_string(),
            initial_prompt: Some("RAW_FIRST_MESSAGE_HASH_INPUT".to_string()),
            working_dir: "/Users/pradeepsingh/code/cc-blackbox/".to_string(),
            first_message_hash: "firstmsg_8f14e45fceea".to_string(),
            prompt_correlation_hash: correlation::prompt_correlation_hash(
                "RAW_FIRST_MESSAGE_HASH_INPUT",
            ),
        })
        .expect("queue session insert");
        tx.send(test_record_request_command(
            "req-correlation-db",
            "session-correlation-db",
            "2026-01-01T00:03:00Z",
        ))
        .expect("queue request");
        drop(tx);
        handle.join().expect("db writer exits");

        let conn = Connection::open(&path).expect("open test db");
        let row: (String, String, String, String, Option<String>, i64) = conn
            .query_row(
                "SELECT session_id, working_dir, first_message_hash, model, initial_prompt, request_count \
                 FROM sessions WHERE session_id = ?1",
                rusqlite::params!["session-correlation-db"],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .expect("query session metadata");

        assert_eq!(row.0, "session-correlation-db");
        assert_eq!(row.1, "/Users/pradeepsingh/code/cc-blackbox");
        assert_eq!(row.2, "firstmsg_8f14e45fceea");
        assert_eq!(row.3, "claude-sonnet");
        assert_eq!(row.5, 1);
        assert!(
            !row.4
                .unwrap_or_default()
                .contains("RAW_FIRST_MESSAGE_HASH_INPUT"),
            "session row stored hash input"
        );

        cleanup_test_db(&path);
    }

    #[test]
    fn jsonl_enriched_findings_persist_source_evidence_and_no_raw_log_text() {
        let path = unique_test_db_path("jsonl-finding-privacy");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));
        let raw_jsonl_text =
            r#"{"message":"RAW_JSONL_MESSAGE_TEXT_77","tool_output":"RAW_TOOL_OUTPUT_78"}"#;

        tx.send(DbCommand::InsertSession {
            session_id: "session-jsonl-finding".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            model: "claude-sonnet".to_string(),
            initial_prompt: None,
            working_dir: "/tmp/cc-blackbox-test".to_string(),
            first_message_hash: "firstmsg_jsonl".to_string(),
            prompt_correlation_hash: String::new(),
        })
        .expect("queue session insert");
        tx.send(DbCommand::WriteGuardFinding {
            finding: guard::GuardFinding {
                rule_id: guard::RuleId::JsonlOnlyToolFailureStreak,
                severity: guard::FindingSeverity::Warning,
                action: guard::GuardAction::DiagnoseOnly,
                evidence_level: guard::EvidenceLevel::DirectJsonl,
                source: guard::FindingSource::Jsonl,
                confidence: 0.92,
                session_id: "session-jsonl-finding".to_string(),
                request_id: None,
                turn_number: Some(3),
                timestamp: "2026-01-01T00:02:00Z".to_string(),
                detail: "2 failed Bash tool calls observed in JSONL metadata.".to_string(),
                suggested_action: Some("Inspect the failing command outside Claude.".to_string()),
            },
        })
        .expect("queue finding write");
        drop(tx);
        handle.join().expect("db writer exits");

        let conn = Connection::open(&path).expect("open test db");
        let row: (String, String, String, String, String, Option<String>) = conn
            .query_row(
                "SELECT rule_id, source, evidence_level, detail, session_id, suggested_action \
                 FROM guard_findings WHERE session_id = ?1",
                rusqlite::params!["session-jsonl-finding"],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .expect("query finding");

        assert_eq!(row.0, "jsonl_only_tool_failure_streak");
        assert_eq!(row.1, "jsonl");
        assert_eq!(row.2, "direct_jsonl");
        assert_eq!(row.4, "session-jsonl-finding");
        let persisted = format!("{} {} {}", row.3, row.5.unwrap_or_default(), row.2);
        assert!(!persisted.contains(raw_jsonl_text));
        assert!(!persisted.contains("RAW_JSONL_MESSAGE_TEXT_77"));
        assert!(!persisted.contains("RAW_TOOL_OUTPUT_78"));

        cleanup_test_db(&path);
    }

    #[test]
    fn global_cooldown_finding_persists_without_existing_session_row() {
        let path = unique_test_db_path("global-cooldown-finding");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));

        tx.send(DbCommand::WriteGuardFinding {
            finding: guard::GuardFinding {
                rule_id: guard::RuleId::ApiErrorCircuitBreakerCooldown,
                severity: guard::FindingSeverity::Critical,
                action: guard::GuardAction::Cooldown,
                evidence_level: guard::EvidenceLevel::DirectProxy,
                source: guard::FindingSource::Proxy,
                confidence: 1.0,
                session_id: "unknown".to_string(),
                request_id: None,
                turn_number: None,
                timestamp: "2026-01-01T00:02:00Z".to_string(),
                detail: "2 consecutive API errors opened a 60s cooldown; 59s remain.".to_string(),
                suggested_action: Some("Wait for the cooldown to expire.".to_string()),
            },
        })
        .expect("queue global cooldown finding");
        drop(tx);
        handle.join().expect("db writer exits");

        let conn = Connection::open(&path).expect("open test db");
        let session_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE session_id = ?1",
                rusqlite::params![super::GLOBAL_GUARD_SESSION_ID],
                |row| row.get(0),
            )
            .expect("query synthetic session");
        assert_eq!(session_count, 1);

        let row: (String, String, String) = conn
            .query_row(
                "SELECT session_id, rule_id, action FROM guard_findings",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("query global cooldown finding");
        assert_eq!(row.0, super::GLOBAL_GUARD_SESSION_ID);
        assert_eq!(row.1, "api_error_circuit_breaker_cooldown");
        assert_eq!(row.2, "cooldown");

        cleanup_test_db(&path);
    }

    fn test_live_turn(turn_number: u32, response_summary: Option<&str>) -> diagnosis::TurnSnapshot {
        diagnosis::TurnSnapshot {
            turn_number,
            timestamp: Instant::now(),
            input_tokens: 1_000,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            cache_ttl_min_secs: 300,
            cache_ttl_max_secs: 300,
            cache_ttl_source: "default_5m".to_string(),
            output_tokens: 100,
            ttft_ms: 10,
            tool_calls: Vec::new(),
            tool_results_failed: 0,
            gap_from_prev_secs: 0.0,
            context_utilization: 0.01,
            context_window_tokens: STANDARD_CONTEXT_WINDOW_TOKENS,
            frustration_signals: 0,
            requested_model: Some("claude-sonnet".to_string()),
            actual_model: Some("claude-sonnet".to_string()),
            response_summary: response_summary.map(str::to_string),
            response_error_type: None,
            response_error_message: None,
        }
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
    fn historical_metrics_prices_cache_waste_with_5m_and_1h_buckets() {
        let conn = create_history_test_db();
        let now = 1_776_700_000;
        let one_hour_ago = epoch_to_iso8601(now - 3_600);
        insert_session(
            &conn,
            "session-cache-buckets",
            &one_hour_ago,
            Some(&one_hour_ago),
            "claude-sonnet-4-6",
            None,
        );
        conn.execute(
            "INSERT INTO requests (
                request_id, session_id, timestamp, model, input_tokens, output_tokens,
                cache_read_tokens, cache_creation_tokens, cache_creation_5m_tokens,
                cache_creation_1h_tokens, cost_dollars, cost_source,
                trusted_for_budget_enforcement, duration_ms, tool_calls, cache_event
            ) VALUES (?1, ?2, ?3, ?4, 0, 0, 0, 300000, 100000, 200000, NULL, NULL, NULL, 0, '[]', 'miss_thrash')",
            rusqlite::params![
                "req-cache-buckets",
                "session-cache-buckets",
                one_hour_ago,
                "claude-sonnet-4-6",
            ],
        )
        .expect("insert bucketed request");

        let windows = query_historical_metrics(&conn, now).expect("query history");
        let one_day = history_window(&windows, "1d");
        let expected_spend = pricing::estimate_cost_dollars_with_cache_buckets(
            "claude-sonnet-4-6",
            0,
            0,
            0,
            100_000,
            200_000,
        )
        .total_cost_dollars;
        let expected_waste = pricing::estimate_cache_rebuild_waste_dollars_with_cache_buckets(
            "claude-sonnet-4-6",
            100_000,
            200_000,
        )
        .total_cost_dollars;

        assert!((one_day.estimated_spend_dollars - expected_spend).abs() < 1e-9);
        assert_eq!(one_day.cache_events.get("miss_thrash"), Some(&1));
        assert_eq!(
            one_day.estimated_cache_waste_dollars_by_model.get("sonnet"),
            Some(&expected_waste)
        );
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
        assert!(body.contains("cc_blackbox_history_sessions"));
        assert!(body.contains("cc_blackbox_history_estimated_spend_dollars"));
        assert!(body.contains("cc_blackbox_history_estimated_spend_dollars_by_model"));
        assert!(body.contains("cc_blackbox_history_avg_estimated_session_cost_dollars"));
        assert!(body.contains("cc_blackbox_history_cache_hit_ratio"));
        assert!(body.contains("cc_blackbox_history_degraded_sessions"));
        assert!(body.contains("cc_blackbox_history_degraded_session_ratio"));
        assert!(body.contains("cc_blackbox_history_degraded_causes"));
        assert!(body.contains("cc_blackbox_history_estimated_cache_waste_dollars"));
        assert!(body.contains("cc_blackbox_history_cache_events"));
        assert!(body.contains("cc_blackbox_history_model_fallbacks"));
        assert!(body.contains("cc_blackbox_history_tool_failures"));
        assert!(body.contains("cc_blackbox_history_refresh_timestamp_seconds 1776700000"));
        assert!(body.contains("window=\"7d\""));
        assert!(body.contains("cause_type=\"context_bloat\""));
        assert!(body.contains("event_type=\"miss_thrash\""));
        assert!(body.contains("requested=\"opus\""));
        assert!(body.contains("actual=\"sonnet\""));
        assert!(body.contains("tool=\"bash\""));
        assert!(body.contains(
            "cc_blackbox_cache_events_total{event_type=\"miss_thrash\",model=\"sonnet\"}"
        ));
        assert!(body.contains("cc_blackbox_estimated_cache_waste_dollars_total{model=\"sonnet\"}"));
        assert!(body
            .contains("cc_blackbox_model_fallback_total{actual=\"sonnet\",requested=\"opus\"} 0"));
        assert!(body.contains("cc_blackbox_tool_failures_total{tool=\"unknown\"} 0"));
        assert!(body
            .contains("cc_blackbox_mcp_tool_calls_total{server=\"unknown\",tool=\"unknown\"} 0"));
        assert!(body.contains("cc_blackbox_mcp_server_calls_total{server=\"unknown\"} 0"));
        assert!(metric_line_has(
            &body,
            "cc_blackbox_mcp_events_total",
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
            "cc_blackbox_skill_events_total",
            &[
                "skill=\"unknown\"",
                "event_type=\"fired\"",
                "source=\"hook\""
            ],
            " 0"
        ));
    }

    #[test]
    fn metrics_render_does_not_recompute_active_sessions_from_session_map() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        metrics::init();

        let (_, before) = metrics::render().expect("render metrics before");
        let before_line = metric_scalar_line(&before, "cc_blackbox_active_sessions")
            .expect("active sessions line before")
            .to_string();

        let hash = 0xfeed_beef_u64;
        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: "session_metrics_scan_guard".to_string(),
                display_name: "metrics-scan-guard".to_string(),
                model: "claude-sonnet-4-6".to_string(),
                initial_prompt: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );

        let (_, after) = metrics::render().expect("render metrics after");
        let after_line = metric_scalar_line(&after, "cc_blackbox_active_sessions")
            .expect("active sessions line after")
            .to_string();
        let _ = diagnosis::SESSIONS.remove(&hash);

        assert_eq!(before_line, after_line);
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
        assert!(body.contains("cc_blackbox_tool_calls_total{tool=\"bash\"} 0"));
        assert!(body.contains("cc_blackbox_tool_failures_total{tool=\"bash\"} 0"));
        assert!(body.contains("cc_blackbox_tool_calls_total{tool=\"read_file\"} 0"));
        assert!(body.contains("cc_blackbox_tool_failures_total{tool=\"read_file\"} 0"));
        assert!(body
            .contains("cc_blackbox_mcp_tool_calls_total{server=\"github\",tool=\"get_issue\"} 0"));
        assert!(body.contains(
            "cc_blackbox_mcp_tool_failures_total{server=\"github\",tool=\"get_issue\"} 0"
        ));
        assert!(body.contains("cc_blackbox_mcp_server_calls_total{server=\"github\"} 0"));
        assert!(body.contains("cc_blackbox_mcp_server_failures_total{server=\"github\"} 0"));
        assert!(metric_line_has(
            &body,
            "cc_blackbox_skill_events_total",
            &[
                "skill=\"openai-docs\"",
                "event_type=\"fired\"",
                "source=\"hook\""
            ],
            " 0"
        ));
        assert!(metric_line_has(
            &body,
            "cc_blackbox_mcp_events_total",
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
            "cc_blackbox_tool_calls_total{tool=\"mcp__metricstest_server__lookup_widget\"} 1"
        ));
        assert!(body.contains(
            "cc_blackbox_tool_failures_total{tool=\"mcp__metricstest_server__lookup_widget\"} 2"
        ));
        assert!(body.contains(
            "cc_blackbox_mcp_tool_calls_total{server=\"metricstest_server\",tool=\"lookup_widget\"} 1"
        ));
        assert!(body.contains(
            "cc_blackbox_mcp_tool_failures_total{server=\"metricstest_server\",tool=\"lookup_widget\"} 2"
        ));
        assert!(
            body.contains("cc_blackbox_mcp_server_calls_total{server=\"metricstest_server\"} 1")
        );
        assert!(
            body.contains("cc_blackbox_mcp_server_failures_total{server=\"metricstest_server\"} 2")
        );
        assert!(metric_line_has(
            &body,
            "cc_blackbox_mcp_events_total",
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
            "cc_blackbox_mcp_events_total",
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
    fn guard_and_finalization_metrics_are_low_cardinality() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        metrics::init();

        metrics::record_guard_finding(
            "per_session_token_budget_exceeded",
            "critical",
            "block",
            "proxy",
        );
        metrics::record_guard_block("per_session_token_budget_exceeded");
        metrics::record_session_finalization("child_exit", "finalized");

        let (_, body) = metrics::render().expect("render metrics");
        assert!(metric_line_has(
            &body,
            "cc_blackbox_guard_findings_total",
            &[
                "rule_id=\"per_session_token_budget_exceeded\"",
                "severity=\"critical\"",
                "action=\"block\"",
                "source=\"proxy\""
            ],
            ""
        ));
        assert!(metric_line_has(
            &body,
            "cc_blackbox_guard_blocks_total",
            &["rule_id=\"per_session_token_budget_exceeded\""],
            ""
        ));
        assert!(metric_line_has(
            &body,
            "cc_blackbox_session_finalizations_total",
            &["reason=\"child_exit\"", "outcome=\"finalized\""],
            ""
        ));
        for line in body.lines().filter(|line| {
            line.starts_with("cc_blackbox_guard_findings_total")
                || line.starts_with("cc_blackbox_guard_blocks_total")
                || line.starts_with("cc_blackbox_session_finalizations_total")
        }) {
            assert!(
                !line.contains("session_id="),
                "unexpected session_id label: {line}"
            );
        }
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
                line.starts_with("cc_blackbox_history_degraded_causes{")
                    && line.contains("window=\"7d\"")
                    && line.contains("cause_type=\"cache_miss_thrash\"")
            })
            .expect("stale cause line");
        assert!(stale_line.ends_with(" 0"));
        let stale_avg_line = body
            .lines()
            .find(|line| {
                line.starts_with("cc_blackbox_history_avg_estimated_session_cost_dollars{")
                    && line.contains("window=\"7d\"")
                    && line.contains("model=\"sonnet\"")
            })
            .expect("stale avg line");
        assert!(stale_avg_line.ends_with(" 0"));
        let stale_spend_line = body
            .lines()
            .find(|line| {
                line.starts_with("cc_blackbox_history_estimated_spend_dollars_by_model{")
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
            total_input_cache_rate: 0.2,
        };
        let week = SummaryWindowData {
            sessions: 3,
            estimated_cost_dollars: 20.0,
            cost_source: pricing::MIXED_COST_SOURCE.to_string(),
            trusted_for_budget_enforcement: false,
            billed_cost_dollars: None,
            billed_sessions: 0,
            cache_hit_ratio: 0.25,
            total_input_cache_rate: 0.1,
        };
        let month = SummaryWindowData {
            sessions: 4,
            estimated_cost_dollars: 30.0,
            cost_source: ESTIMATED_COST_SOURCE.to_string(),
            trusted_for_budget_enforcement: false,
            billed_cost_dollars: Some(18.0),
            billed_sessions: 2,
            cache_hit_ratio: 0.75,
            total_input_cache_rate: 0.3,
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
    fn diagnosis_response_marks_heuristic_cause_details() {
        let json = build_diagnosis_response_json(
            "session_heuristic".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            "Compaction Suspected".to_string(),
            4,
            1.25,
            "builtin_model_family_pricing".to_string(),
            false,
            None,
            None,
            None,
            0.4,
            true,
            Some(2),
            serde_json::json!([
                {
                    "turn_first_noticed": 2,
                    "cause_type": "near_compaction",
                    "detail": "Context reached 84% full.",
                    "estimated_cost": 0.0,
                    "is_heuristic": true
                },
                {
                    "turn_first_noticed": 3,
                    "cause_type": "model_fallback",
                    "detail": "Requested opus; response reported sonnet.",
                    "estimated_cost": 0.0,
                    "is_heuristic": false
                }
            ]),
            serde_json::json!([]),
        );

        assert_eq!(
            json.pointer("/causes/0/detail").and_then(|v| v.as_str()),
            Some("[heuristic] Context reached 84% full.")
        );
        assert_eq!(
            json.pointer("/causes/1/detail").and_then(|v| v.as_str()),
            Some("Requested opus; response reported sonnet.")
        );
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
    fn sessions_response_includes_active_sessions_without_diagnosis_rows() {
        let path = unique_test_db_path("active-sessions-api");
        let conn = Connection::open(&path).expect("open test db");
        conn.execute_batch(SCHEMA).expect("create schema");
        let now = now_iso8601();
        conn.execute(
            "INSERT INTO sessions (
                session_id, started_at, last_activity_at, model, working_dir,
                request_count, total_input_tokens, total_output_tokens,
                total_cache_read_tokens, total_cache_creation_tokens,
                first_message_hash, prompt_correlation_hash, initial_prompt
            ) VALUES (?1,?2,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            rusqlite::params![
                "session-active-api",
                now,
                "claude-haiku-4-5-20251001",
                "/Users/pradeepsingh/code/tools/KubeAttention",
                2,
                10,
                5,
                80,
                10,
                "hash-active",
                "prompt-hash-active",
                "initial prompt captured (redacted, 4 words, 20 chars).",
            ],
        )
        .expect("insert active session");

        let json = build_recent_sessions_response_from_db(&conn, 10, 1)
            .expect("build recent sessions response");
        let sessions = json
            .get("sessions")
            .and_then(Value::as_array)
            .expect("sessions array");

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["session_id"], "session-active-api");
        assert_eq!(sessions[0]["outcome"], "In Progress");
        assert_eq!(sessions[0]["active"], true);
        assert_eq!(
            sessions[0]["working_dir"],
            "/Users/pradeepsingh/code/tools/KubeAttention"
        );
        assert_eq!(sessions[0]["total_turns"], 2);
        assert_eq!(sessions[0]["request_count"], 2);
        assert_eq!(sessions[0]["total_tokens"], 105);

        cleanup_test_db(&path);
    }

    #[test]
    fn postmortem_from_synthetic_db_includes_structured_redacted_evidence() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-post",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:08:00Z"),
            "claude-opus-4-7",
            Some("Investigate /Users/pradeep/code/private auth with token=sk-ant-secretvalue"),
        );
        insert_request(
            &conn,
            "req-post-1",
            "session-post",
            "2026-01-01T00:01:00Z",
            "claude-opus-4-7",
            160_000,
            1_000,
            0,
            12_000,
        );
        insert_request(
            &conn,
            "req-post-2",
            "session-post",
            "2026-01-01T00:03:00Z",
            "claude-sonnet-4-6",
            170_000,
            1_500,
            0,
            14_000,
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Likely Partially Completed', 3, 2.0, 0.10, 1, 2, ?3, ?4)",
            rusqlite::params![
                "session-post",
                "2026-01-01T00:08:00Z",
                r#"[{"turn_first_noticed":2,"cause_type":"model_fallback","detail":"Requested opus for /Users/pradeep/code/private?x=1 with password=hunter2, got sonnet","estimated_cost":0.42,"is_heuristic":false,"requested_model":"claude-opus-4-7","actual_model":"claude-sonnet-4-6"}]"#,
                r#"["Restart with a shorter prompt after fixing /Users/pradeep/code/private."]"#,
            ],
        )
        .expect("insert diagnosis");
        conn.execute(
            "INSERT INTO turn_snapshots (
                session_id, turn_number, timestamp, input_tokens, cache_read_tokens,
                cache_creation_tokens, output_tokens, ttft_ms, tool_calls, tool_failures,
                gap_from_prev_secs, context_utilization, context_window_tokens,
                frustration_signals, requested_model, actual_model, response_summary
            ) VALUES
                ('session-post', 1, '2026-01-01T00:01:00Z', 160000, 0, 12000, 1000, 1200, '[\"Read\"]', 0, 0.0, 0.86, 200000, 0, 'claude-opus-4-7', 'claude-opus-4-7', 'Read /Users/pradeep/code/private'),
                ('session-post', 2, '2026-01-01T00:03:00Z', 170000, 0, 14000, 1500, 2400, '[\"Bash\"]', 1, 600.0, 0.92, 200000, 0, 'claude-opus-4-7', 'claude-sonnet-4-6', 'Ran https://example.com/callback?token=secret'),
                ('session-post', 3, '2026-01-01T00:08:00Z', 175000, 10000, 0, 900, 900, '[\"Edit\"]', 0, 10.0, 0.93, 200000, 0, 'claude-opus-4-7', 'claude-opus-4-7', 'Stopped after fallback')",
            [],
        )
        .expect("insert turns");
        conn.execute(
            "INSERT INTO tool_outcomes (session_id, turn_number, timestamp, tool_name, input_summary, outcome, duration_ms)
             VALUES
                ('session-post', 1, '2026-01-01T00:01:00Z', 'Read', '/Users/pradeep/code/private/src/main.rs', 'success', 10),
                ('session-post', 2, '2026-01-01T00:03:00Z', 'Bash', '{\"command\":\"curl https://example.com?token=secret\"}', 'error', 50),
                ('session-post', 3, '2026-01-01T00:08:00Z', 'Read', '/Users/pradeep/code/private/src/lib.rs', 'success', 12)",
            [],
        )
        .expect("insert tool outcomes");
        conn.execute(
            "INSERT INTO skill_events (session_id, timestamp, skill_name, event_type, confidence, source, detail)
             VALUES ('session-post', '2026-01-01T00:02:00Z', 'tdd', 'fired', 1.0, 'hook', 'ok')",
            [],
        )
        .expect("insert skill event");
        conn.execute(
            "INSERT INTO mcp_events (session_id, timestamp, server, tool, event_type, source, detail)
             VALUES ('session-post', '2026-01-01T00:04:00Z', 'github', 'get_issue', 'called', 'hook', 'ok')",
            [],
        )
        .expect("insert mcp event");
        conn.execute(
            "INSERT INTO session_recall (session_id, initial_prompt, final_response_summary)
             VALUES ('session-post', 'Investigate auth with token=sk-ant-secretvalue', 'Finished via https://example.com/path?secret=yes')",
            [],
        )
        .expect("insert recall");

        let json =
            build_postmortem_response_from_db(&conn, "session-post", true).expect("postmortem");
        assert_eq!(json.get("redacted").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            json.pointer("/diagnosis/likely_cause")
                .and_then(|v| v.as_str()),
            Some("model_fallback")
        );
        assert_eq!(
            json.pointer("/diagnosis/confidence")
                .and_then(|v| v.as_str()),
            Some("high")
        );
        assert_eq!(
            json.pointer("/signals/model/fallbacks/0/requested")
                .and_then(|v| v.as_str()),
            Some("claude-opus-4-7")
        );
        assert_eq!(
            json.pointer("/signals/skills/0/skill_name")
                .and_then(|v| v.as_str()),
            Some("tdd")
        );
        assert_eq!(
            json.pointer("/signals/mcp/0/server")
                .and_then(|v| v.as_str()),
            Some("github")
        );
        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        assert!(rendered.contains("Initial prompt captured (redacted"));
        assert!(!rendered.contains("/Users/pradeep"));
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("sk-ant-secretvalue"));
        assert!(!rendered.contains("secret=yes"));
        assert!(rendered.contains("<path>"));
    }

    #[test]
    fn postmortem_json_includes_phase6_structured_fields_and_labeled_findings() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            "/tmp/cc-blackbox-jsonl-fixtures-do-not-exist",
        );
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-phase6-json",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:08:00Z"),
            "claude-opus-4-7",
            None,
        );
        insert_request(
            &conn,
            "req-phase6-json",
            "session-phase6-json",
            "2026-01-01T00:01:00Z",
            "claude-opus-4-7",
            160_000,
            1_000,
            0,
            12_000,
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Likely Partially Completed', 2, 2.0, 0.10, 1, 2, ?3, ?4)",
            rusqlite::params![
                "session-phase6-json",
                "2026-01-01T00:08:00Z",
                r#"[{"turn_first_noticed":2,"cause_type":"model_fallback","detail":"Requested opus, got sonnet","estimated_cost":0.42,"is_heuristic":false,"requested_model":"claude-opus-4-7","actual_model":"claude-sonnet-4-6"}]"#,
                r#"["Retry with the intended model."]"#,
            ],
        )
        .expect("insert diagnosis");
        conn.execute(
            "INSERT INTO turn_snapshots (
                session_id, turn_number, timestamp, input_tokens, cache_read_tokens,
                cache_creation_tokens, output_tokens, ttft_ms, tool_calls, tool_failures,
                gap_from_prev_secs, context_utilization, context_window_tokens,
                frustration_signals, requested_model, actual_model
            ) VALUES
                ('session-phase6-json', 1, '2026-01-01T00:01:00Z', 160000, 0, 12000, 1000, 1200, '[]', 0, 0.0, 0.86, 200000, 0, 'claude-opus-4-7', 'claude-opus-4-7'),
                ('session-phase6-json', 2, '2026-01-01T00:03:00Z', 170000, 0, 14000, 1500, 2400, '[]', 0, 10.0, 0.92, 200000, 0, 'claude-opus-4-7', 'claude-sonnet-4-6')",
            [],
        )
        .expect("insert turns");

        let json = build_postmortem_response_from_db(&conn, "session-phase6-json", true)
            .expect("postmortem");

        assert!(json
            .get("generated_at")
            .and_then(|value| value.as_str())
            .is_some());
        assert_eq!(
            json.pointer("/proxy_summary/main_degradation")
                .and_then(|value| value.as_str()),
            Some("model_fallback")
        );
        assert_eq!(
            json.pointer("/enrichment_status/status")
                .and_then(|value| value.as_str()),
            Some("proxy_only")
        );
        let findings = json
            .get("findings")
            .and_then(serde_json::Value::as_array)
            .expect("findings");
        let model_finding = findings
            .iter()
            .find(|finding| {
                finding.get("rule_id").and_then(serde_json::Value::as_str) == Some("model_mismatch")
            })
            .expect("model finding");
        assert_eq!(
            model_finding
                .get("source")
                .and_then(serde_json::Value::as_str),
            Some("proxy")
        );
        assert_eq!(
            model_finding
                .get("evidence_level")
                .and_then(serde_json::Value::as_str),
            Some("direct_proxy")
        );
        assert!(model_finding
            .get("confidence")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|confidence| confidence >= 0.9));
        let labels = json
            .get("evidence_labels")
            .and_then(serde_json::Value::as_array)
            .expect("evidence labels");
        assert!(labels.iter().any(|label| {
            label.get("source").and_then(serde_json::Value::as_str) == Some("proxy")
                && label
                    .get("evidence_level")
                    .and_then(serde_json::Value::as_str)
                    == Some("direct_proxy")
        }));
        assert!(json
            .get("recommendations")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| !items.is_empty()));
    }

    #[test]
    fn postmortem_represents_required_degradation_explanations_when_evidence_exists() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            "/tmp/cc-blackbox-jsonl-fixtures-do-not-exist",
        );
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-required-findings",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:08:00Z"),
            "claude-opus-4-7",
            None,
        );
        insert_request(
            &conn,
            "req-required-findings",
            "session-required-findings",
            "2026-01-01T00:01:00Z",
            "claude-opus-4-7",
            160_000,
            1_000,
            0,
            12_000,
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Degraded', 6, 2.0, 0.10, 1, 2, ?3, '[]')",
            rusqlite::params![
                "session-required-findings",
                "2026-01-01T00:08:00Z",
                r#"[
                    {"turn_first_noticed":2,"cause_type":"cache_miss_thrash","detail":"Cache rebuilt repeatedly.","estimated_cost":0.42,"is_heuristic":false},
                    {"turn_first_noticed":3,"cause_type":"context_bloat","detail":"Context crossed warning threshold.","estimated_cost":0.10,"is_heuristic":false},
                    {"turn_first_noticed":4,"cause_type":"model_fallback","detail":"Requested opus, got sonnet.","estimated_cost":0.20,"is_heuristic":false,"requested_model":"claude-opus-4-7","actual_model":"claude-sonnet-4-6"},
                    {"turn_first_noticed":5,"cause_type":"tool_failure_streak","detail":"Tools failed repeatedly.","estimated_cost":0.05,"is_heuristic":false}
                ]"#,
            ],
        )
        .expect("insert diagnosis");
        for (rule_id, action, source, evidence_level, detail) in [
            (
                "per_session_token_budget_exceeded",
                "block",
                "proxy",
                "direct_proxy",
                "Session token budget exceeded.",
            ),
            (
                "api_error_circuit_breaker_cooldown",
                "cooldown",
                "proxy",
                "direct_proxy",
                "API error cooldown is active.",
            ),
        ] {
            conn.execute(
                "INSERT INTO guard_findings (
                    session_id, request_id, turn_number, timestamp, rule_id, severity,
                    action, source, evidence_level, confidence, detail, suggested_action
                ) VALUES (?1,NULL,NULL,?2,?3,'critical',?4,?5,?6,1.0,?7,NULL)",
                rusqlite::params![
                    "session-required-findings",
                    "2026-01-01T00:07:00Z",
                    rule_id,
                    action,
                    source,
                    evidence_level,
                    detail,
                ],
            )
            .expect("insert guard finding");
        }

        let json = build_postmortem_response_from_db(&conn, "session-required-findings", true)
            .expect("postmortem");
        let rule_ids = json
            .get("findings")
            .and_then(serde_json::Value::as_array)
            .expect("findings")
            .iter()
            .filter_map(|finding| finding.get("rule_id").and_then(serde_json::Value::as_str))
            .collect::<std::collections::HashSet<_>>();

        for rule_id in [
            "per_session_token_budget_exceeded",
            "api_error_circuit_breaker_cooldown",
            "repeated_cache_rebuilds",
            "context_near_warning_threshold",
            "model_mismatch",
            "tool_failure_streak",
        ] {
            assert!(
                rule_ids.contains(rule_id),
                "missing {rule_id}: {rule_ids:?}"
            );
        }
    }

    #[test]
    fn postmortem_last_reports_empty_state_when_no_sessions_exist() {
        let conn = create_full_test_db();
        let err = build_postmortem_response_from_db(&conn, "last", true).unwrap_err();
        assert!(matches!(err, PostmortemError::NoSessions));
    }

    #[test]
    fn proxy_only_postmortem_marks_missing_jsonl_enrichment_status() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            "/tmp/cc-blackbox-jsonl-fixtures-do-not-exist",
        );
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-proxy-only",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:02:00Z"),
            "claude-sonnet",
            None,
        );
        insert_request(
            &conn,
            "req-proxy-only",
            "session-proxy-only",
            "2026-01-01T00:01:00Z",
            "claude-sonnet",
            2_000,
            300,
            0,
            0,
        );

        let json = build_postmortem_response_from_db(&conn, "session-proxy-only", true)
            .expect("postmortem");

        assert_eq!(
            json.pointer("/enrichment_status/status")
                .and_then(|value| value.as_str()),
            Some("proxy_only")
        );
        assert_eq!(
            json.pointer("/enrichment_status/reason")
                .and_then(|value| value.as_str()),
            Some("missing_jsonl")
        );
        assert_eq!(
            json.pointer("/enrichment_status/source")
                .and_then(|value| value.as_str()),
            Some("jsonl")
        );
    }

    #[test]
    fn matched_jsonl_postmortem_adds_direct_jsonl_tool_failure_finding() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let jsonl_dir = std::env::temp_dir().join(format!(
            "cc-blackbox-jsonl-match-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&jsonl_dir).expect("create jsonl dir");
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            jsonl_dir.to_str().expect("jsonl dir is utf-8"),
        );
        let conn = create_full_test_db();
        let raw_prompt = "RAW_JSONL_PROMPT_66 fix the failing test";
        let first_message_hash = super::hash_text_hex(raw_prompt);
        conn.execute(
            "INSERT INTO sessions (
                session_id, started_at, last_activity_at, ended_at, model, working_dir,
                first_message_hash, initial_prompt
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
            rusqlite::params![
                "session-jsonl-match",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:05:00Z",
                "2026-01-01T00:06:00Z",
                "claude-sonnet",
                "/tmp/cc-blackbox-jsonl",
                first_message_hash,
            ],
        )
        .expect("insert session");
        insert_request(
            &conn,
            "req-jsonl-match",
            "session-jsonl-match",
            "2026-01-01T00:01:00Z",
            "claude-sonnet",
            5_000,
            500,
            0,
            0,
        );
        for turn in 1..=5 {
            insert_turn_snapshot(
                &conn,
                "session-jsonl-match",
                turn,
                &format!("2026-01-01T00:0{turn}:00Z"),
                0,
                0,
                100,
                0.0,
                0.02,
                None,
            );
        }

        let mut jsonl = String::new();
        jsonl.push_str(&format!(
            r#"{{"type":"user","sessionId":"jsonl-match","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:00Z","message":{{"role":"user","content":[{{"type":"text","text":"{raw_prompt}"}}]}}}}"#
        ));
        jsonl.push('\n');
        for turn in 1..=5 {
            jsonl.push_str(&format!(
                r#"{{"type":"assistant","sessionId":"jsonl-match","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:0{turn}:00Z","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"toolu_{turn}","name":"Bash","input":{{"command":"cargo test -- RAW_JSONL_COMMAND_77"}}}}]}}}}"#
            ));
            jsonl.push('\n');
            jsonl.push_str(&format!(
                r#"{{"type":"user","sessionId":"jsonl-match","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:0{turn}:30Z","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"toolu_{turn}","is_error":true,"content":"RAW_JSONL_OUTPUT_88"}}]}}}}"#
            ));
            jsonl.push('\n');
        }
        let jsonl_path = jsonl_dir.join("session.jsonl");
        fs::write(&jsonl_path, jsonl).expect("write jsonl fixture");

        let json = build_postmortem_response_from_db(&conn, "session-jsonl-match", true)
            .expect("postmortem");

        assert_eq!(
            json.pointer("/enrichment_status/status")
                .and_then(|value| value.as_str()),
            Some("matched")
        );
        assert_eq!(
            json.pointer("/enrichment_status/jsonl_session_id")
                .and_then(|value| value.as_str()),
            Some("jsonl-match")
        );
        let persisted_jsonl_id: String = conn
            .query_row(
                "SELECT jsonl_session_id FROM session_jsonl_correlations \
                 WHERE proxy_session_id = ?1",
                rusqlite::params!["session-jsonl-match"],
                |row| row.get(0),
            )
            .expect("persisted JSONL correlation");
        assert_eq!(persisted_jsonl_id, "jsonl-match");
        let findings = json
            .get("findings")
            .and_then(serde_json::Value::as_array)
            .expect("findings array");
        let jsonl_finding = findings
            .iter()
            .find(|finding| {
                finding.get("rule_id").and_then(serde_json::Value::as_str)
                    == Some("jsonl_only_tool_failure_streak")
            })
            .expect("jsonl tool failure finding");
        assert_eq!(
            jsonl_finding
                .get("source")
                .and_then(serde_json::Value::as_str),
            Some("jsonl")
        );
        assert_eq!(
            jsonl_finding
                .get("evidence_level")
                .and_then(serde_json::Value::as_str),
            Some("direct_jsonl")
        );
        assert!(jsonl_finding
            .get("confidence")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|confidence| confidence >= 0.9));
        assert_eq!(
            json.pointer("/handoff_context/provenance/cc_blackbox_session_id")
                .and_then(|value| value.as_str()),
            Some("session-jsonl-match")
        );
        assert_eq!(
            json.pointer("/handoff_context/provenance/claude_jsonl_session_id")
                .and_then(|value| value.as_str()),
            Some("jsonl-match")
        );
        assert!(json
            .pointer("/handoff_context/recent_activity")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| items.iter().any(|item| item
                .as_str()
                .is_some_and(|line| line.contains("Bash: 5 JSONL calls, 5 failures")))));
        assert!(json
            .pointer("/handoff_context/avoid_repeating")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| items.iter().any(|item| item
                .as_str()
                .is_some_and(|line| line.contains("Fix failing tool preconditions")))));

        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        for forbidden in [
            raw_prompt,
            "RAW_JSONL_COMMAND_77",
            "RAW_JSONL_OUTPUT_88",
            "cargo test --",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "raw JSONL leaked: {rendered}"
            );
        }

        fs::remove_dir_all(jsonl_dir).expect("remove jsonl dir");
    }

    #[test]
    fn current_claude_code_jsonl_shape_matches_postmortem_by_prompt_correlation_hash() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let jsonl_dir = std::env::temp_dir().join(format!(
            "cc-blackbox-current-jsonl-match-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&jsonl_dir).expect("create jsonl dir");
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            jsonl_dir.to_str().expect("jsonl dir is utf-8"),
        );
        let conn = create_full_test_db();
        let visible_prompt = "Read-only architecture audit for this repository";
        let full_api_prompt = format!(
            "<system-reminder>Claude Code injected instructions</system-reminder>\n{visible_prompt}"
        );
        conn.execute(
            "INSERT INTO sessions (
                session_id, started_at, last_activity_at, ended_at, model, working_dir,
                first_message_hash, prompt_correlation_hash, initial_prompt
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
            rusqlite::params![
                "session-current-jsonl-match",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:04Z",
                "2026-01-01T00:00:05Z",
                "claude-haiku-4-5-20251001",
                "/tmp/cc-blackbox-jsonl",
                super::hash_text_hex(&full_api_prompt),
                correlation::prompt_correlation_hash(visible_prompt),
            ],
        )
        .expect("insert session");
        for turn in 1..=2 {
            insert_request(
                &conn,
                &format!("req-current-jsonl-{turn}"),
                "session-current-jsonl-match",
                &format!("2026-01-01T00:00:0{turn}Z"),
                "claude-haiku-4-5-20251001",
                1_000,
                100,
                0,
                0,
            );
            insert_turn_snapshot(
                &conn,
                "session-current-jsonl-match",
                turn,
                &format!("2026-01-01T00:00:0{turn}Z"),
                0,
                0,
                100,
                0.0,
                0.02,
                None,
            );
        }

        let jsonl = format!(
            r#"{{"type":"custom-title","customTitle":"phase7-real","sessionId":"jsonl-current-shape"}}
{{"type":"user","sessionId":"jsonl-current-shape","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:00Z","message":{{"role":"user","content":"{visible_prompt}"}}}}
{{"type":"assistant","sessionId":"jsonl-current-shape","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:01Z","requestId":"req_current_1","message":{{"role":"assistant","content":[{{"type":"thinking","thinking":"RAW_THINKING_TEXT"}}],"usage":{{"input_tokens":10,"output_tokens":10}}}}}}
{{"type":"assistant","sessionId":"jsonl-current-shape","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:02Z","requestId":"req_current_1","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"toolu_1","name":"Read","input":{{"file_path":"/tmp/RAW_FILE_SECRET"}}}}],"usage":{{"input_tokens":10,"output_tokens":10}}}}}}
{{"type":"user","sessionId":"jsonl-current-shape","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:03Z","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"toolu_1","content":"RAW_TOOL_RESULT_SECRET"}}]}}}}
{{"type":"assistant","sessionId":"jsonl-current-shape","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:04Z","requestId":"req_current_2","message":{{"role":"assistant","content":[{{"type":"text","text":"RAW_FINAL_TEXT"}}],"usage":{{"input_tokens":5,"output_tokens":5}}}}}}
"#
        );
        fs::write(jsonl_dir.join("current.jsonl"), jsonl).expect("write current jsonl fixture");

        let json = build_postmortem_response_from_db(&conn, "session-current-jsonl-match", true)
            .expect("postmortem");

        assert_eq!(
            json.pointer("/enrichment_status/status")
                .and_then(|value| value.as_str()),
            Some("matched")
        );
        assert_eq!(
            json.pointer("/enrichment_status/jsonl_session_id")
                .and_then(|value| value.as_str()),
            Some("jsonl-current-shape")
        );
        assert_eq!(
            json.pointer("/signals/jsonl/request_count")
                .and_then(|value| value.as_u64()),
            Some(2)
        );
        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        for forbidden in [
            visible_prompt,
            "RAW_THINKING_TEXT",
            "RAW_FILE_SECRET",
            "RAW_TOOL_RESULT_SECRET",
            "RAW_FINAL_TEXT",
        ] {
            assert!(
                !rendered.contains(forbidden),
                "raw JSONL leaked: {rendered}"
            );
        }

        fs::remove_dir_all(jsonl_dir).expect("remove jsonl dir");
    }

    #[test]
    fn jsonl_session_resolution_maps_claude_session_to_live_proxy_session() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let jsonl_dir = std::env::temp_dir().join(format!(
            "cc-blackbox-live-jsonl-resolution-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&jsonl_dir).expect("create jsonl dir");
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            jsonl_dir.to_str().expect("jsonl dir is utf-8"),
        );
        let conn = create_full_test_db();
        let visible_prompt = "Investigate this live footer mapping";
        conn.execute(
            "INSERT INTO sessions (
                session_id, started_at, last_activity_at, model, working_dir,
                first_message_hash, prompt_correlation_hash, request_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                "session-live-proxy",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:02Z",
                "claude-sonnet-4-6",
                "/tmp/cc-blackbox-jsonl",
                super::hash_text_hex("full proxy prompt differs"),
                correlation::prompt_correlation_hash(visible_prompt),
                1,
            ],
        )
        .expect("insert live proxy session");
        let jsonl = format!(
            r#"{{"type":"user","sessionId":"jsonl-live-session","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:00Z","message":{{"role":"user","content":"{visible_prompt}"}}}}
{{"type":"assistant","sessionId":"jsonl-live-session","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:02Z","requestId":"req_live_1","message":{{"role":"assistant","content":[{{"type":"text","text":"working"}}],"usage":{{"input_tokens":10,"output_tokens":2}}}}}}
"#
        );
        fs::write(jsonl_dir.join("live.jsonl"), jsonl).expect("write live jsonl fixture");

        let response =
            super::build_jsonl_session_resolution_response_from_db(&conn, "jsonl-live-session")
                .expect("resolve jsonl session");

        assert_eq!(
            response.get("status").and_then(Value::as_str),
            Some("matched")
        );
        assert_eq!(
            response.get("proxy_session_id").and_then(Value::as_str),
            Some("session-live-proxy")
        );
        assert_eq!(
            response.get("source").and_then(Value::as_str),
            Some("live_jsonl_correlation")
        );
        let persisted_proxy_id: String = conn
            .query_row(
                "SELECT proxy_session_id FROM session_jsonl_correlations \
                 WHERE jsonl_session_id = ?1",
                rusqlite::params!["jsonl-live-session"],
                |row| row.get(0),
            )
            .expect("persisted live JSONL correlation");
        assert_eq!(persisted_proxy_id, "session-live-proxy");

        fs::remove_dir_all(jsonl_dir).expect("remove jsonl dir");
    }

    #[test]
    fn stored_jsonl_correlation_disambiguates_later_ambiguous_scan() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let jsonl_dir = std::env::temp_dir().join(format!(
            "cc-blackbox-jsonl-saved-correlation-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&jsonl_dir).expect("create jsonl dir");
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            jsonl_dir.to_str().expect("jsonl dir is utf-8"),
        );
        let conn = create_full_test_db();
        let raw_prompt = "RAW_SAVED_MAPPING_PROMPT_201 summarize structure";
        let first_message_hash = super::hash_text_hex(raw_prompt);
        conn.execute(
            "INSERT INTO sessions (
                session_id, started_at, last_activity_at, ended_at, model, working_dir,
                first_message_hash, initial_prompt
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
            rusqlite::params![
                "session-jsonl-saved-map",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:01:00Z",
                "2026-01-01T00:02:00Z",
                "claude-haiku-4-5-20251001",
                "/tmp/cc-blackbox-jsonl",
                first_message_hash,
            ],
        )
        .expect("insert session");
        insert_request(
            &conn,
            "req-jsonl-saved-map",
            "session-jsonl-saved-map",
            "2026-01-01T00:00:30Z",
            "claude-haiku-4-5-20251001",
            500,
            100,
            0,
            0,
        );
        insert_turn_snapshot(
            &conn,
            "session-jsonl-saved-map",
            1,
            "2026-01-01T00:01:00Z",
            0,
            0,
            100,
            0.0,
            0.02,
            None,
        );

        let jsonl_for = |jsonl_session_id: &str| {
            format!(
                r#"{{"type":"user","sessionId":"{jsonl_session_id}","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:00Z","message":{{"role":"user","content":[{{"type":"text","text":"{raw_prompt}"}}]}}}}
{{"type":"assistant","sessionId":"{jsonl_session_id}","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:01:00Z","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"toolu_1","name":"Glob","input":{{"pattern":"*.rs"}}}}],"usage":{{"input_tokens":10,"output_tokens":5}}}}}}
"#
            )
        };
        fs::write(jsonl_dir.join("one.jsonl"), jsonl_for("jsonl-saved-one"))
            .expect("write first jsonl fixture");

        let first_json = build_postmortem_response_from_db(&conn, "session-jsonl-saved-map", true)
            .expect("first postmortem");
        assert_eq!(
            first_json
                .pointer("/enrichment_status/jsonl_session_id")
                .and_then(|value| value.as_str()),
            Some("jsonl-saved-one")
        );

        fs::write(jsonl_dir.join("two.jsonl"), jsonl_for("jsonl-saved-two"))
            .expect("write second jsonl fixture");
        let second_json = build_postmortem_response_from_db(&conn, "session-jsonl-saved-map", true)
            .expect("second postmortem");

        assert_eq!(
            second_json
                .pointer("/enrichment_status/status")
                .and_then(|value| value.as_str()),
            Some("matched")
        );
        assert_eq!(
            second_json
                .pointer("/enrichment_status/jsonl_session_id")
                .and_then(|value| value.as_str()),
            Some("jsonl-saved-one")
        );
        assert!(second_json
            .pointer("/enrichment_status/candidate_count")
            .is_none());

        fs::remove_dir_all(jsonl_dir).expect("remove jsonl dir");
    }

    #[test]
    fn ambiguous_jsonl_match_stays_proxy_only_without_jsonl_findings() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let jsonl_dir = std::env::temp_dir().join(format!(
            "cc-blackbox-jsonl-ambiguous-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&jsonl_dir).expect("create jsonl dir");
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            jsonl_dir.to_str().expect("jsonl dir is utf-8"),
        );
        let conn = create_full_test_db();
        let raw_prompt = "RAW_AMBIGUOUS_PROMPT_91 isolate the flaky tool";
        let first_message_hash = super::hash_text_hex(raw_prompt);
        conn.execute(
            "INSERT INTO sessions (
                session_id, started_at, last_activity_at, ended_at, model, working_dir,
                first_message_hash, initial_prompt
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
            rusqlite::params![
                "session-jsonl-ambiguous",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:05:00Z",
                "2026-01-01T00:06:00Z",
                "claude-sonnet",
                "/tmp/cc-blackbox-jsonl",
                first_message_hash,
            ],
        )
        .expect("insert session");
        insert_request(
            &conn,
            "req-jsonl-ambiguous",
            "session-jsonl-ambiguous",
            "2026-01-01T00:01:00Z",
            "claude-sonnet",
            5_000,
            500,
            0,
            0,
        );
        for turn in 1..=5 {
            insert_turn_snapshot(
                &conn,
                "session-jsonl-ambiguous",
                turn,
                &format!("2026-01-01T00:0{turn}:00Z"),
                0,
                0,
                100,
                0.0,
                0.02,
                None,
            );
        }

        let mut jsonl = String::new();
        jsonl.push_str(&format!(
            r#"{{"type":"user","sessionId":"jsonl-ambiguous","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:00Z","message":{{"role":"user","content":[{{"type":"text","text":"{raw_prompt}"}}]}}}}"#
        ));
        jsonl.push('\n');
        for turn in 1..=5 {
            jsonl.push_str(&format!(
                r#"{{"type":"assistant","sessionId":"jsonl-ambiguous","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:0{turn}:00Z","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"toolu_{turn}","name":"Bash","input":{{"command":"cargo test -- RAW_AMBIGUOUS_COMMAND_92"}}}}]}}}}"#
            ));
            jsonl.push('\n');
            jsonl.push_str(&format!(
                r#"{{"type":"user","sessionId":"jsonl-ambiguous","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:0{turn}:30Z","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"toolu_{turn}","is_error":true,"content":"RAW_AMBIGUOUS_OUTPUT_93"}}]}}}}"#
            ));
            jsonl.push('\n');
        }
        fs::write(jsonl_dir.join("one.jsonl"), &jsonl).expect("write jsonl fixture one");
        fs::write(jsonl_dir.join("two.jsonl"), &jsonl).expect("write jsonl fixture two");

        let json = build_postmortem_response_from_db(&conn, "session-jsonl-ambiguous", true)
            .expect("postmortem");

        assert_eq!(
            json.pointer("/enrichment_status/status")
                .and_then(|value| value.as_str()),
            Some("ambiguous")
        );
        assert_eq!(
            json.pointer("/enrichment_status/candidate_count")
                .and_then(|value| value.as_u64()),
            Some(2)
        );
        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        assert!(!rendered.contains("jsonl_only_tool_failure_streak"));
        assert!(!rendered.contains("RAW_AMBIGUOUS_COMMAND_92"));
        assert!(!rendered.contains("RAW_AMBIGUOUS_OUTPUT_93"));

        fs::remove_dir_all(jsonl_dir).expect("remove jsonl dir");
    }

    #[test]
    fn active_session_postmortem_does_not_run_jsonl_enrichment() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let jsonl_dir = std::env::temp_dir().join(format!(
            "cc-blackbox-jsonl-active-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&jsonl_dir).expect("create jsonl dir");
        let _jsonl_dir = EnvVarGuard::set(
            "CC_BLACKBOX_JSONL_DIR",
            jsonl_dir.to_str().expect("jsonl dir is utf-8"),
        );
        let conn = create_full_test_db();
        let now = now_epoch_secs();
        let now_iso = epoch_to_iso8601(now);
        let raw_prompt = "RAW_ACTIVE_PROMPT_101 keep working";
        let first_message_hash = super::hash_text_hex(raw_prompt);
        conn.execute(
            "INSERT INTO sessions (
                session_id, started_at, last_activity_at, ended_at, model, working_dir,
                first_message_hash, initial_prompt
            ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, NULL)",
            rusqlite::params![
                "session-jsonl-active",
                now_iso,
                now_iso,
                "claude-sonnet",
                "/tmp/cc-blackbox-jsonl",
                first_message_hash,
            ],
        )
        .expect("insert active session");
        let jsonl = format!(
            r#"{{"type":"user","sessionId":"raw-jsonl-active-match-103","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"{now_iso}","message":{{"role":"user","content":[{{"type":"text","text":"{raw_prompt}"}}]}}}}
{{"type":"assistant","sessionId":"raw-jsonl-active-match-103","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"{now_iso}","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"toolu_1","name":"Bash","input":{{"command":"cargo test -- RAW_ACTIVE_COMMAND_102"}}}}]}}}}
"#
        );
        fs::write(jsonl_dir.join("active.jsonl"), jsonl).expect("write jsonl fixture");

        let json = build_postmortem_response_from_db(&conn, "session-jsonl-active", true)
            .expect("postmortem");

        assert_eq!(
            json.pointer("/enrichment_status/status")
                .and_then(|value| value.as_str()),
            Some("proxy_only")
        );
        assert_eq!(
            json.pointer("/enrichment_status/reason")
                .and_then(|value| value.as_str()),
            Some("session_active")
        );
        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        assert!(!rendered.contains("raw-jsonl-active-match-103"));
        assert!(!rendered.contains("RAW_ACTIVE_COMMAND_102"));

        fs::remove_dir_all(jsonl_dir).expect("remove jsonl dir");
    }

    #[test]
    fn postmortem_last_can_render_active_session_snapshot() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-active",
            "2026-01-01T00:00:00Z",
            None,
            "claude-sonnet",
            Some("Work on active session"),
        );
        insert_request(
            &conn,
            "req-active",
            "session-active",
            "2026-01-01T00:01:00Z",
            "claude-sonnet",
            10_000,
            500,
            0,
            1_000,
        );
        insert_turn_snapshot(
            &conn,
            "session-active",
            1,
            "2026-01-01T00:01:00Z",
            500,
            10_000,
            1_000,
            0.0,
            0.02,
            Some("Still active."),
        );

        let json = build_postmortem_response_from_db(&conn, "last", true).expect("postmortem");
        assert_eq!(
            json.get("session_id").and_then(|value| value.as_str()),
            Some("session-active")
        );
        assert_eq!(
            json.get("partial").and_then(|value| value.as_bool()),
            Some(true)
        );
        assert!(json
            .pointer("/summary/ended_at")
            .is_some_and(|value| value.is_null()));
        assert_eq!(
            json.pointer("/summary/outcome")
                .and_then(|value| value.as_str()),
            Some("In Progress")
        );
        assert_eq!(
            json.pointer("/diagnosis/degraded")
                .and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            json.pointer("/diagnosis/likely_cause")
                .and_then(|value| value.as_str()),
            Some("none")
        );
        assert_eq!(
            json.pointer("/diagnosis/detail")
                .and_then(|value| value.as_str()),
            Some("Session is still active; inactivity alone is not a degradation signal.")
        );
        let diagnosis_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_diagnoses WHERE session_id = 'session-active'",
                [],
                |row| row.get(0),
            )
            .expect("count active diagnosis rows");
        assert_eq!(diagnosis_rows, 0);
    }

    #[test]
    fn postmortem_context_signal_ignores_empty_usage_rows() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-context-zero",
            "2026-05-07T09:22:03Z",
            None,
            "claude-sonnet-4-6",
            Some("Keep working after empty usage rows"),
        );
        for (turn, input, cache_read, cache_create, fill) in [
            (1, 3, 59_000, 997, 0.30),
            (2, 1, 61_000, 999, 0.31),
            (3, 0, 0, 0, 0.0),
        ] {
            conn.execute(
                "INSERT INTO turn_snapshots (
                    session_id, turn_number, timestamp, input_tokens, cache_read_tokens,
                    cache_creation_tokens, output_tokens, ttft_ms, tool_calls, tool_failures,
                    gap_from_prev_secs, context_utilization, context_window_tokens,
                    frustration_signals, requested_model, actual_model
                ) VALUES ('session-context-zero', ?1, '2026-05-07T09:22:12Z',
                    ?2, ?3, ?4, 0, 1000, '[]', 0, 0.0, ?5, 200000, 0,
                    'claude-sonnet-4-6', 'claude-sonnet-4-6')",
                rusqlite::params![turn, input, cache_read, cache_create, fill],
            )
            .expect("insert turn snapshot");
        }

        let json = build_postmortem_response_from_db(&conn, "session-context-zero", true)
            .expect("postmortem");

        assert_eq!(
            json.pointer("/signals/context/latest_fill_percent")
                .and_then(|value| value.as_f64()),
            Some(31.0)
        );
        assert_eq!(
            json.pointer("/signals/context/max_fill_percent")
                .and_then(|value| value.as_f64()),
            Some(31.0)
        );
        assert_eq!(
            json.pointer("/signals/context/turns_to_compact")
                .and_then(|value| value.as_u64()),
            Some(54)
        );
        assert_eq!(
            json.pointer("/signals/context/context_window_tokens")
                .and_then(|value| value.as_u64()),
            Some(STANDARD_CONTEXT_WINDOW_TOKENS)
        );
    }

    #[test]
    fn request_persistence_updates_last_activity_without_ending_session() {
        let path = unique_test_db_path("active-last-activity");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));

        tx.send(DbCommand::InsertSession {
            session_id: "session-active-db".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            model: "claude-sonnet".to_string(),
            initial_prompt: Some("Keep working".to_string()),
            working_dir: "/tmp/cc-blackbox-test".to_string(),
            first_message_hash: "firstmsg_test".to_string(),
            prompt_correlation_hash: correlation::prompt_correlation_hash("Keep working"),
        })
        .expect("queue session insert");
        tx.send(test_record_request_command(
            "req-active-db",
            "session-active-db",
            "2026-01-01T00:02:00Z",
        ))
        .expect("queue request");
        tx.send(test_turn_snapshot_command(
            "session-active-db",
            1,
            "2026-01-01T00:02:00Z",
            "Still active.",
        ))
        .expect("queue turn snapshot");
        drop(tx);
        handle.join().expect("db writer exits");

        let conn = Connection::open(&path).expect("open test db");
        let (last_activity_at, ended_at): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT last_activity_at, ended_at FROM sessions WHERE session_id = ?1",
                rusqlite::params!["session-active-db"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query session lifecycle");
        assert_eq!(last_activity_at.as_deref(), Some("2026-01-01T00:02:00Z"));
        assert_eq!(ended_at, None);

        let json = build_postmortem_response_from_db(&conn, "session-active-db", true)
            .expect("postmortem");
        assert_eq!(
            json.get("partial").and_then(|value| value.as_bool()),
            Some(true)
        );
        assert!(json
            .pointer("/summary/ended_at")
            .is_some_and(|value| value.is_null()));
        assert_eq!(
            json.pointer("/summary/last_activity_at")
                .and_then(|value| value.as_str()),
            Some("2026-01-01T00:02:00Z")
        );

        cleanup_test_db(&path);
    }

    #[test]
    fn live_session_with_stale_final_artifacts_stays_partial() {
        let conn = create_full_test_db();
        let session_id = "session-live-stale-final";
        let _ = diagnosis::SESSION_TURNS.remove(session_id);

        insert_session(
            &conn,
            session_id,
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:05:00Z"),
            "claude-sonnet",
            Some("Keep going"),
        );
        insert_request(
            &conn,
            "req-live-stale-final",
            session_id,
            "2026-01-01T00:04:00Z",
            "claude-sonnet",
            1_000,
            100,
            0,
            0,
        );
        insert_turn_snapshot(
            &conn,
            session_id,
            1,
            "2026-01-01T00:04:00Z",
            0,
            0,
            100,
            0.0,
            0.02,
            Some("Live turn summary."),
        );
        insert_diagnosis(
            &conn,
            session_id,
            "2026-01-01T00:05:00Z",
            true,
            r#"[{"turn_first_noticed":1,"cause_type":"stale_final","detail":"This old final diagnosis must not load.","estimated_cost":0.0,"is_heuristic":false}]"#,
        );
        conn.execute(
            "INSERT INTO session_recall (session_id, initial_prompt, final_response_summary) \
             VALUES (?1, ?2, ?3)",
            rusqlite::params![session_id, "Keep going", "Stale final summary."],
        )
        .expect("insert stale recall");
        diagnosis::SESSION_TURNS.insert(
            session_id.to_string(),
            vec![test_live_turn(1, Some("Live turn summary."))],
        );

        let json = build_postmortem_response_from_db(&conn, session_id, false).expect("postmortem");

        assert_eq!(
            json.get("partial").and_then(|value| value.as_bool()),
            Some(true)
        );
        assert!(json
            .pointer("/summary/ended_at")
            .is_some_and(|value| value.is_null()));
        assert_eq!(
            json.pointer("/summary/outcome")
                .and_then(|value| value.as_str()),
            Some("In Progress")
        );
        assert_eq!(
            json.pointer("/diagnosis/likely_cause")
                .and_then(|value| value.as_str()),
            Some("none")
        );
        let final_response_summary = json
            .pointer("/summary/final_response_summary")
            .and_then(|value| value.as_str())
            .expect("final response summary");
        assert!(final_response_summary.contains("assistant response captured (redacted"));
        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        assert!(!rendered.contains("Live turn summary."));

        let _ = diagnosis::SESSION_TURNS.remove(session_id);
    }

    #[test]
    fn finalized_session_postmortem_is_final_with_recall_and_diagnosis() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-final",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:10:00Z"),
            "claude-sonnet",
            Some("Finish the feature"),
        );
        insert_request(
            &conn,
            "req-final",
            "session-final",
            "2026-01-01T00:05:00Z",
            "claude-sonnet",
            2_000,
            300,
            0,
            0,
        );
        insert_turn_snapshot(
            &conn,
            "session-final",
            1,
            "2026-01-01T00:05:00Z",
            0,
            0,
            100,
            0.0,
            0.02,
            Some("Final answer shipped."),
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Likely Completed', 1, 0.01, 0.0, 0, NULL, '[]', ?3)",
            rusqlite::params!["session-final", "2026-01-01T00:10:00Z", r#"["Done."]"#,],
        )
        .expect("insert diagnosis");
        conn.execute(
            "INSERT INTO session_recall (session_id, initial_prompt, final_response_summary) \
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                "session-final",
                "Finish the feature",
                "Final answer shipped."
            ],
        )
        .expect("insert recall");

        let json =
            build_postmortem_response_from_db(&conn, "session-final", false).expect("postmortem");
        assert_eq!(
            json.get("partial").and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            json.pointer("/summary/ended_at")
                .and_then(|value| value.as_str()),
            Some("2026-01-01T00:10:00Z")
        );
        let final_response_summary = json
            .pointer("/summary/final_response_summary")
            .and_then(|value| value.as_str())
            .expect("final response summary");
        assert!(final_response_summary.contains("final response summary captured (redacted"));
        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        assert!(!rendered.contains("Finish the feature"));
        assert!(!rendered.contains("Final answer shipped."));
        assert_eq!(
            json.pointer("/summary/outcome")
                .and_then(|value| value.as_str()),
            Some("Likely Completed")
        );
    }

    #[test]
    fn repair_persisted_session_artifacts_derives_legacy_raw_recall() {
        let conn = create_full_test_db();
        let ended_at =
            epoch_to_iso8601(now_epoch_secs().saturating_sub(session_timeout_secs() + 3_600));
        let raw_prompt = "Investigate payment token sk-ant-legacysecret";
        let raw_summary = "Assistant copied /Users/pradeep/private/source.rs into the answer.";

        insert_session(
            &conn,
            "session-legacy-raw-recall",
            &ended_at,
            Some(&ended_at),
            "claude-sonnet",
            Some(raw_prompt),
        );
        insert_turn_snapshot(
            &conn,
            "session-legacy-raw-recall",
            1,
            &ended_at,
            0,
            0,
            100,
            0.0,
            0.02,
            Some(raw_summary),
        );
        conn.execute(
            "INSERT INTO session_recall (session_id, initial_prompt, final_response_summary) \
             VALUES (?1, ?2, ?3)",
            rusqlite::params!["session-legacy-raw-recall", raw_prompt, raw_summary],
        )
        .expect("insert legacy raw recall");

        repair_persisted_session_artifacts(&conn).expect("repair persisted artifacts");

        let persisted = conn
            .query_row(
                "SELECT COALESCE(s.initial_prompt, '') || ' ' || \
                        COALESCE(r.initial_prompt, '') || ' ' || \
                        COALESCE(r.final_response_summary, '') \
                 FROM sessions s \
                 JOIN session_recall r ON r.session_id = s.session_id \
                 WHERE s.session_id = 'session-legacy-raw-recall'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("load repaired recall text");
        assert!(!persisted.contains(raw_prompt));
        assert!(!persisted.contains(raw_summary));
        assert!(persisted.contains("initial prompt captured (redacted"));
        assert!(persisted.contains("final response summary captured (redacted"));

        let json = build_postmortem_response_from_db(&conn, "session-legacy-raw-recall", false)
            .expect("postmortem");
        let rendered = serde_json::to_string(&json).expect("serialize postmortem");
        assert!(!rendered.contains(raw_prompt));
        assert!(!rendered.contains(raw_summary));
    }

    #[test]
    fn repair_persisted_session_artifacts_derives_legacy_api_error_cause_detail() {
        let conn = create_full_test_db();
        let ended_at =
            epoch_to_iso8601(now_epoch_secs().saturating_sub(session_timeout_secs() + 3_600));
        let raw_error = "server echoed /Users/pradeep/private/source.rs token=sk-ant-legacysecret";

        insert_session(
            &conn,
            "session-legacy-api-error",
            &ended_at,
            Some(&ended_at),
            "claude-sonnet",
            None,
        );
        insert_turn_snapshot(
            &conn,
            "session-legacy-api-error",
            1,
            &ended_at,
            0,
            0,
            100,
            0.0,
            0.02,
            None,
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Degraded', 1, 0.01, 0.0, 1, 1, ?3, '[]')",
            rusqlite::params![
                "session-legacy-api-error",
                ended_at,
                format!(
                    r#"[{{"turn_first_noticed":1,"cause_type":"api_error","detail":"Claude API streaming error at turn 1 (overloaded_error): {raw_error}","estimated_cost":0.0,"is_heuristic":false}}]"#
                ),
            ],
        )
        .expect("insert raw api error diagnosis");

        repair_persisted_session_artifacts(&conn).expect("repair persisted artifacts");

        let causes_json = conn
            .query_row(
                "SELECT causes_json FROM session_diagnoses WHERE session_id = 'session-legacy-api-error'",
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("load repaired causes");
        assert!(!causes_json.contains(raw_error));
        assert!(!causes_json.contains("/Users/pradeep/private/source.rs"));
        assert!(!causes_json.contains("sk-ant-legacysecret"));
        assert!(causes_json.contains("response error message captured as derived metadata"));
    }

    #[test]
    fn finalization_command_persists_artifacts_before_ack() {
        let path = unique_test_db_path("finalization-ack");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));

        tx.send(DbCommand::InsertSession {
            session_id: "session-ack".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            model: "claude-sonnet".to_string(),
            initial_prompt: Some("Summarize final state".to_string()),
            working_dir: "/tmp/cc-blackbox-test".to_string(),
            first_message_hash: "firstmsg_test".to_string(),
            prompt_correlation_hash: correlation::prompt_correlation_hash("Summarize final state"),
        })
        .expect("queue session insert");
        tx.send(test_record_request_command(
            "req-ack",
            "session-ack",
            "2026-01-01T00:03:00Z",
        ))
        .expect("queue request");
        tx.send(test_turn_snapshot_command(
            "session-ack",
            1,
            "2026-01-01T00:03:00Z",
            "Final persisted summary.",
        ))
        .expect("queue turn snapshot");

        let (response_tx, response_rx) = std_mpsc::channel();
        tx.send(DbCommand::FinalizeSession {
            session_id: "session-ack".to_string(),
            ended_at: "2026-01-01T00:04:00Z".to_string(),
            diagnosis: PersistedDiagnosis {
                completed_at: "2026-01-01T00:04:00Z".to_string(),
                outcome: "Likely Completed".to_string(),
                total_turns: 1,
                total_cost: 0.01,
                cache_hit_ratio: 0.0,
                degraded: false,
                degradation_turn: None,
                causes_json: "[]".to_string(),
                advice_json: r#"["Done."]"#.to_string(),
            },
            recall: Some(PersistedRecall {
                initial_prompt: "Summarize final state".to_string(),
                final_response_summary: "Final persisted summary.".to_string(),
            }),
            response_tx,
        })
        .expect("queue finalization");
        response_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("finalization ack")
            .expect("finalization ok");
        drop(tx);
        handle.join().expect("db writer exits");

        let conn = Connection::open(&path).expect("open test db");
        let json =
            build_postmortem_response_from_db(&conn, "session-ack", false).expect("postmortem");
        assert_eq!(
            json.get("partial").and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            json.pointer("/summary/ended_at")
                .and_then(|value| value.as_str()),
            Some("2026-01-01T00:04:00Z")
        );
        assert_eq!(
            json.pointer("/diagnosis/detail")
                .and_then(|value| value.as_str()),
            Some("No primary degradation cause was recorded.")
        );
        assert_eq!(
            json.pointer("/summary/final_response_summary")
                .and_then(|value| value.as_str()),
            Some("final response summary captured (redacted, 3 words, 24 chars).")
        );

        cleanup_test_db(&path);
    }

    #[test]
    fn failed_finalization_keeps_in_memory_turns_for_retry() {
        let session_id = "session-finalization-retry";
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        diagnosis::SESSION_TURNS.insert(
            session_id.to_string(),
            vec![test_live_turn(1, Some("Retryable final summary."))],
        );
        let (tx, rx) = std_mpsc::channel::<DbCommand>();
        drop(rx);

        let finalized = end_session_with_db_tx(
            session_id,
            Some("claude-sonnet".to_string()),
            Some("Retry finalization".to_string()),
            &tx,
        );

        assert!(!finalized);
        assert!(diagnosis::SESSION_TURNS.get(session_id).is_some());

        let _ = diagnosis::SESSION_TURNS.remove(session_id);
    }

    #[test]
    fn no_turn_timeout_finalization_clears_live_state() {
        let session_id = "session-no-turn-timeout";
        let hash = 0x7075_726e_u64;
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        let _ = diagnosis::SESSIONS.remove(&hash);
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 1.0,
                trusted_spend: 1.0,
                untrusted_spend: 0.0,
                total_tokens: 100,
                request_count: 1,
                estimated_cache_waste_dollars: 0.0,
            },
        );
        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: session_id.to_string(),
                display_name: "no-turn-timeout".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: Some("No turn yet".to_string()),
                created_at: Instant::now(),
                last_activity: Instant::now() - Duration::from_secs(3_600),
                session_inserted: false,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        let (tx, rx) = std_mpsc::channel::<DbCommand>();
        drop(rx);

        assert!(end_session_with_db_tx(
            session_id,
            Some("claude-sonnet".to_string()),
            Some("No turn yet".to_string()),
            &tx,
        ));
        assert!(diagnosis::SESSIONS.get(&hash).is_none());
        assert!(SESSION_BUDGETS.get(session_id).is_none());
    }

    #[test]
    fn timeout_expiration_rechecks_recent_activity_before_finalization() {
        let hash = 0x41c7_1017_u64;
        let session_id = "session-timeout-recheck";
        let _ = diagnosis::SESSIONS.remove(&hash);
        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: session_id.to_string(),
                display_name: "timeout-recheck".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: Some("Still active".to_string()),
                created_at: Instant::now(),
                last_activity: Instant::now() - Duration::from_secs(3_600),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        assert!(session_state_if_still_expired(hash, 60, Instant::now()).is_some());

        {
            let mut state = diagnosis::SESSIONS.get_mut(&hash).expect("session state");
            state.last_activity = Instant::now();
        }

        assert!(session_state_if_still_expired(hash, 60, Instant::now()).is_none());

        let _ = diagnosis::SESSIONS.remove(&hash);
    }

    #[test]
    fn successful_finalization_clears_live_marker_before_final_fetch() {
        let path = unique_test_db_path("finalization-live-marker");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));
        let session_id = "session-live-marker-final";
        let hash = 0x5155_10ff_u64;
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        let _ = diagnosis::SESSIONS.remove(&hash);

        tx.send(DbCommand::InsertSession {
            session_id: session_id.to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            model: "claude-sonnet".to_string(),
            initial_prompt: Some("Finish cleanly".to_string()),
            working_dir: "/tmp/cc-blackbox-test".to_string(),
            first_message_hash: "firstmsg_test".to_string(),
            prompt_correlation_hash: correlation::prompt_correlation_hash("Finish cleanly"),
        })
        .expect("queue session insert");
        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: session_id.to_string(),
                display_name: "live-marker-final".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: Some("Finish cleanly".to_string()),
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: false,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        diagnosis::SESSION_TURNS.insert(
            session_id.to_string(),
            vec![test_live_turn(1, Some("Final summary ready."))],
        );

        assert!(end_session_with_db_tx(
            session_id,
            Some("claude-sonnet".to_string()),
            Some("Finish cleanly".to_string()),
            &tx,
        ));
        assert!(diagnosis::SESSIONS.get(&hash).is_none());
        assert!(diagnosis::SESSION_TURNS.get(session_id).is_none());

        let conn = Connection::open(&path).expect("open test db");
        let json =
            build_postmortem_response_from_db(&conn, session_id, false).expect("final postmortem");
        assert_eq!(
            json.get("partial").and_then(|value| value.as_bool()),
            Some(false)
        );
        assert!(json
            .pointer("/summary/ended_at")
            .and_then(|value| value.as_str())
            .is_some());
        assert_eq!(
            json.pointer("/summary/final_response_summary")
                .and_then(|value| value.as_str()),
            Some("final response summary captured (redacted, 3 words, 20 chars).")
        );

        drop(tx);
        handle.join().expect("db writer exits");
        cleanup_test_db(&path);
    }

    #[test]
    fn explicit_session_finalization_persists_artifacts_and_is_idempotent() {
        let path = unique_test_db_path("explicit-finalization");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));
        let session_id = "session-explicit-finalization";
        let hash = 0x51d0_f1a4_u64;
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        let _ = diagnosis::SESSIONS.remove(&hash);

        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            insert_session(
                &conn,
                session_id,
                "2026-01-01T00:00:00Z",
                None,
                "claude-sonnet",
                Some("Implement the finalization API"),
            );
        }

        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: session_id.to_string(),
                display_name: "explicit-finalization".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: Some("Implement the finalization API".to_string()),
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        diagnosis::SESSION_TURNS.insert(
            session_id.to_string(),
            vec![test_live_turn(1, Some("Final response summary."))],
        );

        let response = super::finalize_sessions_with_db_tx(
            &[session_id.to_string()],
            "child_exit",
            &tx,
            &path,
        );
        assert_eq!(response.results[0].outcome, "finalized");
        assert!(diagnosis::SESSIONS.get(&hash).is_none());
        assert!(diagnosis::SESSION_TURNS.get(session_id).is_none());

        let conn = Connection::open(&path).expect("open finalized db");
        let ended_at: Option<String> = conn
            .query_row(
                "SELECT ended_at FROM sessions WHERE session_id = ?1",
                rusqlite::params![session_id],
                |row| row.get(0),
            )
            .expect("query ended_at");
        assert!(ended_at.is_some());
        let diagnosis_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_diagnoses WHERE session_id = ?1",
                rusqlite::params![session_id],
                |row| row.get(0),
            )
            .expect("count diagnoses");
        assert_eq!(diagnosis_rows, 1);
        let postmortem =
            build_postmortem_response_from_db(&conn, session_id, false).expect("postmortem");
        assert_eq!(
            postmortem.get("partial").and_then(|value| value.as_bool()),
            Some(false)
        );

        let retry = super::finalize_sessions_with_db_tx(
            &[session_id.to_string()],
            "child_exit",
            &tx,
            &path,
        );
        assert_eq!(retry.results[0].outcome, "already_finalized");

        let missing = super::finalize_sessions_with_db_tx(
            &["session-missing-finalization".to_string()],
            "child_exit",
            &tx,
            &path,
        );
        assert_eq!(missing.results[0].outcome, "not_found");

        drop(tx);
        handle.join().expect("db writer exits");
        cleanup_test_db(&path);
    }

    #[test]
    fn final_session_artifact_persistence_rolls_back_on_partial_failure() {
        let conn = Connection::open_in_memory().expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                last_activity_at TEXT,
                ended_at TEXT
            );
            CREATE TABLE session_recall (
                session_id TEXT PRIMARY KEY,
                initial_prompt TEXT,
                final_response_summary TEXT
            );
            INSERT INTO sessions (session_id, started_at, last_activity_at, ended_at)
            VALUES ('session-atomic', '2026-01-01T00:00:00Z', '2026-01-01T00:02:00Z', NULL);",
        )
        .expect("create partial schema");

        let err = persist_final_session_artifacts(
            &conn,
            "session-atomic",
            "2026-01-01T00:04:00Z",
            &PersistedDiagnosis {
                completed_at: "2026-01-01T00:04:00Z".to_string(),
                outcome: "Likely Completed".to_string(),
                total_turns: 1,
                total_cost: 0.01,
                cache_hit_ratio: 0.0,
                degraded: false,
                degradation_turn: None,
                causes_json: "[]".to_string(),
                advice_json: "[]".to_string(),
            },
            Some(&PersistedRecall {
                initial_prompt: "Prompt".to_string(),
                final_response_summary: "Summary".to_string(),
            }),
        )
        .expect_err("missing diagnoses table should fail");
        assert!(err.to_string().contains("session_diagnoses"));

        let ended_at: Option<String> = conn
            .query_row(
                "SELECT ended_at FROM sessions WHERE session_id = 'session-atomic'",
                [],
                |row| row.get(0),
            )
            .expect("query ended_at");
        assert_eq!(ended_at, None);

        let recall_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_recall", [], |row| row.get(0))
            .expect("count recall rows");
        assert_eq!(recall_rows, 0);
    }

    #[test]
    fn final_session_artifact_persistence_rejects_missing_session_row() {
        let conn = create_full_test_db();

        let err = persist_final_session_artifacts(
            &conn,
            "session-missing",
            "2026-01-01T00:04:00Z",
            &PersistedDiagnosis {
                completed_at: "2026-01-01T00:04:00Z".to_string(),
                outcome: "Likely Completed".to_string(),
                total_turns: 1,
                total_cost: 0.01,
                cache_hit_ratio: 0.0,
                degraded: false,
                degradation_turn: None,
                causes_json: "[]".to_string(),
                advice_json: "[]".to_string(),
            },
            Some(&PersistedRecall {
                initial_prompt: "Prompt".to_string(),
                final_response_summary: "Summary".to_string(),
            }),
        )
        .expect_err("missing session should fail");
        assert!(matches!(err, rusqlite::Error::QueryReturnedNoRows));

        let diagnosis_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_diagnoses", [], |row| {
                row.get(0)
            })
            .expect("count diagnosis rows");
        let recall_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_recall", [], |row| row.get(0))
            .expect("count recall rows");
        assert_eq!(diagnosis_rows, 0);
        assert_eq!(recall_rows, 0);
    }

    #[test]
    fn compaction_waste_uses_structured_cause_tokens_not_detail_digits() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-compaction",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:05:00Z"),
            "claude-sonnet",
            Some("Investigate compaction"),
        );
        insert_request(
            &conn,
            "req-compaction",
            "session-compaction",
            "2026-01-01T00:01:00Z",
            "claude-sonnet",
            1_000,
            100,
            0,
            0,
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Compaction Suspected', 6, 1.0, 0.5, 1, 4, ?3, '[]')",
            rusqlite::params![
                "session-compaction",
                "2026-01-01T00:05:00Z",
                r#"[{"turn_first_noticed":4,"cause_type":"compaction_suspected","detail":"Rapid-fire requests at turns 4-6; prose number 999 should not be parsed.","estimated_cost":0.0,"estimated_wasted_tokens":321000,"is_heuristic":true}]"#,
            ],
        )
        .expect("insert compaction diagnosis");

        let json = build_postmortem_response_from_db(&conn, "session-compaction", true)
            .expect("postmortem");
        assert_eq!(
            json.pointer("/diagnosis/likely_cause_is_heuristic")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
        assert_eq!(
            json.pointer("/diagnosis/detail")
                .and_then(|value| value.as_str()),
            Some("[heuristic] Rapid-fire requests at turns 4-6; prose number 999 should not be parsed.")
        );
        assert_eq!(
            json.pointer("/impact/estimated_likely_wasted_tokens")
                .and_then(|value| value.as_u64()),
            Some(321_000)
        );
    }

    #[test]
    fn compaction_waste_does_not_double_count_overlapping_cache_rebuild_tokens() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-compaction-overlap",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:05:00Z"),
            "claude-sonnet",
            Some("Investigate compaction overlap"),
        );
        insert_request(
            &conn,
            "req-compaction-overlap",
            "session-compaction-overlap",
            "2026-01-01T00:01:00Z",
            "claude-sonnet",
            1_000,
            100,
            0,
            350,
        );
        insert_turn_snapshot(
            &conn,
            "session-compaction-overlap",
            2,
            "2026-01-01T00:02:00Z",
            0,
            50,
            100,
            0.0,
            0.70,
            Some("Cache rebuild outside compaction loop."),
        );
        insert_turn_snapshot(
            &conn,
            "session-compaction-overlap",
            5,
            "2026-01-01T00:03:00Z",
            0,
            100,
            100,
            1.0,
            0.80,
            Some("Compaction loop repeated work."),
        );
        insert_turn_snapshot(
            &conn,
            "session-compaction-overlap",
            6,
            "2026-01-01T00:04:00Z",
            0,
            200,
            100,
            1.0,
            0.81,
            Some("Compaction loop repeated work again."),
        );
        conn.execute(
            "INSERT INTO session_diagnoses (
                session_id, completed_at, outcome, total_turns, total_cost, cache_hit_ratio,
                degraded, degradation_turn, causes_json, advice_json
            ) VALUES (?1, ?2, 'Compaction Suspected', 6, 1.0, 0.5, 1, 4, ?3, '[]')",
            rusqlite::params![
                "session-compaction-overlap",
                "2026-01-01T00:05:00Z",
                r#"[{"turn_first_noticed":4,"turn_last_noticed":6,"cause_type":"compaction_suspected","detail":"Rapid-fire requests at turns 4-6.","estimated_cost":0.0,"estimated_wasted_tokens":1000,"is_heuristic":true}]"#,
            ],
        )
        .expect("insert compaction diagnosis");

        let json = build_postmortem_response_from_db(&conn, "session-compaction-overlap", true)
            .expect("postmortem");
        assert_eq!(
            json.pointer("/impact/estimated_likely_wasted_tokens")
                .and_then(|value| value.as_u64()),
            Some(1_050)
        );
    }

    #[test]
    fn in_memory_postmortem_totals_summarize_active_turns() {
        let session_id = "session-idle-totals";
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        diagnosis::SESSION_TURNS.insert(
            session_id.to_string(),
            vec![
                diagnosis::TurnSnapshot {
                    turn_number: 1,
                    timestamp: Instant::now(),
                    input_tokens: 100,
                    cache_read_tokens: 50,
                    cache_creation_tokens: 10,
                    cache_ttl_min_secs: 300,
                    cache_ttl_max_secs: 300,
                    cache_ttl_source: "default_5m".to_string(),
                    output_tokens: 20,
                    ttft_ms: 100,
                    tool_calls: vec![],
                    tool_results_failed: 0,
                    gap_from_prev_secs: 0.0,
                    context_utilization: 0.1,
                    context_window_tokens: STANDARD_CONTEXT_WINDOW_TOKENS,
                    frustration_signals: 0,
                    requested_model: None,
                    actual_model: None,
                    response_summary: None,
                    response_error_type: None,
                    response_error_message: None,
                },
                diagnosis::TurnSnapshot {
                    turn_number: 2,
                    timestamp: Instant::now(),
                    input_tokens: 200,
                    cache_read_tokens: 100,
                    cache_creation_tokens: 0,
                    cache_ttl_min_secs: 300,
                    cache_ttl_max_secs: 300,
                    cache_ttl_source: "default_5m".to_string(),
                    output_tokens: 30,
                    ttft_ms: 120,
                    tool_calls: vec![],
                    tool_results_failed: 0,
                    gap_from_prev_secs: 5.0,
                    context_utilization: 0.2,
                    context_window_tokens: STANDARD_CONTEXT_WINDOW_TOKENS,
                    frustration_signals: 0,
                    requested_model: None,
                    actual_model: None,
                    response_summary: None,
                    response_error_type: None,
                    response_error_message: None,
                },
            ],
        );

        assert_eq!(in_memory_postmortem_totals(session_id), Some((510, 2)));
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        assert_eq!(in_memory_postmortem_totals(session_id), None);
    }

    #[test]
    fn trusted_dollar_spend_can_block_session_budget() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let _dollar_budget = EnvVarGuard::set("CC_BLACKBOX_SESSION_BUDGET_DOLLARS", "10");
        let _token_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_TOKENS");
        let session_id = "session-trusted-budget";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 12.0,
                trusted_spend: 12.0,
                untrusted_spend: 0.0,
                total_tokens: 1,
                request_count: 2,
                estimated_cache_waste_dollars: 0.25,
            },
        );

        let policy = request_guard_policy().expect("default policy loads");
        let decision = evaluate_request_guard_for_session(Some(session_id), Some(&policy))
            .expect("budget blocks");
        let block = decision.block.expect("block metadata");
        assert_eq!(block.error_type, "budget_exceeded");
        assert_eq!(
            block.rule_id,
            guard::RuleId::PerSessionTrustedDollarBudgetExceeded
        );
        assert_eq!(block.current_observed_value, "$12.00 trusted spend");
        assert!(decision
            .finding
            .expect("finding")
            .detail
            .contains("display-only untrusted spend is $0.00"));

        SESSION_BUDGETS.remove(session_id);
    }

    #[test]
    fn untrusted_dollar_spend_cannot_hard_block_session_budget() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let _dollar_budget = EnvVarGuard::set("CC_BLACKBOX_SESSION_BUDGET_DOLLARS", "10");
        let _token_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_TOKENS");
        let session_id = "session-untrusted-budget";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 50.0,
                trusted_spend: 0.0,
                untrusted_spend: 50.0,
                total_tokens: 1,
                request_count: 3,
                estimated_cache_waste_dollars: 1.0,
            },
        );

        let policy = request_guard_policy().expect("default policy loads");
        assert!(evaluate_request_guard_for_session(Some(session_id), Some(&policy)).is_none());

        SESSION_BUDGETS.remove(session_id);
    }

    #[test]
    fn token_budget_enforcement_still_blocks() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let _dollar_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_DOLLARS");
        let _token_budget = EnvVarGuard::set("CC_BLACKBOX_SESSION_BUDGET_TOKENS", "100");
        let session_id = "session-token-budget";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 0.0,
                trusted_spend: 0.0,
                untrusted_spend: 0.0,
                total_tokens: 100,
                request_count: 1,
                estimated_cache_waste_dollars: 0.0,
            },
        );

        let policy = request_guard_policy().expect("default policy loads");
        let decision = evaluate_request_guard_for_session(Some(session_id), Some(&policy))
            .expect("token budget blocks");
        let block = decision.block.expect("block metadata");
        assert_eq!(block.error_type, "budget_exceeded");
        assert_eq!(block.rule_id, guard::RuleId::PerSessionTokenBudgetExceeded);
        assert_eq!(block.configured_threshold_or_limit, "100 tokens");

        SESSION_BUDGETS.remove(session_id);
    }

    #[test]
    fn token_budget_block_finalizes_affected_session() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let _dollar_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_DOLLARS");
        let _token_budget = EnvVarGuard::set("CC_BLACKBOX_SESSION_BUDGET_TOKENS", "100");
        let path = unique_test_db_path("token-budget-finalization");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));
        let session_id = "session-token-budget-final";
        let hash = 0x7b0d_9e7u64;
        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        let _ = diagnosis::SESSIONS.remove(&hash);

        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            insert_session(
                &conn,
                session_id,
                "2026-01-01T00:00:00Z",
                None,
                "claude-sonnet",
                Some("Stay within budget"),
            );
        }
        diagnosis::SESSIONS.insert(
            hash,
            diagnosis::SessionState {
                session_id: session_id.to_string(),
                display_name: "token-budget-final".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: Some("Stay within budget".to_string()),
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        diagnosis::SESSION_TURNS.insert(
            session_id.to_string(),
            vec![test_live_turn(1, Some("Budgeted response."))],
        );
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 0.0,
                trusted_spend: 0.0,
                untrusted_spend: 0.0,
                total_tokens: 100,
                request_count: 1,
                estimated_cache_waste_dollars: 0.0,
            },
        );

        let policy = request_guard_policy().expect("default policy loads");
        let decision = evaluate_request_guard_for_session(Some(session_id), Some(&policy))
            .expect("token budget blocks");
        let block = decision.block.expect("block metadata");
        let result =
            super::finalize_terminal_budget_block_with_db_tx(&block, &tx, &path).expect("result");

        assert_eq!(result.outcome, "finalized");
        assert!(diagnosis::SESSIONS.get(&hash).is_none());
        assert!(diagnosis::SESSION_TURNS.get(session_id).is_none());
        assert!(SESSION_BUDGETS.get(session_id).is_none());

        let conn = Connection::open(&path).expect("open finalized db");
        let (ended_at, outcome): (Option<String>, String) = conn
            .query_row(
                "SELECT s.ended_at, d.outcome \
                 FROM sessions s JOIN session_diagnoses d ON d.session_id = s.session_id \
                 WHERE s.session_id = ?1",
                rusqlite::params![session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query final state");
        assert!(ended_at.is_some());
        assert_eq!(outcome, "Budget Exceeded");

        drop(tx);
        handle.join().expect("db writer exits");
        cleanup_test_db(&path);
    }

    #[test]
    fn trusted_dollar_budget_block_finalizes_affected_session() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let path = unique_test_db_path("trusted-budget-finalization");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));
        let session_id = "session-trusted-budget-final";
        let hash = 0x7b0d_7011u64;
        insert_live_session_for_finalization(&path, session_id, hash, "Stay within dollar budget");
        let block = guard::GuardBlock {
            error_type: "budget_exceeded".to_string(),
            reason: "Session trusted-dollar budget exceeded.".to_string(),
            rule_id: guard::RuleId::PerSessionTrustedDollarBudgetExceeded,
            configured_threshold_or_limit: "$10.00 trusted spend".to_string(),
            current_observed_value: "$12.00 trusted spend".to_string(),
            session_id: Some(session_id.to_string()),
            suggested_next_action: "Start a fresh session.".to_string(),
            cooldown_remaining_secs: None,
        };

        let result =
            super::finalize_terminal_budget_block_with_db_tx(&block, &tx, &path).expect("result");

        assert_eq!(result.outcome, "finalized");
        let conn = Connection::open(&path).expect("open finalized db");
        let outcome: String = conn
            .query_row(
                "SELECT outcome FROM session_diagnoses WHERE session_id = ?1",
                rusqlite::params![session_id],
                |row| row.get(0),
            )
            .expect("query diagnosis outcome");
        assert_eq!(outcome, "Budget Exceeded");
        assert!(diagnosis::SESSIONS.get(&hash).is_none());

        drop(tx);
        handle.join().expect("db writer exits");
        cleanup_test_db(&path);
    }

    #[test]
    fn api_cooldown_block_does_not_finalize_an_arbitrary_session() {
        let path = unique_test_db_path("cooldown-no-finalization");
        let (tx, rx) = std_mpsc::channel();
        let writer_path = path.clone();
        let handle = std::thread::spawn(move || db_writer_loop(&writer_path, rx));
        let session_id = "session-cooldown-not-final";
        let hash = 0x7b0d_c001u64;
        insert_live_session_for_finalization(&path, session_id, hash, "Wait for cooldown");
        let block = guard::GuardBlock {
            error_type: "circuit_breaker".to_string(),
            reason: "API error cooldown active.".to_string(),
            rule_id: guard::RuleId::ApiErrorCircuitBreakerCooldown,
            configured_threshold_or_limit: "2 consecutive API errors; cooldown 60s".to_string(),
            current_observed_value: "60s remaining".to_string(),
            session_id: Some(session_id.to_string()),
            suggested_next_action: "Wait for the cooldown to expire.".to_string(),
            cooldown_remaining_secs: Some(60),
        };

        assert!(super::finalize_terminal_budget_block_with_db_tx(&block, &tx, &path).is_none());
        assert!(diagnosis::SESSIONS.get(&hash).is_some());
        let conn = Connection::open(&path).expect("open db");
        let ended_at: Option<String> = conn
            .query_row(
                "SELECT ended_at FROM sessions WHERE session_id = ?1",
                rusqlite::params![session_id],
                |row| row.get(0),
            )
            .expect("query ended_at");
        assert!(ended_at.is_none());

        let _ = diagnosis::SESSION_TURNS.remove(session_id);
        let _ = diagnosis::SESSIONS.remove(&hash);
        drop(tx);
        handle.join().expect("db writer exits");
        cleanup_test_db(&path);
    }

    #[test]
    fn request_guard_token_budget_builds_structured_block_response() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let session_id = "session-structured-token-block";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 0.0,
                trusted_spend: 0.0,
                untrusted_spend: 0.0,
                total_tokens: 100,
                request_count: 1,
                estimated_cache_waste_dollars: 0.0,
            },
        );
        let mut policy = guard::GuardPolicy::default();
        policy
            .rules
            .get_mut(&guard::RuleId::PerSessionTokenBudgetExceeded)
            .expect("default token rule exists")
            .limit_tokens = Some(100);

        let decision = evaluate_request_guard_for_session(Some(session_id), Some(&policy))
            .expect("request should block");
        let response = make_guard_block_response(decision.block.as_ref().expect("block metadata"));
        let immediate = match response.response.expect("immediate response") {
            ExtProcResponse::ImmediateResponse(response) => response,
            other => panic!("expected immediate response, got {other:?}"),
        };
        assert_eq!(
            immediate.status.expect("status").code,
            crate::envoy::r#type::v3::StatusCode::PaymentRequired as i32
        );
        let body = immediate.body;
        let json: serde_json::Value = serde_json::from_str(&body).expect("valid json block body");

        assert_eq!(json["error"]["type"], "budget_exceeded");
        let message = json["error"]["message"].as_str().expect("message");
        assert_eq!(
            json["error"]["rule_id"],
            "per_session_token_budget_exceeded"
        );
        assert_eq!(json["error"]["reason"], "Session token budget exceeded.");
        assert_eq!(json["error"]["configured_threshold_or_limit"], "100 tokens");
        assert_eq!(json["error"]["current_observed_value"], "100 tokens");
        assert_eq!(json["error"]["session_id"], session_id);
        assert!(json["error"]["suggested_next_action"]
            .as_str()
            .expect("suggested action")
            .contains("fresh session"));
        assert!(message.contains("per_session_token_budget_exceeded"));
        assert!(message.contains("100 tokens"));
        assert!(message.contains(session_id));
        assert!(message.contains("recovery:"));

        for forbidden in [
            "raw_prompt",
            "prompt",
            "messages",
            "assistant_text",
            "tool_output",
            "file_contents",
        ] {
            assert!(
                !body.contains(forbidden),
                "block response leaked content field {forbidden}"
            );
        }

        SESSION_BUDGETS.remove(session_id);
    }

    #[test]
    fn guard_block_decision_serializes_watch_event_without_raw_content() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let session_id = "session-guard-event";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 0.0,
                trusted_spend: 0.0,
                untrusted_spend: 0.0,
                total_tokens: 100,
                request_count: 1,
                estimated_cache_waste_dollars: 0.0,
            },
        );
        let mut policy = guard::GuardPolicy::default();
        policy
            .rules
            .get_mut(&guard::RuleId::PerSessionTokenBudgetExceeded)
            .expect("default token rule exists")
            .limit_tokens = Some(100);

        let decision = evaluate_request_guard_for_session(Some(session_id), Some(&policy))
            .expect("request should block");
        let event = guard_finding_watch_event(decision.finding.expect("proxy finding"));
        let json = serde_json::to_string(&event).expect("guard finding event serializes");

        assert!(json.contains(r#""type":"guard_finding""#));
        assert!(json.contains(r#""rule_id":"per_session_token_budget_exceeded""#));
        assert!(json.contains(r#""action":"block""#));
        assert!(json.contains(r#""evidence_level":"direct_proxy""#));
        assert!(json.contains(r#""source":"proxy""#));
        for forbidden in [
            "raw_prompt",
            "prompt",
            "messages",
            "assistant_text",
            "tool_output",
            "file_contents",
            "raw_jsonl",
        ] {
            assert!(
                !json.contains(forbidden),
                "guard finding event leaked content field {forbidden}"
            );
        }

        SESSION_BUDGETS.remove(session_id);
    }

    #[test]
    fn cache_rebuild_waste_does_not_block_under_default_policy() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let session_id = "session-cache-default-allow";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 20.0,
                trusted_spend: 20.0,
                untrusted_spend: 0.0,
                total_tokens: 20_000,
                request_count: 5,
                estimated_cache_waste_dollars: 19.0,
            },
        );
        let policy = guard::GuardPolicy::default();

        assert!(evaluate_request_guard_for_session(Some(session_id), Some(&policy)).is_none());

        SESSION_BUDGETS.remove(session_id);
    }

    #[test]
    fn request_guard_policy_preserves_legacy_env_token_budget() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let _token_budget = EnvVarGuard::set("CC_BLACKBOX_SESSION_BUDGET_TOKENS", "100");
        let _dollar_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_DOLLARS");
        let _policy_path = EnvVarGuard::remove("CC_BLACKBOX_GUARD_POLICY_PATH");
        let session_id = "session-env-token-policy";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 0.0,
                trusted_spend: 0.0,
                untrusted_spend: 0.0,
                total_tokens: 100,
                request_count: 1,
                estimated_cache_waste_dollars: 0.0,
            },
        );

        let policy = request_guard_policy().expect("default policy loads");
        let decision = evaluate_request_guard_for_session(Some(session_id), Some(&policy))
            .expect("legacy env token budget should block");

        assert_eq!(
            decision.block.expect("block").rule_id,
            guard::RuleId::PerSessionTokenBudgetExceeded
        );

        SESSION_BUDGETS.remove(session_id);
    }

    #[test]
    fn guard_policy_report_includes_defaults_source_effective_policy_and_warnings() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let path = unique_temp_path("guard-policy-report");
        fs::write(
            &path,
            r#"
unexpected_root = true

[rules.per_session_token_budget_exceeded]
limit_tokens = 321000

[rules.repeated_cache_rebuilds]
action = "warn"
"#,
        )
        .expect("write policy fixture");
        let _policy_path = EnvVarGuard::set(
            "CC_BLACKBOX_GUARD_POLICY_PATH",
            path.to_str().expect("temp path is utf-8"),
        );
        let _token_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_TOKENS");
        let _dollar_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_DOLLARS");

        let report = build_guard_policy_report_json();

        assert_eq!(report["source"], path.to_string_lossy().as_ref());
        assert_eq!(report["defaults"]["fail_open"], true);
        assert_eq!(report["policy"]["fail_open"], true);
        assert_eq!(
            report["policy"]["rules"]["per_session_token_budget_exceeded"]["action"],
            "block"
        );
        assert_eq!(
            report["policy"]["rules"]["per_session_token_budget_exceeded"]["limit_tokens"],
            321000
        );
        assert!(report["warnings"]
            .as_array()
            .expect("warnings array")
            .iter()
            .any(|warning| warning
                .as_str()
                .unwrap_or_default()
                .contains("unexpected_root")));

        fs::remove_file(path).expect("remove policy fixture");
    }

    #[test]
    fn guard_status_report_derives_blocked_state_from_active_findings() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let path = unique_test_db_path("guard-status-report");
        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            let finding_ts = epoch_to_iso8601(now_epoch_secs());
            conn.execute(
                "INSERT INTO sessions (session_id, started_at, model, working_dir, first_message_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "session-status-block",
                    "2026-05-10T00:00:00Z",
                    "claude-sonnet",
                    "/tmp/project",
                    "hash-status"
                ],
            )
            .expect("insert session");
            conn.execute(
                "INSERT INTO guard_findings (
                    session_id, request_id, turn_number, timestamp, rule_id, severity,
                    action, source, evidence_level, confidence, detail, suggested_action
                ) VALUES (?1,NULL,NULL,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                rusqlite::params![
                    "session-status-block",
                    finding_ts,
                    "per_session_token_budget_exceeded",
                    "critical",
                    "block",
                    "proxy",
                    "direct_proxy",
                    1.0,
                    "Session token budget exceeded.",
                    "Start a fresh session."
                ],
            )
            .expect("insert finding");
        }
        let _db_path = EnvVarGuard::set("CC_BLACKBOX_DB_PATH", &path);
        diagnosis::SESSIONS.insert(
            4242,
            diagnosis::SessionState {
                session_id: "session-status-block".to_string(),
                display_name: "api".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: Some("redacted prompt excerpt".to_string()),
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );

        let report = build_guard_status_report_json(Some("session-status-block"));

        assert_eq!(report["overall_state"], "blocked");
        assert_eq!(report["sessions"][0]["session_id"], "session-status-block");
        assert_eq!(report["sessions"][0]["state"], "blocked");
        assert_eq!(
            report["sessions"][0]["findings"][0]["rule_id"],
            "per_session_token_budget_exceeded"
        );
        assert_eq!(report["sessions"][0]["findings"][0]["action"], "block");
        assert!(report["sessions"][0]["findings"][0]
            .get("raw_prompt")
            .is_none());
        assert!(report["sessions"][0]["findings"][0]
            .get("raw_jsonl")
            .is_none());

        diagnosis::SESSIONS.remove(&4242);
        cleanup_test_db(&path);
    }

    #[test]
    fn guard_status_report_includes_active_cache_event_signal() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let path = unique_test_db_path("guard-status-cache-event");
        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            conn.execute(
                "INSERT INTO sessions (session_id, started_at, model, working_dir, first_message_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "session-status-cache",
                    "2026-05-10T00:00:00Z",
                    "claude-sonnet",
                    "/tmp/project",
                    "hash-cache"
                ],
            )
            .expect("insert session");
        }
        let _db_path = EnvVarGuard::set("CC_BLACKBOX_DB_PATH", &path);
        diagnosis::SESSIONS.insert(
            4747,
            diagnosis::SessionState {
                session_id: "session-status-cache".to_string(),
                display_name: "api".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        super::CACHE_TRACKERS.insert(
            "session-status-cache".to_string(),
            CacheTracker {
                consecutive_misses: 0,
                last_request_time: Some(Instant::now()),
                last_ttl_min_secs: 300,
                last_ttl_max_secs: 3600,
                last_event_type: "miss_rebuild",
                last_event_time: Some(Instant::now()),
                last_rebuild_cost_dollars: Some(0.24),
            },
        );

        let report = build_guard_status_report_json(Some("session-status-cache"));
        let cache = &report["sessions"][0]["signals"]["cache"];

        assert_eq!(cache["event_type"], "miss_rebuild");
        assert_eq!(cache["estimated_rebuild_cost_dollars"], 0.24);
        assert!(cache["event_age_secs"].as_u64().is_some());

        diagnosis::SESSIONS.remove(&4747);
        super::CACHE_TRACKERS.remove("session-status-cache");
        cleanup_test_db(&path);
    }

    #[test]
    fn guard_status_report_omits_uninserted_placeholder_sessions() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let path = unique_test_db_path("guard-status-placeholder");
        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            conn.execute(
                "INSERT INTO sessions (session_id, started_at, model, working_dir, first_message_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "session-status-real",
                    "2026-05-10T00:00:00Z",
                    "claude-haiku",
                    "/tmp/project",
                    "hash-real"
                ],
            )
            .expect("insert persisted session");
        }
        let _db_path = EnvVarGuard::set("CC_BLACKBOX_DB_PATH", &path);
        diagnosis::SESSIONS.insert(
            4545,
            diagnosis::SessionState {
                session_id: "session-status-real".to_string(),
                display_name: "project".to_string(),
                model: "claude-haiku".to_string(),
                initial_prompt: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );
        diagnosis::SESSIONS.insert(
            4646,
            diagnosis::SessionState {
                session_id: "session-status-placeholder".to_string(),
                display_name: "project-dup".to_string(),
                model: "claude-haiku".to_string(),
                initial_prompt: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: false,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );

        let report = build_guard_status_report_json(None);
        let sessions = report["sessions"].as_array().expect("sessions array");

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0]["session_id"], "session-status-real");

        let filtered = build_guard_status_report_json(Some("session-status-placeholder"));
        assert_eq!(filtered["overall_state"], "healthy");
        assert!(filtered["sessions"]
            .as_array()
            .expect("filtered sessions array")
            .is_empty());

        diagnosis::SESSIONS.remove(&4545);
        diagnosis::SESSIONS.remove(&4646);
        cleanup_test_db(&path);
    }

    #[test]
    fn guard_status_report_ignores_expired_findings_for_active_state() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let path = unique_test_db_path("guard-status-expired-finding");
        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            conn.execute(
                "INSERT INTO sessions (session_id, started_at, model, working_dir, first_message_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "session-status-expired",
                    "2000-01-01T00:00:00Z",
                    "claude-sonnet",
                    "/tmp/project",
                    "hash-expired"
                ],
            )
            .expect("insert session");
            conn.execute(
                "INSERT INTO guard_findings (
                    session_id, request_id, turn_number, timestamp, rule_id, severity,
                    action, source, evidence_level, confidence, detail, suggested_action
                ) VALUES (?1,NULL,NULL,?2,?3,?4,?5,?6,?7,?8,?9,NULL)",
                rusqlite::params![
                    "session-status-expired",
                    "2000-01-01T00:01:00Z",
                    "repeated_cache_rebuilds",
                    "warning",
                    "warn",
                    "proxy",
                    "direct_proxy",
                    0.9,
                    "Old cache rebuild warning."
                ],
            )
            .expect("insert old finding");
        }
        let _db_path = EnvVarGuard::set("CC_BLACKBOX_DB_PATH", &path);
        diagnosis::SESSIONS.insert(
            4343,
            diagnosis::SessionState {
                session_id: "session-status-expired".to_string(),
                display_name: "api".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );

        let report = build_guard_status_report_json(Some("session-status-expired"));

        assert_eq!(report["sessions"][0]["state"], "watching");
        assert_eq!(
            report["sessions"][0]["findings"]
                .as_array()
                .expect("findings")
                .len(),
            0
        );

        diagnosis::SESSIONS.remove(&4343);
        cleanup_test_db(&path);
    }

    #[test]
    fn guard_status_report_resolves_active_findings_with_current_policy() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let path = unique_test_db_path("guard-status-policy-change");
        let policy_path = unique_temp_path("guard-status-policy-change");
        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            let finding_ts = epoch_to_iso8601(now_epoch_secs());
            conn.execute(
                "INSERT INTO sessions (session_id, started_at, model, working_dir, first_message_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "session-status-policy-change",
                    finding_ts,
                    "claude-sonnet",
                    "/tmp/project",
                    "hash-policy-change"
                ],
            )
            .expect("insert session");
            conn.execute(
                "INSERT INTO guard_findings (
                    session_id, request_id, turn_number, timestamp, rule_id, severity,
                    action, source, evidence_level, confidence, detail, suggested_action
                ) VALUES (?1,NULL,NULL,?2,?3,?4,?5,?6,?7,?8,?9,NULL)",
                rusqlite::params![
                    "session-status-policy-change",
                    finding_ts,
                    "repeated_cache_rebuilds",
                    "warning",
                    "warn",
                    "proxy",
                    "direct_proxy",
                    0.9,
                    "Cache rebuild warning."
                ],
            )
            .expect("insert finding");
        }
        fs::write(
            &policy_path,
            r#"
[rules.repeated_cache_rebuilds]
action = "diagnose_only"
"#,
        )
        .expect("write policy fixture");

        let _db_path = EnvVarGuard::set("CC_BLACKBOX_DB_PATH", &path);
        let _policy_path = EnvVarGuard::set(
            "CC_BLACKBOX_GUARD_POLICY_PATH",
            policy_path.to_str().expect("temp path is utf-8"),
        );
        diagnosis::SESSIONS.insert(
            4444,
            diagnosis::SessionState {
                session_id: "session-status-policy-change".to_string(),
                display_name: "api".to_string(),
                model: "claude-sonnet".to_string(),
                initial_prompt: None,
                created_at: Instant::now(),
                last_activity: Instant::now(),
                session_inserted: true,
                cache_warning_sent: false,
                idle_postmortem_sent: false,
            },
        );

        let report = build_guard_status_report_json(Some("session-status-policy-change"));

        assert_eq!(report["sessions"][0]["state"], "watching");
        assert_eq!(
            report["sessions"][0]["findings"][0]["action"],
            "diagnose_only"
        );

        diagnosis::SESSIONS.remove(&4444);
        fs::remove_file(policy_path).expect("remove policy fixture");
        cleanup_test_db(&path);
    }

    #[test]
    fn guard_status_report_keeps_ended_state_with_recent_findings() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let path = unique_test_db_path("guard-status-ended-finding");
        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
            let finding_ts = epoch_to_iso8601(now_epoch_secs());
            insert_session(
                &conn,
                "session-status-ended",
                &finding_ts,
                Some(&finding_ts),
                "claude-sonnet",
                None,
            );
            conn.execute(
                "INSERT INTO guard_findings (
                    session_id, request_id, turn_number, timestamp, rule_id, severity,
                    action, source, evidence_level, confidence, detail, suggested_action
                ) VALUES (?1,NULL,NULL,?2,?3,?4,?5,?6,?7,?8,?9,NULL)",
                rusqlite::params![
                    "session-status-ended",
                    finding_ts,
                    "per_session_token_budget_exceeded",
                    "critical",
                    "block",
                    "proxy",
                    "direct_proxy",
                    1.0,
                    "Session token budget exceeded."
                ],
            )
            .expect("insert finding");
        }
        let _db_path = EnvVarGuard::set("CC_BLACKBOX_DB_PATH", &path);

        let report = build_guard_status_report_json(Some("session-status-ended"));

        assert_eq!(report["overall_state"], "ended");
        assert_eq!(report["sessions"][0]["state"], "ended");
        assert_eq!(report["sessions"][0]["findings"][0]["action"], "block");

        cleanup_test_db(&path);
    }

    #[test]
    fn invalid_request_guard_policy_fails_open() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let _token_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_TOKENS");
        let _dollar_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_DOLLARS");
        let path = unique_temp_path("invalid-guard-policy");
        fs::write(
            &path,
            r#"
[rules.per_session_token_budget_exceeded]
action = "page_the_user"
"#,
        )
        .expect("write invalid policy");
        let _policy_path = EnvVarGuard::set(
            "CC_BLACKBOX_GUARD_POLICY_PATH",
            path.to_str().expect("temp path is utf-8"),
        );

        let session_id = "session-invalid-policy-open";
        SESSION_BUDGETS.insert(
            session_id.to_string(),
            SessionBudgetState {
                total_spend: 0.0,
                trusted_spend: 0.0,
                untrusted_spend: 0.0,
                total_tokens: 1_000_000,
                request_count: 9,
                estimated_cache_waste_dollars: 0.0,
            },
        );

        assert!(request_guard_policy().is_none());
        assert!(evaluate_request_guard_for_session(
            Some(session_id),
            request_guard_policy().as_ref()
        )
        .is_none());

        SESSION_BUDGETS.remove(session_id);
        fs::remove_file(path).expect("remove invalid policy fixture");
    }

    #[test]
    fn api_error_streak_trips_policy_cooldown_for_next_request() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let mut policy = guard::GuardPolicy::default();
        let api_rule = policy
            .rules
            .get_mut(&guard::RuleId::ApiErrorCircuitBreakerCooldown)
            .expect("default api cooldown rule exists");
        api_rule.threshold_count = Some(2);
        api_rule.cooldown_secs = Some(60);

        assert!(
            record_api_response_for_guard(Some("req-api-error-1"), 500, true, Some(&policy))
                .is_none()
        );
        assert!(evaluate_request_guard_for_session(None, Some(&policy)).is_none());

        let opened_finding =
            record_api_response_for_guard(Some("req-api-error-2"), 500, true, Some(&policy))
                .expect("cooldown opening should emit a persistable finding");
        assert_eq!(
            opened_finding.rule_id,
            guard::RuleId::ApiErrorCircuitBreakerCooldown
        );
        assert_eq!(opened_finding.session_id, super::GLOBAL_GUARD_SESSION_ID);
        let decision = evaluate_request_guard_for_session(None, Some(&policy))
            .expect("cooldown should block the next request");
        let block = decision.block.expect("cooldown block metadata");

        assert_eq!(block.rule_id, guard::RuleId::ApiErrorCircuitBreakerCooldown);
        assert_eq!(block.cooldown_remaining_secs, Some(60));
        assert_eq!(
            block.configured_threshold_or_limit,
            "2 consecutive API errors; cooldown 60s"
        );
        let response = make_guard_block_response(&block);
        let body = match response.response.expect("immediate response") {
            ExtProcResponse::ImmediateResponse(response) => response.body,
            other => panic!("expected immediate response, got {other:?}"),
        };
        let json: serde_json::Value = serde_json::from_str(&body).expect("valid json block body");
        assert_eq!(json["error"]["type"], "circuit_breaker");
        assert_eq!(
            json["error"]["rule_id"],
            "api_error_circuit_breaker_cooldown"
        );
        assert_eq!(json["error"]["cooldown_remaining_secs"], 60);

        reset_runtime_state_for_test();
    }

    #[test]
    fn api_error_streak_counts_each_request_once() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let mut policy = guard::GuardPolicy::default();
        let api_rule = policy
            .rules
            .get_mut(&guard::RuleId::ApiErrorCircuitBreakerCooldown)
            .expect("default api cooldown rule exists");
        api_rule.threshold_count = Some(2);
        api_rule.cooldown_secs = Some(60);

        assert!(record_api_response_for_guard(Some("req-dup"), 401, true, Some(&policy)).is_none());
        assert!(record_api_response_for_guard(Some("req-dup"), 401, true, Some(&policy)).is_none());
        assert!(evaluate_request_guard_for_session(None, Some(&policy)).is_none());

        let opened_finding =
            record_api_response_for_guard(Some("req-next"), 401, true, Some(&policy))
                .expect("second distinct failed request opens cooldown");
        assert_eq!(
            opened_finding.rule_id,
            guard::RuleId::ApiErrorCircuitBreakerCooldown
        );
        assert!(evaluate_request_guard_for_session(None, Some(&policy)).is_some());

        reset_runtime_state_for_test();
    }

    #[test]
    fn guard_status_reports_active_global_cooldown_without_known_session() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let path = unique_test_db_path("guard-status-global-cooldown");
        {
            let conn = Connection::open(&path).expect("open test db");
            conn.execute_batch(SCHEMA).expect("create schema");
        }
        let _db_path = EnvVarGuard::set("CC_BLACKBOX_DB_PATH", &path);
        let _token_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_TOKENS");
        let _dollar_budget = EnvVarGuard::remove("CC_BLACKBOX_SESSION_BUDGET_DOLLARS");
        let mut policy = guard::GuardPolicy::default();
        let api_rule = policy
            .rules
            .get_mut(&guard::RuleId::ApiErrorCircuitBreakerCooldown)
            .expect("default api cooldown rule exists");
        api_rule.threshold_count = Some(1);
        api_rule.cooldown_secs = Some(60);

        record_api_response_for_guard(Some("req-global-cooldown"), 500, true, Some(&policy));

        let report = build_guard_status_report_json(None);

        assert_eq!(report["overall_state"], "cooldown");
        assert_eq!(
            report["sessions"][0]["session_id"],
            "guard_api_error_cooldown"
        );
        assert_eq!(report["sessions"][0]["state"], "cooldown");
        assert_eq!(
            report["sessions"][0]["findings"][0]["rule_id"],
            "api_error_circuit_breaker_cooldown"
        );

        reset_runtime_state_for_test();
        cleanup_test_db(&path);
    }

    #[test]
    fn successful_api_response_resets_error_streak_and_cooldown() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        reset_runtime_state_for_test();
        let mut policy = guard::GuardPolicy::default();
        let api_rule = policy
            .rules
            .get_mut(&guard::RuleId::ApiErrorCircuitBreakerCooldown)
            .expect("default api cooldown rule exists");
        api_rule.threshold_count = Some(1);
        api_rule.cooldown_secs = Some(60);

        record_api_response_for_guard(Some("req-reset-error"), 500, true, Some(&policy));
        assert!(evaluate_request_guard_for_session(None, Some(&policy)).is_some());

        record_api_response_for_guard(Some("req-reset-success"), 200, true, Some(&policy));
        assert!(evaluate_request_guard_for_session(None, Some(&policy)).is_none());

        reset_runtime_state_for_test();
    }

    #[test]
    fn postmortem_api_redacts_by_default_and_allows_explicit_opt_out() {
        let default_params = std::collections::HashMap::new();
        assert!(query_redact_enabled(&default_params));

        let mut explicit_false = std::collections::HashMap::new();
        explicit_false.insert("redact".to_string(), "false".to_string());
        assert!(!query_redact_enabled(&explicit_false));

        let mut explicit_true = std::collections::HashMap::new();
        explicit_true.insert("redact".to_string(), "true".to_string());
        assert!(query_redact_enabled(&explicit_true));
    }

    #[test]
    fn postmortem_redaction_removes_paths_query_strings_and_secrets() {
        let redacted = redact_operational_text(
            "Run /Users/pradeep/code/app with password=hunter2 and token=sk-ant-abcdef123456 at https://example.com/a?token=secret plus ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcd",
            true,
        );

        assert!(redacted.contains("<path>"));
        assert!(redacted.contains("password=<redacted>"));
        assert!(redacted.contains("token=<redacted>"));
        assert!(redacted.contains("https://example.com/a?<redacted>"));
        assert!(!redacted.contains("/Users/pradeep"));
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("sk-ant-abcdef123456"));
        assert!(!redacted.contains("token=secret"));
        assert!(!redacted.contains("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcd"));
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
    fn retired_context_beta_header_does_not_expand_context_window() {
        let headers = make_http_headers(&[(
            "anthropic-beta",
            "prompt-tools-2025-04-02, context-1m-2025-08-07",
        )]);

        assert!(!request_uses_1m_context(&headers));
        assert_eq!(request_context_window_hint_from_headers(&headers), None);
        assert_eq!(
            resolve_context_window_tokens(None, "claude-haiku-4-5"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(None, "claude-sonnet-4-6"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
    }

    #[test]
    fn resolve_context_window_tokens_uses_explicit_context_signals() {
        assert_eq!(
            resolve_context_window_tokens(None, "claude-sonnet-4-6[1m]"),
            EXTENDED_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(None, "claude-sonnet-4-6"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(None, "claude-opus-4-7"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(None, "claude-4-6-sonnet-20260217"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(None, "claude-sonnet-4-5"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(None, "claude-haiku-4-5"),
            STANDARD_CONTEXT_WINDOW_TOKENS
        );
        assert_eq!(
            resolve_context_window_tokens(Some(123_456), "claude-haiku-4-5"),
            123_456
        );
        assert_eq!(
            resolve_context_window_tokens_with_config(
                Some(400_000),
                Some(123_456),
                "claude-haiku-4-5"
            ),
            400_000
        );
    }

    #[test]
    fn infer_context_window_tokens_matches_claude_code_plain_model_percentage() {
        let window = infer_context_window_tokens(Some("claude-sonnet-4-6"), None, 72, 61_020, 29);
        assert_eq!(window, STANDARD_CONTEXT_WINDOW_TOKENS);
        assert!((context_fill_percent(72, 61_020, 29, window) - 30.5605).abs() < 0.0001);

        assert_eq!(
            infer_context_window_tokens(
                Some("claude-sonnet-4-6[1m]"),
                Some("claude-sonnet-4-6"),
                72,
                61_020,
                29,
            ),
            EXTENDED_CONTEXT_WINDOW_TOKENS
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
    fn model_matches_keeps_known_safe_date_aliases() {
        assert!(super::model_matches(
            "claude-opus-4-7",
            "claude-opus-4-7-20260410"
        ));
        assert!(super::model_matches(
            "claude-sonnet-4-6-20260217",
            "claude-sonnet-4-6"
        ));
        assert!(super::model_matches(
            "claude-haiku-4-5",
            "claude-haiku-4-5-20251001"
        ));
    }

    #[test]
    fn model_matches_rejects_arbitrary_prefix_overlap() {
        assert!(!super::model_matches(
            "custom-model-alpha",
            "custom-model-alpha-canary"
        ));
        assert!(!super::model_matches("claude-opus-4", "claude-opus-4-7"));
        assert!(!super::model_matches(
            "claude-sonnet-4-6",
            "claude-sonnet-4-6-20260101"
        ));
    }

    #[test]
    fn model_matches_rejects_real_family_and_version_differences() {
        assert!(!super::model_matches(
            "claude-opus-4-7",
            "claude-sonnet-4-6"
        ));
        assert!(!super::model_matches(
            "claude-sonnet-4-6-20260217",
            "claude-sonnet-4-5-20250929"
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
    fn repair_turn_snapshot_context_windows_downgrades_legacy_plain_model_1m_rows() {
        let conn = create_full_test_db();
        insert_session(
            &conn,
            "session-plain",
            "2026-05-07T07:42:01Z",
            Some("2026-05-07T07:45:30Z"),
            "claude-sonnet-4-6",
            None,
        );
        insert_session(
            &conn,
            "session-explicit",
            "2026-05-07T07:42:01Z",
            Some("2026-05-07T07:45:30Z"),
            "claude-sonnet-4-6[1m]",
            None,
        );
        for (session_id, requested_model) in [
            ("session-plain", "claude-sonnet-4-6"),
            ("session-explicit", "claude-sonnet-4-6[1m]"),
        ] {
            conn.execute(
                "INSERT INTO turn_snapshots (
                    session_id, turn_number, timestamp, input_tokens, cache_read_tokens,
                    cache_creation_tokens, output_tokens, ttft_ms, tool_calls, tool_failures,
                    gap_from_prev_secs, context_utilization, context_window_tokens,
                    frustration_signals, requested_model, actual_model
                ) VALUES (?1, 1, '2026-05-07T07:45:30Z', 72, 61020, 29, 40, 1000,
                    '[]', 0, 0.0, 0.061121, 1000000, 0, ?2, 'claude-sonnet-4-6')",
                rusqlite::params![session_id, requested_model],
            )
            .expect("insert turn snapshot");
        }

        repair_turn_snapshot_context_windows(&conn).expect("repair turn snapshots");

        let plain: (i64, f64) = conn
            .query_row(
                "SELECT context_window_tokens, context_utilization \
                 FROM turn_snapshots WHERE session_id = 'session-plain'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("load plain row");
        let explicit: (i64, f64) = conn
            .query_row(
                "SELECT context_window_tokens, context_utilization \
                 FROM turn_snapshots WHERE session_id = 'session-explicit'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("load explicit row");

        assert_eq!(plain.0, STANDARD_CONTEXT_WINDOW_TOKENS as i64);
        assert!((plain.1 - 0.305605).abs() < 0.0001);
        assert_eq!(explicit.0, EXTENDED_CONTEXT_WINDOW_TOKENS as i64);
        assert!((explicit.1 - 0.061121).abs() < 0.0001);
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
    fn degradation_view_uses_stored_one_hour_cache_ttl_evidence() {
        let conn = create_full_test_db();
        let ended_at =
            epoch_to_iso8601(now_epoch_secs().saturating_sub(session_timeout_secs() + 3_600));
        insert_session(
            &conn,
            "session-1h-ttl",
            &ended_at,
            Some(&ended_at),
            "claude-sonnet",
            Some("keep long cache warm"),
        );
        insert_turn_snapshot(
            &conn,
            "session-1h-ttl",
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
            "session-1h-ttl",
            2,
            &ended_at,
            0,
            1000,
            1200,
            600.0,
            0.15,
            Some("Full cache rebuild after ten minutes"),
        );
        conn.execute(
            "UPDATE turn_snapshots \
             SET cache_ttl_min_secs = 3600, cache_ttl_max_secs = 3600, cache_ttl_source = 'request_cache_control' \
             WHERE session_id = 'session-1h-ttl'",
            [],
        )
        .expect("set ttl evidence");

        let json =
            load_degradation_view_from_db(&conn, "session-1h-ttl").expect("degradation view");
        let flags = json
            .pointer("/turns/1/flags")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();

        assert!(!flags
            .iter()
            .any(|flag| flag.as_str() == Some("cache_miss_ttl")));
        assert!(flags
            .iter()
            .any(|flag| flag.as_str() == Some("cache_miss_inferred_rebuild")));
    }

    #[test]
    fn cache_rebuilds_api_uses_stored_ttl_evidence() {
        let conn = create_full_test_db();
        let now = now_epoch_secs();
        let timestamp = epoch_to_iso8601(now.saturating_sub(60));
        insert_session(
            &conn,
            "session-cache-rebuilds",
            &timestamp,
            Some(&timestamp),
            "claude-sonnet",
            None,
        );
        insert_turn_snapshot(
            &conn,
            "session-cache-rebuilds",
            1,
            &timestamp,
            0,
            1000,
            1000,
            0.0,
            0.10,
            None,
        );
        insert_turn_snapshot(
            &conn,
            "session-cache-rebuilds",
            2,
            &timestamp,
            0,
            1000,
            1000,
            600.0,
            0.10,
            None,
        );
        conn.execute(
            "UPDATE turn_snapshots \
             SET cache_ttl_min_secs = 3600, cache_ttl_max_secs = 3600, cache_ttl_source = 'request_cache_control' \
             WHERE session_id = 'session-cache-rebuilds'",
            [],
        )
        .expect("set ttl evidence");

        let json = build_cache_rebuilds_response_from_db(&conn, 7, now).expect("cache rebuilds");

        assert_eq!(
            json.get("rebuilds_from_idle_gaps")
                .and_then(|value| value.as_u64()),
            Some(0)
        );
        assert_eq!(
            json.get("rebuilds_without_idle_gap")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
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
            cache_expires_at_latest_epoch: None,
            cache_ttl_source: Some("request_cache_control".to_string()),
            cache_ttl_mixed: None,
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

    #[test]
    fn quota_burn_event_is_not_labeled_as_provider_rate_limit_state() {
        let json = serde_json::to_value(crate::watch::WatchEvent::RateLimitStatus {
            seconds_to_reset: Some(3600),
            requests_remaining: None,
            requests_limit: None,
            input_tokens_remaining: None,
            output_tokens_remaining: None,
            tokens_used_this_week: Some(42),
            tokens_limit: Some(100),
            tokens_remaining: Some(58),
            budget_source: Some("env".to_string()),
            projected_exhaustion_secs: None,
        })
        .expect("serialize quota event");

        assert_eq!(
            json.get("type").and_then(|value| value.as_str()),
            Some("quota_burn_status")
        );
        assert!(serde_json::to_string(&json)
            .expect("serialize quota json")
            .contains("tokens_used_this_week"));
    }

    #[test]
    fn fallback_request_ids_are_unique() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..512 {
            assert!(ids.insert(fallback_request_id()));
        }
    }

    #[test]
    fn lock_or_recover_returns_guard_after_poisoning() {
        let mutex = std::sync::Arc::new(Mutex::new(41_u32));
        let poisoned = mutex.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.lock().expect("lock before poison");
            panic!("poison test mutex");
        })
        .join();

        let mut guard = lock_or_recover(mutex.as_ref(), "poison_test");
        *guard += 1;
        assert_eq!(*guard, 42);
    }

    #[test]
    fn truncation_helpers_are_utf8_safe() {
        assert_eq!(truncate_detail("åβ中🙂done", 4), "åβ中🙂...");
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
            working_dir: "/tmp/cc-blackbox-test".to_string(),
            first_message_hash: "firstmsg_test".to_string(),
            prompt_correlation_hash: String::new(),
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
}
