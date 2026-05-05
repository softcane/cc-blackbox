use std::collections::{BTreeMap, BTreeSet};
use std::sync::{LazyLock, Mutex, MutexGuard};

use prometheus::{
    gather, histogram_opts, opts, register_counter_vec, register_gauge_vec, register_histogram,
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec, CounterVec, Encoder, GaugeVec, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, TextEncoder,
};
use tracing::warn;

pub const HISTORY_WINDOWS: [(&str, u64); 3] = [("1d", 1), ("7d", 7), ("30d", 30)];
pub const HISTORY_MODELS: [&str; 4] = ["opus", "sonnet", "haiku", "other"];
const LIVE_MODELS: [&str; 4] = ["opus", "sonnet", "haiku", "other"];
const LIVE_CACHE_EVENT_TYPES: [&str; 5] = [
    "cold_start",
    "partial",
    "miss_rebuild",
    "miss_ttl",
    "miss_thrash",
];
const LIVE_SKILL_EVENT_TYPES: [&str; 5] = ["expected", "fired", "missed", "misfired", "failed"];
const LIVE_SKILL_EVENT_SOURCES: [&str; 3] = ["hook", "proxy", "heuristic"];
const LIVE_MCP_EVENT_TYPES: [&str; 4] = ["called", "succeeded", "failed", "denied"];
const LIVE_MCP_EVENT_SOURCES: [&str; 2] = ["hook", "proxy"];
pub const HISTORY_CACHE_EVENT_TYPES: [&str; 2] = ["miss_ttl", "miss_thrash"];
pub const HISTORY_CAUSE_TYPES: [&str; 8] = [
    "cache_miss_ttl",
    "context_bloat",
    "model_fallback",
    "near_compaction",
    "harness_pressure",
    "cache_miss_thrash",
    "tool_failure_streak",
    "compaction_suspected",
];

#[derive(Clone, Debug, Default)]
pub struct HistoricalWindowMetrics {
    pub window: &'static str,
    pub sessions: u64,
    pub estimated_spend_dollars: f64,
    pub estimated_spend_dollars_by_model: BTreeMap<&'static str, f64>,
    pub avg_estimated_session_cost_dollars_by_model: BTreeMap<&'static str, f64>,
    pub estimated_cache_waste_dollars_by_model: BTreeMap<&'static str, f64>,
    pub cache_hit_ratio: f64,
    pub cache_events: BTreeMap<&'static str, u64>,
    pub degraded_sessions: u64,
    pub degraded_session_ratio: f64,
    pub degraded_causes: BTreeMap<&'static str, u64>,
    pub model_fallbacks: BTreeMap<(&'static str, &'static str), u64>,
    pub tool_failures_by_tool: BTreeMap<String, u64>,
}

pub struct ClauditorMetrics {
    requests_total: IntCounterVec,
    tokens_total: IntCounterVec,
    estimated_cost_dollars_total: CounterVec,
    cache_events_total: IntCounterVec,
    estimated_cache_waste_dollars_total: CounterVec,
    sessions_total: IntCounterVec,
    degraded_sessions_total: IntCounter,
    sessions_degraded_total: IntCounterVec,
    model_fallback_total: IntCounterVec,
    compaction_suspected_total: IntCounter,
    tool_calls_total: IntCounterVec,
    tool_failures_total: IntCounterVec,
    mcp_tool_calls_total: IntCounterVec,
    mcp_tool_failures_total: IntCounterVec,
    mcp_server_calls_total: IntCounterVec,
    mcp_server_failures_total: IntCounterVec,
    mcp_events_total: IntCounterVec,
    skill_events_total: IntCounterVec,
    active_sessions: IntGauge,
    weekly_tokens_used: IntGauge,
    weekly_tokens_remaining: IntGauge,
    weekly_token_budget: IntGaugeVec,
    projected_exhaustion_seconds: IntGauge,
    history_sessions: IntGaugeVec,
    history_estimated_spend_dollars: GaugeVec,
    history_estimated_spend_dollars_by_model: GaugeVec,
    history_avg_estimated_session_cost_dollars: GaugeVec,
    history_estimated_cache_waste_dollars: GaugeVec,
    history_cache_hit_ratio: GaugeVec,
    history_cache_events: IntGaugeVec,
    history_degraded_sessions: IntGaugeVec,
    history_degraded_session_ratio: GaugeVec,
    history_degraded_causes: IntGaugeVec,
    history_model_fallbacks: IntGaugeVec,
    history_tool_failures: IntGaugeVec,
    history_refresh_timestamp_seconds: IntGauge,
    turn_duration_seconds: HistogramVec,
    estimated_session_cost_dollars: HistogramVec,
    session_turns: Histogram,
}

impl ClauditorMetrics {
    fn register() -> Self {
        let mut turn_duration_buckets = prometheus::DEFAULT_BUCKETS.to_vec();
        turn_duration_buckets.extend([30.0, 60.0, 120.0]);
        turn_duration_buckets.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        turn_duration_buckets.dedup();

        Self {
            requests_total: register_int_counter_vec!(
                opts!(
                    "clauditor_requests_total",
                    "Completed turns observed by clauditor-core."
                ),
                &["model"]
            )
            .expect("register clauditor_requests_total"),
            tokens_total: register_int_counter_vec!(
                opts!(
                    "clauditor_tokens_total",
                    "Tokens observed by turn and token kind."
                ),
                &["model", "kind"]
            )
            .expect("register clauditor_tokens_total"),
            estimated_cost_dollars_total: register_counter_vec!(
                opts!(
                    "clauditor_estimated_cost_dollars_total",
                    "Total estimated cost in USD from the active pricing catalog."
                ),
                &["model"]
            )
            .expect("register clauditor_estimated_cost_dollars_total"),
            cache_events_total: register_int_counter_vec!(
                opts!(
                    "clauditor_cache_events_total",
                    "Session-scoped cache events emitted for tracked sessions."
                ),
                &["model", "event_type"]
            )
            .expect("register clauditor_cache_events_total"),
            estimated_cache_waste_dollars_total: register_counter_vec!(
                opts!(
                    "clauditor_estimated_cache_waste_dollars_total",
                    "Estimated avoidable cache rebuild waste in USD from the active pricing catalog."
                ),
                &["model"]
            )
            .expect("register clauditor_estimated_cache_waste_dollars_total"),
            sessions_total: register_int_counter_vec!(
                opts!("clauditor_sessions_total", "Sessions finalized by outcome."),
                &["outcome"]
            )
            .expect("register clauditor_sessions_total"),
            degraded_sessions_total: register_int_counter!(opts!(
                "clauditor_degraded_sessions_total",
                "Sessions whose diagnosis reported at least one degradation cause."
            ))
            .expect("register clauditor_degraded_sessions_total"),
            sessions_degraded_total: register_int_counter_vec!(
                opts!(
                    "clauditor_sessions_degraded_total",
                    "Degraded sessions by cause type."
                ),
                &["cause_type"]
            )
            .expect("register clauditor_sessions_degraded_total"),
            model_fallback_total: register_int_counter_vec!(
                opts!(
                    "clauditor_model_fallback_total",
                    "Requested/actual model route mismatch events."
                ),
                &["requested", "actual"]
            )
            .expect("register clauditor_model_fallback_total"),
            compaction_suspected_total: register_int_counter!(opts!(
                "clauditor_compaction_suspected_total",
                "Compaction suspicion signals emitted by heuristics or explicit events."
            ))
            .expect("register clauditor_compaction_suspected_total"),
            tool_calls_total: register_int_counter_vec!(
                opts!(
                    "clauditor_tool_calls_total",
                    "Tool calls observed in assistant responses."
                ),
                &["tool"]
            )
            .expect("register clauditor_tool_calls_total"),
            tool_failures_total: register_int_counter_vec!(
                opts!(
                    "clauditor_tool_failures_total",
                    "Tool failures observed from tool_result blocks."
                ),
                &["tool"]
            )
            .expect("register clauditor_tool_failures_total"),
            mcp_tool_calls_total: register_int_counter_vec!(
                opts!(
                    "clauditor_mcp_tool_calls_total",
                    "MCP tool calls observed in assistant responses, split by MCP server and tool."
                ),
                &["server", "tool"]
            )
            .expect("register clauditor_mcp_tool_calls_total"),
            mcp_tool_failures_total: register_int_counter_vec!(
                opts!(
                    "clauditor_mcp_tool_failures_total",
                    "MCP tool failures observed from tool_result blocks, split by MCP server and tool."
                ),
                &["server", "tool"]
            )
            .expect("register clauditor_mcp_tool_failures_total"),
            mcp_server_calls_total: register_int_counter_vec!(
                opts!(
                    "clauditor_mcp_server_calls_total",
                    "MCP tool calls observed in assistant responses, split by MCP server."
                ),
                &["server"]
            )
            .expect("register clauditor_mcp_server_calls_total"),
            mcp_server_failures_total: register_int_counter_vec!(
                opts!(
                    "clauditor_mcp_server_failures_total",
                    "MCP tool failures observed from tool_result blocks, split by MCP server."
                ),
                &["server"]
            )
            .expect("register clauditor_mcp_server_failures_total"),
            mcp_events_total: register_int_counter_vec!(
                opts!(
                    "clauditor_mcp_events_total",
                    "MCP lifecycle events observed by source."
                ),
                &["server", "tool", "event_type", "source"]
            )
            .expect("register clauditor_mcp_events_total"),
            skill_events_total: register_int_counter_vec!(
                opts!(
                    "clauditor_skill_events_total",
                    "Skill lifecycle events observed or inferred by clauditor."
                ),
                &["skill", "event_type", "source"]
            )
            .expect("register clauditor_skill_events_total"),
            active_sessions: register_int_gauge!(
                "clauditor_active_sessions",
                "Tracked active sessions currently in memory."
            )
            .expect("register clauditor_active_sessions"),
            weekly_tokens_used: register_int_gauge!(
                "clauditor_weekly_tokens_used",
                "Tokens used since Monday 00:00 UTC."
            )
            .expect("register clauditor_weekly_tokens_used"),
            weekly_tokens_remaining: register_int_gauge!(
                "clauditor_weekly_tokens_remaining",
                "Remaining weekly tokens under the active or suggested budget."
            )
            .expect("register clauditor_weekly_tokens_remaining"),
            weekly_token_budget: register_int_gauge_vec!(
                "clauditor_weekly_token_budget",
                "Weekly token budget, labeled by source.",
                &["source"]
            )
            .expect("register clauditor_weekly_token_budget"),
            projected_exhaustion_seconds: register_int_gauge!(
                "clauditor_projected_exhaustion_seconds",
                "Projected seconds until the weekly token budget is exhausted."
            )
            .expect("register clauditor_projected_exhaustion_seconds"),
            history_sessions: register_int_gauge_vec!(
                "clauditor_history_sessions",
                "Historical finalized session counts by rolling window.",
                &["window"]
            )
            .expect("register clauditor_history_sessions"),
            history_estimated_spend_dollars: register_gauge_vec!(
                "clauditor_history_estimated_spend_dollars",
                "Historical estimated cost in USD by rolling window.",
                &["window"]
            )
            .expect("register clauditor_history_estimated_spend_dollars"),
            history_estimated_spend_dollars_by_model: register_gauge_vec!(
                "clauditor_history_estimated_spend_dollars_by_model",
                "Historical estimated cost in USD by rolling window and model family.",
                &["window", "model"]
            )
            .expect("register clauditor_history_estimated_spend_dollars_by_model"),
            history_avg_estimated_session_cost_dollars: register_gauge_vec!(
                "clauditor_history_avg_estimated_session_cost_dollars",
                "Historical average estimated session cost in USD by rolling window and model family.",
                &["window", "model"]
            )
            .expect("register clauditor_history_avg_estimated_session_cost_dollars"),
            history_estimated_cache_waste_dollars: register_gauge_vec!(
                "clauditor_history_estimated_cache_waste_dollars",
                "Historical estimated cache rebuild waste in USD by rolling window and model family.",
                &["window", "model"]
            )
            .expect("register clauditor_history_estimated_cache_waste_dollars"),
            history_cache_hit_ratio: register_gauge_vec!(
                "clauditor_history_cache_hit_ratio",
                "Historical token-weighted cache reuse share by rolling window.",
                &["window"]
            )
            .expect("register clauditor_history_cache_hit_ratio"),
            history_cache_events: register_int_gauge_vec!(
                "clauditor_history_cache_events",
                "Historical cache event counts by rolling window and cache event type.",
                &["window", "event_type"]
            )
            .expect("register clauditor_history_cache_events"),
            history_degraded_sessions: register_int_gauge_vec!(
                "clauditor_history_degraded_sessions",
                "Historical degraded session counts by rolling window.",
                &["window"]
            )
            .expect("register clauditor_history_degraded_sessions"),
            history_degraded_session_ratio: register_gauge_vec!(
                "clauditor_history_degraded_session_ratio",
                "Historical degraded session ratio by rolling window.",
                &["window"]
            )
            .expect("register clauditor_history_degraded_session_ratio"),
            history_degraded_causes: register_int_gauge_vec!(
                "clauditor_history_degraded_causes",
                "Historical degraded cause counts by rolling window.",
                &["window", "cause_type"]
            )
            .expect("register clauditor_history_degraded_causes"),
            history_model_fallbacks: register_int_gauge_vec!(
                "clauditor_history_model_fallbacks",
                "Historical requested/actual model route mismatch counts by rolling window.",
                &["window", "requested", "actual"]
            )
            .expect("register clauditor_history_model_fallbacks"),
            history_tool_failures: register_int_gauge_vec!(
                "clauditor_history_tool_failures",
                "Historical tool failure counts by rolling window and tool.",
                &["window", "tool"]
            )
            .expect("register clauditor_history_tool_failures"),
            history_refresh_timestamp_seconds: register_int_gauge!(
                "clauditor_history_refresh_timestamp_seconds",
                "Unix timestamp of the last successful historical gauge refresh."
            )
            .expect("register clauditor_history_refresh_timestamp_seconds"),
            turn_duration_seconds: register_histogram_vec!(
                histogram_opts!(
                    "clauditor_turn_duration_seconds",
                    "Observed turn durations in seconds.",
                    turn_duration_buckets
                ),
                &["model"]
            )
            .expect("register clauditor_turn_duration_seconds"),
            estimated_session_cost_dollars: register_histogram_vec!(
                histogram_opts!(
                    "clauditor_estimated_session_cost_dollars",
                    "Estimated session costs in USD from the active pricing catalog.",
                    vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0]
                ),
                &["model"]
            )
            .expect("register clauditor_estimated_session_cost_dollars"),
            session_turns: register_histogram!(histogram_opts!(
                "clauditor_session_turns",
                "Observed number of turns per session.",
                vec![1.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0]
            ))
            .expect("register clauditor_session_turns"),
        }
    }
}

static METRICS: LazyLock<ClauditorMetrics> = LazyLock::new(ClauditorMetrics::register);
static HISTORY_TOOL_LABELS: LazyLock<Mutex<BTreeSet<String>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

fn history_tool_labels() -> MutexGuard<'static, BTreeSet<String>> {
    match HISTORY_TOOL_LABELS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("recovering poisoned metrics history-tool-label mutex");
            poisoned.into_inner()
        }
    }
}

pub fn init() {
    let metrics = LazyLock::force(&METRICS);
    for model in LIVE_MODELS {
        metrics.requests_total.with_label_values(&[model]);
        metrics.tokens_total.with_label_values(&[model, "input"]);
        metrics.tokens_total.with_label_values(&[model, "output"]);
        metrics
            .tokens_total
            .with_label_values(&[model, "cache_read"]);
        metrics
            .tokens_total
            .with_label_values(&[model, "cache_create"]);
        metrics
            .estimated_cost_dollars_total
            .with_label_values(&[model]);
        metrics
            .estimated_cache_waste_dollars_total
            .with_label_values(&[model]);
        metrics.turn_duration_seconds.with_label_values(&[model]);
        metrics
            .estimated_session_cost_dollars
            .with_label_values(&[model]);
        for actual in LIVE_MODELS {
            metrics
                .model_fallback_total
                .with_label_values(&[model, actual]);
        }
        for event_type in LIVE_CACHE_EVENT_TYPES {
            metrics
                .cache_events_total
                .with_label_values(&[model, event_type]);
        }
    }

    ensure_tool_metric_labels("unknown");
    ensure_mcp_metric_labels_for_parts("unknown", "unknown");
    for event_type in LIVE_MCP_EVENT_TYPES {
        for source in LIVE_MCP_EVENT_SOURCES {
            ensure_mcp_event_metric_labels("unknown", "unknown", event_type, source);
        }
    }
    for event_type in LIVE_SKILL_EVENT_TYPES {
        for source in LIVE_SKILL_EVENT_SOURCES {
            ensure_skill_metric_labels("unknown", event_type, source);
        }
    }
}

pub fn record_request(
    model: &str,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_create_tokens: u64,
    estimated_cost_dollars: f64,
    duration_seconds: f64,
) {
    let model = normalize_model(model);
    METRICS.requests_total.with_label_values(&[model]).inc();
    METRICS
        .tokens_total
        .with_label_values(&[model, "input"])
        .inc_by(input_tokens);
    METRICS
        .tokens_total
        .with_label_values(&[model, "output"])
        .inc_by(output_tokens);
    METRICS
        .tokens_total
        .with_label_values(&[model, "cache_read"])
        .inc_by(cache_read_tokens);
    METRICS
        .tokens_total
        .with_label_values(&[model, "cache_create"])
        .inc_by(cache_create_tokens);
    METRICS
        .estimated_cost_dollars_total
        .with_label_values(&[model])
        .inc_by(estimated_cost_dollars.max(0.0));
    METRICS
        .turn_duration_seconds
        .with_label_values(&[model])
        .observe(duration_seconds.max(0.0));
}

pub fn record_cache_event(model: &str, event_type: &str, estimated_waste_dollars: Option<f64>) {
    let model = normalize_model(model);
    let event_type = normalize_cache_event(event_type);
    METRICS
        .cache_events_total
        .with_label_values(&[model, event_type])
        .inc();
    if matches!(event_type, "miss_ttl" | "miss_thrash") {
        if let Some(cost) = estimated_waste_dollars {
            METRICS
                .estimated_cache_waste_dollars_total
                .with_label_values(&[model])
                .inc_by(cost.max(0.0));
        }
    }
}

pub fn record_session_end(
    outcome: &str,
    model: Option<&str>,
    estimated_total_cost_dollars: f64,
    total_turns: u32,
) {
    METRICS
        .sessions_total
        .with_label_values(&[normalize_outcome(outcome)])
        .inc();
    METRICS
        .estimated_session_cost_dollars
        .with_label_values(&[normalize_model(model.unwrap_or("other"))])
        .observe(estimated_total_cost_dollars.max(0.0));
    METRICS.session_turns.observe(total_turns as f64);
}

pub fn record_degraded_session() {
    METRICS.degraded_sessions_total.inc();
}

pub fn record_degraded_cause(cause_type: &str) {
    METRICS
        .sessions_degraded_total
        .with_label_values(&[normalize_cause(cause_type)])
        .inc();
}

pub fn record_model_fallback(requested: &str, actual: &str) {
    METRICS
        .model_fallback_total
        .with_label_values(&[normalize_model(requested), normalize_model(actual)])
        .inc();
}

pub fn record_compaction_suspected() {
    METRICS.compaction_suspected_total.inc();
}

pub fn record_context_status(turns_to_compact: Option<u32>) {
    if turns_to_compact == Some(0) {
        METRICS.compaction_suspected_total.inc();
    }
}

pub fn record_tool_call(tool_name: &str) {
    let tool = normalize_tool(tool_name);
    METRICS
        .tool_calls_total
        .with_label_values(&[tool.as_str()])
        .inc();
    if let Some((server, mcp_tool)) = mcp_tool_labels(tool_name) {
        METRICS
            .mcp_tool_calls_total
            .with_label_values(&[server.as_str(), mcp_tool.as_str()])
            .inc();
        METRICS
            .mcp_server_calls_total
            .with_label_values(&[server.as_str()])
            .inc();
        record_mcp_event_parts(&server, &mcp_tool, "called", "proxy");
    }
}

pub fn record_tool_failures(tool_name: &str, failures: u64) {
    if failures == 0 {
        return;
    }
    let tool = normalize_tool(tool_name);
    METRICS
        .tool_failures_total
        .with_label_values(&[tool.as_str()])
        .inc_by(failures);
    if let Some((server, mcp_tool)) = mcp_tool_labels(tool_name) {
        METRICS
            .mcp_tool_failures_total
            .with_label_values(&[server.as_str(), mcp_tool.as_str()])
            .inc_by(failures);
        METRICS
            .mcp_server_failures_total
            .with_label_values(&[server.as_str()])
            .inc_by(failures);
        record_mcp_event_parts_by(&server, &mcp_tool, "failed", "proxy", failures);
    }
}

pub fn ensure_tool_metric_labels(tool_name: &str) {
    let tool = normalize_tool(tool_name);
    METRICS.tool_calls_total.with_label_values(&[tool.as_str()]);
    METRICS
        .tool_failures_total
        .with_label_values(&[tool.as_str()]);
    if let Some((server, mcp_tool)) = mcp_tool_labels(tool_name) {
        ensure_mcp_metric_labels_for_parts(&server, &mcp_tool);
    }
}

fn ensure_mcp_metric_labels_for_parts(server: &str, tool: &str) {
    METRICS
        .mcp_tool_calls_total
        .with_label_values(&[server, tool]);
    METRICS
        .mcp_tool_failures_total
        .with_label_values(&[server, tool]);
    METRICS.mcp_server_calls_total.with_label_values(&[server]);
    METRICS
        .mcp_server_failures_total
        .with_label_values(&[server]);
}

pub fn record_mcp_event(server: &str, tool: &str, event_type: &str, source: &str) {
    let server = normalize_tool(server);
    let tool = normalize_tool(tool);
    record_mcp_event_parts(&server, &tool, event_type, source);
}

fn record_mcp_event_parts(server: &str, tool: &str, event_type: &str, source: &str) {
    record_mcp_event_parts_by(server, tool, event_type, source, 1);
}

fn record_mcp_event_parts_by(server: &str, tool: &str, event_type: &str, source: &str, count: u64) {
    if count == 0 {
        return;
    }
    let event_type = normalize_mcp_event_type(event_type);
    let source = normalize_mcp_event_source(source);
    METRICS
        .mcp_events_total
        .with_label_values(&[server, tool, event_type, source])
        .inc_by(count);
}

pub fn record_skill_event(skill_name: &str, event_type: &str, source: &str) {
    let skill = normalize_tool(skill_name);
    let event_type = normalize_skill_event_type(event_type);
    let source = normalize_skill_event_source(source);
    METRICS
        .skill_events_total
        .with_label_values(&[skill.as_str(), event_type, source])
        .inc();
}

pub fn ensure_skill_metric_labels(skill_name: &str, event_type: &str, source: &str) {
    let skill = normalize_tool(skill_name);
    let event_type = normalize_skill_event_type(event_type);
    let source = normalize_skill_event_source(source);
    METRICS
        .skill_events_total
        .with_label_values(&[skill.as_str(), event_type, source]);
}

pub fn ensure_mcp_event_metric_labels(server: &str, tool: &str, event_type: &str, source: &str) {
    let server = normalize_tool(server);
    let tool = normalize_tool(tool);
    let event_type = normalize_mcp_event_type(event_type);
    let source = normalize_mcp_event_source(source);
    METRICS.mcp_events_total.with_label_values(&[
        server.as_str(),
        tool.as_str(),
        event_type,
        source,
    ]);
}

pub fn set_active_sessions(count: usize) {
    METRICS.active_sessions.set(count as i64);
}

pub fn increment_active_sessions() {
    METRICS.active_sessions.inc();
}

pub fn decrement_active_sessions() {
    METRICS.active_sessions.dec();
}

pub fn update_weekly_budget_gauges(
    used_this_week: u64,
    remaining: Option<u64>,
    budget: Option<u64>,
    source: Option<&str>,
    projected_exhaustion_secs: Option<u64>,
) {
    METRICS.weekly_tokens_used.set(used_this_week as i64);
    METRICS
        .weekly_tokens_remaining
        .set(remaining.map(|n| n as i64).unwrap_or(-1));
    METRICS
        .projected_exhaustion_seconds
        .set(projected_exhaustion_secs.map(|n| n as i64).unwrap_or(-1));

    let _ = METRICS.weekly_token_budget.remove_label_values(&["env"]);
    let _ = METRICS
        .weekly_token_budget
        .remove_label_values(&["auto_p95_4w"]);
    if let (Some(budget), Some(source)) = (budget, source) {
        METRICS
            .weekly_token_budget
            .with_label_values(&[source])
            .set(budget as i64);
    }
}

pub fn update_historical_gauges(windows: &[HistoricalWindowMetrics], refreshed_at_epoch: u64) {
    for (window, _) in HISTORY_WINDOWS {
        METRICS.history_sessions.with_label_values(&[window]).set(0);
        METRICS
            .history_estimated_spend_dollars
            .with_label_values(&[window])
            .set(0.0);
        for model in HISTORY_MODELS {
            METRICS
                .history_estimated_spend_dollars_by_model
                .with_label_values(&[window, model])
                .set(0.0);
            METRICS
                .history_avg_estimated_session_cost_dollars
                .with_label_values(&[window, model])
                .set(0.0);
            METRICS
                .history_estimated_cache_waste_dollars
                .with_label_values(&[window, model])
                .set(0.0);
            for actual in HISTORY_MODELS {
                METRICS
                    .history_model_fallbacks
                    .with_label_values(&[window, model, actual])
                    .set(0);
            }
        }
        METRICS
            .history_cache_hit_ratio
            .with_label_values(&[window])
            .set(0.0);
        for event_type in HISTORY_CACHE_EVENT_TYPES {
            METRICS
                .history_cache_events
                .with_label_values(&[window, event_type])
                .set(0);
        }
        METRICS
            .history_degraded_sessions
            .with_label_values(&[window])
            .set(0);
        METRICS
            .history_degraded_session_ratio
            .with_label_values(&[window])
            .set(0.0);
        for cause_type in HISTORY_CAUSE_TYPES {
            METRICS
                .history_degraded_causes
                .with_label_values(&[window, cause_type])
                .set(0);
        }
    }

    let mut known_tools = history_tool_labels();
    for window_metrics in windows {
        for tool in window_metrics.tool_failures_by_tool.keys() {
            known_tools.insert(tool.clone());
        }
    }
    for window in HISTORY_WINDOWS.map(|(window, _)| window) {
        for tool in known_tools.iter() {
            METRICS
                .history_tool_failures
                .with_label_values(&[window, tool.as_str()])
                .set(0);
        }
    }

    for window_metrics in windows {
        METRICS
            .history_sessions
            .with_label_values(&[window_metrics.window])
            .set(window_metrics.sessions as i64);
        METRICS
            .history_estimated_spend_dollars
            .with_label_values(&[window_metrics.window])
            .set(window_metrics.estimated_spend_dollars.max(0.0));
        for (model, spend) in &window_metrics.estimated_spend_dollars_by_model {
            METRICS
                .history_estimated_spend_dollars_by_model
                .with_label_values(&[window_metrics.window, model])
                .set(spend.max(0.0));
        }
        for (model, avg_cost) in &window_metrics.avg_estimated_session_cost_dollars_by_model {
            METRICS
                .history_avg_estimated_session_cost_dollars
                .with_label_values(&[window_metrics.window, model])
                .set(avg_cost.max(0.0));
        }
        for (model, waste) in &window_metrics.estimated_cache_waste_dollars_by_model {
            METRICS
                .history_estimated_cache_waste_dollars
                .with_label_values(&[window_metrics.window, model])
                .set(waste.max(0.0));
        }
        METRICS
            .history_cache_hit_ratio
            .with_label_values(&[window_metrics.window])
            .set(window_metrics.cache_hit_ratio.clamp(0.0, 1.0));
        for (event_type, count) in &window_metrics.cache_events {
            METRICS
                .history_cache_events
                .with_label_values(&[window_metrics.window, event_type])
                .set(*count as i64);
        }
        METRICS
            .history_degraded_sessions
            .with_label_values(&[window_metrics.window])
            .set(window_metrics.degraded_sessions as i64);
        METRICS
            .history_degraded_session_ratio
            .with_label_values(&[window_metrics.window])
            .set(window_metrics.degraded_session_ratio.clamp(0.0, 1.0));

        for (cause_type, count) in &window_metrics.degraded_causes {
            METRICS
                .history_degraded_causes
                .with_label_values(&[window_metrics.window, cause_type])
                .set(*count as i64);
        }
        for ((requested, actual), count) in &window_metrics.model_fallbacks {
            METRICS
                .history_model_fallbacks
                .with_label_values(&[window_metrics.window, requested, actual])
                .set(*count as i64);
        }
        for (tool, count) in &window_metrics.tool_failures_by_tool {
            METRICS
                .history_tool_failures
                .with_label_values(&[window_metrics.window, tool.as_str()])
                .set(*count as i64);
        }
    }
    drop(known_tools);

    METRICS
        .history_refresh_timestamp_seconds
        .set(refreshed_at_epoch as i64);
}

pub fn render() -> Result<(String, String), String> {
    let metric_families = gather();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&metric_families, &mut buf)
        .map_err(|e| e.to_string())?;
    let body = String::from_utf8(buf).map_err(|e| e.to_string())?;
    Ok((encoder.format_type().to_string(), body))
}

fn normalize_model(model: &str) -> &'static str {
    let lower = model.to_ascii_lowercase();
    if lower.contains("opus") {
        "opus"
    } else if lower.contains("sonnet") {
        "sonnet"
    } else if lower.contains("haiku") {
        "haiku"
    } else {
        "other"
    }
}

fn normalize_cache_event(event_type: &str) -> &'static str {
    match event_type {
        "hit" => "hit",
        "partial" => "partial",
        "cold_start" => "cold_start",
        "miss_rebuild" => "miss_rebuild",
        "miss_ttl" => "miss_ttl",
        "miss_thrash" => "miss_thrash",
        _ => "other",
    }
}

fn normalize_outcome(outcome: &str) -> &'static str {
    let lower = outcome.to_ascii_lowercase();
    if lower.contains("budget") {
        "budget_exceeded"
    } else if lower.contains("compaction") {
        "compaction_suspected"
    } else if lower.contains("partially") {
        "partially_completed"
    } else if lower.contains("completed") {
        "completed"
    } else if lower.contains("abandoned") {
        "abandoned"
    } else if lower.contains("timeout") {
        "timeout"
    } else {
        "other"
    }
}

fn normalize_cause(cause_type: &str) -> &str {
    if cause_type.is_empty() {
        "unknown"
    } else {
        cause_type
    }
}

fn normalize_skill_event_type(event_type: &str) -> &'static str {
    match event_type {
        "expected" => "expected",
        "fired" => "fired",
        "missed" => "missed",
        "misfired" => "misfired",
        "failed" => "failed",
        _ => "other",
    }
}

fn normalize_skill_event_source(source: &str) -> &'static str {
    match source {
        "hook" => "hook",
        "proxy" => "proxy",
        "heuristic" => "heuristic",
        _ => "other",
    }
}

fn normalize_mcp_event_type(event_type: &str) -> &'static str {
    match event_type {
        "called" => "called",
        "succeeded" => "succeeded",
        "failed" => "failed",
        "denied" => "denied",
        _ => "other",
    }
}

fn normalize_mcp_event_source(source: &str) -> &'static str {
    match source {
        "hook" => "hook",
        "proxy" => "proxy",
        _ => "other",
    }
}

pub fn historical_cause_label(cause_type: &str) -> Option<&'static str> {
    HISTORY_CAUSE_TYPES
        .iter()
        .copied()
        .find(|c| *c == cause_type)
}

pub fn historical_model_label(model: &str) -> &'static str {
    normalize_model(model)
}

pub fn historical_cache_event_label(event_type: &str) -> Option<&'static str> {
    HISTORY_CACHE_EVENT_TYPES
        .iter()
        .copied()
        .find(|event| *event == event_type)
}

pub fn historical_tool_label(tool_name: &str) -> String {
    normalize_tool(tool_name)
}

fn normalize_tool(tool_name: &str) -> String {
    let trimmed = tool_name.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }

    let mut normalized = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '_' | '-' | '.') {
            normalized.push(ch);
        } else {
            normalized.push('_');
        }
    }

    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

pub fn mcp_tool_labels(tool_name: &str) -> Option<(String, String)> {
    let rest = tool_name.trim().strip_prefix("mcp__")?;
    let (server, tool) = rest.split_once("__")?;
    if server.trim().is_empty() || tool.trim().is_empty() {
        return None;
    }

    Some((normalize_tool(server), normalize_tool(tool)))
}

#[cfg(test)]
mod tests {
    use super::mcp_tool_labels;

    #[test]
    fn parses_mcp_tool_labels() {
        assert_eq!(
            mcp_tool_labels("mcp__github__get_issue"),
            Some(("github".to_string(), "get_issue".to_string()))
        );
        assert_eq!(
            mcp_tool_labels(" mcp__Git Hub__Get Issue "),
            Some(("git_hub".to_string(), "get_issue".to_string()))
        );
        assert_eq!(
            mcp_tool_labels("mcp__server__tool__with_suffix"),
            Some(("server".to_string(), "tool__with_suffix".to_string()))
        );
    }

    #[test]
    fn rejects_non_mcp_tool_labels() {
        assert_eq!(mcp_tool_labels("Bash"), None);
        assert_eq!(mcp_tool_labels("mcp__github"), None);
        assert_eq!(mcp_tool_labels("mcp____get_issue"), None);
        assert_eq!(mcp_tool_labels("mcp__github__"), None);
    }
}
