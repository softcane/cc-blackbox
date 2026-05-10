use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardAction {
    Allow,
    DiagnoseOnly,
    Warn,
    Critical,
    Block,
    Cooldown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardState {
    Healthy,
    Watching,
    Warning,
    Critical,
    Blocked,
    Cooldown,
    Ended,
}

impl GuardState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Watching => "watching",
            Self::Warning => "warning",
            Self::Critical => "critical",
            Self::Blocked => "blocked",
            Self::Cooldown => "cooldown",
            Self::Ended => "ended",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Ended => 0,
            Self::Healthy => 1,
            Self::Watching => 2,
            Self::Warning => 3,
            Self::Critical => 4,
            Self::Blocked => 5,
            Self::Cooldown => 6,
        }
    }
}

impl fmt::Display for GuardState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLevel {
    Heuristic,
    Derived,
    Direct,
    DirectProxy,
    DirectJsonl,
    Inferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSource {
    Proxy,
    Jsonl,
    Heuristic,
    Policy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleId {
    PerSessionTokenBudgetExceeded,
    PerSessionTrustedDollarBudgetExceeded,
    ApiErrorCircuitBreakerCooldown,
    RepeatedCacheRebuilds,
    ContextNearWarningThreshold,
    ModelMismatch,
    SuspectedCompactionLoop,
    ToolFailureStreak,
    HighWeeklyProjectQuotaBurn,
    NoProgressTurns,
    JsonlOnlyToolFailureStreak,
    AmbiguousCacheTtlMiss,
    TaskAbandonmentInference,
}

impl fmt::Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::PerSessionTokenBudgetExceeded => "per_session_token_budget_exceeded",
            Self::PerSessionTrustedDollarBudgetExceeded => {
                "per_session_trusted_dollar_budget_exceeded"
            }
            Self::ApiErrorCircuitBreakerCooldown => "api_error_circuit_breaker_cooldown",
            Self::RepeatedCacheRebuilds => "repeated_cache_rebuilds",
            Self::ContextNearWarningThreshold => "context_near_warning_threshold",
            Self::ModelMismatch => "model_mismatch",
            Self::SuspectedCompactionLoop => "suspected_compaction_loop",
            Self::ToolFailureStreak => "tool_failure_streak",
            Self::HighWeeklyProjectQuotaBurn => "high_weekly_project_quota_burn",
            Self::NoProgressTurns => "no_progress_turns",
            Self::JsonlOnlyToolFailureStreak => "jsonl_only_tool_failure_streak",
            Self::AmbiguousCacheTtlMiss => "ambiguous_cache_ttl_miss",
            Self::TaskAbandonmentInference => "task_abandonment_inference",
        };
        f.write_str(value)
    }
}

impl RuleId {
    pub fn from_policy_key(key: &str) -> Option<Self> {
        match key {
            "per_session_token_budget_exceeded" => Some(Self::PerSessionTokenBudgetExceeded),
            "per_session_trusted_dollar_budget_exceeded" => {
                Some(Self::PerSessionTrustedDollarBudgetExceeded)
            }
            "api_error_circuit_breaker_cooldown" => Some(Self::ApiErrorCircuitBreakerCooldown),
            "repeated_cache_rebuilds" => Some(Self::RepeatedCacheRebuilds),
            "context_near_warning_threshold" => Some(Self::ContextNearWarningThreshold),
            "model_mismatch" => Some(Self::ModelMismatch),
            "suspected_compaction_loop" => Some(Self::SuspectedCompactionLoop),
            "tool_failure_streak" => Some(Self::ToolFailureStreak),
            "high_weekly_project_quota_burn" => Some(Self::HighWeeklyProjectQuotaBurn),
            "no_progress_turns" => Some(Self::NoProgressTurns),
            "jsonl_only_tool_failure_streak" => Some(Self::JsonlOnlyToolFailureStreak),
            "ambiguous_cache_ttl_miss" => Some(Self::AmbiguousCacheTtlMiss),
            "task_abandonment_inference" => Some(Self::TaskAbandonmentInference),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RulePolicy {
    pub action: GuardAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_dollars: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_secs: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GuardPolicy {
    pub fail_open: bool,
    pub rules: BTreeMap<RuleId, RulePolicy>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LoadedPolicy {
    pub policy: GuardPolicy,
    pub source: String,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FindingDraft {
    pub rule_id: RuleId,
    pub severity: FindingSeverity,
    pub evidence_level: EvidenceLevel,
    pub source: FindingSource,
    pub confidence: f64,
    pub session_id: String,
    pub request_id: Option<String>,
    pub turn_number: Option<u32>,
    pub timestamp: String,
    pub detail: String,
    pub suggested_action: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GuardFinding {
    pub rule_id: RuleId,
    pub severity: FindingSeverity,
    pub action: GuardAction,
    pub evidence_level: EvidenceLevel,
    pub source: FindingSource,
    pub confidence: f64,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_number: Option<u32>,
    pub timestamp: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_action: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionBudgetObservation {
    pub session_id: String,
    pub total_tokens: u64,
    pub trusted_spend_dollars: f64,
    pub untrusted_spend_dollars: f64,
    pub total_spend_dollars: f64,
    pub request_count: u64,
    pub estimated_cache_waste_dollars: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApiCooldownObservation {
    pub session_id: Option<String>,
    pub consecutive_errors: u64,
    pub remaining_cooldown_secs: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CacheRebuildObservation {
    pub session_id: String,
    pub request_id: Option<String>,
    pub turn_number: Option<u32>,
    pub timestamp: String,
    pub event_type: String,
    pub estimated_rebuild_cost_dollars: Option<f64>,
    pub cache_creation_tokens: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextObservation {
    pub session_id: String,
    pub request_id: Option<String>,
    pub turn_number: Option<u32>,
    pub timestamp: String,
    pub fill_percent: f64,
    pub context_window_tokens: Option<u64>,
    pub turns_to_compact: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModelMismatchObservation {
    pub session_id: String,
    pub request_id: Option<String>,
    pub turn_number: Option<u32>,
    pub timestamp: String,
    pub requested_model: String,
    pub reported_model: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompactionLoopObservation {
    pub session_id: String,
    pub request_id: Option<String>,
    pub turn_number: Option<u32>,
    pub timestamp: String,
    pub consecutive_turns: u32,
    pub wasted_tokens: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolFailureObservation {
    pub session_id: String,
    pub request_id: Option<String>,
    pub turn_number: Option<u32>,
    pub timestamp: String,
    pub consecutive_failure_turns: u32,
    pub failed_tool_calls: u32,
    pub source: FindingSource,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GuardBlock {
    pub error_type: String,
    pub reason: String,
    pub rule_id: RuleId,
    pub configured_threshold_or_limit: String,
    pub current_observed_value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub suggested_next_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_remaining_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RequestGuardDecision {
    pub block: Option<GuardBlock>,
    pub finding: Option<GuardFinding>,
}

#[derive(Clone, Debug, Default)]
pub struct PolicyManager {
    active: GuardPolicy,
}

#[derive(Debug)]
pub enum PolicyLoadError {
    Io(io::Error),
    InvalidToml {
        path: String,
        message: String,
    },
    InvalidRule {
        path: String,
        rule_key: String,
        message: String,
    },
}

impl fmt::Display for PolicyLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "failed to read guard policy: {err}"),
            Self::InvalidToml { path, message } => {
                write!(f, "invalid guard policy TOML in {path}: {message}")
            }
            Self::InvalidRule {
                path,
                rule_key,
                message,
            } => {
                write!(
                    f,
                    "invalid guard policy rule {rule_key} in {path}: {message}"
                )
            }
        }
    }
}

impl std::error::Error for PolicyLoadError {}

impl FindingDraft {
    pub fn new(
        rule_id: RuleId,
        severity: FindingSeverity,
        evidence_level: EvidenceLevel,
        source: FindingSource,
        confidence: f64,
        session_id: impl Into<String>,
        timestamp: impl Into<String>,
    ) -> Self {
        Self {
            rule_id,
            severity,
            evidence_level,
            source,
            confidence,
            session_id: session_id.into(),
            request_id: None,
            turn_number: None,
            timestamp: timestamp.into(),
            detail: String::new(),
            suggested_action: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = detail.into();
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_turn_number(mut self, turn_number: u32) -> Self {
        self.turn_number = Some(turn_number);
        self
    }

    pub fn with_suggested_action(mut self, suggested_action: impl Into<String>) -> Self {
        self.suggested_action = Some(suggested_action.into());
        self
    }
}

pub fn evaluate_finding(policy: Option<&GuardPolicy>, draft: FindingDraft) -> GuardFinding {
    let action = policy
        .map(|policy| policy.rule_action(draft.rule_id))
        .unwrap_or(GuardAction::Allow);

    finding_with_action(draft, action)
}

fn finding_with_action(draft: FindingDraft, action: GuardAction) -> GuardFinding {
    GuardFinding {
        action,
        rule_id: draft.rule_id,
        severity: draft.severity,
        evidence_level: draft.evidence_level,
        source: draft.source,
        confidence: draft.confidence,
        session_id: draft.session_id,
        request_id: draft.request_id,
        turn_number: draft.turn_number,
        timestamp: draft.timestamp,
        detail: draft.detail,
        suggested_action: draft.suggested_action,
    }
}

impl RulePolicy {
    fn with_action(action: GuardAction) -> Self {
        Self {
            action,
            limit_tokens: None,
            limit_dollars: None,
            threshold_count: None,
            cooldown_secs: None,
        }
    }
}

pub fn evaluate_session_budget_for_request(
    policy: Option<&GuardPolicy>,
    observation: Result<Option<SessionBudgetObservation>, String>,
    timestamp: &str,
) -> RequestGuardDecision {
    let Some(policy) = policy else {
        return RequestGuardDecision::default();
    };
    let Ok(Some(observation)) = observation else {
        return RequestGuardDecision::default();
    };

    if let Some(limit) = policy
        .rules
        .get(&RuleId::PerSessionTokenBudgetExceeded)
        .and_then(|rule| (rule.action == GuardAction::Block).then_some(rule.limit_tokens))
        .flatten()
    {
        if observation.total_tokens >= limit {
            let suggested =
                "Start a fresh session, raise the per-session token limit, or disable the limit."
                    .to_string();
            let detail = format!(
                "Session has used {} tokens across {} requests, crossing the configured {} token limit.",
                observation.total_tokens, observation.request_count, limit
            );
            let draft = FindingDraft::new(
                RuleId::PerSessionTokenBudgetExceeded,
                FindingSeverity::Critical,
                EvidenceLevel::DirectProxy,
                FindingSource::Proxy,
                1.0,
                observation.session_id.clone(),
                timestamp,
            )
            .with_detail(detail)
            .with_suggested_action(suggested.clone());

            return RequestGuardDecision {
                block: Some(GuardBlock {
                    error_type: "budget_exceeded".to_string(),
                    reason: "Session token budget exceeded.".to_string(),
                    rule_id: RuleId::PerSessionTokenBudgetExceeded,
                    configured_threshold_or_limit: format!("{limit} tokens"),
                    current_observed_value: format!("{} tokens", observation.total_tokens),
                    session_id: Some(observation.session_id),
                    suggested_next_action: suggested,
                    cooldown_remaining_secs: None,
                }),
                finding: Some(policy.resolve_finding(draft)),
            };
        }
    }

    if let Some(limit) = policy
        .rules
        .get(&RuleId::PerSessionTrustedDollarBudgetExceeded)
        .and_then(|rule| (rule.action == GuardAction::Block).then_some(rule.limit_dollars))
        .flatten()
    {
        if observation.trusted_spend_dollars >= limit {
            let suggested =
                "Start a fresh session, raise the trusted-dollar limit, or disable the limit."
                    .to_string();
            let detail = format!(
                "Session trusted spend is ${:.2} across {} requests, crossing the configured ${:.2} limit. Total estimated spend is ${:.2}; display-only untrusted spend is ${:.2}; estimated cache rebuild waste is ${:.2}.",
                observation.trusted_spend_dollars,
                observation.request_count,
                limit,
                observation.total_spend_dollars,
                observation.untrusted_spend_dollars,
                observation.estimated_cache_waste_dollars
            );
            let draft = FindingDraft::new(
                RuleId::PerSessionTrustedDollarBudgetExceeded,
                FindingSeverity::Critical,
                EvidenceLevel::DirectProxy,
                FindingSource::Proxy,
                1.0,
                observation.session_id.clone(),
                timestamp,
            )
            .with_detail(detail)
            .with_suggested_action(suggested.clone());

            return RequestGuardDecision {
                block: Some(GuardBlock {
                    error_type: "budget_exceeded".to_string(),
                    reason: "Session trusted-dollar budget exceeded.".to_string(),
                    rule_id: RuleId::PerSessionTrustedDollarBudgetExceeded,
                    configured_threshold_or_limit: format!("${limit:.2} trusted spend"),
                    current_observed_value: format!(
                        "${:.2} trusted spend",
                        observation.trusted_spend_dollars
                    ),
                    session_id: Some(observation.session_id),
                    suggested_next_action: suggested,
                    cooldown_remaining_secs: None,
                }),
                finding: Some(policy.resolve_finding(draft)),
            };
        }
    }

    RequestGuardDecision::default()
}

pub fn evaluate_api_cooldown_for_request(
    policy: Option<&GuardPolicy>,
    observation: Result<Option<ApiCooldownObservation>, String>,
    timestamp: &str,
) -> RequestGuardDecision {
    let Some(policy) = policy else {
        return RequestGuardDecision::default();
    };
    let Ok(Some(observation)) = observation else {
        return RequestGuardDecision::default();
    };
    let Some(rule) = policy.rules.get(&RuleId::ApiErrorCircuitBreakerCooldown) else {
        return RequestGuardDecision::default();
    };
    if !matches!(rule.action, GuardAction::Cooldown | GuardAction::Block) {
        return RequestGuardDecision::default();
    }

    let threshold = rule.threshold_count.unwrap_or(5);
    let cooldown_secs = rule.cooldown_secs.unwrap_or(30);
    let suggested =
        "Wait for the cooldown to expire, fix the API error cause, or raise the cooldown policy."
            .to_string();
    let detail = format!(
        "{} consecutive API errors opened a {}s cooldown; {}s remain.",
        observation.consecutive_errors, cooldown_secs, observation.remaining_cooldown_secs
    );
    let session_id = observation
        .session_id
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let draft = FindingDraft::new(
        RuleId::ApiErrorCircuitBreakerCooldown,
        FindingSeverity::Critical,
        EvidenceLevel::DirectProxy,
        FindingSource::Proxy,
        1.0,
        session_id,
        timestamp,
    )
    .with_detail(detail)
    .with_suggested_action(suggested.clone());

    RequestGuardDecision {
        block: Some(GuardBlock {
            error_type: "circuit_breaker".to_string(),
            reason: "API error cooldown is active.".to_string(),
            rule_id: RuleId::ApiErrorCircuitBreakerCooldown,
            configured_threshold_or_limit: format!(
                "{threshold} consecutive API errors; cooldown {cooldown_secs}s"
            ),
            current_observed_value: format!(
                "{} consecutive API errors",
                observation.consecutive_errors
            ),
            session_id: observation.session_id,
            suggested_next_action: suggested,
            cooldown_remaining_secs: Some(observation.remaining_cooldown_secs),
        }),
        finding: Some(policy.resolve_finding(draft)),
    }
}

pub fn detect_cache_rebuild_finding(
    policy: Option<&GuardPolicy>,
    observation: CacheRebuildObservation,
) -> Option<GuardFinding> {
    if !matches!(
        observation.event_type.as_str(),
        "miss_rebuild" | "miss_ttl" | "miss_thrash"
    ) {
        return None;
    }

    let cost = observation
        .estimated_rebuild_cost_dollars
        .map(|value| format!(" Estimated rebuild cost is ${value:.2}."))
        .unwrap_or_default();
    let detail = format!(
        "Cache rebuild event {} created {} input tokens.{}",
        observation.event_type, observation.cache_creation_tokens, cost
    );
    let mut draft = FindingDraft::new(
        RuleId::RepeatedCacheRebuilds,
        FindingSeverity::Warning,
        EvidenceLevel::DirectProxy,
        FindingSource::Proxy,
        0.9,
        observation.session_id,
        observation.timestamp,
    )
    .with_detail(detail)
    .with_suggested_action(
        "Keep the session warm before expensive follow-up turns or restart with a smaller prompt.",
    );
    draft.request_id = observation.request_id;
    draft.turn_number = observation.turn_number;

    Some(evaluate_finding(policy, draft))
}

pub fn detect_context_finding(
    policy: Option<&GuardPolicy>,
    observation: ContextObservation,
) -> Option<GuardFinding> {
    let threshold = policy
        .and_then(|policy| policy.rules.get(&RuleId::ContextNearWarningThreshold))
        .and_then(|rule| rule.threshold_count)
        .unwrap_or(80) as f64;
    let near_compaction = matches!(observation.turns_to_compact, Some(turns) if turns <= 2);
    if observation.fill_percent < threshold && !near_compaction {
        return None;
    }

    let runway = match observation.turns_to_compact {
        Some(0) => "auto-compaction threshold reached".to_string(),
        Some(1) => "about 1 turn to auto-compaction".to_string(),
        Some(turns) => format!("about {turns} turns to auto-compaction"),
        None => "auto-compaction estimate unavailable".to_string(),
    };
    let window = observation
        .context_window_tokens
        .map(|tokens| format!(" using a {tokens} token window"))
        .unwrap_or_default();
    let detail = format!(
        "Context is {:.0}% full{window}; {runway}.",
        observation.fill_percent
    );
    let mut draft = FindingDraft::new(
        RuleId::ContextNearWarningThreshold,
        FindingSeverity::Warning,
        EvidenceLevel::Derived,
        FindingSource::Proxy,
        0.82,
        observation.session_id,
        observation.timestamp,
    )
    .with_detail(detail)
    .with_suggested_action("Narrow the next request or start a fresh session before compaction.");
    draft.request_id = observation.request_id;
    draft.turn_number = observation.turn_number;

    Some(evaluate_finding(policy, draft))
}

pub fn detect_model_mismatch_finding(
    policy: Option<&GuardPolicy>,
    observation: ModelMismatchObservation,
) -> Option<GuardFinding> {
    if crate::model_matches(&observation.requested_model, &observation.reported_model) {
        return None;
    }

    let detail = format!(
        "Requested model {}; response reported model {}. This records a model-route mismatch and does not prove why routing changed.",
        observation.requested_model, observation.reported_model
    );
    let mut draft = FindingDraft::new(
        RuleId::ModelMismatch,
        FindingSeverity::Warning,
        EvidenceLevel::DirectProxy,
        FindingSource::Proxy,
        0.98,
        observation.session_id,
        observation.timestamp,
    )
    .with_detail(detail)
    .with_suggested_action(
        "Retry or explicitly choose the intended model if this changed results.",
    );
    draft.request_id = observation.request_id;
    draft.turn_number = observation.turn_number;

    Some(evaluate_finding(policy, draft))
}

pub fn detect_compaction_loop_finding(
    policy: Option<&GuardPolicy>,
    observation: CompactionLoopObservation,
) -> Option<GuardFinding> {
    if observation.consecutive_turns < 3 {
        return None;
    }

    let detail = format!(
        "Suspected compaction loop: heuristic detected {} rapid turns with stable high token usage; estimated wasted tokens {}.",
        observation.consecutive_turns, observation.wasted_tokens
    );
    let mut draft = FindingDraft::new(
        RuleId::SuspectedCompactionLoop,
        FindingSeverity::Warning,
        EvidenceLevel::Inferred,
        FindingSource::Heuristic,
        0.72,
        observation.session_id,
        observation.timestamp,
    )
    .with_detail(detail)
    .with_suggested_action("Interrupt and restart from a short state summary if progress stalls.");
    draft.request_id = observation.request_id;
    draft.turn_number = observation.turn_number;

    Some(evaluate_finding(policy, draft))
}

pub fn detect_tool_failure_finding(
    policy: Option<&GuardPolicy>,
    observation: ToolFailureObservation,
) -> Option<GuardFinding> {
    let rule_id = match observation.source {
        FindingSource::Proxy => RuleId::ToolFailureStreak,
        FindingSource::Jsonl => RuleId::JsonlOnlyToolFailureStreak,
        _ => return None,
    };
    let threshold = policy
        .and_then(|policy| policy.rules.get(&rule_id))
        .and_then(|rule| rule.threshold_count)
        .unwrap_or(5) as u32;
    if observation.consecutive_failure_turns < threshold {
        return None;
    }

    let (evidence_level, source, detail_prefix, confidence) = match observation.source {
        FindingSource::Proxy => (
            EvidenceLevel::DirectProxy,
            FindingSource::Proxy,
            "Proxy-visible tool failure streak",
            0.92,
        ),
        FindingSource::Jsonl => (
            EvidenceLevel::DirectJsonl,
            FindingSource::Jsonl,
            "JSONL-only tool failure streak",
            0.9,
        ),
        _ => return None,
    };
    let detail = format!(
        "{detail_prefix}: {} consecutive turns included {} failed tool calls.",
        observation.consecutive_failure_turns, observation.failed_tool_calls
    );
    let mut draft = FindingDraft::new(
        rule_id,
        FindingSeverity::Warning,
        evidence_level,
        source,
        confidence,
        observation.session_id,
        observation.timestamp,
    )
    .with_detail(detail)
    .with_suggested_action("Fix the failing tool precondition or redirect the session.");
    draft.request_id = observation.request_id;
    draft.turn_number = observation.turn_number;

    Some(evaluate_finding(policy, draft))
}

pub fn derive_guard_state(base_state: GuardState, findings: &[GuardFinding]) -> GuardState {
    if base_state == GuardState::Ended {
        return GuardState::Ended;
    }

    let mut state = base_state;
    for finding in findings {
        let candidate = match finding.action {
            GuardAction::Cooldown => GuardState::Cooldown,
            GuardAction::Block => GuardState::Blocked,
            GuardAction::Critical => GuardState::Critical,
            GuardAction::Warn => GuardState::Warning,
            GuardAction::Allow | GuardAction::DiagnoseOnly => base_state,
        };
        if candidate.rank() > state.rank() {
            state = candidate;
        }
    }
    state
}

impl PolicyManager {
    pub fn active_policy(&self) -> &GuardPolicy {
        &self.active
    }

    pub fn reload_from_path(&mut self, path: &Path) -> Result<LoadedPolicy, PolicyLoadError> {
        let loaded = GuardPolicy::load_effective_from_path(path)?;
        self.active = loaded.policy.clone();
        Ok(loaded)
    }
}

#[derive(Deserialize)]
struct PolicyToml {
    fail_open: Option<bool>,
    rules: Option<BTreeMap<String, toml::Value>>,
}

#[derive(Deserialize)]
struct RulePolicyToml {
    action: Option<GuardAction>,
    limit_tokens: Option<u64>,
    limit_dollars: Option<f64>,
    threshold_count: Option<u64>,
    cooldown_secs: Option<u64>,
}

impl GuardPolicy {
    pub fn load_effective_from_path(path: &Path) -> Result<LoadedPolicy, PolicyLoadError> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let source = path.display().to_string();
                let (policy, warnings) = Self::from_toml_overlay(&contents, &source)?;
                Ok(LoadedPolicy {
                    policy,
                    source,
                    warnings,
                })
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(LoadedPolicy {
                policy: Self::default(),
                source: "defaults".to_string(),
                warnings: Vec::new(),
            }),
            Err(err) => Err(PolicyLoadError::Io(err)),
        }
    }

    fn from_toml_overlay(
        contents: &str,
        source: &str,
    ) -> Result<(Self, Vec<String>), PolicyLoadError> {
        let document: toml::Value =
            toml::from_str(contents).map_err(|err| PolicyLoadError::InvalidToml {
                path: source.to_string(),
                message: err.to_string(),
            })?;
        let warnings = unknown_policy_key_warnings(&document);
        let raw: PolicyToml =
            document
                .try_into()
                .map_err(|err: toml::de::Error| PolicyLoadError::InvalidToml {
                    path: source.to_string(),
                    message: err.to_string(),
                })?;
        let mut policy = Self::default();

        if let Some(fail_open) = raw.fail_open {
            policy.fail_open = fail_open;
        }

        if let Some(rules) = raw.rules {
            for (rule_key, value) in rules {
                let Some(rule_id) = RuleId::from_policy_key(&rule_key) else {
                    continue;
                };
                let overlay: RulePolicyToml =
                    value.try_into().map_err(|err: toml::de::Error| {
                        PolicyLoadError::InvalidRule {
                            path: source.to_string(),
                            rule_key: rule_key.clone(),
                            message: err.to_string(),
                        }
                    })?;
                if let Some(action) = overlay.action {
                    policy
                        .rules
                        .entry(rule_id)
                        .and_modify(|rule| rule.action = action)
                        .or_insert_with(|| RulePolicy::with_action(action));
                }
                if let Some(limit_tokens) = overlay.limit_tokens {
                    policy
                        .rules
                        .entry(rule_id)
                        .or_insert_with(|| RulePolicy::with_action(GuardAction::Allow))
                        .limit_tokens = Some(limit_tokens);
                }
                if let Some(limit_dollars) = overlay.limit_dollars {
                    policy
                        .rules
                        .entry(rule_id)
                        .or_insert_with(|| RulePolicy::with_action(GuardAction::Allow))
                        .limit_dollars = Some(limit_dollars);
                }
                if let Some(threshold_count) = overlay.threshold_count {
                    policy
                        .rules
                        .entry(rule_id)
                        .or_insert_with(|| RulePolicy::with_action(GuardAction::Allow))
                        .threshold_count = Some(threshold_count);
                }
                if let Some(cooldown_secs) = overlay.cooldown_secs {
                    policy
                        .rules
                        .entry(rule_id)
                        .or_insert_with(|| RulePolicy::with_action(GuardAction::Allow))
                        .cooldown_secs = Some(cooldown_secs);
                }
            }
        }

        Ok((policy, warnings))
    }

    pub fn rule_action(&self, rule_id: RuleId) -> GuardAction {
        self.rules
            .get(&rule_id)
            .map(|rule| rule.action)
            .unwrap_or(GuardAction::Allow)
    }

    pub fn resolve_finding(&self, draft: FindingDraft) -> GuardFinding {
        evaluate_finding(Some(self), draft)
    }
}

fn unknown_policy_key_warnings(document: &toml::Value) -> Vec<String> {
    let Some(root) = document.as_table() else {
        return Vec::new();
    };

    let mut warnings = Vec::new();
    for key in root.keys() {
        if !matches!(key.as_str(), "fail_open" | "rules") {
            warnings.push(format!("unknown guard policy key `{key}` ignored"));
        }
    }

    let Some(rules) = root.get("rules").and_then(toml::Value::as_table) else {
        return warnings;
    };

    for (rule_key, value) in rules {
        if RuleId::from_policy_key(rule_key).is_none() {
            warnings.push(format!(
                "unknown guard policy rule `rules.{rule_key}` ignored"
            ));
            continue;
        }

        let Some(rule_table) = value.as_table() else {
            continue;
        };
        for field_key in rule_table.keys() {
            if !matches!(
                field_key.as_str(),
                "action" | "limit_tokens" | "limit_dollars" | "threshold_count" | "cooldown_secs"
            ) {
                warnings.push(format!(
                    "unknown guard policy key `rules.{rule_key}.{field_key}` ignored"
                ));
            }
        }
    }

    warnings
}

impl Default for GuardPolicy {
    fn default() -> Self {
        let mut rules = BTreeMap::new();
        rules.insert(
            RuleId::PerSessionTokenBudgetExceeded,
            RulePolicy::with_action(GuardAction::Block),
        );
        rules.insert(
            RuleId::PerSessionTrustedDollarBudgetExceeded,
            RulePolicy::with_action(GuardAction::Block),
        );
        let mut api_error_policy = RulePolicy::with_action(GuardAction::Cooldown);
        api_error_policy.threshold_count = Some(5);
        api_error_policy.cooldown_secs = Some(30);
        rules.insert(RuleId::ApiErrorCircuitBreakerCooldown, api_error_policy);

        for rule_id in [
            RuleId::RepeatedCacheRebuilds,
            RuleId::ContextNearWarningThreshold,
            RuleId::ModelMismatch,
            RuleId::SuspectedCompactionLoop,
            RuleId::ToolFailureStreak,
            RuleId::HighWeeklyProjectQuotaBurn,
        ] {
            rules.insert(rule_id, RulePolicy::with_action(GuardAction::Warn));
        }

        for rule_id in [
            RuleId::NoProgressTurns,
            RuleId::JsonlOnlyToolFailureStreak,
            RuleId::AmbiguousCacheTtlMiss,
            RuleId::TaskAbandonmentInference,
        ] {
            rules.insert(rule_id, RulePolicy::with_action(GuardAction::DiagnoseOnly));
        }

        Self {
            fail_open: true,
            rules,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        detect_cache_rebuild_finding, detect_context_finding, evaluate_api_cooldown_for_request,
        evaluate_finding, evaluate_session_budget_for_request, ApiCooldownObservation,
        CacheRebuildObservation, CompactionLoopObservation, ContextObservation, EvidenceLevel,
        FindingDraft, FindingSeverity, FindingSource, GuardAction, GuardFinding, GuardPolicy,
        GuardState, ModelMismatchObservation, PolicyManager, RequestGuardDecision, RuleId,
        SessionBudgetObservation,
    };

    fn unique_policy_path(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("cc-blackbox-{test_name}-{nanos}.toml"))
    }

    #[test]
    fn effective_policy_uses_documented_defaults_when_config_is_missing() {
        let missing_path = unique_policy_path("missing-defaults");

        let loaded = GuardPolicy::load_effective_from_path(&missing_path)
            .expect("missing config should load defaults");

        assert_eq!(loaded.source, "defaults");
        assert!(loaded.warnings.is_empty());
        assert!(loaded.policy.fail_open);
        assert_eq!(
            loaded
                .policy
                .rule_action(RuleId::PerSessionTokenBudgetExceeded),
            GuardAction::Block
        );
        assert_eq!(
            loaded.policy.rule_action(RuleId::RepeatedCacheRebuilds),
            GuardAction::Warn
        );
        assert_eq!(
            loaded
                .policy
                .rule_action(RuleId::JsonlOnlyToolFailureStreak),
            GuardAction::DiagnoseOnly
        );
    }

    #[test]
    fn toml_overrides_merge_with_documented_defaults() {
        let path = unique_policy_path("merge-defaults");
        fs::write(
            &path,
            r#"
fail_open = false

[rules.per_session_token_budget_exceeded]
action = "warn"

[rules.repeated_cache_rebuilds]
action = "block"
"#,
        )
        .expect("write policy fixture");

        let loaded = GuardPolicy::load_effective_from_path(&path).expect("policy loads");

        assert_eq!(loaded.source, path.display().to_string());
        assert!(!loaded.policy.fail_open);
        assert_eq!(
            loaded
                .policy
                .rule_action(RuleId::PerSessionTokenBudgetExceeded),
            GuardAction::Warn
        );
        assert_eq!(
            loaded.policy.rule_action(RuleId::RepeatedCacheRebuilds),
            GuardAction::Block
        );
        assert_eq!(
            loaded.policy.rule_action(RuleId::ModelMismatch),
            GuardAction::Warn
        );
        assert_eq!(
            loaded
                .policy
                .rule_action(RuleId::JsonlOnlyToolFailureStreak),
            GuardAction::DiagnoseOnly
        );

        fs::remove_file(path).expect("remove policy fixture");
    }

    #[test]
    fn toml_overrides_load_request_enforcement_thresholds() {
        let path = unique_policy_path("request-thresholds");
        fs::write(
            &path,
            r#"
[rules.per_session_token_budget_exceeded]
limit_tokens = 1500

[rules.per_session_trusted_dollar_budget_exceeded]
limit_dollars = 7.25

[rules.api_error_circuit_breaker_cooldown]
threshold_count = 3
cooldown_secs = 90
"#,
        )
        .expect("write policy fixture");

        let loaded = GuardPolicy::load_effective_from_path(&path).expect("policy loads");

        assert_eq!(
            loaded
                .policy
                .rules
                .get(&RuleId::PerSessionTokenBudgetExceeded)
                .and_then(|rule| rule.limit_tokens),
            Some(1500)
        );
        assert_eq!(
            loaded
                .policy
                .rules
                .get(&RuleId::PerSessionTrustedDollarBudgetExceeded)
                .and_then(|rule| rule.limit_dollars),
            Some(7.25)
        );
        let api_rule = loaded
            .policy
            .rules
            .get(&RuleId::ApiErrorCircuitBreakerCooldown)
            .expect("api rule exists");
        assert_eq!(api_rule.threshold_count, Some(3));
        assert_eq!(api_rule.cooldown_secs, Some(90));

        fs::remove_file(path).expect("remove policy fixture");
    }

    #[test]
    fn unknown_policy_keys_generate_warnings_without_rejecting_config() {
        let path = unique_policy_path("unknown-keys");
        fs::write(
            &path,
            r#"
unexpected_top_level = true

[rules.not_a_real_rule]
action = "block"

[rules.repeated_cache_rebuilds]
action = "warn"
"#,
        )
        .expect("write policy fixture");

        let loaded = GuardPolicy::load_effective_from_path(&path).expect("policy loads");

        assert_eq!(
            loaded.policy.rule_action(RuleId::RepeatedCacheRebuilds),
            GuardAction::Warn
        );
        assert!(loaded
            .warnings
            .iter()
            .any(|warning| warning.contains("unexpected_top_level")));
        assert!(loaded
            .warnings
            .iter()
            .any(|warning| warning.contains("not_a_real_rule")));

        fs::remove_file(path).expect("remove policy fixture");
    }

    #[test]
    fn invalid_known_values_fail_load_and_keep_last_valid_policy() {
        let path = unique_policy_path("keep-last-valid");
        fs::write(
            &path,
            r#"
[rules.repeated_cache_rebuilds]
action = "block"
"#,
        )
        .expect("write valid policy");

        let mut manager = PolicyManager::default();
        manager
            .reload_from_path(&path)
            .expect("valid policy should load");
        assert_eq!(
            manager
                .active_policy()
                .rule_action(RuleId::RepeatedCacheRebuilds),
            GuardAction::Block
        );

        fs::write(
            &path,
            r#"
[rules.repeated_cache_rebuilds]
action = "page_the_user"
"#,
        )
        .expect("write invalid policy");

        let error = manager
            .reload_from_path(&path)
            .expect_err("invalid known action should fail");
        assert!(error.to_string().contains("repeated_cache_rebuilds"));
        assert_eq!(
            manager
                .active_policy()
                .rule_action(RuleId::RepeatedCacheRebuilds),
            GuardAction::Block
        );

        fs::remove_file(path).expect("remove policy fixture");
    }

    #[test]
    fn budget_finding_resolves_to_block_under_default_policy() {
        let draft = FindingDraft::new(
            RuleId::PerSessionTokenBudgetExceeded,
            FindingSeverity::Critical,
            EvidenceLevel::Direct,
            FindingSource::Proxy,
            0.99,
            "session-budget",
            "2026-05-10T00:00:00Z",
        )
        .with_detail("This session exceeded the configured token budget.");

        let finding = GuardPolicy::default().resolve_finding(draft);

        assert_eq!(finding.rule_id, RuleId::PerSessionTokenBudgetExceeded);
        assert_eq!(finding.action, GuardAction::Block);
        assert_eq!(finding.session_id, "session-budget");
    }

    #[test]
    fn cache_rebuild_finding_resolves_to_warn_under_default_policy() {
        let draft = FindingDraft::new(
            RuleId::RepeatedCacheRebuilds,
            FindingSeverity::Warning,
            EvidenceLevel::Derived,
            FindingSource::Proxy,
            0.82,
            "session-cache",
            "2026-05-10T00:00:00Z",
        )
        .with_detail("Repeated cache rebuilds are increasing the estimated turn cost.")
        .with_turn_number(4);

        let finding = GuardPolicy::default().resolve_finding(draft);

        assert_eq!(finding.rule_id, RuleId::RepeatedCacheRebuilds);
        assert_eq!(finding.action, GuardAction::Warn);
        assert_eq!(finding.turn_number, Some(4));
    }

    #[test]
    fn policy_evaluation_fails_open_when_no_policy_is_available() {
        let draft = FindingDraft::new(
            RuleId::PerSessionTokenBudgetExceeded,
            FindingSeverity::Critical,
            EvidenceLevel::Direct,
            FindingSource::Proxy,
            0.99,
            "session-open",
            "2026-05-10T00:00:00Z",
        )
        .with_detail("A budget detector fired but no valid policy is available.");

        let finding = evaluate_finding(None, draft);

        assert_eq!(finding.action, GuardAction::Allow);
        assert_eq!(finding.rule_id, RuleId::PerSessionTokenBudgetExceeded);
        assert_eq!(finding.session_id, "session-open");
    }

    #[test]
    fn crossed_token_budget_resolves_to_explainable_request_block() {
        let mut policy = GuardPolicy::default();
        policy
            .rules
            .get_mut(&RuleId::PerSessionTokenBudgetExceeded)
            .expect("default token rule exists")
            .limit_tokens = Some(100);

        let decision = evaluate_session_budget_for_request(
            Some(&policy),
            Ok(Some(SessionBudgetObservation {
                session_id: "session-token-block".to_string(),
                total_tokens: 101,
                trusted_spend_dollars: 0.0,
                untrusted_spend_dollars: 0.0,
                total_spend_dollars: 0.0,
                request_count: 2,
                estimated_cache_waste_dollars: 0.0,
            })),
            "2026-05-10T00:00:00Z",
        );

        let block = decision.block.expect("request should block");
        assert_eq!(block.rule_id, RuleId::PerSessionTokenBudgetExceeded);
        assert_eq!(block.reason, "Session token budget exceeded.");
        assert_eq!(block.configured_threshold_or_limit, "100 tokens");
        assert_eq!(block.current_observed_value, "101 tokens");
        assert_eq!(block.session_id.as_deref(), Some("session-token-block"));
        assert!(block
            .suggested_next_action
            .to_ascii_lowercase()
            .contains("start a fresh session"));

        let finding = decision.finding.expect("block emits finding");
        assert_eq!(finding.action, GuardAction::Block);
        assert_eq!(finding.evidence_level, EvidenceLevel::DirectProxy);
        assert_eq!(finding.source, FindingSource::Proxy);
        assert_eq!(finding.session_id, "session-token-block");
    }

    #[test]
    fn crossed_trusted_dollar_budget_resolves_to_explainable_request_block() {
        let mut policy = GuardPolicy::default();
        policy
            .rules
            .get_mut(&RuleId::PerSessionTrustedDollarBudgetExceeded)
            .expect("default dollar rule exists")
            .limit_dollars = Some(2.5);

        let decision = evaluate_session_budget_for_request(
            Some(&policy),
            Ok(Some(SessionBudgetObservation {
                session_id: "session-dollar-block".to_string(),
                total_tokens: 10,
                trusted_spend_dollars: 2.75,
                untrusted_spend_dollars: 4.0,
                total_spend_dollars: 6.75,
                request_count: 3,
                estimated_cache_waste_dollars: 0.25,
            })),
            "2026-05-10T00:00:00Z",
        );

        let block = decision.block.expect("request should block");
        assert_eq!(block.rule_id, RuleId::PerSessionTrustedDollarBudgetExceeded);
        assert_eq!(block.reason, "Session trusted-dollar budget exceeded.");
        assert_eq!(block.configured_threshold_or_limit, "$2.50 trusted spend");
        assert_eq!(block.current_observed_value, "$2.75 trusted spend");
        assert_eq!(block.session_id.as_deref(), Some("session-dollar-block"));

        let finding = decision.finding.expect("block emits finding");
        assert_eq!(finding.action, GuardAction::Block);
        assert_eq!(
            finding.rule_id,
            RuleId::PerSessionTrustedDollarBudgetExceeded
        );
        assert!(finding.detail.contains("display-only untrusted spend"));
    }

    #[test]
    fn untrusted_dollar_spend_does_not_hard_block_by_default() {
        let mut policy = GuardPolicy::default();
        policy
            .rules
            .get_mut(&RuleId::PerSessionTrustedDollarBudgetExceeded)
            .expect("default dollar rule exists")
            .limit_dollars = Some(2.5);

        let decision = evaluate_session_budget_for_request(
            Some(&policy),
            Ok(Some(SessionBudgetObservation {
                session_id: "session-untrusted-allow".to_string(),
                total_tokens: 10,
                trusted_spend_dollars: 0.0,
                untrusted_spend_dollars: 50.0,
                total_spend_dollars: 50.0,
                request_count: 3,
                estimated_cache_waste_dollars: 2.0,
            })),
            "2026-05-10T00:00:00Z",
        );

        assert_eq!(decision, RequestGuardDecision::default());
    }

    #[test]
    fn active_api_error_cooldown_blocks_with_remaining_cooldown() {
        let mut policy = GuardPolicy::default();
        let api_rule = policy
            .rules
            .get_mut(&RuleId::ApiErrorCircuitBreakerCooldown)
            .expect("default api cooldown rule exists");
        api_rule.threshold_count = Some(3);
        api_rule.cooldown_secs = Some(45);

        let decision = evaluate_api_cooldown_for_request(
            Some(&policy),
            Ok(Some(ApiCooldownObservation {
                session_id: Some("session-cooldown".to_string()),
                consecutive_errors: 3,
                remaining_cooldown_secs: 17,
            })),
            "2026-05-10T00:00:00Z",
        );

        let block = decision.block.expect("cooldown should block");
        assert_eq!(block.error_type, "circuit_breaker");
        assert_eq!(block.rule_id, RuleId::ApiErrorCircuitBreakerCooldown);
        assert_eq!(
            block.configured_threshold_or_limit,
            "3 consecutive API errors; cooldown 45s"
        );
        assert_eq!(block.current_observed_value, "3 consecutive API errors");
        assert_eq!(block.cooldown_remaining_secs, Some(17));
        assert_eq!(block.session_id.as_deref(), Some("session-cooldown"));

        let finding = decision.finding.expect("cooldown emits finding");
        assert_eq!(finding.action, GuardAction::Cooldown);
        assert_eq!(finding.rule_id, RuleId::ApiErrorCircuitBreakerCooldown);
    }

    #[test]
    fn request_enforcement_fails_open_without_policy_or_detector_state() {
        let observation = SessionBudgetObservation {
            session_id: "session-fail-open".to_string(),
            total_tokens: 1_000_000,
            trusted_spend_dollars: 100.0,
            untrusted_spend_dollars: 0.0,
            total_spend_dollars: 100.0,
            request_count: 20,
            estimated_cache_waste_dollars: 0.0,
        };

        assert_eq!(
            evaluate_session_budget_for_request(
                None,
                Ok(Some(observation)),
                "2026-05-10T00:00:00Z",
            ),
            RequestGuardDecision::default()
        );
        assert_eq!(
            evaluate_session_budget_for_request(
                Some(&GuardPolicy::default()),
                Err("detector failed".to_string()),
                "2026-05-10T00:00:00Z",
            ),
            RequestGuardDecision::default()
        );
        assert_eq!(
            evaluate_api_cooldown_for_request(
                Some(&GuardPolicy::default()),
                Err("cooldown detector failed".to_string()),
                "2026-05-10T00:00:00Z",
            ),
            RequestGuardDecision::default()
        );
    }

    #[test]
    fn serialized_findings_carry_metadata_without_raw_content_fields() {
        let draft = FindingDraft::new(
            RuleId::ModelMismatch,
            FindingSeverity::Warning,
            EvidenceLevel::Derived,
            FindingSource::Proxy,
            0.76,
            "session-schema",
            "2026-05-10T00:00:00Z",
        )
        .with_detail("The response reported a different model than the request asked for.")
        .with_request_id("req-schema")
        .with_turn_number(3);

        let finding = GuardPolicy::default().resolve_finding(draft);
        let json = serde_json::to_value(&finding).expect("finding serializes");
        let object = json.as_object().expect("finding serializes as object");

        for key in [
            "rule_id",
            "severity",
            "action",
            "evidence_level",
            "source",
            "confidence",
            "session_id",
            "request_id",
            "turn_number",
            "timestamp",
            "detail",
        ] {
            assert!(object.contains_key(key), "missing {key}");
        }

        assert_eq!(json["rule_id"], "model_mismatch");
        assert_eq!(json["severity"], "warning");
        assert_eq!(json["action"], "warn");
        assert_eq!(json["evidence_level"], "derived");
        assert_eq!(json["source"], "proxy");
        assert_eq!(json["session_id"], "session-schema");
        assert_eq!(json["request_id"], "req-schema");
        assert_eq!(json["turn_number"], 3);
        assert_eq!(json["timestamp"], "2026-05-10T00:00:00Z");
        assert_eq!(
            json["detail"],
            "The response reported a different model than the request asked for."
        );

        for forbidden_key in [
            "raw_prompt",
            "prompt",
            "assistant_text",
            "tool_output",
            "file_contents",
            "raw_jsonl",
            "raw_jsonl_text",
        ] {
            assert!(
                !object.contains_key(forbidden_key),
                "finding exposed content-bearing field {forbidden_key}"
            );
        }
    }

    #[test]
    fn phase3_provenance_labels_cover_proxy_jsonl_and_heuristic_evidence() {
        let labels = serde_json::json!([
            {
                "source": FindingSource::Proxy,
                "evidence_level": EvidenceLevel::DirectProxy,
            },
            {
                "source": FindingSource::Jsonl,
                "evidence_level": EvidenceLevel::DirectJsonl,
            },
            {
                "source": FindingSource::Heuristic,
                "evidence_level": EvidenceLevel::Inferred,
            }
        ]);

        assert_eq!(labels[0]["source"], "proxy");
        assert_eq!(labels[0]["evidence_level"], "direct_proxy");
        assert_eq!(labels[1]["source"], "jsonl");
        assert_eq!(labels[1]["evidence_level"], "direct_jsonl");
        assert_eq!(labels[2]["source"], "heuristic");
        assert_eq!(labels[2]["evidence_level"], "inferred");
    }

    #[test]
    fn cache_rebuild_detector_emits_warning_finding_under_default_policy() {
        let finding = detect_cache_rebuild_finding(
            Some(&GuardPolicy::default()),
            CacheRebuildObservation {
                session_id: "session-cache-warning".to_string(),
                request_id: Some("req-cache-warning".to_string()),
                turn_number: Some(3),
                timestamp: "2026-05-10T00:00:00Z".to_string(),
                event_type: "miss_ttl".to_string(),
                estimated_rebuild_cost_dollars: Some(0.24),
                cache_creation_tokens: 12_000,
            },
        )
        .expect("cache rebuild finding");

        assert_eq!(finding.rule_id, RuleId::RepeatedCacheRebuilds);
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert_eq!(finding.action, GuardAction::Warn);
        assert_eq!(finding.source, FindingSource::Proxy);
        assert_eq!(finding.evidence_level, EvidenceLevel::DirectProxy);
        assert_eq!(finding.session_id, "session-cache-warning");
        assert_eq!(finding.request_id.as_deref(), Some("req-cache-warning"));
        assert_eq!(finding.turn_number, Some(3));
        assert!(finding.detail.contains("miss_ttl"));
        assert!(finding.detail.contains("$0.24"));
    }

    #[test]
    fn context_detector_emits_fill_percent_and_compaction_runway() {
        let finding = detect_context_finding(
            Some(&GuardPolicy::default()),
            ContextObservation {
                session_id: "session-context-warning".to_string(),
                request_id: Some("req-context-warning".to_string()),
                turn_number: Some(5),
                timestamp: "2026-05-10T00:00:00Z".to_string(),
                fill_percent: 82.4,
                context_window_tokens: Some(200_000),
                turns_to_compact: Some(2),
            },
        )
        .expect("context finding");

        assert_eq!(finding.rule_id, RuleId::ContextNearWarningThreshold);
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert_eq!(finding.action, GuardAction::Warn);
        assert_eq!(finding.source, FindingSource::Proxy);
        assert_eq!(finding.evidence_level, EvidenceLevel::Derived);
        assert_eq!(finding.session_id, "session-context-warning");
        assert!(finding.detail.contains("82%"));
        assert!(finding.detail.contains("2 turns"));
        assert!(finding.detail.contains("200000 token window"));
    }

    #[test]
    fn model_mismatch_detector_warns_without_overclaiming_cause() {
        let finding = super::detect_model_mismatch_finding(
            Some(&GuardPolicy::default()),
            ModelMismatchObservation {
                session_id: "session-model-warning".to_string(),
                request_id: Some("req-model-warning".to_string()),
                turn_number: Some(2),
                timestamp: "2026-05-10T00:00:00Z".to_string(),
                requested_model: "claude-sonnet-4-5".to_string(),
                reported_model: "claude-haiku-4-5".to_string(),
            },
        )
        .expect("model mismatch finding");

        assert_eq!(finding.rule_id, RuleId::ModelMismatch);
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert_eq!(finding.action, GuardAction::Warn);
        assert_eq!(finding.source, FindingSource::Proxy);
        assert_eq!(finding.evidence_level, EvidenceLevel::DirectProxy);
        assert!(finding.detail.contains("claude-sonnet-4-5"));
        assert!(finding.detail.contains("claude-haiku-4-5"));
        assert!(finding
            .detail
            .contains("does not prove why routing changed"));
        assert!(!finding.detail.to_ascii_lowercase().contains("quota"));
    }

    #[test]
    fn compaction_loop_detector_emits_heuristic_warning_finding() {
        let finding = super::detect_compaction_loop_finding(
            Some(&GuardPolicy::default()),
            CompactionLoopObservation {
                session_id: "session-compaction-warning".to_string(),
                request_id: Some("req-compaction-warning".to_string()),
                turn_number: Some(7),
                timestamp: "2026-05-10T00:00:00Z".to_string(),
                consecutive_turns: 3,
                wasted_tokens: 42_000,
            },
        )
        .expect("compaction finding");

        assert_eq!(finding.rule_id, RuleId::SuspectedCompactionLoop);
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert_eq!(finding.action, GuardAction::Warn);
        assert_eq!(finding.source, FindingSource::Heuristic);
        assert_eq!(finding.evidence_level, EvidenceLevel::Inferred);
        assert!(finding.detail.contains("heuristic"));
        assert!(finding.detail.contains("3 rapid turns"));
        assert!(finding.detail.contains("42000"));
    }

    #[test]
    fn tool_failure_detector_emits_live_finding_only_from_proxy_visible_data() {
        let finding = super::detect_tool_failure_finding(
            Some(&GuardPolicy::default()),
            super::ToolFailureObservation {
                session_id: "session-tool-warning".to_string(),
                request_id: Some("req-tool-warning".to_string()),
                turn_number: Some(9),
                timestamp: "2026-05-10T00:00:00Z".to_string(),
                consecutive_failure_turns: 5,
                failed_tool_calls: 7,
                source: FindingSource::Proxy,
            },
        )
        .expect("proxy-visible tool failure finding");

        assert_eq!(finding.rule_id, RuleId::ToolFailureStreak);
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert_eq!(finding.action, GuardAction::Warn);
        assert_eq!(finding.source, FindingSource::Proxy);
        assert_eq!(finding.evidence_level, EvidenceLevel::DirectProxy);
        assert!(finding.detail.contains("5 consecutive turns"));
        assert!(finding.detail.contains("7 failed tool calls"));

        let jsonl_only = super::detect_tool_failure_finding(
            Some(&GuardPolicy::default()),
            super::ToolFailureObservation {
                session_id: "session-tool-jsonl".to_string(),
                request_id: None,
                turn_number: Some(9),
                timestamp: "2026-05-10T00:00:00Z".to_string(),
                consecutive_failure_turns: 5,
                failed_tool_calls: 7,
                source: FindingSource::Jsonl,
            },
        )
        .expect("jsonl-only tool failure finding");
        assert_eq!(jsonl_only.rule_id, RuleId::JsonlOnlyToolFailureStreak);
        assert_eq!(jsonl_only.action, GuardAction::DiagnoseOnly);
        assert_eq!(jsonl_only.source, FindingSource::Jsonl);
        assert_eq!(jsonl_only.evidence_level, EvidenceLevel::DirectJsonl);
    }

    #[test]
    fn no_progress_detection_remains_postmortem_first_by_default() {
        let finding = GuardPolicy::default().resolve_finding(
            FindingDraft::new(
                RuleId::NoProgressTurns,
                FindingSeverity::Warning,
                EvidenceLevel::Inferred,
                FindingSource::Heuristic,
                0.45,
                "session-no-progress",
                "2026-05-10T00:00:00Z",
            )
            .with_detail("Postmortem-only no-progress signal."),
        );

        assert_eq!(finding.rule_id, RuleId::NoProgressTurns);
        assert_eq!(finding.action, GuardAction::DiagnoseOnly);
        assert_eq!(finding.source, FindingSource::Heuristic);
        assert_eq!(finding.evidence_level, EvidenceLevel::Inferred);
    }

    fn state_finding(action: GuardAction, severity: FindingSeverity) -> GuardFinding {
        GuardFinding {
            rule_id: RuleId::RepeatedCacheRebuilds,
            severity,
            action,
            evidence_level: EvidenceLevel::DirectProxy,
            source: FindingSource::Proxy,
            confidence: 0.9,
            session_id: "session-state".to_string(),
            request_id: None,
            turn_number: None,
            timestamp: "2026-05-10T00:00:00Z".to_string(),
            detail: "Derived state fixture.".to_string(),
            suggested_action: None,
        }
    }

    #[test]
    fn guard_state_derives_from_active_findings_and_lifecycle() {
        assert_eq!(
            super::derive_guard_state(GuardState::Healthy, &[]),
            GuardState::Healthy
        );
        assert_eq!(
            super::derive_guard_state(GuardState::Watching, &[]),
            GuardState::Watching
        );
        assert_eq!(
            super::derive_guard_state(
                GuardState::Healthy,
                &[state_finding(GuardAction::Warn, FindingSeverity::Warning)]
            ),
            GuardState::Warning
        );
        assert_eq!(
            super::derive_guard_state(
                GuardState::Healthy,
                &[state_finding(
                    GuardAction::Critical,
                    FindingSeverity::Critical
                )]
            ),
            GuardState::Critical
        );
        assert_eq!(
            super::derive_guard_state(
                GuardState::Healthy,
                &[state_finding(GuardAction::Block, FindingSeverity::Critical)]
            ),
            GuardState::Blocked
        );
        assert_eq!(
            super::derive_guard_state(
                GuardState::Healthy,
                &[state_finding(
                    GuardAction::Cooldown,
                    FindingSeverity::Critical
                )]
            ),
            GuardState::Cooldown
        );
        assert_eq!(
            super::derive_guard_state(
                GuardState::Ended,
                &[state_finding(
                    GuardAction::Cooldown,
                    FindingSeverity::Critical
                )]
            ),
            GuardState::Ended
        );
        assert_eq!(
            super::derive_guard_state(
                GuardState::Watching,
                &[state_finding(
                    GuardAction::DiagnoseOnly,
                    FindingSeverity::Warning
                )]
            ),
            GuardState::Watching
        );
    }
}
