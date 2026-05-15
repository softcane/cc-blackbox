use serde::Serialize;

use crate::{format_duration_coarse, format_tokens, json_bool, json_f64, json_str, json_u64};
use crate::{RunGuardEventOrigin, WatchEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DecisionState {
    Healthy,
    Watching,
    Risky,
    StopRecommended,
    Blocked,
    Cooldown,
    Ended,
}

impl DecisionState {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Watching => "Watching",
            Self::Risky => "Risky",
            Self::StopRecommended => "Stop",
            Self::Blocked => "Blocked",
            Self::Cooldown => "Cooldown",
            Self::Ended => "Ended",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Ended => 0,
            Self::Healthy => 1,
            Self::Watching => 2,
            Self::Risky => 3,
            Self::StopRecommended => 4,
            Self::Blocked => 5,
            Self::Cooldown => 6,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DecisionReason {
    pub(crate) code: String,
    pub(crate) label: String,
    pub(crate) short: String,
    pub(crate) detail: String,
    pub(crate) evidence_level: String,
    pub(crate) source: String,
    pub(crate) evidence: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) caveat: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) suggested_action: Option<String>,
    pub(crate) state: DecisionState,
    #[serde(skip)]
    priority: u16,
}

impl DecisionReason {
    fn sort_key(&self) -> (u8, u8, u16) {
        (
            self.state.rank(),
            evidence_rank(&self.evidence_level),
            self.priority,
        )
    }

    fn is_negative(&self) -> bool {
        matches!(
            self.state,
            DecisionState::Risky
                | DecisionState::StopRecommended
                | DecisionState::Blocked
                | DecisionState::Cooldown
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionDecision {
    pub(crate) state: DecisionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) main_reason: Option<DecisionReason>,
    pub(crate) reasons: Vec<DecisionReason>,
    pub(crate) best_action: String,
    pub(crate) footer_action: String,
    pub(crate) alternatives: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) drilldown_command: Option<String>,
}

impl SessionDecision {
    pub(crate) fn fallback(reason: &str, action: &str) -> Self {
        let reason = DecisionReason {
            code: "no_match".to_string(),
            label: "session match".to_string(),
            short: reason.to_string(),
            detail: reason.to_string(),
            evidence_level: "derived".to_string(),
            source: "statusline".to_string(),
            evidence: "derived statusline correlation".to_string(),
            caveat: Some(
                "Conservative fallback: no unambiguous cc-blackbox session matched.".to_string(),
            ),
            suggested_action: None,
            state: DecisionState::Watching,
            priority: 10,
        };
        Self {
            state: DecisionState::Watching,
            session_id: None,
            display_name: None,
            model: None,
            main_reason: Some(reason.clone()),
            reasons: vec![reason],
            best_action: action.to_string(),
            footer_action: action.to_string(),
            alternatives: vec!["Run cc-blackbox guard status for all active sessions.".to_string()],
            drilldown_command: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionDecisionTracker {
    session_id: Option<String>,
    display_name: Option<String>,
    model: Option<String>,
    ended: bool,
    reasons: Vec<DecisionReason>,
}

impl SessionDecisionTracker {
    pub(crate) fn waiting() -> Self {
        Self {
            session_id: None,
            display_name: None,
            model: None,
            ended: false,
            reasons: Vec::new(),
        }
    }

    pub(crate) fn for_session(
        session_id: impl Into<String>,
        display_name: Option<String>,
        model: Option<String>,
    ) -> Self {
        Self {
            session_id: Some(session_id.into()),
            display_name,
            model,
            ended: false,
            reasons: Vec::new(),
        }
    }

    pub(crate) fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    pub(crate) fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub(crate) fn is_waiting(&self) -> bool {
        self.session_id.is_none()
    }

    pub(crate) fn add_reason(&mut self, reason: DecisionReason) {
        if let Some(existing) = self
            .reasons
            .iter_mut()
            .find(|existing| existing.code == reason.code)
        {
            if reason.sort_key() >= existing.sort_key() {
                *existing = reason;
            }
        } else {
            self.reasons.push(reason);
        }
    }

    fn remove_reason(&mut self, code: &str) -> bool {
        let before = self.reasons.len();
        self.reasons.retain(|reason| reason.code != code);
        before != self.reasons.len()
    }

    pub(crate) fn apply_event(&mut self, event: &WatchEvent, origin: RunGuardEventOrigin) -> bool {
        if self.session_id.is_none() {
            if origin == RunGuardEventOrigin::Replay {
                return false;
            }
            if let WatchEvent::SessionStart {
                session_id,
                display_name,
                model,
                ..
            } = event
            {
                self.session_id = Some(session_id.clone());
                self.display_name = Some(display_name.clone());
                self.model = Some(model.clone());
                self.ended = false;
                self.reasons.clear();
                return true;
            }
            return false;
        }

        if let Some(event_session) = crate::event_session_id(event) {
            if self.session_id.as_deref() != Some(event_session) {
                return false;
            }
        }

        let mut changed = false;
        if !matches!(event, WatchEvent::PostmortemReady { .. }) {
            changed |= self.remove_reason("postmortem_ready");
        }

        match event {
            WatchEvent::SessionStart {
                display_name,
                model,
                ..
            } => {
                self.display_name = Some(display_name.clone());
                self.model = Some(model.clone());
                self.ended = false;
                true
            }
            WatchEvent::SessionEnd { outcome, .. } => {
                self.ended = true;
                self.add_reason(DecisionReason {
                    code: "session_ended".to_string(),
                    label: "session ended".to_string(),
                    short: "session ended".to_string(),
                    detail: format!("Session ended with outcome {outcome}."),
                    evidence_level: "direct_proxy".to_string(),
                    source: "proxy".to_string(),
                    evidence: "direct proxy".to_string(),
                    caveat: None,
                    suggested_action: None,
                    state: DecisionState::Ended,
                    priority: 1,
                });
                true
            }
            WatchEvent::PostmortemReady {
                idle_secs,
                total_tokens,
                total_turns,
                ..
            } => {
                self.add_reason(postmortem_ready_reason(
                    *idle_secs,
                    *total_tokens,
                    *total_turns,
                ));
                true
            }
            WatchEvent::GuardFinding { .. } => {
                if let Some(reason) = reason_from_guard_event(event) {
                    self.add_reason(reason);
                    true
                } else {
                    changed
                }
            }
            WatchEvent::ContextStatus {
                fill_percent,
                turns_to_compact,
                ..
            } => {
                self.add_reason(context_reason(*fill_percent, *turns_to_compact));
                true
            }
            WatchEvent::ModelFallback {
                requested, actual, ..
            } => {
                self.add_reason(model_mismatch_reason(requested, actual));
                true
            }
            WatchEvent::CacheEvent {
                event_type,
                estimated_rebuild_cost_dollars,
                ..
            } => {
                self.add_reason(cache_event_reason(
                    event_type,
                    *estimated_rebuild_cost_dollars,
                ));
                true
            }
            WatchEvent::RateLimitStatus {
                seconds_to_reset,
                tokens_used_this_week,
                tokens_limit,
                tokens_remaining,
                budget_source,
                projected_exhaustion_secs,
                ..
            } => {
                if let Some(reason) = quota_reason(
                    *seconds_to_reset,
                    *tokens_used_this_week,
                    *tokens_limit,
                    *tokens_remaining,
                    budget_source.as_deref(),
                    *projected_exhaustion_secs,
                ) {
                    self.add_reason(reason);
                    true
                } else {
                    changed
                }
            }
            WatchEvent::RequestError { error_type, .. } => {
                self.add_reason(request_error_reason(error_type));
                true
            }
            WatchEvent::CacheWarning {
                idle_secs,
                ttl_secs,
                ..
            } => {
                self.add_reason(cache_warning_reason(*idle_secs, *ttl_secs));
                true
            }
            WatchEvent::CompactionLoop {
                consecutive,
                wasted_tokens,
                ..
            } => {
                self.add_reason(compaction_loop_reason(*consecutive, *wasted_tokens));
                true
            }
            WatchEvent::ToolUse { .. }
            | WatchEvent::ToolResult { .. }
            | WatchEvent::SkillEvent { .. }
            | WatchEvent::McpEvent { .. }
            | WatchEvent::FrustrationSignal { .. }
            | WatchEvent::Diagnosis { .. }
            | WatchEvent::Lagged { .. } => true,
        }
    }

    pub(crate) fn decision(&self) -> SessionDecision {
        let mut reasons = self.reasons.clone();
        reasons.sort_by_key(|reason| std::cmp::Reverse(reason.sort_key()));

        let main_reason = reasons.iter().find(|reason| reason.is_negative()).cloned();
        let main_reason = main_reason.or_else(|| reasons.first().cloned());

        let state = if self.ended {
            DecisionState::Ended
        } else if reasons
            .iter()
            .any(|reason| reason.state == DecisionState::Cooldown)
        {
            DecisionState::Cooldown
        } else if reasons
            .iter()
            .any(|reason| reason.state == DecisionState::Blocked)
        {
            DecisionState::Blocked
        } else if reasons
            .iter()
            .any(|reason| reason.state == DecisionState::StopRecommended)
        {
            DecisionState::StopRecommended
        } else if reasons
            .iter()
            .any(|reason| reason.state == DecisionState::Risky)
        {
            DecisionState::Risky
        } else if self.session_id.is_some() {
            DecisionState::Healthy
        } else {
            DecisionState::Watching
        };

        let best_action = main_reason
            .as_ref()
            .map(best_action_for_reason)
            .unwrap_or_else(|| "Continue normally.".to_string());
        let footer_action = main_reason
            .as_ref()
            .map(footer_action_for_reason)
            .unwrap_or_else(|| "continue".to_string());
        let alternatives = alternatives_for_state(state);
        let drilldown_command = matches!(
            state,
            DecisionState::Risky
                | DecisionState::StopRecommended
                | DecisionState::Blocked
                | DecisionState::Cooldown
        )
        .then(|| "cc-blackbox postmortem latest".to_string());

        SessionDecision {
            state,
            session_id: self.session_id.clone(),
            display_name: self.display_name.clone(),
            model: self.model.clone(),
            main_reason,
            reasons,
            best_action,
            footer_action,
            alternatives,
            drilldown_command,
        }
    }
}

pub(crate) fn guard_watch_state_label_for_reason(reason: &DecisionReason) -> &'static str {
    match reason.state {
        DecisionState::Cooldown => "Cooldown",
        DecisionState::Blocked => "Blocked",
        DecisionState::StopRecommended => "Critical",
        DecisionState::Risky => "Warning",
        DecisionState::Ended => "Ended",
        DecisionState::Healthy => "Healthy",
        DecisionState::Watching => "Watching",
    }
}

pub(crate) fn reason_from_guard_event(event: &WatchEvent) -> Option<DecisionReason> {
    let WatchEvent::GuardFinding {
        rule_id,
        severity,
        action,
        evidence_level,
        source,
        confidence: _,
        detail,
        suggested_action,
        ..
    } = event
    else {
        return None;
    };
    Some(guard_finding_reason(
        rule_id,
        severity,
        action,
        evidence_level,
        source,
        detail,
        suggested_action.as_deref(),
    ))
}

pub(crate) fn decision_from_guard_status_session(
    session: &serde_json::Value,
    quota: Option<&serde_json::Value>,
) -> SessionDecision {
    let session_id = json_str(session, "/session_id").unwrap_or("unknown");
    let display_name = json_str(session, "/display_name").map(ToString::to_string);
    let model = json_str(session, "/model").map(ToString::to_string);
    let mut tracker = SessionDecisionTracker::for_session(session_id, display_name, model);

    if let Some(findings) = session.get("findings").and_then(|value| value.as_array()) {
        for finding in findings {
            let reason = guard_finding_reason(
                json_str(finding, "/rule_id").unwrap_or("guard_finding"),
                json_str(finding, "/severity").unwrap_or("warning"),
                json_str(finding, "/action").unwrap_or("warn"),
                json_str(finding, "/evidence_level").unwrap_or("derived"),
                json_str(finding, "/source").unwrap_or("proxy"),
                json_str(finding, "/detail").unwrap_or("Guard finding recorded."),
                json_str(finding, "/suggested_action"),
            );
            tracker.add_reason(reason);
        }
    }

    if let Some(fill) = json_f64(session, "/signals/context/fill_percent") {
        tracker.add_reason(context_reason(
            fill,
            json_u64(session, "/signals/context/turns_to_compact").map(|value| value as u32),
        ));
    }
    if let Some(idle) = json_u64(session, "/signals/idle_secs") {
        if json_bool(session, "/signals/postmortem_ready").unwrap_or(false) {
            tracker.add_reason(postmortem_ready_reason(
                idle,
                json_u64(session, "/signals/total_tokens").unwrap_or(0),
                json_u64(session, "/signals/total_turns").unwrap_or(0) as u32,
            ));
        } else if let Some(remaining) = json_u64(session, "/signals/cache/cache_expires_in_secs") {
            if remaining <= 60 {
                tracker.add_reason(cache_warning_reason(idle, remaining));
            }
        }
    }
    if let Some(cache_event) = json_str(session, "/signals/cache/event_type") {
        tracker.add_reason(cache_event_reason(
            cache_event,
            json_f64(session, "/signals/cache/estimated_rebuild_cost_dollars"),
        ));
    }
    if let Some(quota) = quota {
        if let Some(reason) = quota_reason(
            json_u64(quota, "/seconds_to_reset"),
            json_u64(quota, "/tokens_used_this_week"),
            json_u64(quota, "/tokens_limit"),
            json_u64(quota, "/tokens_remaining"),
            json_str(quota, "/budget_source"),
            json_u64(quota, "/projected_exhaustion_secs"),
        ) {
            tracker.add_reason(reason);
        }
    }

    if json_str(session, "/state") == Some("ended") {
        tracker.ended = true;
    }
    tracker.decision()
}

pub(crate) fn render_footer(decision: &SessionDecision, width: usize) -> String {
    let width = width.max(12);
    let reason = decision
        .main_reason
        .as_ref()
        .map(|reason| reason.short.as_str())
        .unwrap_or("guard clear");
    let secondary = decision
        .reasons
        .iter()
        .find(|candidate| {
            decision
                .main_reason
                .as_ref()
                .map(|main| main.code.as_str() != candidate.code.as_str())
                .unwrap_or(true)
                && matches!(
                    candidate.code.as_str(),
                    "context_status" | "cache_event" | "cache_ratio" | "quota_burn"
                )
        })
        .map(|reason| reason.short.as_str());
    let state = decision.state.label();
    let drilldown = decision.drilldown_command.as_deref();
    let prefix = "cc-blackbox:";

    let mut candidates = Vec::new();
    if let Some(drilldown) = drilldown {
        if let Some(secondary) = secondary {
            candidates.push(format!(
                "{prefix} {state} | {reason} | {secondary} | {} | {drilldown}",
                decision.footer_action
            ));
            candidates.push(format!(
                "bb: {state} | {reason} | {secondary} | {} | {drilldown}",
                decision.footer_action
            ));
        }
        candidates.push(format!(
            "{prefix} {state} | {reason} | {} | {drilldown}",
            decision.footer_action
        ));
        candidates.push(format!(
            "bb: {state} | {reason} | {} | {drilldown}",
            decision.footer_action
        ));
        candidates.push(format!(
            "{state} | {reason} | {} | {drilldown}",
            decision.footer_action
        ));
        candidates.push(format!("{prefix} {state} | {reason} | {drilldown}"));
        candidates.push(format!("bb: {state} | {reason} | {drilldown}"));
        candidates.push(format!("{state} | {reason} | {drilldown}"));
    }
    candidates.push(format!(
        "{prefix} {state} | {reason} | {}",
        decision.footer_action
    ));
    candidates.push(format!("{prefix} {state} | {reason}"));
    candidates.push(format!("bb: {state} | {reason}"));
    candidates.push(format!("{state} | {reason}"));
    candidates.push(format!("{state}: {reason}"));
    candidates.push(format!("bb: {state}"));

    let mut chosen = candidates
        .into_iter()
        .find(|line| line.chars().count() <= width)
        .unwrap_or_else(|| format!("{state}: {reason}"));
    chosen = one_line_ascii(&chosen);
    if chosen.chars().count() > width {
        truncate_ascii(&chosen, width)
    } else {
        chosen
    }
}

fn one_line_ascii(value: &str) -> String {
    value
        .replace(['\n', '\r'], " ")
        .chars()
        .map(|ch| if ch.is_ascii() { ch } else { '?' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_ascii(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut out = value.chars().take(width - 3).collect::<String>();
    out.push_str("...");
    out
}

fn evidence_rank(evidence: &str) -> u8 {
    match evidence {
        "direct" | "direct_proxy" | "direct_jsonl" => 3,
        "derived" | "hook" => 2,
        "heuristic" | "inferred" => 1,
        _ => 0,
    }
}

fn guard_state(action: &str, severity: &str) -> DecisionState {
    match action {
        "cooldown" => DecisionState::Cooldown,
        "block" => DecisionState::Blocked,
        "critical" => DecisionState::StopRecommended,
        "warn" => DecisionState::Risky,
        _ if severity == "critical" => DecisionState::StopRecommended,
        _ if severity == "warning" => DecisionState::Risky,
        _ => DecisionState::Watching,
    }
}

fn guard_finding_reason(
    rule_id: &str,
    severity: &str,
    action: &str,
    evidence_level: &str,
    source: &str,
    detail: &str,
    suggested_action: Option<&str>,
) -> DecisionReason {
    let state = guard_state(action, severity);
    let label = rule_label(rule_id);
    let short = guard_short_reason(rule_id, detail);
    let caveat = heuristic_caveat(rule_id, evidence_level);
    DecisionReason {
        code: rule_id.to_string(),
        label,
        short,
        detail: detail.to_string(),
        evidence_level: evidence_level.to_string(),
        source: source.to_string(),
        evidence: evidence_display(evidence_level, source),
        caveat,
        suggested_action: suggested_action
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string),
        state,
        priority: guard_priority(rule_id, action),
    }
}

fn rule_label(rule_id: &str) -> String {
    match rule_id {
        "per_session_token_budget_exceeded" => "per session token budget exceeded".to_string(),
        "per_session_trusted_dollar_budget_exceeded" => {
            "per session trusted dollar budget exceeded".to_string()
        }
        "api_error_circuit_breaker_cooldown" => "api error circuit breaker cooldown".to_string(),
        "repeated_cache_rebuilds" => "cache rebuilds".to_string(),
        "context_near_warning_threshold" => "context pressure".to_string(),
        "jsonl_compaction_boundary" => "JSONL compaction boundary".to_string(),
        "model_mismatch" => "model route mismatch".to_string(),
        "suspected_compaction_loop" => "possible compaction loop".to_string(),
        "tool_failure_streak" | "jsonl_only_tool_failure_streak" | "tool_failures" => {
            "tool failures".to_string()
        }
        "high_weekly_project_quota_burn" => "quota burn".to_string(),
        other => other.replace('_', " "),
    }
}

fn guard_short_reason(rule_id: &str, detail: &str) -> String {
    match rule_id {
        "tool_failure_streak" | "jsonl_only_tool_failure_streak" | "tool_failures" => {
            failure_short_from_detail(detail).unwrap_or_else(|| "tool failures".to_string())
        }
        "per_session_token_budget_exceeded" => "token budget exceeded".to_string(),
        "per_session_trusted_dollar_budget_exceeded" => "dollar budget exceeded".to_string(),
        "api_error_circuit_breaker_cooldown" => "API cooldown".to_string(),
        "repeated_cache_rebuilds" => "cache rebuilds".to_string(),
        "context_near_warning_threshold" => "context pressure".to_string(),
        "model_mismatch" => "model route mismatch".to_string(),
        "suspected_compaction_loop" => "compaction loop".to_string(),
        "high_weekly_project_quota_burn" => "quota burn".to_string(),
        other => other.replace('_', " "),
    }
}

fn failure_short_from_detail(detail: &str) -> Option<String> {
    let lower = detail.to_ascii_lowercase();
    if !lower.contains("fail") {
        return None;
    }
    let number = detail
        .split(|ch: char| !ch.is_ascii_digit())
        .find(|part| !part.is_empty())
        .and_then(|part| part.parse::<u64>().ok())?;
    let tool = ["Bash", "Edit", "Write", "Read", "MCP", "Skill"]
        .iter()
        .find(|tool| lower.contains(&tool.to_ascii_lowercase()))
        .copied()
        .unwrap_or("tool");
    Some(format!(
        "{number} {tool} {}",
        if number == 1 { "failure" } else { "failures" }
    ))
}

fn guard_priority(rule_id: &str, action: &str) -> u16 {
    if matches!(action, "block" | "cooldown") {
        return 1000;
    }
    match rule_id {
        "tool_failure_streak" | "jsonl_only_tool_failure_streak" | "tool_failures" => 910,
        "per_session_token_budget_exceeded" | "per_session_trusted_dollar_budget_exceeded" => 900,
        "api_error_circuit_breaker_cooldown" => 880,
        "context_near_warning_threshold" => 720,
        "suspected_compaction_loop" => 700,
        "repeated_cache_rebuilds" => 640,
        "high_weekly_project_quota_burn" => 620,
        "model_mismatch" => 560,
        _ => 400,
    }
}

fn evidence_display(level: &str, source: &str) -> String {
    match (level, source) {
        ("direct_proxy", "proxy") => "direct proxy".to_string(),
        ("direct_jsonl", "jsonl") => "direct jsonl".to_string(),
        ("heuristic", _) => format!("heuristic {source}").trim().to_string(),
        ("derived", "proxy") => "derived proxy estimate".to_string(),
        _ if source.is_empty() => level.replace('_', " "),
        _ => format!("{} {}", level.replace('_', " "), source.replace('_', " ")),
    }
}

fn heuristic_caveat(rule_id: &str, evidence_level: &str) -> Option<String> {
    if matches!(evidence_level, "heuristic" | "inferred")
        || matches!(
            rule_id,
            "context_near_warning_threshold"
                | "suspected_compaction_loop"
                | "repeated_cache_rebuilds"
        )
    {
        Some("Heuristic/local proxy inference; not provider-confirmed.".to_string())
    } else {
        None
    }
}

fn context_reason(fill_percent: f64, turns_to_compact: Option<u32>) -> DecisionReason {
    let fill = fill_percent.round().clamp(0.0, 999.0);
    let (state, priority) = if fill_percent >= 80.0 {
        (DecisionState::StopRecommended, 760)
    } else if fill_percent >= 60.0 {
        (DecisionState::Risky, 560)
    } else {
        (DecisionState::Healthy, 100)
    };
    let runway = match turns_to_compact {
        Some(0) => "auto-compaction may start now".to_string(),
        Some(1) => "about 1 turn to auto-compaction".to_string(),
        Some(turns) => format!("about {turns} turns to auto-compaction"),
        None => "auto-compaction estimate unknown".to_string(),
    };
    DecisionReason {
        code: "context_status".to_string(),
        label: "context pressure".to_string(),
        short: format!("context {:.0}%", fill),
        detail: format!("Context is about {:.0}% full; {runway}.", fill),
        evidence_level: "heuristic".to_string(),
        source: "proxy".to_string(),
        evidence: "derived proxy estimate".to_string(),
        caveat: Some(
            "Context runway is a local heuristic from recent fill-rate changes.".to_string(),
        ),
        suggested_action: None,
        state,
        priority,
    }
}

fn cache_event_reason(event_type: &str, cost: Option<f64>) -> DecisionReason {
    let (state, label, short, detail, evidence_level, caveat, priority) = match event_type {
        "hit" | "partial" => (
            DecisionState::Healthy,
            "cache".to_string(),
            "cache good".to_string(),
            format!("Prompt cache {event_type} detected."),
            "direct_proxy",
            None,
            90,
        ),
        "cold_start" => (
            DecisionState::Watching,
            "cache".to_string(),
            "cache cold".to_string(),
            "Prompt cache cold start detected.".to_string(),
            "direct_proxy",
            None,
            80,
        ),
        "miss_ttl" => (
            DecisionState::Risky,
            "cache rebuild".to_string(),
            "cache rebuild likely".to_string(),
            "Prompt cache miss after TTL gap inferred locally.".to_string(),
            "heuristic",
            Some("Cache TTL miss is inferred locally from idle gap and cache buckets.".to_string()),
            650,
        ),
        "miss_thrash" => (
            DecisionState::Risky,
            "cache rebuild".to_string(),
            "cache thrash likely".to_string(),
            "Repeated prompt cache rebuild pattern inferred locally.".to_string(),
            "heuristic",
            Some("Cache thrash is a local heuristic from repeated cache creation.".to_string()),
            640,
        ),
        "miss_rebuild" => (
            DecisionState::Risky,
            "cache rebuild".to_string(),
            "cache rebuild".to_string(),
            "Prompt cache rebuild detected.".to_string(),
            "direct_proxy",
            None,
            630,
        ),
        other => (
            DecisionState::Watching,
            "cache".to_string(),
            other.replace('_', " "),
            format!("Prompt cache event {other} observed."),
            "derived",
            None,
            50,
        ),
    };
    let detail = cost
        .map(|value| format!("{detail}; rebuild ${value:.2}."))
        .unwrap_or(detail);
    DecisionReason {
        code: "cache_event".to_string(),
        label,
        short,
        detail,
        evidence_level: evidence_level.to_string(),
        source: "proxy".to_string(),
        evidence: evidence_display(evidence_level, "proxy"),
        caveat,
        suggested_action: None,
        state,
        priority,
    }
}

fn cache_warning_reason(idle_secs: u64, ttl_secs: u64) -> DecisionReason {
    DecisionReason {
        code: "cache_warning".to_string(),
        label: "cache expiry".to_string(),
        short: format!("idle {}", format_duration_coarse(idle_secs)),
        detail: format!(
            "Prompt cache is estimated to expire in about {} after {} idle.",
            format_duration_coarse(ttl_secs),
            format_duration_coarse(idle_secs)
        ),
        evidence_level: "heuristic".to_string(),
        source: "proxy".to_string(),
        evidence: "derived proxy estimate".to_string(),
        caveat: Some("Cache expiry is inferred from local TTL evidence and idle time.".to_string()),
        suggested_action: None,
        state: DecisionState::Risky,
        priority: 660,
    }
}

fn model_mismatch_reason(requested: &str, actual: &str) -> DecisionReason {
    DecisionReason {
        code: "model_mismatch".to_string(),
        label: "model route mismatch".to_string(),
        short: "model route mismatch".to_string(),
        detail: format!("Requested {requested}, but the response reported {actual}."),
        evidence_level: "direct_proxy".to_string(),
        source: "proxy".to_string(),
        evidence: "direct proxy".to_string(),
        caveat: Some(
            "This detects route mismatch only; it is not provider-quota evidence.".to_string(),
        ),
        suggested_action: None,
        state: DecisionState::Risky,
        priority: 570,
    }
}

fn request_error_reason(error_type: &str) -> DecisionReason {
    DecisionReason {
        code: "request_error".to_string(),
        label: "request error".to_string(),
        short: format!("API error {error_type}"),
        detail: format!("The proxy observed API error {error_type}."),
        evidence_level: "direct_proxy".to_string(),
        source: "proxy".to_string(),
        evidence: "direct proxy".to_string(),
        caveat: None,
        suggested_action: None,
        state: DecisionState::Risky,
        priority: 700,
    }
}

fn compaction_loop_reason(consecutive: u32, wasted_tokens: u64) -> DecisionReason {
    DecisionReason {
        code: "compaction_loop".to_string(),
        label: "possible compaction loop".to_string(),
        short: "compaction loop".to_string(),
        detail: format!(
            "{consecutive} rapid turns may have wasted about {} tokens.",
            format_tokens(wasted_tokens)
        ),
        evidence_level: "heuristic".to_string(),
        source: "proxy".to_string(),
        evidence: "derived proxy estimate".to_string(),
        caveat: Some("Compaction-loop detection is a local heuristic.".to_string()),
        suggested_action: None,
        state: DecisionState::StopRecommended,
        priority: 740,
    }
}

fn postmortem_ready_reason(idle_secs: u64, total_tokens: u64, total_turns: u32) -> DecisionReason {
    DecisionReason {
        code: "postmortem_ready".to_string(),
        label: "postmortem ready".to_string(),
        short: format!("idle {}", format_duration_coarse(idle_secs)),
        detail: format!(
            "The session has enough evidence for an after-run report. {} tokens across {total_turns} turns.",
            format_tokens(total_tokens)
        ),
        evidence_level: "derived".to_string(),
        source: "proxy".to_string(),
        evidence: "derived proxy estimate".to_string(),
        caveat: Some("Idle readiness does not prove degradation by itself.".to_string()),
        suggested_action: None,
        state: DecisionState::Risky,
        priority: 500,
    }
}

fn quota_reason(
    seconds_to_reset: Option<u64>,
    used: Option<u64>,
    limit: Option<u64>,
    remaining: Option<u64>,
    budget_source: Option<&str>,
    projected_exhaustion_secs: Option<u64>,
) -> Option<DecisionReason> {
    let used = used?;
    let state = match (projected_exhaustion_secs, seconds_to_reset) {
        (Some(exhaust), Some(reset)) if exhaust < reset => DecisionState::Risky,
        _ if limit.is_none() => DecisionState::Watching,
        _ => DecisionState::Watching,
    };
    let cap = limit
        .map(format_tokens)
        .unwrap_or_else(|| "uncapped".to_string());
    let remaining_text = remaining
        .map(|value| format!("; {} left", format_tokens(value)))
        .unwrap_or_default();
    let projected = projected_exhaustion_secs
        .map(|secs| format!("; projected exhaustion in {}", format_duration_coarse(secs)))
        .unwrap_or_default();
    let source = budget_source
        .map(|value| format!("; cap source {value}"))
        .unwrap_or_default();
    Some(DecisionReason {
        code: "quota_burn".to_string(),
        label: "quota burn".to_string(),
        short: "quota burn".to_string(),
        detail: format!(
            "Used {} tokens this week against {cap}{remaining_text}{projected}{source}.",
            format_tokens(used)
        ),
        evidence_level: "derived".to_string(),
        source: "proxy".to_string(),
        evidence: "derived proxy estimate".to_string(),
        caveat: Some("Quota burn is computed locally; Claude Code OAuth traffic does not expose Anthropic rate-limit headers.".to_string()),
        suggested_action: None,
        state,
        priority: 610,
    })
}

fn best_action_for_reason(reason: &DecisionReason) -> String {
    if let Some(action) = reason
        .suggested_action
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return action.to_string();
    }
    match reason.code.as_str() {
        "tool_failure_streak" | "jsonl_only_tool_failure_streak" | "tool_failures" => {
            "Fix the failing precondition before retrying.".to_string()
        }
        "per_session_token_budget_exceeded" | "per_session_trusted_dollar_budget_exceeded" => {
            "Start a fresh, narrower session or adjust the guard budget.".to_string()
        }
        "api_error_circuit_breaker_cooldown" => {
            "Wait for cooldown, fix the API error cause, then retry.".to_string()
        }
        "context_status" | "context_near_warning_threshold" | "near_compaction" => {
            if reason.state == DecisionState::StopRecommended {
                "restart from a short summary.".to_string()
            } else {
                "Narrow the next request.".to_string()
            }
        }
        "compaction_loop" | "suspected_compaction_loop" | "compaction_suspected" => {
            "Stop the loop and restart from a compact summary.".to_string()
        }
        "cache_event" | "cache_warning" | "repeated_cache_rebuilds" | "cache_miss_ttl" => {
            "Send the next useful request while cache is warm, or restart intentionally."
                .to_string()
        }
        "quota_burn" | "high_weekly_project_quota_burn" => {
            "Narrow the next request or pause before another large turn.".to_string()
        }
        "model_mismatch" => {
            "Record the mismatch; do not treat it as provider-quota evidence by itself.".to_string()
        }
        "request_error" | "api_error" => {
            "Check API status or credentials before retrying repeatedly.".to_string()
        }
        "postmortem_ready" => {
            "Run cc-blackbox postmortem latest before continuing broadly.".to_string()
        }
        _ => match reason.state {
            DecisionState::Healthy => "Continue normally.".to_string(),
            DecisionState::Watching => "Keep watching.".to_string(),
            DecisionState::Risky => "Continue only with a narrow next request.".to_string(),
            DecisionState::StopRecommended => {
                "Stop and fix the main issue before retrying.".to_string()
            }
            DecisionState::Blocked => "Resolve the guard block before restarting.".to_string(),
            DecisionState::Cooldown => "Wait for cooldown before retrying.".to_string(),
            DecisionState::Ended => "Inspect the postmortem if needed.".to_string(),
        },
    }
}

fn footer_action_for_reason(reason: &DecisionReason) -> String {
    match reason.code.as_str() {
        "tool_failure_streak" | "jsonl_only_tool_failure_streak" | "tool_failures" => {
            "fix before retry".to_string()
        }
        "per_session_token_budget_exceeded" | "per_session_trusted_dollar_budget_exceeded" => {
            "restart narrower".to_string()
        }
        "api_error_circuit_breaker_cooldown" => "wait cooldown".to_string(),
        "context_status" | "context_near_warning_threshold" | "near_compaction" => {
            if reason.state == DecisionState::StopRecommended {
                "compact or restart".to_string()
            } else {
                "narrow next".to_string()
            }
        }
        "compaction_loop" | "suspected_compaction_loop" | "compaction_suspected" => {
            "restart summary".to_string()
        }
        "cache_event" | "cache_warning" | "repeated_cache_rebuilds" | "cache_miss_ttl" => {
            "cache-aware next".to_string()
        }
        "quota_burn" | "high_weekly_project_quota_burn" => "narrow next".to_string(),
        "model_mismatch" => "verify route".to_string(),
        "request_error" | "api_error" => "check API".to_string(),
        "postmortem_ready" => "review before broad prompt".to_string(),
        _ => match reason.state {
            DecisionState::Healthy => "continue".to_string(),
            DecisionState::Watching => "watch".to_string(),
            DecisionState::Risky => "narrow next".to_string(),
            DecisionState::StopRecommended => "stop first".to_string(),
            DecisionState::Blocked => "resolve block".to_string(),
            DecisionState::Cooldown => "wait".to_string(),
            DecisionState::Ended => "postmortem".to_string(),
        },
    }
}

fn alternatives_for_state(state: DecisionState) -> Vec<String> {
    match state {
        DecisionState::Healthy => vec!["Continue normally.".to_string()],
        DecisionState::Watching => vec!["Wait for the first proxied Claude request.".to_string()],
        DecisionState::Risky => vec![
            "Continue with a narrow prompt.".to_string(),
            "Run cc-blackbox postmortem latest before a broad prompt.".to_string(),
        ],
        DecisionState::StopRecommended => vec![
            "Restart from a short summary after fixing the precondition.".to_string(),
            "Run cc-blackbox postmortem latest for full evidence.".to_string(),
        ],
        DecisionState::Blocked => vec![
            "Resolve the guard block.".to_string(),
            "Start a narrower session if the block is expected.".to_string(),
        ],
        DecisionState::Cooldown => vec![
            "Wait for cooldown.".to_string(),
            "Fix API status, credentials, or network errors before retrying.".to_string(),
        ],
        DecisionState::Ended => vec!["Run cc-blackbox postmortem latest if needed.".to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(tracker: &mut SessionDecisionTracker) {
        assert!(tracker.apply_event(
            &WatchEvent::SessionStart {
                session_id: "session_test".to_string(),
                display_name: "repo".to_string(),
                model: "sonnet".to_string(),
                initial_prompt: None,
            },
            RunGuardEventOrigin::Live,
        ));
    }

    #[test]
    fn decision_ranking_prefers_block_over_stop_and_risky() {
        let mut tracker = SessionDecisionTracker::waiting();
        start(&mut tracker);
        tracker.apply_event(
            &WatchEvent::ContextStatus {
                session_id: "session_test".to_string(),
                fill_percent: 91.0,
                context_window_tokens: Some(200_000),
                turns_to_compact: Some(0),
            },
            RunGuardEventOrigin::Live,
        );
        tracker.apply_event(
            &WatchEvent::GuardFinding {
                session_id: "session_test".to_string(),
                rule_id: "per_session_token_budget_exceeded".to_string(),
                severity: "critical".to_string(),
                action: "block".to_string(),
                evidence_level: "direct_proxy".to_string(),
                source: "proxy".to_string(),
                confidence: 1.0,
                timestamp: "2026-05-14T00:00:00Z".to_string(),
                detail: "Session token budget exceeded.".to_string(),
                suggested_action: None,
            },
            RunGuardEventOrigin::Live,
        );

        let decision = tracker.decision();
        assert_eq!(decision.state, DecisionState::Blocked);
        assert_eq!(
            decision
                .main_reason
                .as_ref()
                .map(|reason| reason.code.as_str()),
            Some("per_session_token_budget_exceeded")
        );
    }

    #[test]
    fn direct_reason_outranks_heuristic_reason_at_same_state() {
        let mut tracker = SessionDecisionTracker::for_session("session_test", None, None);
        tracker.add_reason(DecisionReason {
            code: "heuristic_stop".to_string(),
            label: "heuristic stop".to_string(),
            short: "heuristic stop".to_string(),
            detail: "heuristic".to_string(),
            evidence_level: "heuristic".to_string(),
            source: "proxy".to_string(),
            evidence: "heuristic proxy".to_string(),
            caveat: Some("heuristic".to_string()),
            suggested_action: None,
            state: DecisionState::StopRecommended,
            priority: 900,
        });
        tracker.add_reason(DecisionReason {
            code: "direct_stop".to_string(),
            label: "direct stop".to_string(),
            short: "direct stop".to_string(),
            detail: "direct".to_string(),
            evidence_level: "direct_proxy".to_string(),
            source: "proxy".to_string(),
            evidence: "direct proxy".to_string(),
            caveat: None,
            suggested_action: None,
            state: DecisionState::StopRecommended,
            priority: 100,
        });

        let decision = tracker.decision();
        assert_eq!(
            decision
                .main_reason
                .as_ref()
                .map(|reason| reason.code.as_str()),
            Some("direct_stop")
        );
    }

    #[test]
    fn context_reason_marks_heuristic_caveat() {
        let mut tracker = SessionDecisionTracker::for_session("session_test", None, None);
        tracker.add_reason(context_reason(84.0, Some(1)));

        let decision = tracker.decision();
        let reason = decision.main_reason.as_ref().expect("reason");
        assert_eq!(decision.state, DecisionState::StopRecommended);
        assert_eq!(reason.evidence_level, "heuristic");
        assert!(reason.caveat.as_deref().unwrap_or("").contains("heuristic"));
    }

    #[test]
    fn footer_renderer_degrades_by_width() {
        let mut tracker = SessionDecisionTracker::for_session("session_test", None, None);
        tracker.add_reason(DecisionReason {
            code: "tool_failure_streak".to_string(),
            label: "tool failures".to_string(),
            short: "4 Bash failures".to_string(),
            detail: "4 Bash failures".to_string(),
            evidence_level: "direct_proxy".to_string(),
            source: "proxy".to_string(),
            evidence: "direct proxy".to_string(),
            caveat: None,
            suggested_action: None,
            state: DecisionState::StopRecommended,
            priority: 900,
        });
        tracker.add_reason(context_reason(87.0, Some(1)));
        let decision = tracker.decision();

        let wide = render_footer(&decision, 110);
        assert!(wide.contains("cc-blackbox: Stop"));
        assert!(wide.contains("4 Bash failures"));
        assert!(wide.contains("context 87%"));
        assert!(wide.contains("cc-blackbox postmortem latest"));
        assert!(!wide.contains('\n'));

        let common_terminal = render_footer(&decision, 80);
        assert!(common_terminal.contains("4 Bash failures"));
        assert!(common_terminal.contains("cc-blackbox postmortem latest"));
        assert!(!common_terminal.contains('\n'));

        let medium = render_footer(&decision, 60);
        assert!(medium.contains("4 Bash failures"));
        assert!(medium.contains("cc-blackbox postmortem latest"));
        assert!(!medium.contains("fix before retry"));

        let short = render_footer(&decision, 44);
        assert!(short.contains("4 Bash failures"));
        assert!(!short.contains("fix before retry"));

        let narrow = render_footer(&decision, 24);
        assert!(narrow.contains("4 Bash failures"), "{narrow}");
        assert!(narrow.chars().count() <= 24);

        let minimal = render_footer(&decision, 12);
        assert!(minimal.chars().count() <= 12);
        assert!(!minimal.contains('\n'));
    }
}
