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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSource {
    Proxy,
    Jsonl,
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

impl RuleId {
    fn from_policy_key(key: &str) -> Option<Self> {
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
}

#[derive(Clone, Debug)]
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
        detail: impl Into<String>,
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
            detail: detail.into(),
        }
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_turn_number(mut self, turn_number: u32) -> Self {
        self.turn_number = Some(turn_number);
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
    }
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

impl Default for PolicyManager {
    fn default() -> Self {
        Self {
            active: GuardPolicy::default(),
        }
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
}

impl GuardPolicy {
    pub fn load_effective_from_path(path: &Path) -> Result<LoadedPolicy, PolicyLoadError> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let source = path.display().to_string();
                let policy = Self::from_toml_overlay(&contents, &source)?;
                Ok(LoadedPolicy {
                    policy,
                    source,
                    warnings: Vec::new(),
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

    fn from_toml_overlay(contents: &str, source: &str) -> Result<Self, PolicyLoadError> {
        let raw: PolicyToml =
            toml::from_str(contents).map_err(|err| PolicyLoadError::InvalidToml {
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
                    policy.rules.insert(rule_id, RulePolicy { action });
                }
            }
        }

        Ok(policy)
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

impl Default for GuardPolicy {
    fn default() -> Self {
        let mut rules = BTreeMap::new();
        rules.insert(
            RuleId::PerSessionTokenBudgetExceeded,
            RulePolicy {
                action: GuardAction::Block,
            },
        );
        rules.insert(
            RuleId::PerSessionTrustedDollarBudgetExceeded,
            RulePolicy {
                action: GuardAction::Block,
            },
        );
        rules.insert(
            RuleId::ApiErrorCircuitBreakerCooldown,
            RulePolicy {
                action: GuardAction::Cooldown,
            },
        );

        for rule_id in [
            RuleId::RepeatedCacheRebuilds,
            RuleId::ContextNearWarningThreshold,
            RuleId::ModelMismatch,
            RuleId::SuspectedCompactionLoop,
            RuleId::ToolFailureStreak,
            RuleId::HighWeeklyProjectQuotaBurn,
        ] {
            rules.insert(
                rule_id,
                RulePolicy {
                    action: GuardAction::Warn,
                },
            );
        }

        for rule_id in [
            RuleId::NoProgressTurns,
            RuleId::JsonlOnlyToolFailureStreak,
            RuleId::AmbiguousCacheTtlMiss,
            RuleId::TaskAbandonmentInference,
        ] {
            rules.insert(
                rule_id,
                RulePolicy {
                    action: GuardAction::DiagnoseOnly,
                },
            );
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
        evaluate_finding, EvidenceLevel, FindingDraft, FindingSeverity, FindingSource, GuardAction,
        GuardPolicy, PolicyManager, RuleId,
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
            "This session exceeded the configured token budget.",
        );

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
            "Repeated cache rebuilds are increasing the estimated turn cost.",
        )
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
            "A budget detector fired but no valid policy is available.",
        );

        let finding = evaluate_finding(None, draft);

        assert_eq!(finding.action, GuardAction::Allow);
        assert_eq!(finding.rule_id, RuleId::PerSessionTokenBudgetExceeded);
        assert_eq!(finding.session_id, "session-open");
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
            "The response reported a different model than the request asked for.",
        )
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
}
