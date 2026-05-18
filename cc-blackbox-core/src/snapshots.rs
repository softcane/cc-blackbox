use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tracing::warn;

pub const CAPTURE_PROMPTS_ENV: &str = "CC_BLACKBOX_CAPTURE_PROMPTS";
pub const DRIFT_ANALYZER_ENV: &str = "CC_BLACKBOX_DRIFT_ANALYZER";
pub const DRIFT_ANALYZER_COMMAND_ENV: &str = "CC_BLACKBOX_DRIFT_ANALYZER_COMMAND";
pub const DRIFT_ANALYZER_CONTEXT_ENV: &str = "CC_BLACKBOX_DRIFT_ANALYZER_CONTEXT";
pub const DRIFT_ANALYZER_MOCK_RESPONSE_ENV: &str = "CC_BLACKBOX_DRIFT_ANALYZER_MOCK_RESPONSE";

const DEFAULT_MAX_SNAPSHOTS_PER_SESSION: usize = 5;
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(60 * 60);
const SAFE_EXCERPT_CHARS: usize = 240;
const FULL_EXCERPT_CHARS: usize = 4_000;
const TEXT_CAPTURE_CHARS: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptCaptureMode {
    #[serde(rename = "safe_redacted")]
    Safe,
    #[serde(rename = "full_local")]
    Full,
}

impl PromptCaptureMode {
    pub fn from_env_value(value: Option<&str>) -> Option<Self> {
        let value = value.unwrap_or("").trim().to_ascii_lowercase();
        match value.as_str() {
            "" | "0" | "false" | "no" | "off" | "disabled" => None,
            "1" | "true" | "yes" | "on" | "summary" | "redacted" | "safe" => Some(Self::Safe),
            "full" | "raw" | "full_local" => Some(Self::Full),
            _ => None,
        }
    }

    pub fn from_env() -> Option<Self> {
        Self::from_env_value(std::env::var(CAPTURE_PROMPTS_ENV).ok().as_deref())
    }

    pub fn as_label(self) -> &'static str {
        match self {
            Self::Safe => "safe_redacted",
            Self::Full => "full_local",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestSnapshotInput {
    pub model: String,
    pub working_dir: String,
    pub message_count: usize,
    pub system_prompt_length: usize,
    pub estimated_input_tokens: usize,
    pub first_message_hash: String,
    pub first_user_message: String,
    pub user_prompt_excerpt: String,
    pub compacted_state_hash: String,
}

#[derive(Clone, Debug)]
pub struct RequestSnapshotCandidate {
    pub capture_mode: PromptCaptureMode,
    pub model: String,
    pub working_dir: String,
    pub message_count: usize,
    pub system_prompt_length: usize,
    pub estimated_input_tokens: usize,
    pub first_message_hash: String,
    pub system_prompt_hash: String,
    pub compacted_state_hash: String,
    pub initial_objective_text: String,
    pub compacted_objective_text: String,
    pub compact_summary_text: Option<String>,
    detection_text: String,
    explicit_compaction_marker: bool,
}

#[derive(Clone, Debug)]
struct RequestObservation {
    #[allow(dead_code)]
    session_id: String,
    working_dir: String,
    message_count: usize,
    system_prompt_length: usize,
    estimated_input_tokens: usize,
    first_message_hash: String,
    #[allow(dead_code)]
    system_prompt_hash: String,
    #[allow(dead_code)]
    compacted_state_hash: String,
    initial_objective_text: String,
    observed_at: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptExcerpt {
    pub text: String,
    pub source_chars: usize,
    pub rendered_chars: usize,
    pub truncated: bool,
    pub redacted: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotExcerpts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_objective: Option<PromptExcerpt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compacted_objective: Option<PromptExcerpt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact_summary: Option<PromptExcerpt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_initial_objective: Option<PromptExcerpt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_compacted_objective: Option<PromptExcerpt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_compact_summary: Option<PromptExcerpt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompactionDetectionStatus {
    Suspected,
    Detected,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CompactionDetection {
    pub status: CompactionDetectionStatus,
    pub reason: String,
    pub confidence: f64,
    pub signals: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRequestShape {
    pub message_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_message_count: Option<usize>,
    pub system_prompt_length: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_system_prompt_length: Option<usize>,
    pub estimated_input_tokens: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_estimated_input_tokens: Option<usize>,
    pub first_message_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_first_message_hash: Option<String>,
    pub system_prompt_hash: String,
    pub compacted_state_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DriftScoreSource {
    Deterministic,
    ClaudeAssistedRedacted,
    ClaudeAssistedFull,
}

impl DriftScoreSource {
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::ClaudeAssistedRedacted => "claude_assisted_redacted",
            Self::ClaudeAssistedFull => "claude_assisted_full",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriftDimensionScore {
    pub score: u8,
    pub label: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DriftScore {
    pub source: DriftScoreSource,
    pub objective_alignment: DriftDimensionScore,
    pub state_preservation: DriftDimensionScore,
    pub scope_drift: DriftDimensionScore,
    pub actionability: DriftDimensionScore,
    pub risk: DriftDimensionScore,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_facts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_framing: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caveats: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CompactionSnapshot {
    pub sequence: u64,
    pub timestamp: String,
    pub session_id: String,
    pub request_id: String,
    pub model: String,
    pub capture_mode: PromptCaptureMode,
    pub local_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_capture_warning: Option<String>,
    pub detection: CompactionDetection,
    pub request: SnapshotRequestShape,
    pub excerpts: SnapshotExcerpts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drift: Option<DriftScore>,
}

struct SnapshotSession {
    snapshots: VecDeque<CompactionSnapshot>,
    last_observation: Option<RequestObservation>,
    next_sequence: u64,
    last_activity: Instant,
}

struct SnapshotStoreInner {
    sessions: HashMap<String, SnapshotSession>,
    by_working_dir: HashMap<String, RequestObservation>,
}

pub struct SnapshotStore {
    max_snapshots_per_session: usize,
    ttl: Duration,
    inner: Mutex<SnapshotStoreInner>,
}

impl SnapshotStore {
    pub fn new(max_snapshots_per_session: usize, ttl: Duration) -> Self {
        Self {
            max_snapshots_per_session: max_snapshots_per_session.max(1),
            ttl,
            inner: Mutex::new(SnapshotStoreInner {
                sessions: HashMap::new(),
                by_working_dir: HashMap::new(),
            }),
        }
    }

    pub fn from_env() -> Self {
        let max = std::env::var("CC_BLACKBOX_PROMPT_SNAPSHOT_LIMIT")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_SNAPSHOTS_PER_SESSION);
        let ttl_secs = std::env::var("CC_BLACKBOX_PROMPT_SNAPSHOT_TTL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SESSION_TTL.as_secs());
        Self::new(max, Duration::from_secs(ttl_secs))
    }

    fn lock(&self) -> MutexGuard<'_, SnapshotStoreInner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("recovering poisoned prompt snapshot store mutex");
                poisoned.into_inner()
            }
        }
    }

    pub fn record_request(
        &self,
        candidate: RequestSnapshotCandidate,
        session_id: String,
        request_id: String,
        timestamp: String,
        previous_context_fill_percent: Option<f64>,
    ) -> Option<CompactionSnapshot> {
        let now = Instant::now();
        let mut inner = self.lock();

        let previous = inner
            .sessions
            .get(&session_id)
            .and_then(|session| session.last_observation.clone())
            .or_else(|| {
                inner
                    .by_working_dir
                    .get(&candidate.working_dir)
                    .filter(|previous| previous.first_message_hash != candidate.first_message_hash)
                    .cloned()
            });

        let detection =
            detect_compaction(&candidate, previous.as_ref(), previous_context_fill_percent);
        let observation = RequestObservation {
            session_id: session_id.clone(),
            working_dir: candidate.working_dir.clone(),
            message_count: candidate.message_count,
            system_prompt_length: candidate.system_prompt_length,
            estimated_input_tokens: candidate.estimated_input_tokens,
            first_message_hash: candidate.first_message_hash.clone(),
            system_prompt_hash: candidate.system_prompt_hash.clone(),
            compacted_state_hash: candidate.compacted_state_hash.clone(),
            initial_objective_text: candidate.initial_objective_text.clone(),
            observed_at: now,
        };

        inner
            .by_working_dir
            .insert(candidate.working_dir.clone(), observation.clone());
        let session = inner
            .sessions
            .entry(session_id.clone())
            .or_insert_with(|| SnapshotSession {
                snapshots: VecDeque::new(),
                last_observation: None,
                next_sequence: 1,
                last_activity: now,
            });
        session.last_activity = now;
        session.last_observation = Some(observation);

        let detection = detection?;
        let previous_snapshot = session.snapshots.back().cloned();
        let sequence = session.next_sequence.max(1);
        session.next_sequence = sequence.saturating_add(1);
        let initial_text = previous
            .as_ref()
            .map(|previous| previous.initial_objective_text.as_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(candidate.initial_objective_text.as_str());
        let request = SnapshotRequestShape {
            message_count: candidate.message_count,
            previous_message_count: previous.as_ref().map(|previous| previous.message_count),
            system_prompt_length: candidate.system_prompt_length,
            previous_system_prompt_length: previous
                .as_ref()
                .map(|previous| previous.system_prompt_length),
            estimated_input_tokens: candidate.estimated_input_tokens,
            previous_estimated_input_tokens: previous
                .as_ref()
                .map(|previous| previous.estimated_input_tokens),
            first_message_hash: candidate.first_message_hash.clone(),
            previous_first_message_hash: previous
                .as_ref()
                .map(|previous| previous.first_message_hash.clone()),
            system_prompt_hash: candidate.system_prompt_hash.clone(),
            compacted_state_hash: candidate.compacted_state_hash.clone(),
        };
        let excerpts = build_excerpts(&candidate, initial_text);
        let mut snapshot = CompactionSnapshot {
            sequence,
            timestamp,
            session_id: session_id.clone(),
            request_id,
            model: candidate.model.clone(),
            capture_mode: candidate.capture_mode,
            local_only: true,
            full_capture_warning: (candidate.capture_mode == PromptCaptureMode::Full).then(|| {
                "FULL LOCAL-ONLY PROMPT CAPTURE ENABLED: excerpts may contain sensitive prompt text."
                    .to_string()
            }),
            detection,
            request,
            excerpts,
            drift: None,
        };
        snapshot.drift = Some(score_drift_deterministic(
            initial_text,
            previous_snapshot.as_ref(),
            &snapshot,
        ));

        if session.snapshots.len() >= self.max_snapshots_per_session {
            session.snapshots.pop_front();
        }
        session.snapshots.push_back(snapshot.clone());

        Some(snapshot)
    }

    pub fn latest_session_id_for_working_dir(&self, working_dir: &str) -> Option<String> {
        let inner = self.lock();
        inner
            .by_working_dir
            .get(working_dir)
            .map(|observation| observation.session_id.clone())
    }

    pub fn record_generated_summary(
        &self,
        mut candidate: RequestSnapshotCandidate,
        session_id: String,
        request_id: String,
        timestamp: String,
        response_text: &str,
    ) -> Option<CompactionSnapshot> {
        let summary = compacted_summary_from_response(response_text)?;
        candidate.compacted_objective_text = summary.clone();
        candidate.compact_summary_text = Some(summary.clone());
        candidate.detection_text = summary;
        candidate.explicit_compaction_marker = true;

        let now = Instant::now();
        let mut inner = self.lock();
        let previous = inner
            .sessions
            .get(&session_id)
            .and_then(|session| session.last_observation.clone())
            .or_else(|| inner.by_working_dir.get(&candidate.working_dir).cloned());
        let session = inner
            .sessions
            .entry(session_id.clone())
            .or_insert_with(|| SnapshotSession {
                snapshots: VecDeque::new(),
                last_observation: None,
                next_sequence: 1,
                last_activity: now,
            });
        session.last_activity = now;

        let previous_snapshot = session.snapshots.back().cloned();
        let sequence = session.next_sequence.max(1);
        session.next_sequence = sequence.saturating_add(1);
        let initial_text = previous
            .as_ref()
            .map(|previous| previous.initial_objective_text.as_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(candidate.initial_objective_text.as_str());
        let request = SnapshotRequestShape {
            message_count: candidate.message_count,
            previous_message_count: previous.as_ref().map(|previous| previous.message_count),
            system_prompt_length: candidate.system_prompt_length,
            previous_system_prompt_length: previous
                .as_ref()
                .map(|previous| previous.system_prompt_length),
            estimated_input_tokens: candidate.estimated_input_tokens,
            previous_estimated_input_tokens: previous
                .as_ref()
                .map(|previous| previous.estimated_input_tokens),
            first_message_hash: candidate.first_message_hash.clone(),
            previous_first_message_hash: previous
                .as_ref()
                .map(|previous| previous.first_message_hash.clone()),
            system_prompt_hash: candidate.system_prompt_hash.clone(),
            compacted_state_hash: candidate.compacted_state_hash.clone(),
        };
        let excerpts = build_excerpts(&candidate, initial_text);
        let detection = CompactionDetection {
            status: CompactionDetectionStatus::Detected,
            reason: "Claude Code generated compacted conversation summary".to_string(),
            confidence: 0.95,
            signals: vec!["compaction summary response".to_string()],
        };
        let mut snapshot = CompactionSnapshot {
            sequence,
            timestamp,
            session_id: session_id.clone(),
            request_id,
            model: candidate.model.clone(),
            capture_mode: candidate.capture_mode,
            local_only: true,
            full_capture_warning: (candidate.capture_mode == PromptCaptureMode::Full).then(|| {
                "FULL LOCAL-ONLY PROMPT CAPTURE ENABLED: excerpts may contain sensitive prompt text."
                    .to_string()
            }),
            detection,
            request,
            excerpts,
            drift: None,
        };
        snapshot.drift = Some(score_drift_deterministic(
            initial_text,
            previous_snapshot.as_ref(),
            &snapshot,
        ));

        if session.snapshots.len() >= self.max_snapshots_per_session {
            session.snapshots.pop_front();
        }
        session.snapshots.push_back(snapshot.clone());

        Some(snapshot)
    }

    pub fn list_session_snapshots(&self, session_id: &str) -> Vec<CompactionSnapshot> {
        let inner = self.lock();
        inner
            .sessions
            .get(session_id)
            .map(|session| session.snapshots.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn latest_pair(
        &self,
        session_id: &str,
    ) -> Option<(Option<CompactionSnapshot>, CompactionSnapshot)> {
        let inner = self.lock();
        let session = inner.sessions.get(session_id)?;
        let latest = session.snapshots.back()?.clone();
        let previous = if session.snapshots.len() >= 2 {
            session
                .snapshots
                .get(session.snapshots.len().saturating_sub(2))
                .cloned()
        } else {
            None
        };
        Some((previous, latest))
    }

    pub fn update_drift(
        &self,
        session_id: &str,
        sequence: u64,
        drift: DriftScore,
    ) -> Option<CompactionSnapshot> {
        let mut inner = self.lock();
        let session = inner.sessions.get_mut(session_id)?;
        let snapshot = session
            .snapshots
            .iter_mut()
            .find(|snapshot| snapshot.sequence == sequence)?;
        snapshot.drift = Some(drift);
        Some(snapshot.clone())
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut inner = self.lock();
        let cutoff = Instant::now().checked_sub(self.ttl);
        let before = inner.sessions.len();
        if let Some(cutoff) = cutoff {
            inner
                .sessions
                .retain(|_, session| session.last_activity >= cutoff);
            inner
                .by_working_dir
                .retain(|_, observation| observation.observed_at >= cutoff);
        }
        before.saturating_sub(inner.sessions.len())
    }
}

pub static SNAPSHOT_STORE: LazyLock<SnapshotStore> = LazyLock::new(SnapshotStore::from_env);

pub fn hash_bytes_hex(bytes: &[u8]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    format!("hash_{:016x}", hasher.finish())
}

fn hash_text_hex(raw: &str) -> String {
    hash_bytes_hex(raw.as_bytes())
}

pub fn build_candidate(
    value: &Value,
    capture_mode: PromptCaptureMode,
    input: RequestSnapshotInput,
) -> RequestSnapshotCandidate {
    let system_text = collect_system_text(value);
    let message_text = collect_message_text(value, TEXT_CAPTURE_CHARS);
    let cleaned_message_text = strip_snapshot_noise_blocks(&message_text);
    let compact_summary_text = collect_compact_summary_text(value);
    let detection_text = compact_summary_text
        .as_deref()
        .unwrap_or(cleaned_message_text.as_str())
        .to_string();
    let compacted_objective_text = compact_summary_text.clone().unwrap_or_else(|| {
        if input.user_prompt_excerpt.trim().is_empty() {
            truncate_string(&cleaned_message_text, TEXT_CAPTURE_CHARS)
        } else {
            input.user_prompt_excerpt.clone()
        }
    });
    let explicit_compaction_marker = contains_explicit_compaction_marker(&detection_text);

    RequestSnapshotCandidate {
        capture_mode,
        model: input.model,
        working_dir: input.working_dir,
        message_count: input.message_count,
        system_prompt_length: input.system_prompt_length,
        estimated_input_tokens: input.estimated_input_tokens,
        first_message_hash: input.first_message_hash,
        system_prompt_hash: hash_text_hex(&system_text),
        compacted_state_hash: input.compacted_state_hash,
        initial_objective_text: input.user_prompt_excerpt,
        compacted_objective_text,
        compact_summary_text,
        detection_text,
        explicit_compaction_marker,
    }
}

fn collect_system_text(value: &Value) -> String {
    match value.get("system") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn collect_message_text(value: &Value, max_chars: usize) -> String {
    let mut out = String::new();
    let Some(messages) = value.get("messages").and_then(Value::as_array) else {
        return out;
    };
    for message in messages {
        collect_text_from_value(
            message.get("content").unwrap_or(&Value::Null),
            &mut out,
            max_chars,
        );
        if out.chars().count() >= max_chars {
            break;
        }
    }
    truncate_string(&out, max_chars)
}

fn collect_compact_summary_text(value: &Value) -> Option<String> {
    let messages = value.get("messages").and_then(Value::as_array)?;
    let mut fallback = None;
    for message in messages {
        let mut text = String::new();
        collect_text_from_value(
            message.get("content").unwrap_or(&Value::Null),
            &mut text,
            16_000,
        );
        if text.trim().is_empty() {
            continue;
        }
        let cleaned = strip_snapshot_noise_blocks(&text);
        if cleaned.trim().is_empty() {
            continue;
        }
        if contains_explicit_compaction_marker(&cleaned) {
            return Some(truncate_string(&cleaned, TEXT_CAPTURE_CHARS));
        }
        if fallback.is_none() && looks_like_compact_summary(&cleaned) {
            fallback = Some(truncate_string(&cleaned, TEXT_CAPTURE_CHARS));
        }
    }
    fallback
}

fn strip_snapshot_noise_blocks(raw: &str) -> String {
    static NOISE_TAGS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        [
            r"(?s)<system-reminder\b[^>]*>.*?</system-reminder>",
            r"(?s)<command-name\b[^>]*>.*?</command-name>",
            r"(?s)<command-message\b[^>]*>.*?</command-message>",
            r"(?s)<command-args\b[^>]*>.*?</command-args>",
            r"(?s)<local-command-stdout\b[^>]*>.*?</local-command-stdout>",
            r"(?s)<local-command-stderr\b[^>]*>.*?</local-command-stderr>",
        ]
        .iter()
        .filter_map(|pattern| Regex::new(pattern).ok())
        .collect()
    });

    let mut cleaned = raw.to_string();
    for _ in 0..4 {
        let mut changed = false;
        for regex in NOISE_TAGS.iter() {
            let next = regex.replace_all(&cleaned, "").to_string();
            if next != cleaned {
                cleaned = next;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    cleaned
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn compacted_summary_from_response(raw: &str) -> Option<String> {
    let cleaned = strip_snapshot_noise_blocks(raw);
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        return None;
    }

    let lower = trimmed.to_ascii_lowercase();
    if let (Some(start), Some(end)) = (lower.find("<summary>"), lower.rfind("</summary>")) {
        let content_start = start + "<summary>".len();
        if end > content_start {
            let summary = trimmed[content_start..end].trim();
            if !summary.is_empty() {
                return Some(summary.to_string());
            }
        }
    }

    Some(trimmed.to_string())
}

fn collect_text_from_value(value: &Value, out: &mut String, max_chars: usize) {
    if out.chars().count() >= max_chars {
        return;
    }
    match value {
        Value::String(text) => push_capped(out, text, max_chars),
        Value::Array(items) => {
            for item in items {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    push_capped(out, text, max_chars);
                } else {
                    collect_text_from_value(item, out, max_chars);
                }
                if out.chars().count() >= max_chars {
                    break;
                }
            }
        }
        Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(Value::as_str) {
                push_capped(out, text, max_chars);
            }
        }
        _ => {}
    }
}

fn push_capped(out: &mut String, text: &str, max_chars: usize) {
    if out.chars().count() >= max_chars {
        return;
    }
    if !out.is_empty() {
        out.push('\n');
    }
    let remaining = max_chars.saturating_sub(out.chars().count());
    out.push_str(&text.chars().take(remaining).collect::<String>());
}

fn truncate_string(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        value.chars().take(max_chars).collect()
    }
}

fn contains_explicit_compaction_marker(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("compaction summary:")
        || lower.contains("compact summary:")
        || lower.contains("conversation summary:")
        || lower.contains("session summary:")
        || lower.contains("conversation compacted")
        || lower.contains("conversation was compacted")
        || lower.contains("previous conversation has been summarized")
        || lower.contains("previous conversation was summarized")
}

fn looks_like_compact_summary(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let markers = [
        "summary",
        "conversation summary",
        "session summary",
        "previous conversation",
        "continue from",
        "next steps",
        "remaining work",
        "handoff",
    ];
    markers
        .iter()
        .filter(|marker| lower.contains(**marker))
        .count()
        >= 2
}

fn detect_compaction(
    candidate: &RequestSnapshotCandidate,
    previous: Option<&RequestObservation>,
    previous_context_fill_percent: Option<f64>,
) -> Option<CompactionDetection> {
    let mut signals = Vec::new();
    let mut compaction_shape_signals = 0usize;

    if candidate.explicit_compaction_marker {
        signals.push("explicit compact-summary marker".to_string());
        compaction_shape_signals += 1;
    } else if looks_like_compact_summary(&candidate.detection_text) {
        signals.push("compact-summary-looking text block".to_string());
    }

    if let Some(previous) = previous {
        if previous.message_count >= 4
            && candidate.message_count.saturating_mul(2) <= previous.message_count
        {
            signals.push(format!(
                "message count dropped {} -> {}",
                previous.message_count, candidate.message_count
            ));
            compaction_shape_signals += 1;
        }
        if previous.estimated_input_tokens >= 2_000
            && candidate.estimated_input_tokens.saturating_mul(100)
                <= previous.estimated_input_tokens.saturating_mul(60)
        {
            signals.push(format!(
                "estimated input tokens dropped {} -> {}",
                previous.estimated_input_tokens, candidate.estimated_input_tokens
            ));
            compaction_shape_signals += 1;
        }
        if previous.system_prompt_length > 0 {
            let delta = previous
                .system_prompt_length
                .abs_diff(candidate.system_prompt_length);
            if delta.saturating_mul(100) >= previous.system_prompt_length.saturating_mul(25) {
                signals.push(format!(
                    "system prompt length changed {} -> {}",
                    previous.system_prompt_length, candidate.system_prompt_length
                ));
                compaction_shape_signals += 1;
            }
        }
        if previous.working_dir == candidate.working_dir
            && previous.first_message_hash != candidate.first_message_hash
        {
            signals.push("first message hash changed in same working directory".to_string());
            compaction_shape_signals += 1;
        }
    }

    if previous_context_fill_percent.is_some_and(|fill| fill >= 80.0) {
        signals.push("previous context fill was near compaction threshold".to_string());
    }

    if compaction_shape_signals == 0 {
        return None;
    }

    let detected = candidate.explicit_compaction_marker
        || signals.len() >= 3
        || (signals
            .iter()
            .any(|signal| signal.contains("compact-summary-looking"))
            && signals.iter().any(|signal| signal.contains("dropped")));
    let status = if detected {
        CompactionDetectionStatus::Detected
    } else {
        CompactionDetectionStatus::Suspected
    };
    let confidence = if detected {
        (0.68 + signals.len() as f64 * 0.06).min(0.95)
    } else {
        (0.35 + signals.len() as f64 * 0.10).min(0.64)
    };
    Some(CompactionDetection {
        status,
        reason: signals.join("; "),
        confidence,
        signals,
    })
}

fn build_excerpts(candidate: &RequestSnapshotCandidate, initial_text: &str) -> SnapshotExcerpts {
    let mut excerpts = SnapshotExcerpts {
        initial_objective: safe_excerpt(initial_text, SAFE_EXCERPT_CHARS),
        compacted_objective: safe_excerpt(&candidate.compacted_objective_text, SAFE_EXCERPT_CHARS),
        compact_summary: candidate
            .compact_summary_text
            .as_ref()
            .and_then(|text| safe_excerpt(text, SAFE_EXCERPT_CHARS)),
        ..SnapshotExcerpts::default()
    };

    if candidate.capture_mode == PromptCaptureMode::Full {
        excerpts.raw_initial_objective = raw_excerpt(initial_text, FULL_EXCERPT_CHARS);
        excerpts.raw_compacted_objective = raw_excerpt(
            &candidate.compacted_objective_text,
            candidate.compacted_objective_text.chars().count(),
        );
        excerpts.raw_compact_summary = candidate
            .compact_summary_text
            .as_ref()
            .and_then(|text| raw_excerpt(text, text.chars().count()));
    }

    excerpts
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
static FENCED_CODE_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)```[A-Za-z0-9_-]*\n.*?```"#).expect("valid fenced code redaction regex")
});
static COMMAND_OUTPUT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<local-command-(stdout|stderr)\b[^>]*>.*?</local-command-(stdout|stderr)>"#)
        .expect("valid command output redaction regex")
});
static COMMAND_TAG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)<command-(name|message|args)\b[^>]*>.*?</command-(name|message|args)>"#)
        .expect("valid command tag redaction regex")
});

pub fn safe_excerpt(value: &str, max_chars: usize) -> Option<PromptExcerpt> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let source_chars = trimmed.chars().count();
    let redacted = redact_safe_text(trimmed);
    let capped = cap_with_metadata(&redacted, max_chars);
    Some(PromptExcerpt {
        text: capped.0.clone(),
        source_chars,
        rendered_chars: capped.0.chars().count(),
        truncated: capped.1 || redacted.chars().count() < source_chars,
        redacted: true,
    })
}

fn raw_excerpt(value: &str, max_chars: usize) -> Option<PromptExcerpt> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let source_chars = trimmed.chars().count();
    let capped = cap_with_metadata(trimmed, max_chars);
    Some(PromptExcerpt {
        text: capped.0.clone(),
        source_chars,
        rendered_chars: capped.0.chars().count(),
        truncated: capped.1,
        redacted: false,
    })
}

fn cap_with_metadata(value: &str, max_chars: usize) -> (String, bool) {
    if value.chars().count() <= max_chars {
        return (value.to_string(), false);
    }
    let take = max_chars.saturating_sub(3);
    let mut out = value.chars().take(take).collect::<String>();
    out.push_str("...");
    (out, true)
}

fn redact_safe_text(value: &str) -> String {
    let mut out = value.to_string();
    out = COMMAND_OUTPUT_RE
        .replace_all(&out, "<command output redacted>")
        .to_string();
    out = COMMAND_TAG_RE
        .replace_all(&out, "<command context redacted>")
        .to_string();
    out = FENCED_CODE_RE
        .replace_all(&out, "<code block redacted>")
        .to_string();
    out = redact_code_like_lines(&out);
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
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn redact_code_like_lines(value: &str) -> String {
    let mut out = Vec::new();
    let mut in_redacted_block = false;
    for line in value.lines() {
        if looks_like_code_or_file_content(line) {
            if !in_redacted_block {
                out.push("<code/file content redacted>".to_string());
                in_redacted_block = true;
            }
        } else {
            in_redacted_block = false;
            out.push(line.to_string());
        }
    }
    out.join("\n")
}

fn looks_like_code_or_file_content(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.len() > 140
        && (trimmed.contains('{') || trimmed.contains('=') || trimmed.contains("=>"))
    {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    trimmed.starts_with("fn ")
        || trimmed.starts_with("pub ")
        || trimmed.starts_with("use ")
        || trimmed.starts_with("impl ")
        || trimmed.starts_with("class ")
        || trimmed.starts_with("def ")
        || trimmed.starts_with("function ")
        || trimmed.starts_with("import ")
        || lower.starts_with("stack trace")
        || lower.starts_with("thread '")
        || (trimmed.ends_with(';') && (trimmed.contains('=') || trimmed.contains("::")))
        || (trimmed.contains('{') && trimmed.contains('}') && trimmed.len() > 30)
        || lower.contains("begin file")
        || lower.contains("file contents")
}

fn score_label_higher_better(score: u8) -> String {
    match score {
        80..=100 => "high".to_string(),
        55..=79 => "medium".to_string(),
        30..=54 => "low".to_string(),
        _ => "very low".to_string(),
    }
}

fn score_label_higher_worse(score: u8) -> String {
    match score {
        80..=100 => "high".to_string(),
        55..=79 => "medium".to_string(),
        25..=54 => "low".to_string(),
        _ => "very low".to_string(),
    }
}

fn excerpt_text(snapshot: &CompactionSnapshot) -> String {
    snapshot
        .excerpts
        .compacted_objective
        .as_ref()
        .or(snapshot.excerpts.compact_summary.as_ref())
        .map(|excerpt| excerpt.text.clone())
        .unwrap_or_default()
}

pub fn score_drift_deterministic(
    initial_objective: &str,
    previous_snapshot: Option<&CompactionSnapshot>,
    latest_snapshot: &CompactionSnapshot,
) -> DriftScore {
    let latest_text = excerpt_text(latest_snapshot);
    let previous_text = previous_snapshot.map(excerpt_text).unwrap_or_default();
    let initial_terms = keyword_set(initial_objective);
    let latest_terms = keyword_set(&latest_text);
    let previous_terms = keyword_set(&previous_text);

    let objective_overlap = overlap_ratio(&initial_terms, &latest_terms);
    let preservation_basis = if previous_terms.is_empty() {
        initial_terms.clone()
    } else {
        previous_terms.clone()
    };
    let state_overlap = overlap_ratio(&preservation_basis, &latest_terms);
    let missing_facts = missing_important_facts(initial_objective, &previous_text, &latest_text);
    let novel_ratio = novel_ratio(&latest_terms, &initial_terms, &previous_terms);

    let objective_score = ratio_score(objective_overlap);
    let mut preservation_score = ratio_score(state_overlap);
    preservation_score = preservation_score.saturating_sub((missing_facts.len() as u8) * 8);
    let scope_drift_score = ((novel_ratio * 100.0).round() as u8)
        .saturating_add(if objective_score < 45 { 25 } else { 0 })
        .min(100);
    let actionability_score =
        actionability_score(&latest_text, objective_score, preservation_score);
    let risk_score = (100u8.saturating_sub(objective_score) / 2)
        .saturating_add(100u8.saturating_sub(preservation_score) / 3)
        .saturating_add((missing_facts.len() as u8).saturating_mul(8))
        .min(100);

    let changed_framing = (objective_score < 45).then(|| {
        format!(
            "\"{}\" became \"{}\"",
            truncate_plain(initial_objective, 80),
            truncate_plain(&latest_text, 80)
        )
    });

    DriftScore {
        source: DriftScoreSource::Deterministic,
        objective_alignment: DriftDimensionScore {
            score: objective_score,
            label: score_label_higher_better(objective_score),
            evidence: overlap_evidence(&initial_terms, &latest_terms, "objective terms"),
        },
        state_preservation: DriftDimensionScore {
            score: preservation_score,
            label: score_label_higher_better(preservation_score),
            evidence: overlap_evidence(&preservation_basis, &latest_terms, "state terms"),
        },
        scope_drift: DriftDimensionScore {
            score: scope_drift_score,
            label: score_label_higher_worse(scope_drift_score),
            evidence: (novel_ratio > 0.35)
                .then(|| format!("{:.0}% of latest keywords are new", novel_ratio * 100.0))
                .into_iter()
                .collect(),
        },
        actionability: DriftDimensionScore {
            score: actionability_score,
            label: score_label_higher_better(actionability_score),
            evidence: actionability_evidence(&latest_text),
        },
        risk: DriftDimensionScore {
            score: risk_score,
            label: score_label_higher_worse(risk_score),
            evidence: risk_evidence(objective_score, preservation_score, missing_facts.len()),
        },
        missing_facts,
        changed_framing,
        caveats: vec![
            "Deterministic drift scoring is heuristic and uses redacted/capped evidence."
                .to_string(),
        ],
    }
}

fn ratio_score(ratio: f64) -> u8 {
    (ratio.clamp(0.0, 1.0) * 100.0).round() as u8
}

fn keyword_set(value: &str) -> HashSet<String> {
    const STOP: &[&str] = &[
        "about", "after", "again", "also", "because", "before", "being", "blackbox", "claude",
        "could", "from", "have", "into", "latest", "more", "must", "need", "only", "please",
        "prompt", "request", "session", "should", "summary", "that", "their", "there", "these",
        "this", "through", "turn", "using", "with", "work", "would", "your",
    ];
    let stop = STOP.iter().copied().collect::<HashSet<_>>();
    value
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '-')
        .filter_map(|token| {
            let token = token.trim_matches('-');
            if token.len() < 4
                || stop.contains(token)
                || token.chars().all(|ch| ch.is_ascii_digit())
            {
                None
            } else {
                Some(token.to_string())
            }
        })
        .take(96)
        .collect()
}

fn overlap_ratio(expected: &HashSet<String>, actual: &HashSet<String>) -> f64 {
    if expected.is_empty() {
        return 0.5;
    }
    expected.intersection(actual).count() as f64 / expected.len() as f64
}

fn novel_ratio(
    latest: &HashSet<String>,
    initial: &HashSet<String>,
    previous: &HashSet<String>,
) -> f64 {
    if latest.is_empty() {
        return 0.0;
    }
    latest
        .iter()
        .filter(|term| !initial.contains(*term) && !previous.contains(*term))
        .count() as f64
        / latest.len() as f64
}

fn missing_important_facts(initial: &str, previous: &str, latest: &str) -> Vec<String> {
    let latest_terms = keyword_set(latest);
    let mut terms = keyword_set(&format!("{initial} {previous}"))
        .into_iter()
        .filter(|term| !latest_terms.contains(term))
        .collect::<Vec<_>>();
    terms.sort_by(|a, b| {
        is_important_term(b)
            .cmp(&is_important_term(a))
            .then_with(|| a.cmp(b))
    });

    let mut facts = Vec::new();
    for term in terms {
        if is_important_term(&term) || facts.len() < 3 {
            facts.push(term);
        }
        if facts.len() >= 6 {
            break;
        }
    }
    facts
}

fn is_important_term(term: &str) -> bool {
    matches!(
        term,
        "constraint"
            | "constraints"
            | "failing"
            | "failure"
            | "migration"
            | "privacy"
            | "secret"
            | "redacted"
            | "budget"
            | "sqlite"
            | "tmux"
            | "watch"
            | "test"
            | "tests"
    ) || term.contains('.')
        || term.contains('_')
        || term.contains('-')
}

fn actionability_score(latest: &str, objective_score: u8, preservation_score: u8) -> u8 {
    let lower = latest.to_ascii_lowercase();
    let mut score = ((objective_score as u16 + preservation_score as u16) / 2) as u8;
    if [
        "next",
        "todo",
        "continue",
        "remaining",
        "failing",
        "test",
        "fix",
        "implement",
        "verify",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        score = score.saturating_add(18);
    }
    if lower.contains("unknown") || lower.contains("missing") {
        score = score.saturating_sub(12);
    }
    score.min(100)
}

fn overlap_evidence(
    expected: &HashSet<String>,
    actual: &HashSet<String>,
    label: &str,
) -> Vec<String> {
    if expected.is_empty() {
        return vec![format!("{label} unavailable")];
    }
    let matched = expected.intersection(actual).count();
    vec![format!("{matched}/{} {label} retained", expected.len())]
}

fn actionability_evidence(latest: &str) -> Vec<String> {
    let lower = latest.to_ascii_lowercase();
    let mut evidence = Vec::new();
    if lower.contains("next") || lower.contains("continue") || lower.contains("remaining") {
        evidence.push("latest snapshot names continuation work".to_string());
    }
    if lower.contains("test") || lower.contains("failing") || lower.contains("verify") {
        evidence.push("latest snapshot mentions verification or failing tests".to_string());
    }
    if evidence.is_empty() {
        evidence.push("latest snapshot has limited next-action evidence".to_string());
    }
    evidence
}

fn risk_evidence(objective_score: u8, preservation_score: u8, missing_count: usize) -> Vec<String> {
    let mut evidence = Vec::new();
    if objective_score < 55 {
        evidence.push("objective overlap is weak".to_string());
    }
    if preservation_score < 55 {
        evidence.push("state preservation is weak".to_string());
    }
    if missing_count > 0 {
        evidence.push(format!("{missing_count} missing fact(s) surfaced"));
    }
    if evidence.is_empty() {
        evidence.push("no major deterministic risk signal".to_string());
    }
    evidence
}

fn truncate_plain(value: &str, max_chars: usize) -> String {
    let text = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() <= max_chars {
        text
    } else {
        let mut out = text
            .chars()
            .take(max_chars.saturating_sub(3))
            .collect::<String>();
        out.push_str("...");
        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaudeDriftAnalyzerMode {
    Disabled,
    Mock {
        response: String,
    },
    Claude {
        command: PathBuf,
        source: DriftScoreSource,
        timeout: Duration,
    },
}

impl ClaudeDriftAnalyzerMode {
    pub fn from_env() -> Self {
        let value = std::env::var(DRIFT_ANALYZER_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match value.as_str() {
            "" | "0" | "false" | "no" | "off" | "disabled" => Self::Disabled,
            "mock" => Self::Mock {
                response: std::env::var(DRIFT_ANALYZER_MOCK_RESPONSE_ENV).unwrap_or_default(),
            },
            "1" | "true" | "claude" => {
                let command = std::env::var(DRIFT_ANALYZER_COMMAND_ENV)
                    .map(PathBuf::from)
                    .unwrap_or_else(|_| PathBuf::from("claude"));
                let source = if std::env::var(DRIFT_ANALYZER_CONTEXT_ENV)
                    .unwrap_or_default()
                    .trim()
                    .eq_ignore_ascii_case("full")
                {
                    DriftScoreSource::ClaudeAssistedFull
                } else {
                    DriftScoreSource::ClaudeAssistedRedacted
                };
                Self::Claude {
                    command,
                    source,
                    timeout: Duration::from_secs(120),
                }
            }
            _ => Self::Disabled,
        }
    }
}

pub async fn run_configured_claude_drift_analysis(
    mode: ClaudeDriftAnalyzerMode,
    snapshot: &CompactionSnapshot,
) -> Result<DriftScore, String> {
    match mode {
        ClaudeDriftAnalyzerMode::Disabled => Err("drift analyzer disabled".to_string()),
        ClaudeDriftAnalyzerMode::Mock { response } => {
            parse_claude_drift_response(&response, DriftScoreSource::ClaudeAssistedRedacted)
        }
        ClaudeDriftAnalyzerMode::Claude {
            command,
            source,
            timeout,
        } => run_claude_drift_analysis_with_command(&command, snapshot, source, timeout).await,
    }
}

pub async fn run_claude_drift_analysis_with_command(
    command: &Path,
    snapshot: &CompactionSnapshot,
    source: DriftScoreSource,
    timeout: Duration,
) -> Result<DriftScore, String> {
    let prompt = build_claude_drift_prompt(snapshot, source.clone());
    let mut child = tokio::process::Command::new(command)
        .arg("-p")
        .arg("--output-format")
        .arg("text")
        .arg("--no-session-persistence")
        .arg("--max-budget-usd")
        .arg("0.25")
        .arg("--tools")
        .arg("")
        .env_remove("ANTHROPIC_BASE_URL")
        .env("CC_BLACKBOX_DRIFT_ANALYSIS", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| format!("failed to start claude drift analyzer: {err}"))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "failed to open claude drift analyzer stdin".to_string())?;
    stdin
        .write_all(prompt.as_bytes())
        .await
        .map_err(|err| format!("failed to write drift prompt: {err}"))?;
    drop(stdin);

    let output = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_err(|_| "drift analysis timed out".to_string())?
        .map_err(|err| format!("failed to wait for drift analyzer: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return if stderr.is_empty() {
            Err(format!(
                "claude drift analyzer exited with {}",
                output.status
            ))
        } else {
            Err(format!(
                "claude drift analyzer exited with {}: {stderr}",
                output.status
            ))
        };
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Err("claude drift analyzer returned empty output".to_string());
    }
    parse_claude_drift_response(&stdout, source)
}

fn build_claude_drift_prompt(snapshot: &CompactionSnapshot, source: DriftScoreSource) -> String {
    let mut evidence = serde_json::json!({
        "session_id": snapshot.session_id,
        "sequence": snapshot.sequence,
        "capture_mode": snapshot.capture_mode.as_label(),
        "detection": snapshot.detection,
        "request": snapshot.request,
        "excerpts": snapshot.excerpts,
    });
    if source == DriftScoreSource::ClaudeAssistedRedacted {
        if let Some(excerpts) = evidence.get_mut("excerpts").and_then(Value::as_object_mut) {
            excerpts.remove("raw_initial_objective");
            excerpts.remove("raw_compacted_objective");
            excerpts.remove("raw_compact_summary");
        }
    }
    let bundle = serde_json::to_string_pretty(&evidence).unwrap_or_else(|_| evidence.to_string());
    let bundle = truncate_plain(&bundle, 24_000);
    let source_label = source.as_label();
    format!(
        "You are cc-blackbox's compaction drift analyst.\n\
Use only the JSON evidence below. Return strict JSON only, with no Markdown.\n\
Score dimensions from 0 to 100. For objective_alignment, state_preservation, and actionability, higher is better. For scope_drift and risk, higher is worse.\n\
Do not invent raw prompt details. Preserve caveats when evidence is redacted, capped, or heuristic.\n\
Required JSON shape:\n\
{{\"source\":\"{source_label}\",\"objective_alignment\":{{\"score\":0,\"label\":\"low\",\"evidence\":[]}},\"state_preservation\":{{\"score\":0,\"label\":\"low\",\"evidence\":[]}},\"scope_drift\":{{\"score\":0,\"label\":\"low\",\"evidence\":[]}},\"actionability\":{{\"score\":0,\"label\":\"low\",\"evidence\":[]}},\"risk\":{{\"score\":0,\"label\":\"low\",\"evidence\":[]}},\"missing_facts\":[],\"changed_framing\":null,\"caveats\":[]}}\n\n\
Evidence JSON:\n```json\n{bundle}\n```"
    )
}

pub fn parse_claude_drift_response(
    response: &str,
    fallback_source: DriftScoreSource,
) -> Result<DriftScore, String> {
    let json = extract_json_object(response).ok_or_else(|| "missing JSON object".to_string())?;
    let mut score: DriftScore =
        serde_json::from_str(json).map_err(|err| format!("invalid drift JSON: {err}"))?;
    if !matches!(
        score.source,
        DriftScoreSource::ClaudeAssistedRedacted | DriftScoreSource::ClaudeAssistedFull
    ) {
        score.source = fallback_source;
    }
    if score.caveats.is_empty() {
        score
            .caveats
            .push("Claude-assisted drift scoring is advisory.".to_string());
    }
    Ok(score)
}

fn extract_json_object(response: &str) -> Option<&str> {
    let start = response.find('{')?;
    let end = response.rfind('}')?;
    (end >= start).then_some(&response[start..=end])
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use serde_json::json;

    use super::*;

    fn input(model: &str, message_count: usize, first: &str) -> RequestSnapshotInput {
        RequestSnapshotInput {
            model: model.to_string(),
            working_dir: "/tmp/project".to_string(),
            message_count,
            system_prompt_length: 1000,
            estimated_input_tokens: 10_000,
            first_message_hash: hash_text_hex(first),
            first_user_message: first.to_string(),
            user_prompt_excerpt: first.to_string(),
            compacted_state_hash: hash_text_hex(first),
        }
    }

    fn normal_candidate(
        first: &str,
        message_count: usize,
        tokens: usize,
    ) -> RequestSnapshotCandidate {
        let mut candidate = build_candidate(
            &json!({
                "system": "Primary working directory: /tmp/project",
                "messages": [{"role":"user","content": first}]
            }),
            PromptCaptureMode::Safe,
            input("claude-sonnet", message_count, first),
        );
        candidate.estimated_input_tokens = tokens;
        candidate
    }

    fn compact_candidate(
        summary: &str,
        message_count: usize,
        tokens: usize,
    ) -> RequestSnapshotCandidate {
        let mut candidate = build_candidate(
            &json!({
                "system": "Primary working directory: /tmp/project",
                "messages": [{"role":"user","content": summary}]
            }),
            PromptCaptureMode::Safe,
            input("claude-sonnet", message_count, summary),
        );
        candidate.estimated_input_tokens = tokens;
        candidate
    }

    #[test]
    fn capture_mode_parses_disabled_safe_and_full_values() {
        assert_eq!(PromptCaptureMode::from_env_value(None), None);
        assert_eq!(PromptCaptureMode::from_env_value(Some("")), None);
        assert_eq!(PromptCaptureMode::from_env_value(Some("0")), None);
        assert_eq!(PromptCaptureMode::from_env_value(Some("false")), None);
        assert_eq!(
            PromptCaptureMode::from_env_value(Some("1")),
            Some(PromptCaptureMode::Safe)
        );
        assert_eq!(
            PromptCaptureMode::from_env_value(Some("redacted")),
            Some(PromptCaptureMode::Safe)
        );
        assert_eq!(
            PromptCaptureMode::from_env_value(Some("full")),
            Some(PromptCaptureMode::Full)
        );
    }

    #[test]
    fn safe_excerpt_redacts_and_caps_secrets_code_command_output_and_file_content() {
        let raw = "token=sk-ant-secretvalue\n\
<local-command-stdout>PRIVATE_COMMAND_OUTPUT</local-command-stdout>\n\
```rust\nfn secret() { println!(\"do not leak\"); }\n```\n\
pub fn file_content() { let password = \"hunter2\"; }\n\
See /Users/pradeep/code/private/src/main.rs and https://example.com/a?token=secret";
        let excerpt = safe_excerpt(raw, 180).expect("excerpt");
        assert!(!excerpt.text.contains("sk-ant-secretvalue"));
        assert!(!excerpt.text.contains("PRIVATE_COMMAND_OUTPUT"));
        assert!(!excerpt.text.contains("println!"));
        assert!(!excerpt.text.contains("hunter2"));
        assert!(!excerpt.text.contains("/Users/pradeep"));
        assert!(!excerpt.text.contains("token=secret"));
        assert!(excerpt.text.contains("<command output redacted>"));
        assert!(
            excerpt.text.contains("<code block redacted>")
                || excerpt.text.contains("<code/file content redacted>")
        );
        assert!(excerpt.rendered_chars <= 180);
        assert!(excerpt.redacted);
    }

    #[test]
    fn full_mode_excerpts_are_labeled_and_include_raw_local_fields() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        let first = normal_candidate("Fix login timeout without changing migrations", 8, 12_000);
        assert!(store
            .record_request(
                first,
                "session_a".to_string(),
                "req_1".to_string(),
                "2026-01-01T00:00:00Z".to_string(),
                None,
            )
            .is_none());

        let mut latest = compact_candidate(
            "Compaction summary: continue fixing login timeout. Next step run auth_timeout_test.",
            2,
            3_000,
        );
        latest.capture_mode = PromptCaptureMode::Full;
        let snapshot = store
            .record_request(
                latest,
                "session_a".to_string(),
                "req_2".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(86.0),
            )
            .expect("snapshot");

        assert_eq!(snapshot.capture_mode, PromptCaptureMode::Full);
        assert!(snapshot.full_capture_warning.is_some());
        assert!(snapshot.excerpts.raw_compacted_objective.is_some());
    }

    #[test]
    fn store_bounds_retention_cleanup_and_latest_pair() {
        let store = SnapshotStore::new(2, Duration::from_millis(1));
        let first = normal_candidate("Fix auth", 10, 10_000);
        assert!(store
            .record_request(
                first,
                "session_a".to_string(),
                "req_0".to_string(),
                "2026-01-01T00:00:00Z".to_string(),
                None,
            )
            .is_none());
        for idx in 1..=3 {
            let snapshot = compact_candidate(
                &format!("Compaction summary: Fix auth. Next step {idx}."),
                2,
                2_000,
            );
            store.record_request(
                snapshot,
                "session_a".to_string(),
                format!("req_{idx}"),
                format!("2026-01-01T00:0{idx}:00Z"),
                Some(86.0),
            );
        }
        let snapshots = store.list_session_snapshots("session_a");
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].sequence, 2);
        let (previous, latest) = store.latest_pair("session_a").expect("pair");
        assert_eq!(previous.expect("previous").sequence, 2);
        assert_eq!(latest.sequence, 3);

        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(store.cleanup_expired(), 1);
        assert!(store.list_session_snapshots("session_a").is_empty());
    }

    #[test]
    fn detection_distinguishes_suspected_and_detected_compaction() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate("Fix auth timeout", 10, 20_000),
            "session_a".to_string(),
            "req_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );

        let suspected = store
            .record_request(
                normal_candidate("Fix auth timeout", 4, 18_000),
                "session_a".to_string(),
                "req_s".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(84.0),
            )
            .expect("suspected snapshot");
        assert_eq!(
            suspected.detection.status,
            CompactionDetectionStatus::Suspected
        );

        let detected = store
            .record_request(
                compact_candidate(
                    "Compaction summary: continue fixing auth timeout. Next steps: run failing auth_timeout_test.",
                    2,
                    2_000,
                ),
                "session_a".to_string(),
                "req_d".to_string(),
                "2026-01-01T00:02:00Z".to_string(),
                Some(86.0),
            )
            .expect("detected snapshot");
        assert_eq!(
            detected.detection.status,
            CompactionDetectionStatus::Detected
        );
    }

    #[test]
    fn deferred_tool_system_reminders_do_not_emit_compaction_snapshots() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        let initial_prompt = "Lets investigate Playwright traces-only reporter";
        store.record_request(
            normal_candidate(initial_prompt, 79, 125_383),
            "session_a".to_string(),
            "req_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );

        let mut candidate = build_candidate(
            &json!({
                "system": "Primary working directory: /tmp/project",
                "messages": [{
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "<system-reminder>\nThe following deferred tools are now available via ToolSearch. Their schemas are NOT loaded. Use them for compaction snapshot debugging.\n</system-reminder>"
                        },
                        {
                            "type": "text",
                            "text": "Continue checking playwright-opentelemetry-reporter traces-only behavior."
                        }
                    ]
                }]
            }),
            PromptCaptureMode::Full,
            input("claude-sonnet", 81, initial_prompt),
        );
        candidate.estimated_input_tokens = 126_961;

        assert!(store
            .record_request(
                candidate,
                "session_a".to_string(),
                "req_2".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(84.0),
            )
            .is_none());
    }

    #[test]
    fn generated_compaction_summary_uses_response_summary_block() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate("Fix Playwright traces-only reporter", 79, 125_383),
            "session_a".to_string(),
            "req_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );

        let mut candidate = compact_candidate(
            "CRITICAL: Respond with TEXT ONLY. Your task is to create a detailed summary of the conversation so far. Optional Next Step.",
            1,
            126_000,
        );
        candidate.capture_mode = PromptCaptureMode::Full;
        let snapshot = store
            .record_generated_summary(
                candidate,
                "session_a".to_string(),
                "req_summary".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                "<analysis>private scratch</analysis>\n<summary>Continue fixing playwright-opentelemetry-reporter traces-only test.</summary>",
            )
            .expect("generated summary snapshot");

        let raw = snapshot
            .excerpts
            .raw_compacted_objective
            .as_ref()
            .expect("raw compacted objective");
        assert_eq!(
            raw.text,
            "Continue fixing playwright-opentelemetry-reporter traces-only test."
        );
        assert!(!raw.text.contains("private scratch"));
        assert_eq!(
            snapshot.detection.status,
            CompactionDetectionStatus::Detected
        );
        assert!(snapshot
            .detection
            .reason
            .contains("generated compacted conversation summary"));
    }

    #[test]
    fn full_mode_generated_compaction_summary_raw_excerpt_is_not_preview_capped() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate(
                "Ship Playwright OpenTelemetry metrics rollout",
                120,
                130_965,
            ),
            "session_a".to_string(),
            "req_before".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );

        let long_context =
            " producer docs, collector wiring, dashboard checks, and review notes".repeat(100);
        let tail_marker = "FINAL_OPTIONAL_NEXT_STEP_MARKER";
        let response = format!(
            "<analysis>private scratch</analysis>\n<summary>1. Primary Request and Intent: The user is shipping a three-PR OpenTelemetry metrics rollout for Playwright e2e tests.{long_context}\n9. Optional Next Step: {tail_marker}</summary>"
        );

        let mut candidate = compact_candidate(
            "CRITICAL: Respond with TEXT ONLY. Your task is to create a detailed summary of the conversation so far. Optional Next Step.",
            3,
            73_554,
        );
        candidate.capture_mode = PromptCaptureMode::Full;
        let snapshot = store
            .record_generated_summary(
                candidate,
                "session_a".to_string(),
                "req_summary".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                &response,
            )
            .expect("generated summary snapshot");

        let raw = snapshot
            .excerpts
            .raw_compacted_objective
            .as_ref()
            .expect("raw compacted objective");
        assert!(raw.rendered_chars > FULL_EXCERPT_CHARS);
        assert!(!raw.truncated);
        assert!(raw.text.contains(tail_marker));
        assert!(!raw.text.contains("private scratch"));

        let safe = snapshot
            .excerpts
            .compacted_objective
            .as_ref()
            .expect("safe compacted objective");
        assert!(safe.truncated);
        assert!(safe.rendered_chars <= SAFE_EXCERPT_CHARS);
    }

    #[test]
    fn carried_forward_compact_summary_is_not_re_emitted_each_turn() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate(
                "Create Playwright OpenTelemetry metrics reporter",
                3,
                67_495,
            ),
            "session_a".to_string(),
            "req_before".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );

        let carried_summary = "This session is being continued from a previous conversation that ran out of context. The summary below covers the earlier portion of the conversation. Summary: The user wants to add an OpenTelemetry metrics reporter to Playwright e2e tests.";
        let mut first_continuation = compact_candidate(carried_summary, 4, 64_957);
        first_continuation.first_message_hash = hash_text_hex(carried_summary);
        let first = store
            .record_request(
                first_continuation,
                "session_a".to_string(),
                "req_first_continuation".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(82.0),
            )
            .expect("first carried summary should emit once");
        assert_eq!(first.sequence, 1);

        let mut later_continuation = compact_candidate(carried_summary, 6, 70_977);
        later_continuation.first_message_hash = hash_text_hex(carried_summary);
        assert!(store
            .record_request(
                later_continuation,
                "session_a".to_string(),
                "req_later_continuation".to_string(),
                "2026-01-01T00:02:00Z".to_string(),
                Some(35.0),
            )
            .is_none());
    }

    #[test]
    fn deterministic_drift_scores_required_dimensions() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate(
                "Fix login timeout, preserve migration constraint, keep failing test auth_timeout_test visible",
                10,
                20_000,
            ),
            "session_a".to_string(),
            "req_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );
        let aligned = store
            .record_request(
                compact_candidate(
                    "Compaction summary: continue to fix login timeout. Preserve migration constraint. Failing test auth_timeout_test remains next.",
                    2,
                    2_000,
                ),
                "session_a".to_string(),
                "req_2".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(86.0),
            )
            .expect("aligned snapshot");
        let drift = aligned.drift.as_ref().expect("drift");
        assert!(drift.objective_alignment.score >= 55);
        assert!(drift.state_preservation.score >= 55);
        assert!(drift.scope_drift.score < 60);
        assert!(drift.actionability.score >= 55);
        assert!(drift.risk.score < 60);

        let drifted = store
            .record_request(
                compact_candidate(
                    "Compaction summary: refactor the entire auth flow. Next create new service abstraction.",
                    2,
                    2_000,
                ),
                "session_a".to_string(),
                "req_3".to_string(),
                "2026-01-01T00:02:00Z".to_string(),
                Some(86.0),
            )
            .expect("drifted snapshot");
        let drift = drifted.drift.as_ref().expect("drift");
        assert!(drift.objective_alignment.score < 60);
        assert!(drift.scope_drift.score >= 40);
        assert!(drift
            .missing_facts
            .iter()
            .any(|fact| fact.contains("migration")));
        assert!(drift
            .missing_facts
            .iter()
            .any(|fact| fact.contains("constraint")));
        assert!(drift.changed_framing.is_some());
    }

    #[test]
    fn deterministic_drift_does_not_punish_compression_alone() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate("Implement snapshot API and watch rendering", 10, 20_000),
            "session_a".to_string(),
            "req_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );
        let snapshot = store
            .record_request(
                compact_candidate("Compaction summary: snapshot API and watch rendering remain the task. Next verify tests.", 2, 2_000),
                "session_a".to_string(),
                "req_2".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(86.0),
            )
            .expect("snapshot");
        let drift = snapshot.drift.expect("drift");
        assert!(drift.objective_alignment.score >= 60);
        assert!(drift.risk.score < 50);
    }

    #[test]
    fn watch_snapshot_json_omits_raw_fields_in_safe_mode() {
        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate("Fix auth", 8, 10_000),
            "session_a".to_string(),
            "req_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );
        let snapshot = store
            .record_request(
                compact_candidate("Compaction summary: fix auth. Next run tests.", 2, 2_000),
                "session_a".to_string(),
                "req_2".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(86.0),
            )
            .expect("snapshot");
        let json = serde_json::to_value(&snapshot).expect("json");
        assert_eq!(json["capture_mode"], "safe_redacted");
        assert!(json.pointer("/excerpts/raw_compacted_objective").is_none());
    }

    #[tokio::test]
    async fn claude_assisted_scoring_success_and_failure_are_structured() {
        let response = r#"{
          "source": "claude_assisted_redacted",
          "objective_alignment": {"score": 80, "label": "high", "evidence": ["kept objective"]},
          "state_preservation": {"score": 70, "label": "medium", "evidence": []},
          "scope_drift": {"score": 20, "label": "low", "evidence": []},
          "actionability": {"score": 90, "label": "high", "evidence": []},
          "risk": {"score": 20, "label": "low", "evidence": []},
          "missing_facts": [],
          "changed_framing": null,
          "caveats": ["redacted evidence"]
        }"#;
        let parsed =
            parse_claude_drift_response(response, DriftScoreSource::ClaudeAssistedRedacted)
                .expect("parse");
        assert_eq!(parsed.source, DriftScoreSource::ClaudeAssistedRedacted);
        assert_eq!(parsed.objective_alignment.score, 80);

        let bad = parse_claude_drift_response("not json", DriftScoreSource::ClaudeAssistedRedacted);
        assert!(bad.is_err());
    }

    #[tokio::test]
    async fn claude_runner_accepts_mock_command_and_removes_proxy_env() {
        let dir =
            std::env::temp_dir().join(format!("cc-blackbox-drift-fake-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("create dir");
        let script = dir.join("claude");
        fs::write(
            &script,
            "#!/bin/sh\nif [ -n \"$ANTHROPIC_BASE_URL\" ]; then exit 9; fi\ncat >/dev/null\necho '{\"source\":\"claude_assisted_redacted\",\"objective_alignment\":{\"score\":80,\"label\":\"high\",\"evidence\":[]},\"state_preservation\":{\"score\":70,\"label\":\"medium\",\"evidence\":[]},\"scope_drift\":{\"score\":20,\"label\":\"low\",\"evidence\":[]},\"actionability\":{\"score\":90,\"label\":\"high\",\"evidence\":[]},\"risk\":{\"score\":20,\"label\":\"low\",\"evidence\":[]},\"missing_facts\":[],\"changed_framing\":null,\"caveats\":[]}'\n",
        )
        .expect("write fake");
        let mut perms = fs::metadata(&script).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod");
        std::env::set_var("ANTHROPIC_BASE_URL", "http://localhost:10000");

        let store = SnapshotStore::new(5, Duration::from_secs(60));
        store.record_request(
            normal_candidate("Fix auth", 8, 10_000),
            "session_a".to_string(),
            "req_1".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
            None,
        );
        let snapshot = store
            .record_request(
                compact_candidate("Compaction summary: fix auth. Next run tests.", 2, 2_000),
                "session_a".to_string(),
                "req_2".to_string(),
                "2026-01-01T00:01:00Z".to_string(),
                Some(86.0),
            )
            .expect("snapshot");

        let score = run_claude_drift_analysis_with_command(
            &script,
            &snapshot,
            DriftScoreSource::ClaudeAssistedRedacted,
            Duration::from_secs(2),
        )
        .await
        .expect("score");
        assert_eq!(score.actionability.score, 90);
        std::env::remove_var("ANTHROPIC_BASE_URL");
        let _ = fs::remove_dir_all(dir);
    }
}
