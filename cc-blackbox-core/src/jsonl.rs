use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JsonlUsageTotals {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JsonlDerivedSession {
    pub jsonl_session_id: String,
    pub cwd: String,
    pub started_at_epoch_secs: u64,
    pub last_activity_at_epoch_secs: u64,
    pub first_message_hash: String,
    pub user_turn_count: u64,
    pub assistant_turn_count: u64,
    pub request_count: u64,
    pub tool_calls: BTreeMap<String, u64>,
    pub tool_failures: BTreeMap<String, u64>,
    pub failed_tool_turns: Vec<u64>,
    pub mcp_tool_names: BTreeSet<String>,
    pub skill_names: BTreeSet<String>,
    pub bash_command_categories: BTreeMap<String, u64>,
    pub file_operation_categories: BTreeMap<String, u64>,
    pub usage: JsonlUsageTotals,
    pub system_event_categories: BTreeMap<String, u64>,
}

impl JsonlDerivedSession {
    pub fn correlation(&self) -> crate::correlation::JsonlSessionCorrelation {
        crate::correlation::JsonlSessionCorrelation {
            jsonl_session_id: self.jsonl_session_id.clone(),
            cwd: self.cwd.clone(),
            started_at_epoch_secs: self.started_at_epoch_secs,
            last_activity_at_epoch_secs: self.last_activity_at_epoch_secs,
            first_message_hash: self.first_message_hash.clone(),
            request_count: self.request_count,
            turn_count: self.assistant_turn_count,
        }
    }

    pub fn total_tool_failures(&self) -> u64 {
        self.tool_failures.values().sum()
    }

    pub fn longest_tool_failure_streak(&self) -> u64 {
        let mut turns = self.failed_tool_turns.clone();
        turns.sort_unstable();
        turns.dedup();
        let mut longest = 0u64;
        let mut current = 0u64;
        let mut previous = None;
        for turn in turns {
            current = if previous.is_some_and(|prev| turn == prev + 1) {
                current + 1
            } else {
                1
            };
            previous = Some(turn);
            longest = longest.max(current);
        }
        longest
    }

    pub fn last_failed_tool_turn(&self) -> Option<u64> {
        self.failed_tool_turns.iter().copied().max()
    }
}

pub fn parse_jsonl_file(path: &Path) -> io::Result<Option<JsonlDerivedSession>> {
    let mut raw = String::new();
    File::open(path)?.read_to_string(&mut raw)?;
    Ok(parse_jsonl_str(&raw, &path.display().to_string()))
}

pub fn parse_jsonl_str(raw: &str, source_id: &str) -> Option<JsonlDerivedSession> {
    let mut session = JsonlDerivedSession {
        jsonl_session_id: source_id.to_string(),
        ..JsonlDerivedSession::default()
    };
    let mut tool_ids = BTreeMap::<String, String>::new();
    let mut current_assistant_turn = 0u64;

    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };

        if let Some(timestamp) = value.get("timestamp").and_then(Value::as_str) {
            if let Some(epoch) = parse_epoch_secs(timestamp) {
                if session.started_at_epoch_secs == 0 || epoch < session.started_at_epoch_secs {
                    session.started_at_epoch_secs = epoch;
                }
                if epoch > session.last_activity_at_epoch_secs {
                    session.last_activity_at_epoch_secs = epoch;
                }
            }
        }
        if session.cwd.is_empty() {
            if let Some(cwd) = value.get("cwd").and_then(Value::as_str) {
                session.cwd = normalize_cwd(cwd);
            }
        }
        if session.jsonl_session_id == source_id {
            if let Some(id) = value
                .get("sessionId")
                .or_else(|| value.get("session_id"))
                .and_then(Value::as_str)
            {
                session.jsonl_session_id = id.to_string();
            }
        }

        let top_type = value.get("type").and_then(Value::as_str);
        if top_type == Some("system") {
            let category = system_event_category(&value);
            *session.system_event_categories.entry(category).or_insert(0) += 1;
            continue;
        }

        let message = value.get("message").unwrap_or(&value);
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .or(top_type)
            .unwrap_or_default();

        match role {
            "user" => {
                if let Some(text) = user_text_content(message.get("content")) {
                    if session.first_message_hash.is_empty() {
                        session.first_message_hash = hash_text_hex(&text);
                    }
                    session.user_turn_count += 1;
                }
                collect_tool_results(
                    message.get("content"),
                    current_assistant_turn,
                    &tool_ids,
                    &mut session,
                );
            }
            "assistant" => {
                session.assistant_turn_count += 1;
                current_assistant_turn = session.assistant_turn_count;
                collect_usage(message.get("usage"), &mut session.usage);
                collect_tool_uses(
                    message.get("content"),
                    current_assistant_turn,
                    &mut tool_ids,
                    &mut session,
                );
            }
            _ => {
                if top_type == Some("assistant") {
                    session.assistant_turn_count += 1;
                    current_assistant_turn = session.assistant_turn_count;
                    collect_usage(message.get("usage"), &mut session.usage);
                    collect_tool_uses(
                        message.get("content"),
                        current_assistant_turn,
                        &mut tool_ids,
                        &mut session,
                    );
                }
            }
        }
    }

    session.request_count = session.assistant_turn_count;
    if session.started_at_epoch_secs == 0
        && session.last_activity_at_epoch_secs == 0
        && session.cwd.is_empty()
        && session.first_message_hash.is_empty()
        && session.tool_calls.is_empty()
    {
        None
    } else {
        Some(session)
    }
}

fn normalize_cwd(raw: &str) -> String {
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

fn parse_epoch_secs(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    let without_z = raw.strip_suffix('Z').unwrap_or(raw);
    let main = without_z.split(['.', '+']).next().unwrap_or(without_z);
    let (date, time) = main.split_once('T')?;
    let mut date_parts = date.split('-').filter_map(|part| part.parse::<i64>().ok());
    let year = date_parts.next()? as i32;
    let month = date_parts.next()? as u32;
    let day = date_parts.next()? as u32;
    let mut time_parts = time.split(':').filter_map(|part| part.parse::<u64>().ok());
    let hour = time_parts.next()?;
    let minute = time_parts.next()?;
    let second = time_parts.next()?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let days = days_from_civil(year, month, day)?;
    Some(days as u64 * 86_400 + hour * 3_600 + minute * 60 + second.min(59))
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era as i64 * 146_097 + doe as i64 - 719_468;
    (days >= 0).then_some(days)
}

fn user_text_content(content: Option<&Value>) -> Option<String> {
    match content? {
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Array(items) => {
            let text = items
                .iter()
                .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            (!text.trim().is_empty()).then_some(text)
        }
        _ => None,
    }
}

fn content_blocks(content: Option<&Value>) -> Vec<&Value> {
    match content {
        Some(Value::Array(items)) => items.iter().collect(),
        Some(value @ Value::Object(_)) => vec![value],
        _ => Vec::new(),
    }
}

fn add_count(map: &mut BTreeMap<String, u64>, key: impl Into<String>) {
    *map.entry(key.into()).or_insert(0) += 1;
}

fn collect_usage(usage: Option<&Value>, totals: &mut JsonlUsageTotals) {
    let Some(usage) = usage else {
        return;
    };
    totals.input_tokens += usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    totals.output_tokens += usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    totals.cache_read_tokens += usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    totals.cache_creation_tokens += usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
}

fn collect_tool_uses(
    content: Option<&Value>,
    _turn: u64,
    tool_ids: &mut BTreeMap<String, String>,
    session: &mut JsonlDerivedSession,
) {
    for block in content_blocks(content) {
        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
            continue;
        }
        let Some(name) = block.get("name").and_then(Value::as_str) else {
            continue;
        };
        add_count(&mut session.tool_calls, name);
        if let Some(id) = block.get("id").and_then(Value::as_str) {
            tool_ids.insert(id.to_string(), name.to_string());
        }
        if let Some(mcp) = mcp_tool_name(name) {
            session.mcp_tool_names.insert(mcp);
        }
        if let Some(category) = file_operation_category(name) {
            add_count(&mut session.file_operation_categories, category);
        }
        if name.eq_ignore_ascii_case("Bash") {
            let command = block
                .get("input")
                .and_then(|input| input.get("command"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            add_count(
                &mut session.bash_command_categories,
                bash_command_category(command),
            );
        }
        if name.eq_ignore_ascii_case("Skill") {
            if let Some(skill) = block.get("input").and_then(skill_name_from_input) {
                session.skill_names.insert(skill);
            }
        }
    }
}

fn collect_tool_results(
    content: Option<&Value>,
    turn: u64,
    tool_ids: &BTreeMap<String, String>,
    session: &mut JsonlDerivedSession,
) {
    for block in content_blocks(content) {
        if block.get("type").and_then(Value::as_str) != Some("tool_result") {
            continue;
        }
        let is_error = block
            .get("is_error")
            .or_else(|| block.get("error"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !is_error {
            continue;
        }
        let tool_name = block
            .get("tool_use_id")
            .and_then(Value::as_str)
            .and_then(|id| tool_ids.get(id))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        add_count(&mut session.tool_failures, tool_name);
        if turn > 0 {
            session.failed_tool_turns.push(turn);
        }
    }
}

fn mcp_tool_name(name: &str) -> Option<String> {
    let rest = name.strip_prefix("mcp__")?;
    let mut parts = rest.split("__");
    let server = parts.next()?.trim();
    let tool = parts.next()?.trim();
    if server.is_empty() || tool.is_empty() {
        None
    } else {
        Some(format!("{server}.{tool}"))
    }
}

fn skill_name_from_input(input: &Value) -> Option<String> {
    ["skill_name", "skill", "name"]
        .iter()
        .find_map(|key| input.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn bash_command_category(command: &str) -> &'static str {
    let lower = command.trim().to_ascii_lowercase();
    if lower.contains("cargo test")
        || lower.contains("npm test")
        || lower.contains("pnpm test")
        || lower.contains("pytest")
        || lower.contains("go test")
    {
        "test"
    } else if lower.starts_with("git ") {
        "git"
    } else if lower.starts_with("rg ") || lower.contains(" grep ") || lower.starts_with("grep ") {
        "search"
    } else if lower.starts_with("ls ") || lower == "ls" || lower == "pwd" {
        "inspect"
    } else if lower.starts_with("curl ") || lower.starts_with("wget ") {
        "network"
    } else if lower.starts_with("mkdir ")
        || lower.starts_with("rm ")
        || lower.starts_with("cp ")
        || lower.starts_with("mv ")
        || lower.starts_with("chmod ")
    {
        "filesystem"
    } else {
        "other"
    }
}

fn file_operation_category(name: &str) -> Option<&'static str> {
    match name {
        "Read" | "Glob" | "Grep" | "LS" => Some("read"),
        "Edit" | "MultiEdit" | "NotebookEdit" => Some("edit"),
        "Write" => Some("write"),
        _ => None,
    }
}

fn system_event_category(value: &Value) -> String {
    let subtype = value
        .get("subtype")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("system")
        .to_ascii_lowercase();
    if subtype.contains("compact") {
        "compaction".to_string()
    } else {
        subtype
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect::<String>()
            .trim_matches('_')
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::parse_jsonl_str;

    #[test]
    fn parser_extracts_derived_facts_without_raw_jsonl_content() {
        let raw = r#"
{"type":"user","sessionId":"jsonl-session-1","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":[{"type":"text","text":"RAW_PROMPT_SECRET plan around /tmp/RAW_FILE_CONTENT_SECRET.rs"}]}}
{"type":"assistant","sessionId":"jsonl-session-1","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:10Z","message":{"role":"assistant","usage":{"input_tokens":1000,"output_tokens":100,"cache_read_input_tokens":200,"cache_creation_input_tokens":50},"content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"cargo test -- RAW_TOOL_INPUT_SECRET"}},{"type":"tool_use","id":"toolu_2","name":"mcp__github__get_issue","input":{"issue":"RAW_TOOL_INPUT_SECRET"}},{"type":"tool_use","id":"toolu_3","name":"Skill","input":{"skill_name":"tdd"}},{"type":"tool_use","id":"toolu_4","name":"Read","input":{"file_path":"/tmp/RAW_FILE_CONTENT_SECRET.rs"}},{"type":"tool_use","id":"toolu_5","name":"Edit","input":{"file_path":"/tmp/RAW_FILE_CONTENT_SECRET.rs","new_string":"RAW_TOOL_INPUT_SECRET"}},{"type":"tool_use","id":"toolu_6","name":"Write","input":{"file_path":"/tmp/RAW_FILE_CONTENT_SECRET.rs","content":"RAW_FILE_CONTENT_SECRET"}}]}}
{"type":"user","sessionId":"jsonl-session-1","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:20Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","is_error":true,"content":"RAW_TOOL_OUTPUT_SECRET"},{"type":"tool_result","tool_use_id":"toolu_2","content":"RAW_TOOL_OUTPUT_SECRET"}]}}
{"type":"system","sessionId":"jsonl-session-1","cwd":"/tmp/cc-blackbox-jsonl","timestamp":"2026-01-01T00:00:30Z","subtype":"compact"}
"#;

        let parsed = parse_jsonl_str(raw, "fixture.jsonl").expect("parsed jsonl");

        assert_eq!(parsed.jsonl_session_id, "jsonl-session-1");
        assert_eq!(parsed.cwd, "/tmp/cc-blackbox-jsonl");
        assert_eq!(parsed.started_at_epoch_secs, 1_767_225_600);
        assert_eq!(parsed.last_activity_at_epoch_secs, 1_767_225_630);
        assert_eq!(parsed.user_turn_count, 1);
        assert_eq!(parsed.assistant_turn_count, 1);
        assert_eq!(parsed.request_count, 1);
        assert_eq!(parsed.tool_calls.get("Bash"), Some(&1));
        assert_eq!(parsed.tool_failures.get("Bash"), Some(&1));
        assert!(parsed.mcp_tool_names.contains("github.get_issue"));
        assert!(parsed.skill_names.contains("tdd"));
        assert_eq!(parsed.bash_command_categories.get("test"), Some(&1));
        assert_eq!(parsed.file_operation_categories.get("read"), Some(&1));
        assert_eq!(parsed.file_operation_categories.get("edit"), Some(&1));
        assert_eq!(parsed.file_operation_categories.get("write"), Some(&1));
        assert_eq!(parsed.usage.input_tokens, 1_000);
        assert_eq!(parsed.usage.output_tokens, 100);
        assert_eq!(parsed.usage.cache_read_tokens, 200);
        assert_eq!(parsed.usage.cache_creation_tokens, 50);
        assert_eq!(parsed.system_event_categories.get("compaction"), Some(&1));

        let derived = format!("{parsed:?}");
        for forbidden in [
            "RAW_PROMPT_SECRET",
            "RAW_TOOL_INPUT_SECRET",
            "RAW_TOOL_OUTPUT_SECRET",
            "RAW_FILE_CONTENT_SECRET",
            "cargo test",
        ] {
            assert!(
                !derived.contains(forbidden),
                "raw content leaked: {derived}"
            );
        }
    }
}
