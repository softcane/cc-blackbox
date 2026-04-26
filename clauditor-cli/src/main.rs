mod tmux;

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Local};
use clap::{Parser, Subcommand};
use colored::Colorize;
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(name = "clauditor", about = "CLI for clauditor observability proxy")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Live stream of Claude Code activity
    Watch {
        /// Base URL of clauditor-core
        #[arg(long, default_value = "http://localhost:9091")]
        url: String,

        /// Hide cache events
        #[arg(long)]
        no_cache: bool,

        /// Hide frustration signal events
        #[arg(long)]
        no_signals: bool,

        /// Filter to a specific session ID
        #[arg(long)]
        session: Option<String>,

        /// Split each session into its own tmux pane
        #[arg(long, conflicts_with = "session")]
        tmux: bool,

        /// Max tmux panes before refusing new sessions
        #[arg(long, default_value = "4")]
        tmux_max_panes: usize,
    },

    /// Show recent sessions
    Sessions {
        /// Base URL of clauditor-core
        #[arg(long, default_value = "http://localhost:9091")]
        url: String,

        /// Number of sessions to show
        #[arg(long, default_value = "20")]
        limit: u32,

        /// Days to look back
        #[arg(long, default_value = "7")]
        days: u32,
    },

    /// Search across past session prompts and final summaries
    Recall {
        /// Base URL of clauditor-core
        #[arg(long, default_value = "http://localhost:9091")]
        url: String,

        /// Number of matches to show
        #[arg(long, default_value = "5")]
        limit: u32,

        /// Days to look back
        #[arg(long, default_value = "30")]
        days: u32,

        /// Search query
        #[arg(required = true, num_args = 1..)]
        query: Vec<String>,
    },

    /// Import a billed-cost reconciliation for a session
    Reconcile {
        /// Base URL of clauditor-core
        #[arg(long, default_value = "http://localhost:9091")]
        url: String,

        /// Session ID to reconcile
        #[arg(long)]
        session: String,

        /// Billed cost in USD
        #[arg(long)]
        billed_cost: f64,

        /// Billing source label, e.g. invoice_2026q2
        #[arg(long)]
        source: String,

        /// Optional import timestamp in UTC ISO 8601
        #[arg(long)]
        imported_at: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WatchEvent {
    ToolUse {
        session_id: String,
        timestamp: String,
        tool_name: String,
        summary: String,
    },
    ToolResult {
        session_id: String,
        tool_name: String,
        outcome: String,
        duration_ms: u64,
    },
    SkillEvent {
        session_id: String,
        timestamp: String,
        skill_name: String,
        event_type: String,
        source: String,
        confidence: f64,
        #[serde(default)]
        detail: Option<String>,
    },
    McpEvent {
        session_id: String,
        timestamp: String,
        server: String,
        tool: String,
        event_type: String,
        source: String,
        #[serde(default)]
        detail: Option<String>,
    },
    CacheEvent {
        session_id: String,
        event_type: String,
        #[serde(default)]
        cache_expires_at_epoch: Option<u64>,
        #[serde(default)]
        estimated_rebuild_cost_dollars: Option<f64>,
    },
    SessionStart {
        session_id: String,
        display_name: String,
        model: String,
        #[serde(default)]
        initial_prompt: Option<String>,
    },
    SessionEnd {
        session_id: String,
        outcome: String,
        total_tokens: u64,
        total_turns: u32,
    },
    FrustrationSignal {
        session_id: String,
        signal_type: String,
    },
    CompactionLoop {
        session_id: String,
        consecutive: u32,
        wasted_tokens: u64,
    },
    Diagnosis {
        session_id: String,
        report: DiagnosisReport,
    },
    CacheWarning {
        session_id: String,
        idle_secs: u64,
        ttl_secs: u64,
    },
    ModelFallback {
        session_id: String,
        requested: String,
        actual: String,
    },
    ContextStatus {
        session_id: String,
        fill_percent: f64,
        #[allow(dead_code)]
        #[serde(default)]
        context_window_tokens: Option<u64>,
        #[serde(default)]
        turns_to_compact: Option<u32>,
    },
    RateLimitStatus {
        #[serde(default)]
        seconds_to_reset: Option<u64>,
        #[serde(default)]
        requests_remaining: Option<u64>,
        #[serde(default)]
        requests_limit: Option<u64>,
        #[serde(default)]
        input_tokens_remaining: Option<u64>,
        #[serde(default)]
        output_tokens_remaining: Option<u64>,
        #[serde(default)]
        tokens_used_this_week: Option<u64>,
        #[serde(default)]
        tokens_limit: Option<u64>,
        #[serde(default)]
        tokens_remaining: Option<u64>,
        #[serde(default)]
        budget_source: Option<String>,
        #[serde(default)]
        projected_exhaustion_secs: Option<u64>,
    },
    // For the "lagged" pseudo-event from the server.
    #[serde(rename = "lagged")]
    Lagged { missed: u64 },
}

#[derive(Debug, Deserialize)]
struct DiagnosisReport {
    outcome: String,
    total_turns: u32,
    total_tokens: u64,
    #[allow(dead_code)]
    #[serde(default)]
    estimated_total_cost_dollars: Option<f64>,
    #[allow(dead_code)]
    cost_source: Option<String>,
    #[serde(default)]
    cache_hit_ratio: f64,
    degraded: bool,
    degradation_turn: Option<u32>,
    causes: Vec<DegradationCause>,
    advice: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DegradationCause {
    turn_first_noticed: u32,
    cause_type: String,
    detail: String,
    #[allow(dead_code)]
    estimated_cost: f64,
    is_heuristic: bool,
}

#[derive(Debug, Serialize)]
struct BillingReconciliationInput {
    session_id: String,
    source: String,
    billed_cost_dollars: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    imported_at: Option<String>,
}

fn parse_local_datetime(iso: &str) -> Option<DateTime<Local>> {
    DateTime::parse_from_rfc3339(iso)
        .ok()
        .map(|dt| dt.with_timezone(&Local))
}

fn local_time_from_iso(iso: &str) -> String {
    parse_local_datetime(iso)
        .map(|dt| dt.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "??:??:??".to_string())
}

fn compact_datetime_from_iso(iso: &str) -> String {
    parse_local_datetime(iso)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| iso.to_string())
}

fn format_tokens(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{}K", tokens / 1_000)
    } else {
        format!("{}", tokens)
    }
}

fn format_duration_coarse(secs: u64) -> String {
    const M: u64 = 60;
    const H: u64 = 60 * M;
    const D: u64 = 24 * H;
    if secs < M {
        format!("{}s", secs)
    } else if secs < H {
        format!("{}m", secs / M)
    } else if secs < D {
        let h = secs / H;
        let m = (secs % H) / M;
        if m == 0 {
            format!("{}h", h)
        } else {
            format!("{}h {}m", h, m)
        }
    } else {
        let d = secs / D;
        let h = (secs % D) / H;
        if h == 0 {
            format!("{}d", d)
        } else {
            format!("{}d {}h", d, h)
        }
    }
}

fn truncate_for_box(s: &str, max_chars: usize) -> String {
    // Char-count safe; final ellipsis is counted within the cap.
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let mut out: String = chars.iter().take(max_chars.saturating_sub(1)).collect();
        out.push('\u{2026}');
        out
    }
}

fn now_hms() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

/// Extract session_id from any WatchEvent variant. Returns None for global
/// events (Lagged, RateLimitStatus).
pub(crate) fn event_session_id(event: &WatchEvent) -> Option<&str> {
    match event {
        WatchEvent::ToolUse { session_id, .. }
        | WatchEvent::ToolResult { session_id, .. }
        | WatchEvent::SkillEvent { session_id, .. }
        | WatchEvent::McpEvent { session_id, .. }
        | WatchEvent::CacheEvent { session_id, .. }
        | WatchEvent::SessionStart { session_id, .. }
        | WatchEvent::SessionEnd { session_id, .. }
        | WatchEvent::FrustrationSignal { session_id, .. }
        | WatchEvent::CompactionLoop { session_id, .. }
        | WatchEvent::Diagnosis { session_id, .. }
        | WatchEvent::CacheWarning { session_id, .. }
        | WatchEvent::ModelFallback { session_id, .. }
        | WatchEvent::ContextStatus { session_id, .. } => Some(session_id.as_str()),
        WatchEvent::Lagged { .. } | WatchEvent::RateLimitStatus { .. } => None,
    }
}

/// Tracks active sessions for prefix tag display.
struct ActiveSessions {
    /// session_id -> display_name
    sessions: HashMap<String, String>,
}

impl ActiveSessions {
    fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    fn add(&mut self, session_id: &str, display_name: &str) {
        self.sessions
            .insert(session_id.to_string(), display_name.to_string());
    }

    fn remove(&mut self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    fn is_multi(&self) -> bool {
        self.sessions.len() > 1
    }

    /// Returns the prefix tag for a given session_id, padded to the max name width.
    /// Returns empty string when only 1 or 0 sessions are active.
    fn tag_for(&self, session_id: &str) -> String {
        if !self.is_multi() {
            return String::new();
        }
        let max_width = self.sessions.values().map(|n| n.len()).max().unwrap_or(0);
        let name = self
            .sessions
            .get(session_id)
            .map(|s| s.as_str())
            .unwrap_or("?");
        format!("[{:width$}]  ", name, width = max_width)
    }
}

/// Print a line with an optional session tag prefix.
fn print_tagged(tag: &str, line: &str) {
    if tag.is_empty() {
        println!("{}", line);
    } else {
        println!("{}{}", tag.dimmed(), line);
    }
}

fn parse_mcp_tool_name(tool_name: &str) -> Option<(&str, &str)> {
    let rest = tool_name.trim().strip_prefix("mcp__")?;
    let (server, tool) = rest.split_once("__")?;
    if server.is_empty() || tool.is_empty() {
        None
    } else {
        Some((server, tool))
    }
}

fn render_event(
    event: &WatchEvent,
    no_cache: bool,
    no_signals: bool,
    session_filter: &Option<String>,
    active: &mut ActiveSessions,
) {
    // Apply session filter to all events (except Lagged which has no session).
    if let Some(filter) = session_filter {
        if let Some(sid) = event_session_id(event) {
            if sid != filter {
                return;
            }
        }
    }

    // Update active session tracking BEFORE rendering.
    match event {
        WatchEvent::SessionStart {
            session_id,
            display_name,
            ..
        } => {
            active.add(session_id, display_name);
        }
        WatchEvent::SessionEnd { .. } => {
            // Remove AFTER rendering — so the tag is still available for the SessionEnd line.
        }
        _ => {}
    }

    // Compute tag for this event's session.
    let tag = event_session_id(event)
        .map(|sid| active.tag_for(sid))
        .unwrap_or_default();

    match event {
        WatchEvent::SessionStart {
            session_id: _,
            display_name,
            model,
            initial_prompt,
        } => {
            let time = now_hms();
            let header_inner = format!(
                "  {}  \u{00b7}  {}  \u{00b7}  {}  ",
                display_name, model, time
            );
            // Second line carries the user's prompt, if we captured one.
            let prompt_inner = initial_prompt
                .as_ref()
                .map(|p| format!("  \u{2192} {}  ", truncate_for_box(p, 90)));
            let width = header_inner
                .len()
                .max(prompt_inner.as_ref().map(|p| p.len()).unwrap_or(0))
                .max(57);
            println!();
            print_tagged(
                &tag,
                &format!("\u{250c}{}\u{2510}", "\u{2500}".repeat(width))
                    .cyan()
                    .to_string(),
            );
            print_tagged(
                &tag,
                &format!("\u{2502}{:width$}\u{2502}", header_inner, width = width)
                    .cyan()
                    .to_string(),
            );
            if let Some(line) = prompt_inner {
                print_tagged(
                    &tag,
                    &format!("\u{2502}{:width$}\u{2502}", line, width = width)
                        .cyan()
                        .dimmed()
                        .to_string(),
                );
            }
            print_tagged(
                &tag,
                &format!("\u{2514}{}\u{2518}", "\u{2500}".repeat(width))
                    .cyan()
                    .to_string(),
            );
        }

        WatchEvent::SessionEnd {
            session_id,
            outcome,
            total_tokens,
            total_turns,
        } => {
            let bar = "\u{2501}".repeat(58);
            print_tagged(&tag, &bar.dimmed().to_string());
            let outcome_colored = if outcome.contains("Completed") && !outcome.contains("Partially")
            {
                format!("{} \u{2713}", outcome).green().to_string()
            } else if outcome.contains("Partially Completed") {
                outcome.to_string().yellow().to_string()
            } else if outcome.contains("Abandoned") {
                outcome.to_string().dimmed().to_string()
            } else {
                outcome.to_string().yellow().to_string()
            };
            let tokens_display = format_tokens(*total_tokens);
            print_tagged(
                &tag,
                &format!(
                    "SESSION COMPLETE \u{00b7} {} tokens \u{00b7} {} turns \u{00b7} {}",
                    tokens_display, total_turns, outcome_colored
                ),
            );
            print_tagged(&tag, &bar.dimmed().to_string());

            // Remove after rendering.
            active.remove(session_id);
        }

        WatchEvent::ToolUse {
            session_id: _,
            timestamp,
            tool_name,
            summary,
        } => {
            let time = local_time_from_iso(timestamp);
            if let Some((server, tool)) = parse_mcp_tool_name(tool_name) {
                let summary_display = if summary.len() > 80 {
                    format!("{}...", &summary[..77])
                } else {
                    summary.clone()
                };
                let line = if summary_display.is_empty() {
                    format!("{}  MCP     {}.{}", time, server, tool)
                } else {
                    format!("{}  MCP     {}.{}  {}", time, server, tool, summary_display)
                };
                print_tagged(&tag, &line.cyan().to_string());
                return;
            }
            let label = format!("{:<6}", tool_name.to_uppercase());
            let summary_display = if summary.len() > 80 {
                format!("{}...", &summary[..77])
            } else {
                summary.clone()
            };

            let line = format!("{}  {}  {}", time, label, summary_display);
            let colored_line = match tool_name.as_str() {
                "Read" => line.cyan().to_string(),
                "Edit" => line.yellow().to_string(),
                "Write" => line.yellow().bold().to_string(),
                "Bash" | "bash" => line.white().to_string(),
                "Glob" | "Grep" => line.dimmed().to_string(),
                _ => line.white().to_string(),
            };
            print_tagged(&tag, &colored_line);
        }

        WatchEvent::SkillEvent {
            session_id: _,
            timestamp,
            skill_name,
            event_type,
            source,
            confidence,
            detail,
        } => {
            let time = local_time_from_iso(timestamp);
            let detail_suffix = detail
                .as_ref()
                .filter(|value| !value.is_empty())
                .map(|value| format!("  {}", truncate_for_box(value, 80)))
                .unwrap_or_default();
            let line = format!(
                "{}  SKILL   {} {} [{} {:.0}%]{}",
                time,
                skill_name,
                event_type,
                source,
                confidence * 100.0,
                detail_suffix
            );
            let colored = match event_type.as_str() {
                "fired" => line.green().to_string(),
                "expected" => line.dimmed().to_string(),
                "missed" | "misfired" => line.yellow().bold().to_string(),
                "failed" => line.red().bold().to_string(),
                _ => line.white().to_string(),
            };
            print_tagged(&tag, &colored);
        }

        WatchEvent::McpEvent {
            session_id: _,
            timestamp,
            server,
            tool,
            event_type,
            source,
            detail,
        } => {
            let time = local_time_from_iso(timestamp);
            let detail_suffix = detail
                .as_ref()
                .filter(|value| !value.is_empty())
                .map(|value| format!("  {}", truncate_for_box(value, 80)))
                .unwrap_or_default();
            let line = format!(
                "{}  MCP     {}.{} {} [{}]{}",
                time, server, tool, event_type, source, detail_suffix
            );
            let colored = match event_type.as_str() {
                "succeeded" => line.green().dimmed().to_string(),
                "failed" | "denied" => line.red().bold().to_string(),
                "called" => line.cyan().to_string(),
                _ => line.white().to_string(),
            };
            print_tagged(&tag, &colored);
        }

        WatchEvent::ToolResult {
            session_id: _,
            tool_name,
            outcome,
            duration_ms,
        } => {
            let prefix = " ".repeat(14);
            let duration_suffix = if *duration_ms > 0 {
                format!("  [{}ms]", duration_ms)
            } else {
                String::new()
            };
            let tool_label = if tool_name.is_empty() {
                outcome.to_string()
            } else {
                format!("{} · {}", tool_name, outcome)
            };
            if outcome == "success" || outcome == "unknown" {
                print_tagged(
                    &tag,
                    &format!("{}  \u{2713} {}{}", prefix, tool_label, duration_suffix)
                        .green()
                        .to_string(),
                );
            } else {
                print_tagged(
                    &tag,
                    &format!("{}  \u{2717} {}{}", prefix, tool_label, duration_suffix)
                        .red()
                        .to_string(),
                );
            }
        }

        WatchEvent::CacheEvent {
            session_id: _,
            event_type,
            cache_expires_at_epoch,
            estimated_rebuild_cost_dollars,
        } => {
            if no_cache {
                return;
            }
            let time = now_hms();
            let base = match event_type.as_str() {
                "hit" => format!("{}  CACHE   \u{25cf} hit", time)
                    .green()
                    .dimmed()
                    .to_string(),
                "partial" => format!("{}  CACHE   \u{25cf} partial", time)
                    .green()
                    .dimmed()
                    .to_string(),
                "cold_start" => format!("{}  CACHE   \u{25cf} cold start", time)
                    .cyan()
                    .dimmed()
                    .to_string(),
                "miss_ttl" => format!("{}  CACHE   \u{25cb} miss (TTL)", time)
                    .yellow()
                    .to_string(),
                "miss_thrash" => format!("{}  CACHE   \u{25cb} miss (thrash)", time)
                    .red()
                    .dimmed()
                    .to_string(),
                other => format!("{}  CACHE   {}", time, other).dimmed().to_string(),
            };
            // Append "expires in Nm · est. rebuild $X.XX" when we have it.
            let mut line = base;
            if let Some(exp) = cache_expires_at_epoch {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let remaining = exp.saturating_sub(now);
                let mins = remaining / 60;
                let secs = remaining % 60;
                let suffix = if let Some(c) = estimated_rebuild_cost_dollars {
                    format!(
                        "  \u{00b7} expires in {}m{:02}s \u{00b7} est. rebuild ${:.2}",
                        mins, secs, c
                    )
                } else {
                    format!("  \u{00b7} expires in {}m{:02}s", mins, secs)
                };
                line.push_str(&suffix.dimmed().to_string());
            }
            print_tagged(&tag, &line);
        }

        WatchEvent::ModelFallback {
            session_id: _,
            requested,
            actual,
        } => {
            let time = now_hms();
            print_tagged(
                &tag,
                &format!(
                    "{}  \u{26a0}  MODEL FALLBACK  requested {}, got {}",
                    time, requested, actual
                )
                .yellow()
                .bold()
                .to_string(),
            );
        }

        WatchEvent::ContextStatus {
            session_id: _,
            fill_percent,
            context_window_tokens: _,
            turns_to_compact,
        } => {
            // Only show context status when it actually matters — avoid
            // noise every turn when we're nowhere near compaction.
            if *fill_percent < 60.0 {
                return;
            }
            let time = now_hms();
            let label = match turns_to_compact {
                Some(n) if *n == 0 => "AT COMPACTION THRESHOLD".to_string(),
                Some(n) => format!("~{} turns to auto-compact", n),
                None => "trajectory unknown".to_string(),
            };
            let line = format!(
                "{}  CONTEXT  {:.0}% full \u{00b7} {}",
                time, fill_percent, label
            );
            let colored = if *fill_percent >= 80.0 {
                line.red().bold().to_string()
            } else {
                line.yellow().to_string()
            };
            print_tagged(&tag, &colored);
        }

        WatchEvent::RateLimitStatus {
            seconds_to_reset,
            requests_remaining: _,
            requests_limit: _,
            input_tokens_remaining,
            output_tokens_remaining,
            tokens_used_this_week,
            tokens_limit,
            tokens_remaining,
            budget_source,
            projected_exhaustion_secs,
        } => {
            // Inline mode stays mostly quiet, but auto-calibrated caps should
            // still surface so users can see the suggestion when no env is set.
            let primary =
                tokens_remaining.or(match (*input_tokens_remaining, *output_tokens_remaining) {
                    (Some(input), Some(output)) => Some(input.min(output)),
                    (Some(input), None) => Some(input),
                    (None, Some(output)) => Some(output),
                    (None, None) => None,
                });
            let source_is_auto = budget_source.as_deref() == Some("auto_p95_4w");
            let used_and_limit = tokens_used_this_week.zip(*tokens_limit);
            let Some(remaining) = primary else {
                return;
            };
            let time = now_hms();
            let reset_str = seconds_to_reset
                .map(|s| format!("resets in {}", format_duration_coarse(s)))
                .unwrap_or_else(|| "reset time unknown".to_string());
            let exhaust_str = projected_exhaustion_secs.map(|s| {
                format!(
                    " · will exhaust in ~{} at current rate",
                    format_duration_coarse(s)
                )
            });
            let alarm = projected_exhaustion_secs
                .zip(*seconds_to_reset)
                .map(|(ex, rst)| ex < rst)
                .unwrap_or(false);
            if !alarm && remaining > 1000 && !source_is_auto {
                return;
            }
            let mut line = if let Some((used, limit)) = used_and_limit {
                format!(
                    "{}  QUOTA   {} / {} tokens · {}",
                    time,
                    format_tokens(used),
                    format_tokens(limit),
                    reset_str
                )
            } else {
                match (*input_tokens_remaining, *output_tokens_remaining) {
                    (Some(input), Some(output)) => format!(
                        "{}  QUOTA   input/output left {}/{} · {}",
                        time,
                        format_tokens(input),
                        format_tokens(output),
                        reset_str
                    ),
                    (Some(input), None) => format!(
                        "{}  QUOTA   input left {} · {}",
                        time,
                        format_tokens(input),
                        reset_str
                    ),
                    (None, Some(output)) => format!(
                        "{}  QUOTA   output left {} · {}",
                        time,
                        format_tokens(output),
                        reset_str
                    ),
                    (None, None) => format!(
                        "{}  QUOTA   {} tokens left · {}",
                        time,
                        format_tokens(remaining),
                        reset_str
                    ),
                }
            };
            if source_is_auto {
                line.push_str(" · auto cap from last 4 weeks");
            }
            if let Some(e) = exhaust_str {
                line.push_str(&e);
            }
            let colored = if alarm {
                line.red().bold().to_string()
            } else {
                line.yellow().to_string()
            };
            print_tagged(&tag, &colored);
        }

        WatchEvent::FrustrationSignal {
            session_id: _,
            signal_type,
        } => {
            if no_signals {
                return;
            }
            let time = now_hms();
            print_tagged(
                &tag,
                &format!(
                    "{}  \u{26a0} SIGNAL  {} pattern detected",
                    time, signal_type
                )
                .yellow()
                .to_string(),
            );
        }

        WatchEvent::CompactionLoop {
            session_id: _,
            consecutive,
            wasted_tokens,
        } => {
            let tokens_display = format_tokens(*wasted_tokens);
            let inner1 = format!(
                "  \u{1f504} POSSIBLE LOOP \u{00b7} {} rapid turns \u{00b7} ~{} tokens wasted?  ",
                consecutive, tokens_display
            );
            let inner2 = "  If Claude seems stuck: Ctrl+C and restart";
            let width = inner1.len().max(inner2.len()).max(57);
            print_tagged(
                &tag,
                &format!("\u{250c}{}\u{2510}", "\u{2500}".repeat(width))
                    .yellow()
                    .to_string(),
            );
            print_tagged(
                &tag,
                &format!("\u{2502}{:width$}\u{2502}", inner1, width = width)
                    .yellow()
                    .to_string(),
            );
            print_tagged(
                &tag,
                &format!("\u{2502}{:width$}\u{2502}", inner2, width = width)
                    .yellow()
                    .to_string(),
            );
            print_tagged(
                &tag,
                &format!("\u{2514}{}\u{2518}", "\u{2500}".repeat(width))
                    .yellow()
                    .to_string(),
            );
        }

        WatchEvent::CacheWarning {
            session_id: _,
            idle_secs,
            ttl_secs,
        } => {
            if no_cache {
                return;
            }
            let time = now_hms();
            let idle_min = idle_secs / 60;
            let remaining_min = ttl_secs / 60;
            print_tagged(
                &tag,
                &format!(
                    "{}  \u{23f1} CACHE   idle {}m \u{2014} cache expires in ~{}m. Send a message to keep it warm.",
                    time, idle_min, remaining_min
                )
                .yellow()
                .to_string(),
            );
        }

        WatchEvent::Lagged { missed } => {
            println!(
                "{}",
                format!("[{} events missed \u{2014} channel overflowed]", missed).dimmed()
            );
        }

        WatchEvent::Diagnosis {
            session_id: _,
            report,
        } => {
            render_diagnosis(report, &tag);
        }
    }
}

fn cause_icon(cause_type: &str) -> &'static str {
    match cause_type {
        "cache_miss_ttl" | "cache_miss_thrash" => "\u{26a1}", // ⚡
        "context_bloat" => "\u{1f4e6}",                       // 📦
        "near_compaction" => "\u{23f3}",                      // ⏳
        "compaction_suspected" => "\u{1f504}",                // 🔄
        "model_fallback" => "\u{21c4}",                       // ⇄
        "tool_failure_streak" => "\u{1f527}",                 // 🔧
        "harness_pressure" => "\u{26a0}\u{fe0f}",             // ⚠️
        _ => "\u{2022}",                                      // •
    }
}

fn render_diagnosis(report: &DiagnosisReport, tag: &str) {
    if !report.degraded {
        return;
    }

    let bar = "\u{2501}".repeat(58);
    println!();
    print_tagged(tag, &bar.yellow().to_string());
    let tokens_display = format_tokens(report.total_tokens);
    print_tagged(
        tag,
        &format!(
            "SESSION COMPLETE \u{00b7} {} turns \u{00b7} {} tokens \u{00b7} cache {:.0}% \u{00b7} {}",
            report.total_turns,
            tokens_display,
            report.cache_hit_ratio * 100.0,
            report.outcome
        )
        .yellow()
        .to_string(),
    );
    print_tagged(tag, "");

    if let Some(turn) = report.degradation_turn {
        print_tagged(
            tag,
            &format!("Why it slowed down (from turn {}):", turn)
                .yellow()
                .to_string(),
        );
    } else {
        print_tagged(tag, &"Why it slowed down:".yellow().to_string());
    }

    for cause in &report.causes {
        let icon = cause_icon(&cause.cause_type);
        let heuristic_suffix = if cause.is_heuristic {
            format!("  {}", "(estimate)".dimmed())
        } else {
            String::new()
        };
        print_tagged(
            tag,
            &format!(
                "  {} turn {} \u{00b7} {}{}",
                icon, cause.turn_first_noticed, cause.detail, heuristic_suffix
            ),
        );
    }

    if !report.advice.is_empty() {
        print_tagged(tag, "");
        print_tagged(tag, &"Next time:".green().to_string());
        for a in &report.advice {
            print_tagged(tag, &format!("  {} {}", "\u{2192}".green(), a));
        }
    }

    print_tagged(tag, &bar.yellow().to_string());
    println!();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Watch {
            url,
            no_cache,
            no_signals,
            session,
            tmux,
            tmux_max_panes,
        } => {
            if tmux {
                // Tmux orchestrator mode. Self-bootstrap into a tmux session
                // if we're not already inside one, so the user just runs
                // `clauditor watch --tmux` once.
                if let Err(e) =
                    tmux::bootstrap_into_tmux(&url, no_cache, no_signals, tmux_max_panes)
                {
                    eprintln!("{}", e.red());
                    std::process::exit(1);
                }
                let orchestrator = match tmux::TmuxOrchestrator::new(
                    url.clone(),
                    no_cache,
                    no_signals,
                    tmux_max_panes,
                ) {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("{}", format!("tmux init failed: {}", e).red());
                        std::process::exit(1);
                    }
                };
                let watch_url = format!("{}/watch", url.trim_end_matches('/'));
                if let Err(e) = orchestrator.run(&watch_url).await {
                    eprintln!("{}", format!("Orchestrator error: {}", e).red());
                    std::process::exit(1);
                }
            } else {
                // Existing inline watch mode. When --session is set, pass it
                // as ?session=X so the server can inject a synthetic
                // SessionStart for mid-session joiners. Session ids are
                // server-generated and URL-safe by construction
                // (`session_<ts>_<hex>`), no escaping needed.
                let watch_url = match &session {
                    Some(sid) => format!("{}/watch?session={}", url.trim_end_matches('/'), sid),
                    None => format!("{}/watch", url.trim_end_matches('/')),
                };
                println!("Connecting to {}...", watch_url);
                let mut active = ActiveSessions::new();

                loop {
                    match connect_and_stream(
                        &watch_url,
                        no_cache,
                        no_signals,
                        &session,
                        &mut active,
                    )
                    .await
                    {
                        Ok(()) => {
                            eprintln!("{}", "Connection closed. Reconnecting in 3s...".dimmed());
                        }
                        Err(e) => {
                            eprintln!(
                                "{}",
                                format!("Waiting for clauditor-core... ({})", e).dimmed()
                            );
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
        Commands::Sessions { url, limit, days } => {
            let sessions_url = format!(
                "{}/api/sessions?limit={}&days={}",
                url.trim_end_matches('/'),
                limit,
                days
            );
            match fetch_sessions(&sessions_url).await {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("{}", format!("Error: {}", e).red());
                    std::process::exit(1);
                }
            }
        }
        Commands::Recall {
            url,
            limit,
            days,
            query,
        } => {
            let query = query.join(" ");
            let recall_url = format!("{}/api/recall", url.trim_end_matches('/'));
            match fetch_recall(&recall_url, &query, limit, days).await {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("{}", format!("Error: {}", e).red());
                    std::process::exit(1);
                }
            }
        }
        Commands::Reconcile {
            url,
            session,
            billed_cost,
            source,
            imported_at,
        } => {
            let reconcile_url =
                format!("{}/api/billing-reconciliations", url.trim_end_matches('/'));
            match post_reconciliation(
                &reconcile_url,
                &session,
                billed_cost,
                &source,
                imported_at.as_deref(),
            )
            .await
            {
                Ok(()) => {}
                Err(e) => {
                    eprintln!("{}", format!("Error: {}", e).red());
                    std::process::exit(1);
                }
            }
        }
    }
}

async fn fetch_sessions(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let resp = reqwest::Client::new().get(url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }
    let body: serde_json::Value = resp.json().await?;
    let cost_source = body
        .get("cost_source")
        .and_then(|s| s.as_str())
        .unwrap_or("builtin_model_family_pricing");
    let trusted_for_budget_enforcement = body
        .get("trusted_for_budget_enforcement")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let sessions = body.get("sessions").and_then(|s| s.as_array());
    let Some(sessions) = sessions else {
        println!("No sessions found.");
        return Ok(());
    };
    if sessions.is_empty() {
        println!("No sessions found.");
        return Ok(());
    }

    let cost_source_label = match cost_source {
        "builtin_model_family_pricing" => "built-in model-family pricing",
        other => other,
    };
    let trust_label = if trusted_for_budget_enforcement {
        "hard-stop dollar budgets enabled"
    } else {
        "dollar budgets advisory only"
    };
    println!(
        "{}",
        format!(
            "Estimated cost source: {} · {}",
            cost_source_label, trust_label
        )
        .dimmed()
    );

    // Header
    println!(
        "{:<22} {:<18} {:<8} {:<22} {:<16} {:<14} {:<8} CAUSE",
        "SESSION", "MODEL", "TURNS", "OUTCOME", "ESTIMATED COST", "BILLED COST", "CACHE%"
    );
    println!("{}", "-".repeat(122));

    for s in sessions {
        let sid = s.get("session_id").and_then(|v| v.as_str()).unwrap_or("?");
        let short_sid = if sid.len() > 20 { &sid[..20] } else { sid };
        let model = s.get("model").and_then(|v| v.as_str()).unwrap_or("?");
        let short_model = model.replace("claude-", "").replace("-20250514", "");
        let turns = s.get("total_turns").and_then(|v| v.as_i64()).unwrap_or(0);
        let outcome = s.get("outcome").and_then(|v| v.as_str()).unwrap_or("?");
        let cost = s
            .get("estimated_total_cost_dollars")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let billed_cost = s.get("billed_cost_dollars").and_then(|v| v.as_f64());
        let cache = s
            .get("cache_hit_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
            * 100.0;
        let cause = s
            .get("primary_cause")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let degraded = s.get("degraded").and_then(|v| v.as_bool()).unwrap_or(false);
        let billed_display = billed_cost
            .map(|value| format!("${value:.2}"))
            .unwrap_or_else(|| "-".to_string());

        let line = format!(
            "{:<22} {:<18} {:<8} {:<22} ${:<15.2} {:<14} {:<8.0}% {}",
            short_sid, short_model, turns, outcome, cost, billed_display, cache, cause
        );

        if degraded {
            println!("{}", line.yellow());
        } else if outcome.contains("Completed") && !outcome.contains("Partially") {
            println!("{}", line.green());
        } else if outcome.contains("Abandoned") {
            println!("{}", line.dimmed());
        } else {
            println!("{}", line);
        }
    }

    Ok(())
}

async fn post_reconciliation(
    url: &str,
    session: &str,
    billed_cost: f64,
    source: &str,
    imported_at: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = BillingReconciliationInput {
        session_id: session.to_string(),
        source: source.to_string(),
        billed_cost_dollars: billed_cost,
        imported_at: imported_at.map(|value| value.to_string()),
    };
    let resp = reqwest::Client::new()
        .post(url)
        .json(&payload)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }
    let body: serde_json::Value = resp.json().await?;
    let inserted = body.get("inserted").and_then(|v| v.as_u64()).unwrap_or(0);
    println!(
        "{}",
        format!(
            "Imported {} billed reconciliation{}.",
            inserted,
            if inserted == 1 { "" } else { "s" }
        )
        .green()
    );
    Ok(())
}

async fn fetch_recall(
    url: &str,
    query: &str,
    limit: u32,
    days: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = reqwest::Client::new()
        .get(url)
        .query(&[
            ("q", query.to_string()),
            ("limit", limit.to_string()),
            ("days", days.to_string()),
        ])
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }

    let body: serde_json::Value = resp.json().await?;
    let hits = body.get("hits").and_then(|h| h.as_array());
    let Some(hits) = hits else {
        println!("No matches.");
        return Ok(());
    };
    if hits.is_empty() {
        println!("No matches for \"{}\".", query);
        return Ok(());
    }

    println!("Recall results for \"{}\":", query);
    println!();

    for (idx, hit) in hits.iter().enumerate() {
        let score = hit.get("score").and_then(|v| v.as_i64()).unwrap_or(0);
        let session_id = hit
            .get("session_id")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let started_at = hit
            .get("started_at")
            .and_then(|v| v.as_str())
            .map(compact_datetime_from_iso)
            .unwrap_or_else(|| "unknown time".to_string());
        let model = hit
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .replace("claude-", "");
        let outcome = hit.get("outcome").and_then(|v| v.as_str()).unwrap_or("?");
        let initial_prompt = hit
            .get("initial_prompt")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let final_response_summary = hit
            .get("final_response_summary")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let rank_line = format!(
            "{}. [{}] {} · {} · {}",
            idx + 1,
            score,
            session_id,
            model,
            started_at
        );
        println!("{}", rank_line.bold());
        println!("    Outcome: {}", outcome);
        if !initial_prompt.is_empty() {
            println!("    Prompt: {}", initial_prompt);
        }
        if !final_response_summary.is_empty() {
            println!("    Landed: {}", final_response_summary);
        }
        println!();
    }

    Ok(())
}

async fn connect_and_stream(
    url: &str,
    no_cache: bool,
    no_signals: bool,
    session_filter: &Option<String>,
    active: &mut ActiveSessions,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = reqwest::Client::new()
        .get(url)
        .header("Accept", "text/event-stream")
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }

    eprintln!("{}", "Connected. Watching for events...".green());

    let mut stream = resp.bytes_stream();
    use futures_util::StreamExt;

    let mut line_buffer = String::new();
    let mut data_buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let text = String::from_utf8_lossy(&chunk);
        line_buffer.push_str(&text);

        while let Some(newline_pos) = line_buffer.find('\n') {
            let line = line_buffer[..newline_pos]
                .trim_end_matches('\r')
                .to_string();
            line_buffer = line_buffer[newline_pos + 1..].to_string();

            if let Some(data) = line.strip_prefix("data: ") {
                data_buffer.push_str(data);
            } else if line.starts_with(": ") || line.starts_with(':') {
                continue;
            } else if line.is_empty() && !data_buffer.is_empty() {
                if let Ok(event) = serde_json::from_str::<WatchEvent>(&data_buffer) {
                    render_event(&event, no_cache, no_signals, session_filter, active);
                }
                data_buffer.clear();
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{compact_datetime_from_iso, local_time_from_iso};
    use chrono::{DateTime, Local};

    #[test]
    fn local_time_from_iso_converts_from_rfc3339() {
        let iso = "2026-04-21T13:04:39Z";
        let expected = DateTime::parse_from_rfc3339(iso)
            .expect("valid RFC3339 timestamp")
            .with_timezone(&Local)
            .format("%H:%M:%S")
            .to_string();
        assert_eq!(local_time_from_iso(iso), expected);
    }

    #[test]
    fn compact_datetime_from_iso_converts_from_rfc3339() {
        let iso = "2026-04-21T13:04:39Z";
        let expected = DateTime::parse_from_rfc3339(iso)
            .expect("valid RFC3339 timestamp")
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M")
            .to_string();
        assert_eq!(compact_datetime_from_iso(iso), expected);
    }

    #[test]
    fn local_time_from_iso_falls_back_for_invalid_timestamps() {
        assert_eq!(local_time_from_iso("not-a-timestamp"), "??:??:??");
    }
}
