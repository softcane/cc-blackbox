use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Per-terminal session registry — keyed by hash of (working_directory,
// first_user_message_prefix). Working directory alone collides when multiple
// Claude sessions run in the same repo in parallel; mixing in the first user
// message (stable across turns within a session, different across fresh
// invocations) keeps parallel sessions distinct.
// ---------------------------------------------------------------------------
pub struct SessionState {
    pub session_id: String,
    pub display_name: String,
    /// Model string seen on the first request for this session.
    pub model: String,
    /// Cleaned excerpt of the user's first prompt. Populated when available;
    /// used to synthesize a SessionStart for subscribers joining mid-session.
    pub initial_prompt: Option<String>,
    pub created_at: Instant,
    pub last_activity: Instant,
    pub session_inserted: bool,
    pub cache_warning_sent: bool,
    pub idle_postmortem_sent: bool,
}

/// Key: hash of system prompt first 1KB. Value: per-terminal session state.
pub static SESSIONS: LazyLock<DashMap<u64, SessionState>> = LazyLock::new(DashMap::new);

// ---------------------------------------------------------------------------
// Per-session turn snapshot storage (in-memory)
// ---------------------------------------------------------------------------
pub static SESSION_TURNS: LazyLock<DashMap<String, Vec<TurnSnapshot>>> =
    LazyLock::new(DashMap::new);

/// Track the number of tool_result failures parsed from the latest request body.
/// Set during REQUEST_BODY phase, consumed during end_of_stream.
pub static PENDING_TOOL_FAILURES: LazyLock<DashMap<String, u32>> = LazyLock::new(DashMap::new);

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------
#[derive(Clone, Debug)]
pub struct TurnSnapshot {
    pub turn_number: u32,
    pub timestamp: Instant,
    pub input_tokens: u32,
    pub cache_read_tokens: u32,
    pub cache_creation_tokens: u32,
    pub output_tokens: u32,
    pub ttft_ms: u64,
    pub tool_calls: Vec<String>,
    pub tool_results_failed: u32,
    pub gap_from_prev_secs: f64,
    pub context_utilization: f64,
    pub context_window_tokens: u64,
    pub frustration_signals: u32,
    pub requested_model: Option<String>,
    pub actual_model: Option<String>,
    pub response_summary: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionLoopSignal {
    pub start_turn: u32,
    pub end_turn: u32,
    pub consecutive_turns: u32,
    pub wasted_tokens: u64,
}

#[derive(Clone, Debug, Serialize)]
pub enum TaskOutcome {
    Completed,
    PartiallyCompleted,
    Abandoned,
    CompactionSuspected,
    BudgetExceeded,
}

impl std::fmt::Display for TaskOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => write!(f, "Likely Completed"),
            Self::PartiallyCompleted => write!(f, "Likely Partially Completed"),
            Self::Abandoned => write!(f, "Likely Abandoned"),
            Self::CompactionSuspected => write!(f, "Compaction Suspected"),
            Self::BudgetExceeded => write!(f, "Budget Exceeded"),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DegradationCause {
    pub turn_first_noticed: u32,
    pub cause_type: String,
    pub detail: String,
    pub estimated_cost: f64,
    pub is_heuristic: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_model: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiagnosisReport {
    pub session_id: String,
    pub outcome: String,
    pub total_turns: u32,
    pub total_tokens: u64,
    pub estimated_total_cost_dollars: f64,
    pub cost_source: String,
    pub trusted_for_budget_enforcement: bool,
    pub cache_hit_ratio: f64,
    pub degraded: bool,
    pub degradation_turn: Option<u32>,
    pub causes: Vec<DegradationCause>,
    pub advice: Vec<String>,
}

fn turn_total_tokens(turn: &TurnSnapshot) -> u64 {
    turn.input_tokens as u64
        + turn.cache_read_tokens as u64
        + turn.cache_creation_tokens as u64
        + turn.output_tokens as u64
}

fn is_compaction_pair(prev: &TurnSnapshot, current: &TurnSnapshot) -> bool {
    if current.gap_from_prev_secs >= 3.0 || current.context_utilization <= 0.75 {
        return false;
    }

    if prev.input_tokens == 0 {
        return false;
    }

    let ratio =
        (current.input_tokens as f64 - prev.input_tokens as f64).abs() / prev.input_tokens as f64;
    ratio < 0.10
}

/// Detect a compaction-loop signature ending at `end_idx`.
///
/// This mirrors the heuristic used by session diagnosis: rapid-fire turns,
/// high context utilization, and stable input-token counts. The returned
/// `wasted_tokens` is an estimate of the repeated turns after the initial
/// "baseline" turn that kicked off the suspected loop.
pub fn compaction_loop_signal_ending_at(
    turns: &[TurnSnapshot],
    end_idx: usize,
) -> Option<CompactionLoopSignal> {
    if end_idx == 0 || end_idx >= turns.len() {
        return None;
    }

    let mut start_idx = end_idx;
    let mut qualifying_pairs = 0u32;
    while start_idx > 0 && is_compaction_pair(&turns[start_idx - 1], &turns[start_idx]) {
        start_idx -= 1;
        qualifying_pairs += 1;
    }

    if qualifying_pairs < 2 {
        return None;
    }

    let start_turn = turns[start_idx].turn_number;
    let end_turn = turns[end_idx].turn_number;
    let consecutive_turns = end_turn.saturating_sub(start_turn) + 1;
    let wasted_tokens = turns[start_idx + 1..=end_idx]
        .iter()
        .map(turn_total_tokens)
        .sum();

    Some(CompactionLoopSignal {
        start_turn,
        end_turn,
        consecutive_turns,
        wasted_tokens,
    })
}

// ---------------------------------------------------------------------------
// Degradation analyzer
// ---------------------------------------------------------------------------
pub fn analyze_session(session_id: &str, turns: &[TurnSnapshot]) -> DiagnosisReport {
    let total_turns = turns.len() as u32;
    let mut total_cost = 0.0;
    let mut cost_sources = HashSet::new();
    let mut trusted_for_budget_enforcement = true;
    for turn in turns {
        let model = turn
            .actual_model
            .as_deref()
            .or(turn.requested_model.as_deref())
            .unwrap_or("sonnet");
        let breakdown = crate::pricing::estimate_cost_dollars(
            model,
            turn.input_tokens as u64,
            turn.output_tokens as u64,
            turn.cache_read_tokens as u64,
            turn.cache_creation_tokens as u64,
        );
        total_cost += breakdown.total_cost_dollars;
        cost_sources.insert(breakdown.cost_source);
        trusted_for_budget_enforcement &= breakdown.trusted_for_budget_enforcement;
    }
    let cost_source = crate::pricing::summarize_cost_sources(&cost_sources);

    let total_tokens: u64 = turns
        .iter()
        .map(|t| {
            t.input_tokens as u64
                + t.output_tokens as u64
                + t.cache_read_tokens as u64
                + t.cache_creation_tokens as u64
        })
        .sum();

    let total_cache_read: u64 = turns.iter().map(|t| t.cache_read_tokens as u64).sum();
    let total_cache_create: u64 = turns.iter().map(|t| t.cache_creation_tokens as u64).sum();
    let cache_hit_ratio = if total_cache_read + total_cache_create > 0 {
        total_cache_read as f64 / (total_cache_read + total_cache_create) as f64
    } else {
        0.0
    };

    // Short sessions: skip analysis.
    if total_turns < 3 {
        let outcome = determine_outcome(turns);
        return DiagnosisReport {
            session_id: session_id.to_string(),
            outcome: outcome.to_string(),
            total_turns,
            total_tokens,
            estimated_total_cost_dollars: total_cost,
            cost_source: cost_source.clone(),
            trusted_for_budget_enforcement,
            cache_hit_ratio,
            degraded: false,
            degradation_turn: None,
            causes: vec![],
            advice: vec![],
        };
    }

    let outcome = determine_outcome(turns);
    let mut causes = Vec::new();

    // Turn-duration baseline: average of first 3 turns.
    let ttft_baseline = turns.iter().take(3).map(|t| t.ttft_ms as f64).sum::<f64>() / 3.0;

    let mut degradation_turn: Option<u32> = None;
    let mut set_degradation = |turn: u32| {
        if degradation_turn.is_none() || turn < degradation_turn.unwrap() {
            degradation_turn = Some(turn);
        }
    };

    // Check each turn for degradation signals.
    for (i, turn) in turns.iter().enumerate() {
        // CAUSE: cache_miss_ttl
        if turn.cache_creation_tokens > 0
            && turn.cache_read_tokens == 0
            && turn.gap_from_prev_secs > 300.0
        {
            let prev_ttft = if i > 0 { turns[i - 1].ttft_ms } else { 0 };
            let ttl_model = turn
                .actual_model
                .as_deref()
                .or(turn.requested_model.as_deref())
                .unwrap_or("sonnet");
            causes.push(DegradationCause {
                turn_first_noticed: turn.turn_number,
                cause_type: "cache_miss_ttl".to_string(),
                detail: format!(
                    "Cache expired at turn {} \u{2014} {:.0}min gap between turns. Turn duration jumped from {}ms to {}ms.",
                    turn.turn_number, turn.gap_from_prev_secs / 60.0, prev_ttft, turn.ttft_ms
                ),
                estimated_cost: crate::pricing::estimate_cache_rebuild_waste_dollars(
                    ttl_model,
                    turn.cache_creation_tokens as u64,
                )
                .total_cost_dollars,
                is_heuristic: false,
                requested_model: None,
                actual_model: None,
            });
            set_degradation(turn.turn_number);
        }

        // CAUSE: cache_miss_thrash — tracked via counter, fires at 3+ consecutive misses.
        // (single context changes between turns are normal, not thrash)

        // CAUSE: context_bloat
        if turn.context_utilization > 0.60 {
            // Only add once — for the first turn that crosses 60%.
            if !causes.iter().any(|c| c.cause_type == "context_bloat") {
                causes.push(DegradationCause {
                    turn_first_noticed: turn.turn_number,
                    cause_type: "context_bloat".to_string(),
                    detail: format!(
                        "Context hit ~{:.0}% full by turn {}. Large file reads likely \
                         dominated \u{2014} reading whole files fills context faster than conversation.",
                        turn.context_utilization * 100.0, turn.turn_number
                    ),
                    estimated_cost: 0.0,
                    is_heuristic: true,
                    requested_model: None,
                    actual_model: None,
                });
                set_degradation(turn.turn_number);
            }
        }

        // CAUSE: model_fallback
        if !causes.iter().any(|c| c.cause_type == "model_fallback") {
            if let (Some(requested), Some(actual)) =
                (turn.requested_model.as_ref(), turn.actual_model.as_ref())
            {
                if !crate::model_matches(requested, actual) {
                    causes.push(DegradationCause {
                        turn_first_noticed: turn.turn_number,
                        cause_type: "model_fallback".to_string(),
                        detail: format!(
                            "Anthropic routed this from {} to {} at turn {}. \
                             This usually means the requested tier was unavailable.",
                            requested, actual, turn.turn_number
                        ),
                        estimated_cost: 0.0,
                        is_heuristic: false,
                        requested_model: Some(requested.clone()),
                        actual_model: Some(actual.clone()),
                    });
                    set_degradation(turn.turn_number);
                }
            }
        }

        // CAUSE: near_compaction
        if i > 0 && !causes.iter().any(|c| c.cause_type == "near_compaction") {
            let fill_percent = turn.context_utilization * 100.0;
            let prev_fill_percent = turns[i - 1].context_utilization * 100.0;
            let turns_to_compact =
                crate::project_turns_until_compaction(prev_fill_percent, fill_percent);
            if fill_percent >= 80.0 || matches!(turns_to_compact, Some(n) if n <= 2) {
                let runway = match turns_to_compact {
                    Some(0) => "auto-compaction threshold reached".to_string(),
                    Some(1) => "~1 turn from auto-compaction".to_string(),
                    Some(n) => format!("~{} turns from auto-compaction", n),
                    None => "trajectory to auto-compaction was steep".to_string(),
                };
                causes.push(DegradationCause {
                    turn_first_noticed: turn.turn_number,
                    cause_type: "near_compaction".to_string(),
                    detail: format!(
                        "Context reached ~{:.0}% full by turn {} and was {}.",
                        fill_percent, turn.turn_number, runway
                    ),
                    estimated_cost: 0.0,
                    is_heuristic: true,
                    requested_model: None,
                    actual_model: None,
                });
                set_degradation(turn.turn_number);
            }
        }

        // CAUSE: TTFT regression (> 2x baseline)
        if ttft_baseline > 0.0
            && turn.ttft_ms as f64 > ttft_baseline * 2.0
            && i >= 3
            && !causes.iter().any(|c| c.cause_type == "ttft_regression")
        {
            set_degradation(turn.turn_number);
        }

        // CAUSE: frustration signals
        if turn.frustration_signals > 0
            && !causes.iter().any(|c| c.cause_type == "harness_pressure")
        {
            let total_signals: u32 = turns.iter().map(|t| t.frustration_signals).sum();
            if total_signals >= 2 {
                causes.push(DegradationCause {
                    turn_first_noticed: turn.turn_number,
                    cause_type: "harness_pressure".to_string(),
                    detail: format!(
                        "{} early-stop or token-pressure signals detected in Claude's output. \
                         This suggests the harness was constraining response quality.",
                        total_signals
                    ),
                    estimated_cost: 0.0,
                    is_heuristic: true,
                    requested_model: None,
                    actual_model: None,
                });
                set_degradation(turn.turn_number);
            }
        }
    }

    // CAUSE: cache_miss_thrash — 3+ consecutive full cache misses within the TTL.
    // A single miss between turns is a normal context change, not thrash.
    {
        let mut consecutive_misses = 0u32;
        let mut thrash_start = 0u32;
        for turn in turns.iter() {
            let is_full_miss = turn.turn_number > 1
                && turn.cache_creation_tokens > 0
                && turn.cache_read_tokens == 0
                && turn.gap_from_prev_secs <= 300.0;
            if is_full_miss {
                if consecutive_misses == 0 {
                    thrash_start = turn.turn_number;
                }
                consecutive_misses += 1;
                if consecutive_misses >= 3
                    && !causes.iter().any(|c| c.cause_type == "cache_miss_thrash")
                {
                    let wasted: u64 = turns
                        .iter()
                        .filter(|t| {
                            t.turn_number >= thrash_start && t.turn_number <= turn.turn_number
                        })
                        .map(|t| t.cache_creation_tokens as u64)
                        .sum();
                    let estimated_cost = turns
                        .iter()
                        .filter(|t| {
                            t.turn_number >= thrash_start && t.turn_number <= turn.turn_number
                        })
                        .map(|t| {
                            let thrash_model = t
                                .actual_model
                                .as_deref()
                                .or(t.requested_model.as_deref())
                                .unwrap_or("sonnet");
                            crate::pricing::estimate_cache_rebuild_waste_dollars(
                                thrash_model,
                                t.cache_creation_tokens as u64,
                            )
                            .total_cost_dollars
                        })
                        .sum();
                    causes.push(DegradationCause {
                        turn_first_noticed: thrash_start,
                        cause_type: "cache_miss_thrash".to_string(),
                        detail: format!(
                            "{} consecutive cache rebuilds (turns {}\u{2013}{}) within TTL. \
                             ~{}K tokens wasted. Check if CLAUDE.md, hooks, or MCP config changed.",
                            consecutive_misses,
                            thrash_start,
                            turn.turn_number,
                            wasted / 1000
                        ),
                        estimated_cost,
                        is_heuristic: false,
                        requested_model: None,
                        actual_model: None,
                    });
                    set_degradation(thrash_start);
                }
            } else {
                consecutive_misses = 0;
            }
        }
    }

    // CAUSE: tool_failure_streak — 5+ consecutive turns where >50% of tool calls
    // failed, after turn 6. Threshold is high because Claude Code routinely gets
    // is_error:true from exploratory commands (missing files, failed builds, etc.)
    // that are normal workflow, not degradation.
    let mut consecutive_failures = 0u32;
    let mut streak_start = 0u32;
    for turn in turns.iter() {
        let total_tools = turn.tool_calls.len() as u32;
        let failure_ratio = if total_tools > 0 {
            turn.tool_results_failed as f64 / total_tools as f64
        } else {
            0.0
        };
        if turn.turn_number >= 6 && turn.tool_results_failed > 0 && failure_ratio > 0.5 {
            if consecutive_failures == 0 {
                streak_start = turn.turn_number;
            }
            consecutive_failures += 1;
            if consecutive_failures >= 5
                && !causes.iter().any(|c| c.cause_type == "tool_failure_streak")
            {
                causes.push(DegradationCause {
                    turn_first_noticed: streak_start,
                    cause_type: "tool_failure_streak".to_string(),
                    detail: format!(
                        "Tool calls failed in {} consecutive turns ({}\u{2013}{}). \
                         Claude may have been retrying a broken approach.",
                        consecutive_failures, streak_start, turn.turn_number
                    ),
                    estimated_cost: 0.0,
                    is_heuristic: false,
                    requested_model: None,
                    actual_model: None,
                });
                set_degradation(streak_start);
            }
        } else {
            consecutive_failures = 0;
        }
    }

    // CAUSE: compaction_suspected — rapid-fire turns with stable token counts
    // and high utilization.
    for end_idx in 1..turns.len() {
        let Some(signal) = compaction_loop_signal_ending_at(turns, end_idx) else {
            continue;
        };
        if !causes
            .iter()
            .any(|c| c.cause_type == "compaction_suspected")
        {
            causes.push(DegradationCause {
                turn_first_noticed: signal.start_turn,
                cause_type: "compaction_suspected".to_string(),
                detail: format!(
                    "Rapid-fire requests with stable token counts detected at turns \
                     {}\u{2013}{}. This pattern may indicate a compaction loop (heuristic \
                     \u{2014} not directly observable).",
                    signal.start_turn, signal.end_turn
                ),
                estimated_cost: 0.0,
                is_heuristic: true,
                requested_model: None,
                actual_model: None,
            });
            set_degradation(signal.start_turn);
        }
    }

    let degraded = causes.iter().any(|cause| !cause.is_heuristic)
        || matches!(outcome, TaskOutcome::CompactionSuspected);

    // Generate advice — deduplicated, max 3, prioritized.
    let priority = [
        "model_fallback",
        "compaction_suspected",
        "near_compaction",
        "cache_miss_ttl",
        "cache_miss_thrash",
        "context_bloat",
        "tool_failure_streak",
        "harness_pressure",
    ];
    let mut advice: Vec<String> = Vec::new();
    for cause_type in &priority {
        if advice.len() >= 3 {
            break;
        }
        if let Some(cause) = causes.iter().find(|c| c.cause_type == *cause_type) {
            let a = advice_for_cause(cause);
            if !a.is_empty() && !advice.contains(&a) {
                advice.push(a);
            }
        }
    }

    let degradation_turn = if degraded { degradation_turn } else { None };

    DiagnosisReport {
        session_id: session_id.to_string(),
        outcome: outcome.to_string(),
        total_turns,
        total_tokens,
        estimated_total_cost_dollars: total_cost,
        cost_source,
        trusted_for_budget_enforcement,
        cache_hit_ratio,
        degraded,
        degradation_turn,
        causes,
        advice,
    }
}

fn determine_outcome(turns: &[TurnSnapshot]) -> TaskOutcome {
    if turns.len() < 3 {
        return TaskOutcome::Abandoned;
    }

    let has_write = turns
        .iter()
        .any(|t| t.tool_calls.iter().any(|tc| tc == "Write" || tc == "Edit"));

    if !has_write {
        return TaskOutcome::Abandoned;
    }

    // Find last write/edit turn index.
    let last_write_idx = turns
        .iter()
        .rposition(|t| t.tool_calls.iter().any(|tc| tc == "Write" || tc == "Edit"))
        .unwrap();

    // Check if any subsequent turn has no tool failures.
    let has_passing_follow_up = turns[last_write_idx + 1..]
        .iter()
        .any(|t| t.tool_results_failed == 0 && !t.tool_calls.is_empty());

    if has_passing_follow_up {
        TaskOutcome::Completed
    } else {
        TaskOutcome::PartiallyCompleted
    }
}

fn advice_for_cause(cause: &DegradationCause) -> String {
    match cause.cause_type.as_str() {
        "cache_miss_ttl" => "Cache expired from idle gap > 5 min. Send a message before the TTL to keep it warm.".to_string(),
        "cache_miss_thrash" => "Full cache rebuilds within 5 min. Check if CLAUDE.md, hooks, or MCP config changed mid-session.".to_string(),
        "context_bloat" => "Point Claude at specific functions, not whole files.".to_string(),
        "model_fallback" => {
            let actual = cause
                .actual_model
                .as_deref()
                .map(friendly_model_name)
                .unwrap_or_else(|| "a fallback model".to_string());
            format!(
                "Anthropic routed this to {} at turn {}. Check quota on console.anthropic.com and retry when your preferred tier resets.",
                actual,
                cause.turn_first_noticed
            )
        }
        "near_compaction" => "Context was close to auto-compaction. Start a fresh session or narrow the next turn to specific files or functions.".to_string(),
        "compaction_suspected" => "Kill and restart if Claude seems stuck. Ctrl+C, then start fresh.".to_string(),
        "tool_failure_streak" => "If tools fail 5 turns in a row, interrupt and redirect.".to_string(),
        "harness_pressure" => "Shorter, more focused tasks perform better than long open-ended ones.".to_string(),
        _ => String::new(),
    }
}

fn friendly_model_name(model: &str) -> String {
    let lower = model.to_ascii_lowercase();
    if lower.contains("opus") {
        "Opus".to_string()
    } else if lower.contains("sonnet") {
        "Sonnet".to_string()
    } else if lower.contains("haiku") {
        "Haiku".to_string()
    } else {
        model.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::{analyze_session, compaction_loop_signal_ending_at, TurnSnapshot};

    fn snapshot(
        turn_number: u32,
        input_tokens: u32,
        output_tokens: u32,
        gap_from_prev_secs: f64,
        context_utilization: f64,
    ) -> TurnSnapshot {
        TurnSnapshot {
            turn_number,
            timestamp: Instant::now(),
            input_tokens,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            output_tokens,
            ttft_ms: 1000,
            tool_calls: vec![],
            tool_results_failed: 0,
            gap_from_prev_secs,
            context_utilization,
            context_window_tokens: 200_000,
            frustration_signals: 0,
            requested_model: None,
            actual_model: None,
            response_summary: None,
        }
    }

    #[test]
    fn compaction_loop_signal_requires_three_rapid_similar_turns() {
        let turns = vec![
            snapshot(1, 160_000, 2_000, 0.0, 0.80),
            snapshot(2, 161_000, 1_800, 1.2, 0.81),
            snapshot(3, 159_500, 1_900, 1.4, 0.82),
        ];

        let signal = compaction_loop_signal_ending_at(&turns, 2).expect("loop signal");
        assert_eq!(signal.start_turn, 1);
        assert_eq!(signal.end_turn, 3);
        assert_eq!(signal.consecutive_turns, 3);
        assert_eq!(signal.wasted_tokens, 161_000 + 1_800 + 159_500 + 1_900);
    }

    #[test]
    fn compaction_loop_signal_does_not_fire_on_large_token_shift() {
        let turns = vec![
            snapshot(1, 160_000, 2_000, 0.0, 0.80),
            snapshot(2, 195_000, 1_800, 1.2, 0.81),
            snapshot(3, 196_000, 1_900, 1.4, 0.82),
        ];

        assert!(compaction_loop_signal_ending_at(&turns, 2).is_none());
    }

    #[test]
    fn heuristic_only_sessions_do_not_report_degradation_turn() {
        let mut turns = vec![
            snapshot(1, 130_000, 500, 0.0, 0.61),
            snapshot(2, 131_000, 400, 5.0, 0.62),
            snapshot(3, 132_000, 300, 5.0, 0.63),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        turns[1].tool_calls = vec!["Write".to_string()];
        turns[2].tool_calls = vec!["Bash".to_string()];

        let report = analyze_session("session-heuristic", &turns);
        assert!(!report.degraded);
        assert_eq!(report.degradation_turn, None);
        assert_eq!(report.causes.len(), 1);
        assert_eq!(report.causes[0].cause_type, "context_bloat");
    }

    #[test]
    fn analyze_session_reports_model_fallback_as_degradation() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 10.0, 0.22),
            snapshot(3, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        turns[1].tool_calls = vec!["Bash".to_string()];
        turns[1].requested_model = Some("claude-opus-4-5".to_string());
        turns[1].actual_model = Some("claude-sonnet-4-5".to_string());
        turns[2].tool_calls = vec!["Bash".to_string()];

        let report = analyze_session("session-model-fallback", &turns);

        assert!(report.degraded);
        assert_eq!(report.degradation_turn, Some(2));
        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "model_fallback")
            .expect("model fallback cause");
        assert_eq!(cause.requested_model.as_deref(), Some("claude-opus-4-5"));
        assert_eq!(cause.actual_model.as_deref(), Some("claude-sonnet-4-5"));
        assert!(report.advice.iter().any(|advice| advice.contains("Sonnet")));
    }

    #[test]
    fn analyze_session_reports_cache_ttl_miss_as_degradation() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 360.0, 0.22),
            snapshot(3, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        turns[1].tool_calls = vec!["Bash".to_string()];
        turns[1].cache_creation_tokens = 25_000;
        turns[2].tool_calls = vec!["Bash".to_string()];

        let report = analyze_session("session-cache-ttl", &turns);

        assert!(report.degraded);
        assert_eq!(report.degradation_turn, Some(2));
        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "cache_miss_ttl")
            .expect("cache TTL cause");
        assert_eq!(cause.turn_first_noticed, 2);
        assert!(cause.estimated_cost > 0.0);
    }

    #[test]
    fn analyze_session_reports_cache_thrash_after_repeated_rebuilds() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 20.0, 0.22),
            snapshot(3, 12_000, 700, 20.0, 0.24),
            snapshot(4, 13_000, 800, 20.0, 0.26),
            snapshot(5, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        for turn in turns.iter_mut().skip(1) {
            turn.tool_calls = vec!["Bash".to_string()];
        }
        for turn in turns.iter_mut().take(4).skip(1) {
            turn.cache_creation_tokens = 10_000;
            turn.cache_read_tokens = 0;
        }

        let report = analyze_session("session-cache-thrash", &turns);

        assert!(report.degraded);
        assert_eq!(report.degradation_turn, Some(2));
        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "cache_miss_thrash")
            .expect("cache thrash cause");
        assert_eq!(cause.turn_first_noticed, 2);
        assert!(cause.detail.contains("3 consecutive cache rebuilds"));
    }

    #[test]
    fn analyze_session_reports_tool_failure_streak_after_turn_six() {
        let mut turns = (1..=10)
            .map(|turn| snapshot(turn, 10_000 + turn * 100, 500, 10.0, 0.20))
            .collect::<Vec<_>>();
        turns[0].tool_calls = vec!["Edit".to_string()];
        for turn in turns.iter_mut().skip(1) {
            turn.tool_calls = vec!["Bash".to_string()];
        }
        for turn in turns.iter_mut().skip(5) {
            turn.tool_results_failed = 1;
        }

        let report = analyze_session("session-tool-failures", &turns);

        assert!(report.degraded);
        assert_eq!(report.degradation_turn, Some(6));
        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "tool_failure_streak")
            .expect("tool failure cause");
        assert_eq!(cause.turn_first_noticed, 6);
        assert!(cause.detail.contains("5 consecutive turns"));
    }
}
