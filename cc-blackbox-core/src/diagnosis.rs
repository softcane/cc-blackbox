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
    pub cache_ttl_min_secs: u64,
    pub cache_ttl_max_secs: u64,
    pub cache_ttl_source: String,
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
    pub response_error_type: Option<String>,
    pub response_error_message: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_last_noticed: Option<u32>,
    pub cause_type: String,
    pub detail: String,
    pub estimated_cost: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_wasted_tokens: Option<u64>,
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
    pub cache_reusable_prefix_ratio: f64,
    pub cache_hit_ratio: f64,
    pub total_input_cache_rate: f64,
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

fn cache_ttl_min_secs(turn: &TurnSnapshot) -> u64 {
    turn.cache_ttl_min_secs.max(1)
}

fn cache_ttl_max_secs(turn: &TurnSnapshot) -> u64 {
    turn.cache_ttl_max_secs.max(cache_ttl_min_secs(turn))
}

fn is_cache_cause(cause_type: &str) -> bool {
    matches!(cause_type, "cache_miss_ttl" | "cache_miss_thrash")
}

fn cause_counts_as_degradation(cause: &DegradationCause) -> bool {
    !cause.is_heuristic || is_cache_cause(&cause.cause_type) || cause.cause_type == "api_error"
}

fn cache_creation_rebuild_buckets_for_turn(turn: &TurnSnapshot) -> (u64, u64) {
    let tokens = turn.cache_creation_tokens as u64;
    if tokens == 0 {
        return (0, 0);
    }

    if cache_ttl_max_secs(turn) >= 3600 {
        // Turn snapshots retain TTL evidence, not exact response bucket splits.
        // If evidence says the cache was 1h or mixed 5m/1h, use the 1h write
        // rate as the conservative rebuild estimate instead of underpricing it
        // as a 5m cache.
        (0, tokens)
    } else {
        (tokens, 0)
    }
}

fn estimate_cache_rebuild_waste_for_turn(model: &str, turn: &TurnSnapshot) -> f64 {
    let (cache_create_5m, cache_create_1h) = cache_creation_rebuild_buckets_for_turn(turn);
    crate::pricing::estimate_cache_rebuild_waste_dollars_with_cache_buckets(
        model,
        cache_create_5m,
        cache_create_1h,
    )
    .total_cost_dollars
}

fn response_error_message_metadata(message: Option<&str>) -> String {
    let Some(message) = message.map(str::trim).filter(|message| !message.is_empty()) else {
        return "no response error message was captured".to_string();
    };
    let words = message.split_whitespace().count();
    let chars = message.chars().count();
    format!("response error message captured (redacted, {words} words, {chars} chars)")
}

fn api_error_cause(turn: &TurnSnapshot) -> Option<DegradationCause> {
    let error_type = turn.response_error_type.as_deref()?;
    let message_metadata = response_error_message_metadata(turn.response_error_message.as_deref());
    Some(DegradationCause {
        turn_first_noticed: turn.turn_number,
        turn_last_noticed: None,
        cause_type: "api_error".to_string(),
        detail: format!(
            "Claude API streaming error at turn {} ({}); {}.",
            turn.turn_number, error_type, message_metadata
        ),
        estimated_cost: 0.0,
        estimated_wasted_tokens: None,
        is_heuristic: false,
        requested_model: None,
        actual_model: None,
    })
}

fn prioritized_advice(causes: &[DegradationCause]) -> Vec<String> {
    let priority = [
        "api_error",
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
    advice
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
    let total_input_side_tokens: u64 = turns
        .iter()
        .map(|t| {
            t.input_tokens as u64 + t.cache_read_tokens as u64 + t.cache_creation_tokens as u64
        })
        .sum();
    let total_input_cache_rate = if total_input_side_tokens > 0 {
        total_cache_read as f64 / total_input_side_tokens as f64
    } else {
        0.0
    };

    // Short sessions: skip analysis.
    if total_turns < 3 {
        let outcome = determine_outcome(turns);
        let causes = turns
            .iter()
            .filter_map(api_error_cause)
            .take(1)
            .collect::<Vec<_>>();
        let degraded = causes.iter().any(cause_counts_as_degradation);
        let degradation_turn = degraded
            .then(|| causes.first().map(|cause| cause.turn_first_noticed))
            .flatten();
        let advice = prioritized_advice(&causes);
        return DiagnosisReport {
            session_id: session_id.to_string(),
            outcome: outcome.to_string(),
            total_turns,
            total_tokens,
            estimated_total_cost_dollars: total_cost,
            cost_source: cost_source.clone(),
            trusted_for_budget_enforcement,
            cache_reusable_prefix_ratio: cache_hit_ratio,
            cache_hit_ratio,
            total_input_cache_rate,
            degraded,
            degradation_turn,
            causes,
            advice,
        };
    }

    let outcome = determine_outcome(turns);
    let mut causes = Vec::new();

    // Turn-duration baseline: average of first 3 turns.
    let ttft_baseline = turns.iter().take(3).map(|t| t.ttft_ms as f64).sum::<f64>() / 3.0;

    let mut degradation_turn: Option<u32> = None;
    let mut set_degradation = |turn: u32| {
        if degradation_turn
            .map(|existing| turn < existing)
            .unwrap_or(true)
        {
            degradation_turn = Some(turn);
        }
    };

    // Check each turn for degradation signals.
    for (i, turn) in turns.iter().enumerate() {
        // CAUSE: cache_miss_ttl. Anthropic does not expose the exact cache
        // miss reason, so this remains a local inference even when the idle
        // gap exceeds all TTL evidence we captured.
        if turn.cache_creation_tokens > 0
            && turn.cache_read_tokens == 0
            && turn.gap_from_prev_secs > cache_ttl_max_secs(turn) as f64
        {
            let prev_ttft = if i > 0 { turns[i - 1].ttft_ms } else { 0 };
            let ttl_model = turn
                .actual_model
                .as_deref()
                .or(turn.requested_model.as_deref())
                .unwrap_or("sonnet");
            causes.push(DegradationCause {
                turn_first_noticed: turn.turn_number,
                turn_last_noticed: None,
                cause_type: "cache_miss_ttl".to_string(),
                detail: format!(
                    "Inferred cache TTL miss at turn {}: {:.0}min gap exceeded observed TTL evidence ({}s). Turn duration jumped from {}ms to {}ms.",
                    turn.turn_number,
                    turn.gap_from_prev_secs / 60.0,
                    cache_ttl_max_secs(turn),
                    prev_ttft,
                    turn.ttft_ms
                ),
                estimated_cost: estimate_cache_rebuild_waste_for_turn(ttl_model, turn),
                estimated_wasted_tokens: None,
                is_heuristic: true,
                requested_model: None,
                actual_model: None,
            });
            set_degradation(turn.turn_number);
        }

        if !causes.iter().any(|c| c.cause_type == "api_error") {
            if let Some(cause) = api_error_cause(turn) {
                causes.push(cause);
                set_degradation(turn.turn_number);
            }
        }

        // CAUSE: cache_miss_thrash — tracked via counter, fires at 3+ consecutive misses.
        // (single context changes between turns are normal, not thrash)

        // CAUSE: context_bloat
        if turn.context_utilization > 0.60 {
            // Only add once — for the first turn that crosses 60%.
            if !causes.iter().any(|c| c.cause_type == "context_bloat") {
                causes.push(DegradationCause {
                    turn_first_noticed: turn.turn_number,
                    turn_last_noticed: None,
                    cause_type: "context_bloat".to_string(),
                    detail: format!(
                        "Context hit ~{:.0}% full by turn {}. Large file reads likely \
                         dominated \u{2014} reading whole files fills context faster than conversation.",
                        turn.context_utilization * 100.0, turn.turn_number
                    ),
                    estimated_cost: 0.0,
                    estimated_wasted_tokens: None,
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
                        turn_last_noticed: None,
                        cause_type: "model_fallback".to_string(),
                        detail: format!(
                            "Requested model {}; response reported model {} at turn {}. \
                             This records a model-route mismatch; it does not prove why routing changed.",
                            requested, actual, turn.turn_number
                        ),
                        estimated_cost: 0.0,
                        estimated_wasted_tokens: None,
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
                    turn_last_noticed: None,
                    cause_type: "near_compaction".to_string(),
                    detail: format!(
                        "Inferred context runway: context reached ~{:.0}% full by turn {} and was {}.",
                        fill_percent, turn.turn_number, runway
                    ),
                    estimated_cost: 0.0,
                    estimated_wasted_tokens: None,
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
                    turn_last_noticed: None,
                    cause_type: "harness_pressure".to_string(),
                    detail: format!(
                        "{} early-stop or token-pressure signals detected in Claude's output. \
                         This suggests the harness was constraining response quality.",
                        total_signals
                    ),
                    estimated_cost: 0.0,
                    estimated_wasted_tokens: None,
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
                && turn.gap_from_prev_secs <= cache_ttl_min_secs(turn) as f64;
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
                            estimate_cache_rebuild_waste_for_turn(thrash_model, t)
                        })
                        .sum();
                    causes.push(DegradationCause {
                        turn_first_noticed: thrash_start,
                        turn_last_noticed: Some(turn.turn_number),
                        cause_type: "cache_miss_thrash".to_string(),
                        detail: format!(
                            "Inferred cache thrash: {} consecutive cache rebuilds (turns {}\u{2013}{}) within observed TTL evidence. \
                             ~{}K tokens wasted. Check if CLAUDE.md, hooks, MCP config, prompts, tools, or images changed.",
                            consecutive_misses,
                            thrash_start,
                            turn.turn_number,
                            wasted / 1000
                        ),
                        estimated_cost,
                        estimated_wasted_tokens: Some(wasted),
                        is_heuristic: true,
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
                    turn_last_noticed: Some(turn.turn_number),
                    cause_type: "tool_failure_streak".to_string(),
                    detail: format!(
                        "Tool calls failed in {} consecutive turns ({}\u{2013}{}). \
                         Claude may have been retrying a broken approach.",
                        consecutive_failures, streak_start, turn.turn_number
                    ),
                    estimated_cost: 0.0,
                    estimated_wasted_tokens: None,
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
                turn_last_noticed: Some(signal.end_turn),
                cause_type: "compaction_suspected".to_string(),
                detail: format!(
                    "Rapid-fire requests with stable token counts detected at turns \
                     {}\u{2013}{}. This pattern may indicate a compaction loop (heuristic \
                     \u{2014} not directly observable).",
                    signal.start_turn, signal.end_turn
                ),
                estimated_cost: 0.0,
                estimated_wasted_tokens: Some(signal.wasted_tokens),
                is_heuristic: true,
                requested_model: None,
                actual_model: None,
            });
            set_degradation(signal.start_turn);
        }
    }

    let degraded = causes.iter().any(cause_counts_as_degradation)
        || matches!(outcome, TaskOutcome::CompactionSuspected);

    // Generate advice — deduplicated, max 3, prioritized.
    let advice = prioritized_advice(&causes);

    let degradation_turn = if degraded { degradation_turn } else { None };

    DiagnosisReport {
        session_id: session_id.to_string(),
        outcome: outcome.to_string(),
        total_turns,
        total_tokens,
        estimated_total_cost_dollars: total_cost,
        cost_source,
        trusted_for_budget_enforcement,
        cache_reusable_prefix_ratio: cache_hit_ratio,
        cache_hit_ratio,
        total_input_cache_rate,
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
        .unwrap_or(0);

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
        "cache_miss_ttl" => "Cache likely expired after an idle gap beyond the observed TTL evidence. Send a message before the shown TTL countdown elapses to keep it warm.".to_string(),
        "cache_miss_thrash" => "Likely cache thrash: full cache rebuilds occurred within the observed TTL. Check whether prompts, tools, images, CLAUDE.md, hooks, or MCP config changed mid-session.".to_string(),
        "api_error" => "The API returned a streaming error. Treat this turn as failed and retry after fixing the reported provider-side error.".to_string(),
        "context_bloat" => "Point Claude at specific functions, not whole files.".to_string(),
        "model_fallback" => {
            let actual = cause
                .actual_model
                .as_deref()
                .map(friendly_model_name)
                .unwrap_or_else(|| "a different model".to_string());
            format!(
                "Response reported {} at turn {} after a different model was requested. Retry or explicitly choose a model if this routing changed the result.",
                actual,
                cause.turn_first_noticed
            )
        }
        "near_compaction" => "Inferred context runway was close to auto-compaction. Start a fresh session or narrow the next turn to specific files or functions.".to_string(),
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
            cache_ttl_min_secs: 300,
            cache_ttl_max_secs: 300,
            cache_ttl_source: "default_5m".to_string(),
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
            response_error_type: None,
            response_error_message: None,
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
        assert!(cause.detail.contains("model-route mismatch"));
        assert!(report
            .advice
            .iter()
            .any(|advice| advice.contains("Response reported")));
        assert!(!report
            .advice
            .iter()
            .any(|advice| advice.to_ascii_lowercase().contains("quota")));
    }

    #[test]
    fn analyze_session_reports_model_version_prefix_overlap_as_route_mismatch() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 10.0, 0.22),
            snapshot(3, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        turns[1].tool_calls = vec!["Bash".to_string()];
        turns[1].requested_model = Some("claude-opus-4".to_string());
        turns[1].actual_model = Some("claude-opus-4-7".to_string());
        turns[2].tool_calls = vec!["Bash".to_string()];

        let report = analyze_session("session-model-version-mismatch", &turns);

        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "model_fallback")
            .expect("model route mismatch cause");
        assert!(!cause.is_heuristic);
        assert!(cause.detail.contains("Requested model claude-opus-4"));
        assert!(cause
            .detail
            .contains("response reported model claude-opus-4-7"));
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
        assert!(cause.is_heuristic);
    }

    #[test]
    fn analyze_session_prices_one_hour_cache_ttl_miss_with_one_hour_rate() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 3700.0, 0.22),
            snapshot(3, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        turns[1].tool_calls = vec!["Bash".to_string()];
        turns[1].cache_creation_tokens = 1_000_000;
        turns[1].cache_ttl_min_secs = 3600;
        turns[1].cache_ttl_max_secs = 3600;
        turns[1].cache_ttl_source = "request_cache_control".to_string();
        turns[2].tool_calls = vec!["Bash".to_string()];

        let report = analyze_session("session-cache-ttl-1h", &turns);
        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "cache_miss_ttl")
            .expect("cache TTL cause");

        assert!((cause.estimated_cost - 5.70).abs() < 1e-9);
        assert!(report
            .advice
            .iter()
            .any(|advice| advice.contains("observed TTL")));
        assert!(!report
            .advice
            .iter()
            .any(|advice| advice.contains("> 5 min")));
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
        assert!(cause.is_heuristic);
    }

    #[test]
    fn analyze_session_prices_one_hour_cache_thrash_with_one_hour_rate() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 600.0, 0.22),
            snapshot(3, 12_000, 700, 600.0, 0.24),
            snapshot(4, 13_000, 800, 600.0, 0.26),
            snapshot(5, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        for turn in turns.iter_mut().skip(1) {
            turn.tool_calls = vec!["Bash".to_string()];
            turn.cache_ttl_min_secs = 3600;
            turn.cache_ttl_max_secs = 3600;
            turn.cache_ttl_source = "request_cache_control".to_string();
        }
        for turn in turns.iter_mut().take(4).skip(1) {
            turn.cache_creation_tokens = 1_000_000;
            turn.cache_read_tokens = 0;
        }

        let report = analyze_session("session-cache-thrash-1h", &turns);
        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "cache_miss_thrash")
            .expect("cache thrash cause");

        assert!((cause.estimated_cost - 17.10).abs() < 1e-9);
    }

    #[test]
    fn analyze_session_reports_streaming_api_error() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 10.0, 0.22),
            snapshot(3, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        turns[1].tool_calls = vec!["Bash".to_string()];
        turns[1].response_error_type = Some("overloaded_error".to_string());
        turns[1].response_error_message = Some("server overloaded".to_string());
        turns[2].tool_calls = vec!["Bash".to_string()];

        let report = analyze_session("session-api-error", &turns);

        assert!(report.degraded);
        assert_eq!(report.degradation_turn, Some(2));
        let cause = report
            .causes
            .iter()
            .find(|cause| cause.cause_type == "api_error")
            .expect("api error cause");
        assert!(cause.detail.contains("overloaded_error"));
        assert!(!cause.detail.contains("server overloaded"));
        assert!(cause
            .detail
            .contains("response error message captured (redacted"));
        assert!(!cause.is_heuristic);
    }

    #[test]
    fn analyze_session_reports_api_error_for_short_session() {
        let mut turns = vec![snapshot(1, 10_000, 500, 0.0, 0.20)];
        turns[0].response_error_type = Some("overloaded_error".to_string());
        turns[0].response_error_message = Some("server overloaded".to_string());

        let report = analyze_session("session-short-api-error", &turns);

        assert!(report.degraded);
        assert_eq!(report.degradation_turn, Some(1));
        assert_eq!(report.causes.len(), 1);
        assert_eq!(report.causes[0].cause_type, "api_error");
        assert!(report
            .advice
            .iter()
            .any(|advice| advice.contains("streaming error")));
    }

    #[test]
    fn analyze_session_respects_one_hour_cache_ttl_evidence() {
        let mut turns = vec![
            snapshot(1, 10_000, 500, 0.0, 0.20),
            snapshot(2, 11_000, 600, 600.0, 0.22),
            snapshot(3, 9_000, 400, 10.0, 0.18),
        ];
        turns[0].tool_calls = vec!["Edit".to_string()];
        turns[1].tool_calls = vec!["Bash".to_string()];
        turns[1].cache_creation_tokens = 25_000;
        turns[1].cache_ttl_min_secs = 3600;
        turns[1].cache_ttl_max_secs = 3600;
        turns[1].cache_ttl_source = "request_cache_control".to_string();
        turns[2].tool_calls = vec!["Bash".to_string()];

        let report = analyze_session("session-cache-1h", &turns);

        assert!(!report
            .causes
            .iter()
            .any(|cause| cause.cause_type == "cache_miss_ttl"));
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
