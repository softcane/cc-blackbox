use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use colored::Colorize;
use futures_util::StreamExt;

use crate::{event_session_id, WatchEvent};

// ---------------------------------------------------------------------------
// Tmux environment checks
// ---------------------------------------------------------------------------

/// Verify tmux is installed. Caller is responsible for ensuring we're inside
/// a tmux session (via `bootstrap_into_tmux` or otherwise).
pub fn check_tmux_installed() -> Result<(), String> {
    let status = Command::new("tmux")
        .arg("-V")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    match status {
        Ok(s) if s.success() => Ok(()),
        _ => Err("tmux not found. Install tmux first (brew install tmux).".into()),
    }
}

/// If we're not already inside a tmux session, replace this process with a new
/// tmux session that re-runs the same CLI with `--tmux` — so the user just
/// runs `clauditor watch --tmux` and lands directly in the orchestrator.
///
/// Returns Ok(()) when we were already inside tmux (caller continues).
/// On successful exec, this function does not return.
pub fn bootstrap_into_tmux(
    url: &str,
    no_cache: bool,
    no_signals: bool,
    postmortem: bool,
    no_analyze_with_claude: bool,
    tmux_max_panes: usize,
) -> Result<(), String> {
    if std::env::var("TMUX").is_ok() {
        return Ok(());
    }
    check_tmux_installed()?;

    let cli =
        std::env::current_exe().map_err(|e| format!("cannot locate current executable: {}", e))?;
    let cli_str = cli
        .to_str()
        .ok_or_else(|| "executable path is not UTF-8".to_string())?
        .to_string();

    // Use a unique session name so parallel invocations don't collide.
    let session_name = format!("clauditor-{}", std::process::id());
    let panes_str = tmux_max_panes.to_string();

    let mut args: Vec<String> = vec![
        "new-session".into(),
        "-s".into(),
        session_name,
        cli_str,
        "watch".into(),
        "--tmux".into(),
        "--url".into(),
        url.to_string(),
        "--tmux-max-panes".into(),
        panes_str,
    ];
    if no_cache {
        args.push("--no-cache".into());
    }
    if no_signals {
        args.push("--no-signals".into());
    }
    if postmortem {
        args.push("--postmortem".into());
    }
    if no_analyze_with_claude {
        args.push("--no-analyze-with-claude".into());
    }

    use std::os::unix::process::CommandExt;
    let err = Command::new("tmux").args(&args).exec();
    // exec only returns on failure.
    Err(format!("failed to exec tmux: {}", err))
}

fn get_own_pane_id() -> Result<String, String> {
    let output = Command::new("tmux")
        .args(["display-message", "-p", "#{pane_id}"])
        .output()
        .map_err(|e| format!("Failed to get pane id: {}", e))?;
    if !output.status.success() {
        return Err("tmux display-message failed".into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn get_own_window_id() -> Result<String, String> {
    let output = Command::new("tmux")
        .args(["display-message", "-p", "#{window_id}"])
        .output()
        .map_err(|e| format!("Failed to get window id: {}", e))?;
    if !output.status.success() {
        return Err("tmux display-message failed".into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn run_tmux(args: &[&str]) -> Result<(), String> {
    let output = Command::new("tmux")
        .args(args)
        .output()
        .map_err(|e| format!("tmux command failed: {}", e))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return Err(stderr);
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return Err(stdout);
    }
    Err("unknown tmux error".into())
}

fn resolve_cli_path() -> String {
    std::env::current_exe()
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| "clauditor".into())
}

// ---------------------------------------------------------------------------
// Managed pane state
// ---------------------------------------------------------------------------

struct ManagedPane {
    pane_id: String,
    #[allow(dead_code)]
    session_id: String,
    display_name: String,
    model: String,
    /// Observed tool-use events seen by this watcher for the session.
    observed_tool_calls: u32,
    ended: bool,
    /// Time of the most recent event attributed to this session. Used to
    /// color the orchestrator row green (recent), yellow (warming down), or
    /// red (gone idle).
    last_activity: Instant,
    /// Latest cache snapshot — drives the TTL countdown + rebuild-cost hint.
    cache_expires_at_epoch: Option<u64>,
    estimated_rebuild_cost_dollars: Option<f64>,
    /// Latest context-fill snapshot — drives the compaction runway hint.
    fill_percent: Option<f64>,
    turns_to_compact: Option<u32>,
    /// If Anthropic routed to a different model than requested.
    model_fallback: Option<(String, String)>,
    applied_activity: Option<Activity>,
}

/// Activity level derived from `last_activity` elapsed time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Activity {
    Active,
    Warm,
    Idle,
    Ended,
}

/// Compact human formatting: 12345 → "12K", 3_400_000 → "3.4M".
fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{}K", n / 1_000)
    } else {
        n.to_string()
    }
}

fn format_observed_tool_calls(n: u32) -> String {
    match n {
        0 => "no tool calls seen".to_string(),
        1 => "1 tool call seen".to_string(),
        _ => format!("{n} tool calls seen"),
    }
}

/// Coarse human duration: "45s" / "12m" / "3h 20m" / "5d 14h".
fn format_duration(secs: u64) -> String {
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

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn shell_join(parts: &[String]) -> String {
    parts
        .iter()
        .map(|part| shell_quote(part))
        .collect::<Vec<_>>()
        .join(" ")
}

fn build_child_watch_command(
    cli_path: &str,
    session_id: &str,
    watch_url: &str,
    no_cache: bool,
    no_signals: bool,
    postmortem: bool,
    no_analyze_with_claude: bool,
) -> String {
    let mut cmd_parts = vec![
        cli_path.to_string(),
        "watch".to_string(),
        "--session".to_string(),
        session_id.to_string(),
        "--url".to_string(),
        watch_url.to_string(),
    ];
    if no_cache {
        cmd_parts.push("--no-cache".to_string());
    }
    if no_signals {
        cmd_parts.push("--no-signals".to_string());
    }
    if postmortem {
        cmd_parts.push("--postmortem".to_string());
    }
    if no_analyze_with_claude {
        cmd_parts.push("--no-analyze-with-claude".to_string());
    }
    shell_join(&cmd_parts)
}

impl ManagedPane {
    fn activity(&self) -> Activity {
        if self.ended {
            return Activity::Ended;
        }
        let idle = self.last_activity.elapsed();
        if idle < Duration::from_secs(10) {
            Activity::Active
        } else if idle < Duration::from_secs(30) {
            Activity::Warm
        } else {
            Activity::Idle
        }
    }
}

// ---------------------------------------------------------------------------
// TmuxOrchestrator
// ---------------------------------------------------------------------------

/// Last known local weekly quota/budget snapshot, rendered in the top strip.
struct RateLimitSummary {
    seconds_to_reset: Option<u64>,
    tokens_used_this_week: Option<u64>,
    tokens_limit: Option<u64>,
    tokens_remaining: Option<u64>,
    budget_source: Option<String>,
    projected_exhaustion_secs: Option<u64>,
    requests_remaining: Option<u64>,
    requests_limit: Option<u64>,
}

pub struct TmuxOrchestrator {
    panes: HashMap<String, ManagedPane>,
    own_pane_id: String,
    own_window_id: String,
    first_session_pane_id: Option<String>,
    watch_url: String,
    cli_path: String,
    no_cache: bool,
    no_signals: bool,
    postmortem: bool,
    no_analyze_with_claude: bool,
    max_panes: usize,
    rate_limit: Option<RateLimitSummary>,
}

impl TmuxOrchestrator {
    pub fn new(
        watch_url: String,
        no_cache: bool,
        no_signals: bool,
        postmortem: bool,
        no_analyze_with_claude: bool,
        max_panes: usize,
    ) -> Result<Self, String> {
        let own_pane_id = get_own_pane_id()?;
        let own_window_id = get_own_window_id()?;
        let cli_path = resolve_cli_path();
        let orchestrator = Self {
            panes: HashMap::new(),
            own_pane_id,
            own_window_id,
            first_session_pane_id: None,
            watch_url,
            cli_path,
            no_cache,
            no_signals,
            postmortem,
            no_analyze_with_claude,
            max_panes,
            rate_limit: None,
        };
        orchestrator.configure_pane_borders();
        Ok(orchestrator)
    }

    /// Create a tmux pane for a session. Returns the new pane_id.
    fn create_pane(
        &mut self,
        session_id: &str,
        display_name: &str,
        model: &str,
    ) -> Result<String, String> {
        // Build child command.
        let child_cmd = build_child_watch_command(
            &self.cli_path,
            session_id,
            &self.watch_url,
            self.no_cache,
            self.no_signals,
            self.postmortem,
            self.no_analyze_with_claude,
        );

        // Determine split strategy.
        // Always target a specific pane so the split happens in the orchestrator's window.
        let mut args: Vec<String> = vec!["split-window".into()];
        if self.first_session_pane_id.is_none() {
            // First session pane: split the orchestrator pane vertically,
            // new pane above, give it 85%.
            args.extend_from_slice(&[
                "-v".into(),
                "-b".into(),
                "-l".into(),
                "85%".into(),
                "-t".into(),
                self.own_pane_id.clone(),
            ]);
        } else {
            // Subsequent panes: split the first session pane horizontally.
            args.extend_from_slice(&[
                "-h".into(),
                "-t".into(),
                self.first_session_pane_id.clone().unwrap(),
            ]);
        }
        // Don't steal focus, print new pane id.
        args.extend_from_slice(&[
            "-d".into(),
            "-P".into(),
            "-F".into(),
            "#{pane_id}".into(),
            child_cmd,
        ]);

        let output = Command::new("tmux")
            .args(&args)
            .output()
            .map_err(|e| format!("tmux split-window failed: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("tmux split-window error: {}", stderr.trim()));
        }

        let pane_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if pane_id.is_empty() {
            return Err("tmux split-window returned empty pane id".into());
        }

        // Track the first session pane for subsequent splits.
        if self.first_session_pane_id.is_none() {
            self.first_session_pane_id = Some(pane_id.clone());
        }

        // Set pane title.
        let _ = Command::new("tmux")
            .args(["select-pane", "-t", &pane_id, "-T", display_name])
            .output();

        // Rebalance layout: tiled for session panes, then shrink orchestrator.
        self.rebalance_layout();

        // Store the managed pane.
        self.panes.insert(
            session_id.to_string(),
            ManagedPane {
                pane_id: pane_id.clone(),
                session_id: session_id.to_string(),
                display_name: display_name.to_string(),
                model: model.to_string(),
                observed_tool_calls: 0,
                ended: false,
                last_activity: Instant::now(),
                cache_expires_at_epoch: None,
                estimated_rebuild_cost_dollars: None,
                fill_percent: None,
                turns_to_compact: None,
                model_fallback: None,
                applied_activity: None,
            },
        );

        Ok(pane_id)
    }

    fn configure_pane_borders(&self) {
        if let Err(err) = run_tmux(&[
            "set-window-option",
            "-t",
            &self.own_window_id,
            "pane-border-status",
            "top",
        ]) {
            eprintln!(
                "{}",
                format!("tmux pane-border-status failed: {}", err).dimmed()
            );
        }
        if let Err(err) = run_tmux(&[
            "set-window-option",
            "-t",
            &self.own_window_id,
            "pane-border-format",
            "#{?@clauditor_active,●,○} #{pane_title}",
        ]) {
            eprintln!(
                "{}",
                format!("tmux pane-border-format failed: {}", err).dimmed()
            );
        }
    }

    fn apply_pane_activity_style(pane_id: &str, activity: Activity) {
        let (active_flag, activity_label, border_style) = match activity {
            Activity::Active => ("1", "active", "fg=green,bold"),
            Activity::Warm => ("1", "warm", "fg=yellow"),
            Activity::Idle => ("0", "idle", "fg=red"),
            Activity::Ended => ("0", "ended", "fg=colour244"),
        };
        for (label, args) in [
            (
                "@clauditor_active",
                vec![
                    "set-option",
                    "-p",
                    "-t",
                    pane_id,
                    "@clauditor_active",
                    active_flag,
                ],
            ),
            (
                "@clauditor_activity",
                vec![
                    "set-option",
                    "-p",
                    "-t",
                    pane_id,
                    "@clauditor_activity",
                    activity_label,
                ],
            ),
            (
                "pane-border-style",
                vec![
                    "set-option",
                    "-p",
                    "-t",
                    pane_id,
                    "pane-border-style",
                    border_style,
                ],
            ),
            (
                "pane-active-border-style",
                vec![
                    "set-option",
                    "-p",
                    "-t",
                    pane_id,
                    "pane-active-border-style",
                    border_style,
                ],
            ),
        ] {
            if let Err(err) = run_tmux(&args) {
                eprintln!(
                    "{}",
                    format!("tmux {} update failed for {}: {}", label, pane_id, err).dimmed()
                );
            }
        }
    }

    fn rebalance_layout(&self) {
        // Target the orchestrator's pane so layout applies to its window.
        let _ = Command::new("tmux")
            .args(["select-layout", "-t", &self.own_pane_id, "tiled"])
            .output();
        let _ = Command::new("tmux")
            .args(["resize-pane", "-t", &self.own_pane_id, "-y", "7"])
            .output();
    }

    fn render_status(&mut self) {
        // Clear screen and move cursor to top-left.
        print!("\x1b[2J\x1b[H");

        println!("{}", "clauditor watch (tmux mode)".bold());

        // Top strip: weekly budget meter. This can come from an explicit env
        // cap or from the auto-calibrated last-4-weeks suggestion.
        if let Some(rl) = &self.rate_limit {
            let has_quota_signal = rl.tokens_used_this_week.is_some()
                || rl.tokens_limit.is_some()
                || rl.tokens_remaining.is_some()
                || rl.requests_remaining.is_some()
                || rl.projected_exhaustion_secs.is_some();
            if has_quota_signal {
                let mut parts: Vec<String> = Vec::new();
                if let (Some(used), Some(limit)) = (rl.tokens_used_this_week, rl.tokens_limit) {
                    parts.push(format!(
                        "{} / {} tokens",
                        format_count(used),
                        format_count(limit)
                    ));
                } else if let Some(t) = rl.tokens_remaining {
                    parts.push(format!("{} tokens", format_count(t)));
                }
                if let (Some(r), Some(lim)) = (rl.requests_remaining, rl.requests_limit) {
                    parts.push(format!("{}/{} req", r, lim));
                }
                if rl.budget_source.as_deref() == Some("auto_p95_4w") {
                    parts.push("auto cap from last 4 weeks".to_string());
                }
                if let Some(s) = rl.seconds_to_reset {
                    parts.push(format!("resets in {}", format_duration(s)));
                }
                let alarm = rl
                    .projected_exhaustion_secs
                    .zip(rl.seconds_to_reset)
                    .map(|(ex, rst)| ex < rst)
                    .unwrap_or(false);
                if let Some(ex) = rl.projected_exhaustion_secs {
                    parts.push(format!("\u{21d2} exhausts in {}", format_duration(ex)));
                }
                let line = format!("QUOTA  {}", parts.join(" · "));
                let colored = if alarm {
                    line.red().bold().to_string()
                } else {
                    line.yellow().to_string()
                };
                println!("{}", colored);
            }
        }

        println!("{}", "\u{2500}".repeat(42).dimmed());

        if self.panes.is_empty() {
            println!("{}", "Waiting for sessions...".dimmed());
        } else {
            // Pad columns for alignment.
            let max_name = self
                .panes
                .values()
                .map(|p| p.display_name.len())
                .max()
                .unwrap_or(0)
                .max(8);
            let max_model = self
                .panes
                .values()
                .map(|p| p.model.len())
                .max()
                .unwrap_or(0)
                .max(5);

            for pane in self.panes.values_mut() {
                let activity = pane.activity();
                if pane.applied_activity != Some(activity) {
                    Self::apply_pane_activity_style(&pane.pane_id, activity);
                    pane.applied_activity = Some(activity);
                }
                let dot = match activity {
                    Activity::Active => "\u{25cf}".green(),
                    Activity::Warm => "\u{25cf}".yellow(),
                    Activity::Idle => "\u{25cf}".red(),
                    Activity::Ended => "\u{25cf}".dimmed(),
                };
                let status = match activity {
                    Activity::Ended => "ended".dimmed().to_string(),
                    Activity::Active => format!(
                        "{} · active",
                        format_observed_tool_calls(pane.observed_tool_calls)
                    )
                    .green()
                    .to_string(),
                    Activity::Warm => format_observed_tool_calls(pane.observed_tool_calls)
                        .yellow()
                        .to_string(),
                    Activity::Idle => format!(
                        "{} · idle {}s",
                        format_observed_tool_calls(pane.observed_tool_calls),
                        pane.last_activity.elapsed().as_secs()
                    )
                    .red()
                    .to_string(),
                };
                let name_colored = match activity {
                    Activity::Active => pane.display_name.green().to_string(),
                    Activity::Warm => pane.display_name.yellow().to_string(),
                    Activity::Idle => pane.display_name.red().to_string(),
                    Activity::Ended => pane.display_name.dimmed().to_string(),
                };
                // Pad raw display name so columns align even after color codes;
                // colored::ColoredString doesn't count width for us.
                let name_padding = max_name.saturating_sub(pane.display_name.len());
                println!(
                    "  {} {}{}  {:<width_m$}  {}",
                    dot,
                    name_colored,
                    " ".repeat(name_padding),
                    pane.model,
                    status,
                    width_m = max_model,
                );
                // Sub-row: cache, context, fallback hints. Indent under the
                // pane row so the eye groups them. Only render hints that
                // are active / actionable.
                let name_indent = " ".repeat(4 + max_name);
                if let Some(exp) = pane.cache_expires_at_epoch {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let remaining = exp.saturating_sub(now);
                    // Suppress once the cache is stale; orchestrator tick will
                    // refresh every 2s so it self-hides quickly.
                    if remaining > 0 && !pane.ended {
                        let cost_part = pane
                            .estimated_rebuild_cost_dollars
                            .map(|c| format!(" · est. rebuild ${:.2}", c))
                            .unwrap_or_default();
                        let msg = format!(
                            "{}cache expires in {}m{:02}s{}",
                            name_indent,
                            remaining / 60,
                            remaining % 60,
                            cost_part,
                        );
                        let colored = if remaining < 60 {
                            msg.yellow().bold().to_string()
                        } else {
                            msg.dimmed().to_string()
                        };
                        println!("{}", colored);
                    }
                }
                if let (Some(fill), _) = (pane.fill_percent, pane.turns_to_compact) {
                    if fill >= 60.0 && !pane.ended {
                        let tail = match pane.turns_to_compact {
                            Some(0) => "inferred at threshold".to_string(),
                            Some(n) => format!("inferred ~{} turns to auto-compact", n),
                            None => "inferred trajectory unknown".to_string(),
                        };
                        let msg = format!("{}context {:.0}% full · {}", name_indent, fill, tail);
                        let colored = if fill >= 80.0 {
                            msg.red().bold().to_string()
                        } else {
                            msg.yellow().to_string()
                        };
                        println!("{}", colored);
                    }
                }
                if let Some((req, actual)) = &pane.model_fallback {
                    if !pane.ended {
                        println!(
                            "{}",
                            format!(
                                "{}\u{26a0}  routed: requested {}, got {}",
                                name_indent, req, actual
                            )
                            .yellow()
                            .bold()
                        );
                    }
                }
            }
        }

        println!();
        println!("{}", "Ctrl+C to stop all panes".dimmed());
    }

    #[allow(dead_code)]
    fn cleanup(&self) {
        for pane in self.panes.values() {
            let _ = Command::new("tmux")
                .args(["kill-pane", "-t", &pane.pane_id])
                .output();
        }
    }

    /// Lazy discovery: create a pane for a session we haven't seen a SessionStart for.
    fn ensure_pane_exists(&mut self, session_id: &str, cleanup_pane_ids: &Arc<Mutex<Vec<String>>>) {
        if self.panes.contains_key(session_id) {
            return;
        }
        if self.panes.len() >= self.max_panes {
            return;
        }
        // Derive a short name from the session_id.
        let short_name = if session_id.len() > 12 {
            &session_id[8..12.min(session_id.len())]
        } else {
            session_id
        };
        match self.create_pane(session_id, short_name, "?") {
            Ok(pane_id) => {
                if let Ok(mut ids) = cleanup_pane_ids.lock() {
                    ids.push(pane_id);
                }
            }
            Err(e) => {
                eprintln!("{}", format!("Failed to create pane: {}", e).dimmed());
            }
        }
    }

    fn bump_activity(&mut self, session_id: &str) {
        if let Some(pane) = self.panes.get_mut(session_id) {
            if !pane.ended {
                pane.last_activity = Instant::now();
            }
        }
    }

    fn handle_event(&mut self, event: &WatchEvent, cleanup_pane_ids: &Arc<Mutex<Vec<String>>>) {
        // Touch activity timestamp on any session-scoped event so the status
        // coloring reflects real-time pulse, not only ToolUse.
        if !matches!(event, WatchEvent::PostmortemReady { .. }) {
            if let Some(sid) = event_session_id(event) {
                self.bump_activity(sid);
            }
        }
        match event {
            WatchEvent::SessionStart {
                session_id,
                display_name,
                model,
                initial_prompt: _,
            } => {
                if let Some(pane) = self.panes.get_mut(session_id.as_str()) {
                    // Pane was lazy-discovered from an earlier ToolUse with a
                    // placeholder name/model. Promote it now that we have the
                    // real values. Rename the tmux pane title too so
                    // pane-border-status displays the right label.
                    let pane_id = pane.pane_id.clone();
                    pane.display_name = display_name.clone();
                    pane.model = model.clone();
                    let _ = Command::new("tmux")
                        .args(["select-pane", "-t", &pane_id, "-T", display_name])
                        .output();
                } else if self.panes.len() < self.max_panes {
                    match self.create_pane(session_id, display_name, model) {
                        Ok(pane_id) => {
                            if let Ok(mut ids) = cleanup_pane_ids.lock() {
                                ids.push(pane_id);
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "{}",
                                format!("Failed to create pane for {}: {}", display_name, e)
                                    .dimmed()
                            );
                        }
                    }
                }
                self.render_status();
            }

            WatchEvent::SessionEnd { session_id, .. } => {
                if let Some(pane) = self.panes.get_mut(session_id.as_str()) {
                    pane.ended = true;
                }
                self.render_status();
            }

            WatchEvent::PostmortemReady { .. } => {
                self.render_status();
            }

            WatchEvent::ToolUse { session_id, .. } => {
                self.ensure_pane_exists(session_id, cleanup_pane_ids);
                if let Some(pane) = self.panes.get_mut(session_id.as_str()) {
                    pane.observed_tool_calls += 1;
                }
                self.render_status();
            }

            WatchEvent::McpEvent { session_id, .. } => {
                self.ensure_pane_exists(session_id, cleanup_pane_ids);
                self.render_status();
            }

            WatchEvent::SkillEvent { session_id, .. } => {
                self.ensure_pane_exists(session_id, cleanup_pane_ids);
                self.render_status();
            }

            WatchEvent::CacheEvent {
                session_id,
                cache_expires_at_epoch,
                estimated_rebuild_cost_dollars,
                ..
            } => {
                self.ensure_pane_exists(session_id, cleanup_pane_ids);
                if let Some(pane) = self.panes.get_mut(session_id.as_str()) {
                    if cache_expires_at_epoch.is_some() {
                        pane.cache_expires_at_epoch = *cache_expires_at_epoch;
                    }
                    if estimated_rebuild_cost_dollars.is_some() {
                        pane.estimated_rebuild_cost_dollars = *estimated_rebuild_cost_dollars;
                    }
                }
                self.render_status();
            }

            WatchEvent::ContextStatus {
                session_id,
                fill_percent,
                context_window_tokens: _,
                turns_to_compact,
            } => {
                self.ensure_pane_exists(session_id, cleanup_pane_ids);
                if let Some(pane) = self.panes.get_mut(session_id.as_str()) {
                    pane.fill_percent = Some(*fill_percent);
                    pane.turns_to_compact = *turns_to_compact;
                }
                self.render_status();
            }

            WatchEvent::ModelFallback {
                session_id,
                requested,
                actual,
            } => {
                self.ensure_pane_exists(session_id, cleanup_pane_ids);
                if let Some(pane) = self.panes.get_mut(session_id.as_str()) {
                    pane.model_fallback = Some((requested.clone(), actual.clone()));
                }
                self.render_status();
            }

            WatchEvent::RateLimitStatus {
                seconds_to_reset,
                requests_remaining,
                requests_limit,
                tokens_used_this_week,
                tokens_limit,
                tokens_remaining,
                budget_source,
                projected_exhaustion_secs,
                ..
            } => {
                self.rate_limit = Some(RateLimitSummary {
                    seconds_to_reset: *seconds_to_reset,
                    tokens_used_this_week: *tokens_used_this_week,
                    tokens_limit: *tokens_limit,
                    tokens_remaining: *tokens_remaining,
                    budget_source: budget_source.clone(),
                    projected_exhaustion_secs: *projected_exhaustion_secs,
                    requests_remaining: *requests_remaining,
                    requests_limit: *requests_limit,
                });
                self.render_status();
            }

            WatchEvent::Lagged { .. } => {}

            _ => {
                // For other event types, ensure the session has a pane (lazy discovery)
                // but don't re-render status on every cache/signal event.
                if let Some(sid) = event_session_id(event) {
                    self.ensure_pane_exists(sid, cleanup_pane_ids);
                }
            }
        }
    }

    async fn connect_and_process(
        &mut self,
        url: &str,
        cleanup_pane_ids: &Arc<Mutex<Vec<String>>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let resp = reqwest::Client::new()
            .get(url)
            .header("Accept", "text/event-stream")
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()).into());
        }

        let mut stream = resp.bytes_stream();
        let mut line_buffer = Vec::new();
        let mut data_buffer = String::new();

        // Tick so the orchestrator re-renders even when the SSE stream is
        // quiet — otherwise the activity-colored dots stay green after a
        // session goes idle because no event triggers a refresh.
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_chunk = stream.next() => {
                    let chunk = match maybe_chunk {
                        Some(Ok(c)) => c,
                        Some(Err(e)) => return Err(e.into()),
                        None => break,
                    };
                    line_buffer.extend_from_slice(&chunk);

                    while let Some(newline_pos) = line_buffer.iter().position(|byte| *byte == b'\n') {
                        let line_bytes = line_buffer[..newline_pos].to_vec();
                        line_buffer.drain(..=newline_pos);
                        let Ok(line) = std::str::from_utf8(&line_bytes) else {
                            continue;
                        };
                        let line = line.trim_end_matches('\r');

                        if let Some(data) = line.strip_prefix("data: ") {
                            data_buffer.push_str(data);
                        } else if line.starts_with(": ") || line.starts_with(':') {
                            continue;
                        } else if line.is_empty() && !data_buffer.is_empty() {
                            if let Ok(event) = serde_json::from_str::<WatchEvent>(&data_buffer) {
                                self.handle_event(&event, cleanup_pane_ids);
                            }
                            data_buffer.clear();
                        }
                    }
                }
                _ = ticker.tick() => {
                    // Just re-render; pane state already has timestamps.
                    self.render_status();
                }
            }
        }

        Ok(())
    }

    pub async fn run(mut self, watch_url: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.render_status();

        // Shared list of pane IDs for the Ctrl+C handler to clean up.
        let cleanup_pane_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let cleanup_ref = cleanup_pane_ids.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            let pane_ids = cleanup_ref.lock().unwrap().clone();
            for pane_id in &pane_ids {
                let _ = Command::new("tmux")
                    .args(["kill-pane", "-t", pane_id])
                    .output();
            }
            eprintln!("\nCleaned up {} panes.", pane_ids.len());
            std::process::exit(0);
        });

        // Reconnect loop — same pattern as the regular watch mode.
        loop {
            match self.connect_and_process(watch_url, &cleanup_pane_ids).await {
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
            self.render_status();
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{format_count, format_duration, format_observed_tool_calls, Activity, ManagedPane};
    use crate::WatchEvent;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn pane_with_last_activity(last_activity: Instant, ended: bool) -> ManagedPane {
        ManagedPane {
            pane_id: "%1".to_string(),
            session_id: "session_test".to_string(),
            display_name: "test".to_string(),
            model: "sonnet".to_string(),
            observed_tool_calls: 0,
            ended,
            last_activity,
            cache_expires_at_epoch: None,
            estimated_rebuild_cost_dollars: None,
            fill_percent: None,
            turns_to_compact: None,
            model_fallback: None,
            applied_activity: None,
        }
    }

    fn active_pane() -> ManagedPane {
        let mut pane = pane_with_last_activity(Instant::now(), false);
        pane.applied_activity = Some(Activity::Active);
        pane
    }

    fn test_orchestrator(max_panes: usize) -> super::TmuxOrchestrator {
        super::TmuxOrchestrator {
            panes: HashMap::new(),
            own_pane_id: "%0".to_string(),
            own_window_id: "@0".to_string(),
            first_session_pane_id: None,
            watch_url: "http://localhost:9091".to_string(),
            cli_path: "clauditor".to_string(),
            no_cache: false,
            no_signals: false,
            postmortem: false,
            no_analyze_with_claude: false,
            max_panes,
            rate_limit: None,
        }
    }

    #[test]
    fn count_and_duration_formatting_are_compact() {
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(12_345), "12K");
        assert_eq!(format_count(3_400_000), "3.4M");

        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(12 * 60), "12m");
        assert_eq!(format_duration(3 * 60 * 60 + 20 * 60), "3h 20m");
        assert_eq!(format_duration(5 * 24 * 60 * 60 + 14 * 60 * 60), "5d 14h");
    }

    #[test]
    fn observed_tool_call_labels_match_counts() {
        assert_eq!(format_observed_tool_calls(0), "no tool calls seen");
        assert_eq!(format_observed_tool_calls(1), "1 tool call seen");
        assert_eq!(format_observed_tool_calls(2), "2 tool calls seen");
    }

    #[test]
    fn child_watch_command_preserves_session_url_and_visibility_flags() {
        assert_eq!(
            super::build_child_watch_command(
                "clauditor",
                "session_a",
                "http://localhost:9091",
                false,
                false,
                false,
                false,
            ),
            "clauditor watch --session session_a --url http://localhost:9091"
        );

        assert_eq!(
            super::build_child_watch_command(
                "/tmp/clauditor cli",
                "session with spaces",
                "http://localhost:9091/watch?session=session with spaces",
                true,
                true,
                true,
                true,
            ),
            "'/tmp/clauditor cli' watch --session 'session with spaces' --url 'http://localhost:9091/watch?session=session with spaces' --no-cache --no-signals --postmortem --no-analyze-with-claude"
        );
    }

    #[test]
    fn pane_activity_tracks_idle_time_and_end_state() {
        assert_eq!(
            pane_with_last_activity(Instant::now(), false).activity(),
            Activity::Active
        );
        assert_eq!(
            pane_with_last_activity(Instant::now() - Duration::from_secs(15), false).activity(),
            Activity::Warm
        );
        assert_eq!(
            pane_with_last_activity(Instant::now() - Duration::from_secs(31), false).activity(),
            Activity::Idle
        );
        assert_eq!(
            pane_with_last_activity(Instant::now(), true).activity(),
            Activity::Ended
        );
    }

    #[test]
    fn ensure_pane_exists_respects_max_panes_before_shelling_out() {
        let mut orchestrator = test_orchestrator(0);
        let cleanup = Arc::new(Mutex::new(Vec::new()));

        orchestrator.ensure_pane_exists("session_new", &cleanup);

        assert!(orchestrator.panes.is_empty());
        assert!(cleanup.lock().expect("cleanup ids").is_empty());
    }

    #[test]
    fn session_start_respects_max_panes_before_shelling_out() {
        let mut orchestrator = test_orchestrator(0);
        let cleanup = Arc::new(Mutex::new(Vec::new()));

        orchestrator.handle_event(
            &WatchEvent::SessionStart {
                session_id: "session_new".to_string(),
                display_name: "api".to_string(),
                model: "sonnet".to_string(),
                initial_prompt: Some("hello".to_string()),
            },
            &cleanup,
        );

        assert!(orchestrator.panes.is_empty());
        assert!(cleanup.lock().expect("cleanup ids").is_empty());
    }

    #[test]
    fn handle_event_updates_existing_pane_state() {
        let mut orchestrator = test_orchestrator(4);
        orchestrator
            .panes
            .insert("session_a".to_string(), active_pane());
        let cleanup = Arc::new(Mutex::new(Vec::new()));

        orchestrator.handle_event(
            &WatchEvent::SessionStart {
                session_id: "session_a".to_string(),
                display_name: "api".to_string(),
                model: "sonnet".to_string(),
                initial_prompt: Some("hello".to_string()),
            },
            &cleanup,
        );
        let pane = orchestrator.panes.get("session_a").expect("pane");
        assert_eq!(pane.display_name, "api");
        assert_eq!(pane.model, "sonnet");

        orchestrator.handle_event(
            &WatchEvent::ToolUse {
                session_id: "session_a".to_string(),
                timestamp: "2026-04-28T00:00:00Z".to_string(),
                tool_name: "Read".to_string(),
                summary: "src/main.rs".to_string(),
            },
            &cleanup,
        );
        assert_eq!(
            orchestrator
                .panes
                .get("session_a")
                .expect("pane")
                .observed_tool_calls,
            1
        );

        orchestrator.handle_event(
            &WatchEvent::CacheEvent {
                session_id: "session_a".to_string(),
                event_type: "hit".to_string(),
                cache_expires_at_epoch: Some(4_102_444_800),
                cache_expires_at_latest_epoch: None,
                cache_ttl_source: None,
                cache_ttl_mixed: None,
                estimated_rebuild_cost_dollars: Some(1.23),
            },
            &cleanup,
        );
        let pane = orchestrator.panes.get("session_a").expect("pane");
        assert_eq!(pane.cache_expires_at_epoch, Some(4_102_444_800));
        assert_eq!(pane.estimated_rebuild_cost_dollars, Some(1.23));

        orchestrator.handle_event(
            &WatchEvent::ContextStatus {
                session_id: "session_a".to_string(),
                fill_percent: 82.0,
                context_window_tokens: Some(200_000),
                turns_to_compact: Some(1),
            },
            &cleanup,
        );
        let pane = orchestrator.panes.get("session_a").expect("pane");
        assert_eq!(pane.fill_percent, Some(82.0));
        assert_eq!(pane.turns_to_compact, Some(1));

        orchestrator.handle_event(
            &WatchEvent::ModelFallback {
                session_id: "session_a".to_string(),
                requested: "opus".to_string(),
                actual: "sonnet".to_string(),
            },
            &cleanup,
        );
        assert_eq!(
            orchestrator
                .panes
                .get("session_a")
                .expect("pane")
                .model_fallback,
            Some(("opus".to_string(), "sonnet".to_string()))
        );

        orchestrator
            .panes
            .get_mut("session_a")
            .expect("pane")
            .applied_activity = Some(Activity::Ended);
        orchestrator.handle_event(
            &WatchEvent::SessionEnd {
                session_id: "session_a".to_string(),
                outcome: "Likely Completed".to_string(),
                total_tokens: 123,
                total_turns: 4,
            },
            &cleanup,
        );
        assert!(orchestrator.panes.get("session_a").expect("pane").ended);
    }

    #[test]
    fn handle_event_tracks_rate_limit_summary() {
        let mut orchestrator = test_orchestrator(4);
        let cleanup = Arc::new(Mutex::new(Vec::new()));

        orchestrator.handle_event(
            &WatchEvent::RateLimitStatus {
                seconds_to_reset: Some(3600),
                requests_remaining: Some(2),
                requests_limit: Some(10),
                input_tokens_remaining: None,
                output_tokens_remaining: None,
                tokens_used_this_week: Some(1_000),
                tokens_limit: Some(10_000),
                tokens_remaining: Some(9_000),
                budget_source: Some("auto_p95_4w".to_string()),
                projected_exhaustion_secs: Some(1800),
            },
            &cleanup,
        );

        let rate_limit = orchestrator.rate_limit.expect("rate limit");
        assert_eq!(rate_limit.seconds_to_reset, Some(3600));
        assert_eq!(rate_limit.requests_remaining, Some(2));
        assert_eq!(rate_limit.tokens_remaining, Some(9_000));
        assert_eq!(rate_limit.budget_source.as_deref(), Some("auto_p95_4w"));
        assert_eq!(rate_limit.projected_exhaustion_secs, Some(1800));
    }

    #[test]
    fn postmortem_ready_does_not_lazy_create_tmux_pane() {
        let mut orchestrator = test_orchestrator(4);
        let cleanup = Arc::new(Mutex::new(Vec::new()));

        orchestrator.handle_event(
            &WatchEvent::PostmortemReady {
                session_id: "session_idle".to_string(),
                idle_secs: 90,
                total_tokens: 123,
                total_turns: 1,
            },
            &cleanup,
        );

        assert!(orchestrator.panes.is_empty());
    }
}
