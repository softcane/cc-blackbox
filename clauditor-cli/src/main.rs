mod tmux;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, ErrorKind, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use clap::{Parser, Subcommand};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

const RUN_FINAL_POSTMORTEM_ATTEMPTS: usize = 30;
const RUN_FINAL_POSTMORTEM_RETRY_DELAY_MS: u64 = 500;

#[derive(Debug, Parser)]
#[command(
    name = "clauditor",
    version,
    about = "CLI for clauditor observability proxy"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Check local developer prerequisites and stack health
    Doctor,

    /// Start the local Clauditor stack
    Up {
        /// Start without Grafana once compose profiles support it
        #[arg(long)]
        no_grafana: bool,
    },

    /// Run a command through the local Clauditor proxy
    Run {
        /// Start clauditor watch alongside the child command
        #[arg(long)]
        watch: bool,

        /// Command and arguments to run
        #[arg(required = true, num_args = 1.., trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },

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

        /// Render redacted postmortems automatically in watch output
        #[arg(long, conflicts_with = "no_postmortem")]
        postmortem: bool,

        /// Deprecated: watch disables automatic postmortems by default
        #[arg(long)]
        #[arg(hide = true)]
        no_postmortem: bool,

        /// Ask Claude to synthesize automatic postmortems when --postmortem is enabled
        #[arg(long, conflicts_with = "no_analyze_with_claude")]
        analyze_with_claude: bool,

        /// Render automatic postmortems without asking Claude when --postmortem is enabled
        #[arg(long)]
        no_analyze_with_claude: bool,

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

    /// Generate a markdown postmortem for a session
    Postmortem {
        /// Base URL of clauditor-core
        #[arg(long, default_value = "http://localhost:9091")]
        url: String,

        /// Session id, or "last" for the latest completed session
        target: String,

        /// Deprecated: postmortems redact by default
        #[arg(long, hide = true, conflicts_with = "no_redact")]
        redact: bool,

        /// Render local unredacted evidence
        #[arg(long)]
        no_redact: bool,

        /// Ask Claude to synthesize the postmortem (default)
        #[arg(long, conflicts_with = "no_analyze_with_claude")]
        analyze_with_claude: bool,

        /// Render only the deterministic local postmortem
        #[arg(long)]
        no_analyze_with_claude: bool,

        /// Write markdown to this file instead of stdout
        #[arg(long)]
        output: Option<PathBuf>,
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
        cache_expires_at_latest_epoch: Option<u64>,
        #[serde(default)]
        cache_ttl_source: Option<String>,
        #[serde(default)]
        cache_ttl_mixed: Option<bool>,
        #[serde(default)]
        estimated_rebuild_cost_dollars: Option<f64>,
    },
    RequestError {
        session_id: String,
        error_type: String,
        message: String,
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
    PostmortemReady {
        session_id: String,
        idle_secs: u64,
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
    #[serde(rename = "quota_burn_status", alias = "rate_limit_status")]
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

const ENVOY_PROXY_URL: &str = "http://127.0.0.1:10000";
const CLAUDITOR_CORE_URL: &str = "http://127.0.0.1:9091";
const CLAUDITOR_CORE_HEALTH_URL: &str = "http://127.0.0.1:9091/health";
const GRAFANA_URL: &str = "http://127.0.0.1:3000";
const GRAFANA_DASHBOARD_URL: &str = "http://127.0.0.1:3000/d/clauditor-main";
const DEFAULT_CORE_IMAGE: &str = concat!(
    "ghcr.io/softcane/clauditor-core:v",
    env!("CARGO_PKG_VERSION")
);
const BUNDLED_ENVOY_YAML: &str = include_str!("../../envoy/envoy.yaml");
const BUNDLED_PROMETHEUS_YAML: &str = include_str!("../../prometheus/prometheus.yml");
const BUNDLED_GRAFANA_DASHBOARD_PROVIDER_YAML: &str =
    include_str!("../../grafana/provisioning/dashboards/clauditor.yml");
const BUNDLED_GRAFANA_PROMETHEUS_DATASOURCE_YAML: &str =
    include_str!("../../grafana/provisioning/datasources/prometheus.yml");
const BUNDLED_GRAFANA_DASHBOARD_JSON: &str =
    include_str!("../../grafana/dashboards/clauditor.json");

#[derive(Debug, Clone)]
struct ComposeCommand {
    program: String,
    args: Vec<String>,
    display: String,
}

#[derive(Debug)]
enum PortState {
    Available,
    ClauditorService(String),
    Busy,
}

#[derive(Debug)]
enum WatchHandle {
    Plain(Child),
    TmuxSession(String),
    #[cfg(test)]
    Test(std::sync::Arc<std::sync::Mutex<Vec<String>>>),
}

impl WatchHandle {
    fn stop(&mut self) {
        match self {
            WatchHandle::Plain(child) => {
                let _ = child.kill();
                let _ = child.wait();
            }
            WatchHandle::TmuxSession(session) => {
                let _ = Command::new("tmux")
                    .args(["kill-session", "-t", session])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            #[cfg(test)]
            WatchHandle::Test(events) => {
                events
                    .lock()
                    .expect("test watch events lock")
                    .push("stop".to_string());
            }
        }
    }
}

fn envoy_proxy_url() -> String {
    std::env::var("CLAUDITOR_ENVOY_PROXY_URL").unwrap_or_else(|_| ENVOY_PROXY_URL.to_string())
}

fn clauditor_core_url() -> String {
    std::env::var("CLAUDITOR_CORE_URL").unwrap_or_else(|_| CLAUDITOR_CORE_URL.to_string())
}

fn clauditor_core_health_url() -> String {
    std::env::var("CLAUDITOR_CORE_HEALTH_URL")
        .unwrap_or_else(|_| format!("{}/health", clauditor_core_url().trim_end_matches('/')))
}

fn command_exists(name: &str) -> bool {
    command_path(name).is_some()
}

fn command_path(name: &str) -> Option<PathBuf> {
    let path = Path::new(name);
    if path.components().count() > 1 {
        return is_executable_file(path).then(|| path.to_path_buf());
    }

    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        let candidate = dir.join(name);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
    }
    None
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn run_quiet(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn docker_daemon_running() -> bool {
    command_exists("docker") && run_quiet("docker", &["info"])
}

fn docker_compose_command() -> Option<ComposeCommand> {
    if command_exists("docker") && run_quiet("docker", &["compose", "version"]) {
        return Some(ComposeCommand {
            program: "docker".to_string(),
            args: vec!["compose".to_string()],
            display: "docker compose".to_string(),
        });
    }

    if run_quiet("docker-compose", &["version"]) {
        return Some(ComposeCommand {
            program: "docker-compose".to_string(),
            args: Vec::new(),
            display: "docker-compose".to_string(),
        });
    }

    None
}

fn is_clauditor_repo_root(path: &Path) -> bool {
    path.join("clauditor-core").is_dir() && path.join("envoy").is_dir()
}

fn find_repo_compose_file() -> Option<PathBuf> {
    let mut starts = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        starts.push(cwd);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            starts.push(parent.to_path_buf());
        }
    }
    starts.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));

    for start in starts {
        for ancestor in start.ancestors() {
            if !is_clauditor_repo_root(ancestor) {
                continue;
            }
            for name in [
                "docker-compose.yml",
                "docker-compose.yaml",
                "compose.yaml",
                "compose.yml",
            ] {
                let candidate = ancestor.join(name);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
    }

    None
}

fn clauditor_data_dir() -> Result<PathBuf, String> {
    if let Some(dir) = std::env::var_os("CLAUDITOR_HOME") {
        return Ok(PathBuf::from(dir));
    }
    if let Some(dir) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(dir).join("clauditor"));
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".local/share/clauditor"));
    }
    Err("Could not determine a data directory; set CLAUDITOR_HOME.".to_string())
}

fn yaml_quote(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

fn yaml_quote_volume(source: &Path, target: &str, mode: &str) -> String {
    yaml_quote(&format!("{}:{target}:{mode}", source.to_string_lossy()))
}

fn write_if_changed(path: &Path, contents: &str) -> Result<(), String> {
    if path.is_file() {
        if let Ok(existing) = fs::read_to_string(path) {
            if existing == contents {
                return Ok(());
            }
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create {}: {}", parent.display(), err))?;
    }
    fs::write(path, contents).map_err(|err| format!("failed to write {}: {}", path.display(), err))
}

fn bundled_compose_yaml(stack_dir: &Path) -> String {
    let envoy_config = yaml_quote_volume(
        &stack_dir.join("envoy/envoy.yaml"),
        "/etc/envoy/envoy.yaml",
        "ro",
    );
    let prometheus_config = yaml_quote_volume(
        &stack_dir.join("prometheus/prometheus.yml"),
        "/etc/prometheus/prometheus.yml",
        "ro",
    );
    let grafana_provisioning = yaml_quote_volume(
        &stack_dir.join("grafana/provisioning"),
        "/etc/grafana/provisioning",
        "ro",
    );
    let grafana_dashboards = yaml_quote_volume(
        &stack_dir.join("grafana/dashboards"),
        "/var/lib/grafana/dashboards",
        "ro",
    );

    format!(
        r#"services:
  envoy:
    image: envoyproxy/envoy:v1.32-latest
    volumes:
      - {envoy_config}
    ports:
      - "127.0.0.1:10000:10000"
    depends_on:
      clauditor-core:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'echo > /dev/tcp/localhost/10000'"]
      interval: 5s
      timeout: 3s
      retries: 5

  clauditor-core:
    image: ${{CLAUDITOR_CORE_IMAGE:-{DEFAULT_CORE_IMAGE}}}
    expose:
      - "50051"
    ports:
      - "127.0.0.1:9091:9090"
    environment:
      - RUST_LOG=info
      - CLAUDITOR_SESSION_BUDGET_DOLLARS=0
      - CLAUDITOR_SESSION_BUDGET_TOKENS=0
      - CLAUDITOR_CIRCUIT_BREAKER_THRESHOLD=5
    volumes:
      - clauditor_data:/data
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:9090/health"]
      interval: 5s
      timeout: 3s
      retries: 5

  prometheus:
    image: prom/prometheus:v2.52.0
    ports:
      - "127.0.0.1:9092:9090"
    volumes:
      - {prometheus_config}
      - prometheus_data:/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"
      - "--storage.tsdb.retention.time=30d"
      - "--web.enable-lifecycle"
    depends_on:
      clauditor-core:
        condition: service_healthy

  grafana:
    image: grafana/grafana:11.1.0
    ports:
      - "127.0.0.1:3000:3000"
    volumes:
      - {grafana_provisioning}
      - {grafana_dashboards}
      - grafana_data:/var/lib/grafana
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_USERS_ALLOW_SIGN_UP=false
    depends_on:
      - prometheus

volumes:
  clauditor_data:
  prometheus_data:
  grafana_data:
"#
    )
}

fn prepare_bundled_stack() -> Result<PathBuf, String> {
    let stack_dir = clauditor_data_dir()?
        .join("stack")
        .join(env!("CARGO_PKG_VERSION"));
    write_if_changed(&stack_dir.join("envoy/envoy.yaml"), BUNDLED_ENVOY_YAML)?;
    write_if_changed(
        &stack_dir.join("prometheus/prometheus.yml"),
        BUNDLED_PROMETHEUS_YAML,
    )?;
    write_if_changed(
        &stack_dir.join("grafana/provisioning/dashboards/clauditor.yml"),
        BUNDLED_GRAFANA_DASHBOARD_PROVIDER_YAML,
    )?;
    write_if_changed(
        &stack_dir.join("grafana/provisioning/datasources/prometheus.yml"),
        BUNDLED_GRAFANA_PROMETHEUS_DATASOURCE_YAML,
    )?;
    write_if_changed(
        &stack_dir.join("grafana/dashboards/clauditor.json"),
        BUNDLED_GRAFANA_DASHBOARD_JSON,
    )?;
    let compose_path = stack_dir.join("docker-compose.yml");
    write_if_changed(&compose_path, &bundled_compose_yaml(&stack_dir))?;
    Ok(compose_path)
}

fn resolve_compose_file() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os("CLAUDITOR_COMPOSE_FILE") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "CLAUDITOR_COMPOSE_FILE points to {}, but that file does not exist.",
            path.display()
        ));
    }

    let force_bundled = std::env::var_os("CLAUDITOR_USE_BUNDLED_STACK").is_some();
    if !force_bundled {
        if let Some(path) = find_repo_compose_file() {
            return Ok(path);
        }
    }

    prepare_bundled_stack()
}

fn is_port_available(port: u16) -> bool {
    let loopback_addrs = [
        SocketAddr::from(([127, 0, 0, 1], port)),
        SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], port)),
    ];

    for addr in loopback_addrs {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return false;
        }
    }

    let v4_free = TcpListener::bind(("127.0.0.1", port)).is_ok();
    let v6_free = match TcpListener::bind(("::1", port)) {
        Ok(_) => true,
        Err(err) if err.kind() == ErrorKind::AddrNotAvailable => true,
        Err(_) => false,
    };

    v4_free && v6_free
}

fn clauditor_container_for_port(port: u16) -> Option<String> {
    let output = Command::new("docker")
        .args(["ps", "--format", "{{.Names}}\t{{.Ports}}"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let marker = format!(":{port}->");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let Some((name, ports)) = line.split_once('\t') else {
            continue;
        };
        if name.to_ascii_lowercase().contains("clauditor") && ports.contains(&marker) {
            return Some(name.to_string());
        }
    }

    None
}

fn port_state(port: u16) -> PortState {
    if is_port_available(port) {
        PortState::Available
    } else if let Some(container) = clauditor_container_for_port(port) {
        PortState::ClauditorService(container)
    } else {
        PortState::Busy
    }
}

async fn health_check(url: &str) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    else {
        return false;
    };

    client
        .get(url)
        .send()
        .await
        .map(|resp| resp.status().is_success())
        .unwrap_or(false)
}

async fn wait_for_health(url: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if health_check(url).await {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    false
}

fn print_check(symbol: &str, message: impl AsRef<str>) {
    println!("{} {}", symbol, message.as_ref());
}

fn push_unique(lines: &mut Vec<String>, line: impl Into<String>) {
    let line = line.into();
    if !lines.iter().any(|existing| existing == &line) {
        lines.push(line);
    }
}

async fn run_doctor() -> i32 {
    println!("Clauditor doctor");
    println!();

    let mut failed = false;
    let mut fixes = Vec::new();

    print_check("✓", format!("clauditor {}", env!("CARGO_PKG_VERSION")));

    let docker_found = command_exists("docker");
    if docker_found {
        print_check("✓", "docker found");
    } else {
        failed = true;
        print_check("✗", "docker not found in PATH");
        push_unique(
            &mut fixes,
            "Install Docker Desktop or Docker Engine, then ensure `docker` is on PATH.",
        );
    }

    if docker_found && docker_daemon_running() {
        print_check("✓", "docker daemon running");
    } else {
        failed = true;
        print_check("✗", "docker daemon not reachable");
        push_unique(
            &mut fixes,
            "Start Docker Desktop or your Docker daemon, then rerun `clauditor up`.",
        );
    }

    if let Some(compose) = docker_compose_command() {
        print_check(
            "✓",
            format!("docker compose available ({})", compose.display),
        );
    } else {
        failed = true;
        print_check("✗", "docker compose not available");
        push_unique(
            &mut fixes,
            "Install Docker Compose v2 or make `docker-compose` available on PATH.",
        );
    }

    if command_exists("claude") {
        print_check("✓", "claude found");
    } else {
        failed = true;
        print_check("✗", "claude not found in PATH");
        push_unique(
            &mut fixes,
            "Install Claude Code and ensure the `claude` command is on PATH.",
        );
    }

    if command_exists("tmux") {
        print_check("✓", "tmux found");
    } else {
        print_check("⚠", "tmux not found; --tmux watch mode will not work");
    }

    for port in [10000, 9091, 3000] {
        match port_state(port) {
            PortState::Available => print_check("✓", format!("port {port} available")),
            PortState::ClauditorService(container) => {
                print_check("✓", format!("port {port} used by Clauditor ({container})"));
            }
            PortState::Busy => {
                failed = true;
                print_check("✗", format!("port {port} is already in use"));
                push_unique(
                    &mut fixes,
                    format!(
                        "Free port {port}, or stop the process using it before running `clauditor up`."
                    ),
                );
            }
        }
    }

    let core_healthy = health_check(CLAUDITOR_CORE_HEALTH_URL).await;
    if core_healthy {
        print_check("✓", "clauditor-core healthy");
    } else {
        failed = true;
        print_check("✗", "clauditor-core not healthy");
        push_unique(&mut fixes, "Run: clauditor up");
    }

    if health_check(GRAFANA_URL).await {
        print_check("✓", "Grafana reachable");
    } else {
        failed = true;
        print_check("✗", "Grafana not reachable");
        push_unique(&mut fixes, "Run: clauditor up");
    }

    match std::env::var("ANTHROPIC_BASE_URL") {
        Ok(value) if value == ENVOY_PROXY_URL => {
            print_check("✓", "ANTHROPIC_BASE_URL points at Clauditor");
        }
        Ok(value) => {
            print_check(
                "⚠",
                format!("ANTHROPIC_BASE_URL is {value}; expected {ENVOY_PROXY_URL}"),
            );
            push_unique(
                &mut fixes,
                format!("export ANTHROPIC_BASE_URL={ENVOY_PROXY_URL}"),
            );
        }
        Err(_) => {
            print_check("⚠", "ANTHROPIC_BASE_URL unset");
            push_unique(
                &mut fixes,
                format!("export ANTHROPIC_BASE_URL={ENVOY_PROXY_URL}"),
            );
        }
    }

    if !fixes.is_empty() {
        println!();
        println!("Fix:");
        for fix in fixes {
            println!("  {fix}");
        }
    }

    if failed {
        1
    } else {
        0
    }
}

async fn start_stack(no_grafana: bool) -> Result<(), String> {
    if no_grafana {
        // TODO: make Grafana optional through a compose profile without
        // changing the default all-in-one local stack.
        println!("⚠ --no-grafana is not implemented yet; starting the default stack");
    }

    let compose = docker_compose_command()
        .ok_or_else(|| "docker compose is not available. Run `clauditor doctor`.".to_string())?;

    if command_exists("docker") && !docker_daemon_running() {
        return Err(
            "Docker daemon is not reachable. Start Docker Desktop or your Docker daemon first."
                .to_string(),
        );
    }

    let compose_file = resolve_compose_file()?;
    let compose_root = compose_file
        .parent()
        .ok_or_else(|| "compose file has no parent directory".to_string())?;

    println!("Starting Clauditor stack with {}...", compose.display);
    let _ = io::stdout().flush();
    let mut command = Command::new(&compose.program);
    command
        .args(&compose.args)
        .args(["-p", "clauditor"])
        .arg("-f")
        .arg(&compose_file)
        .args(["up", "-d"])
        .current_dir(compose_root)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = command
        .status()
        .map_err(|err| format!("failed to run {}: {}", compose.display, err))?;
    if !status.success() {
        return Err(format!("{} up -d failed", compose.display));
    }

    println!("Waiting for clauditor-core health...");
    let _ = io::stdout().flush();
    if !wait_for_health(CLAUDITOR_CORE_HEALTH_URL, Duration::from_secs(90)).await {
        return Err(format!(
            "clauditor-core did not become healthy at {CLAUDITOR_CORE_HEALTH_URL}"
        ));
    }

    Ok(())
}

async fn run_up(no_grafana: bool) -> i32 {
    match start_stack(no_grafana).await {
        Ok(()) => {
            println!();
            println!("Clauditor is up.");
            println!("  Envoy proxy:    {ENVOY_PROXY_URL}");
            println!("  Clauditor core: {CLAUDITOR_CORE_URL}");
            println!("  Grafana:        {GRAFANA_DASHBOARD_URL}");
            println!();
            println!("Next:");
            println!("  clauditor run claude --watch");
            0
        }
        Err(err) => {
            eprintln!("Error: {err}");
            1
        }
    }
}

fn extract_run_watch(watch_flag: bool, command: Vec<String>) -> (bool, Vec<String>) {
    let mut watch = watch_flag;
    let mut child_command = Vec::with_capacity(command.len());
    for arg in command {
        if arg == "--watch" {
            watch = true;
        } else {
            child_command.push(arg);
        }
    }
    (watch, child_command)
}

async fn ensure_stack_running() -> Result<(), String> {
    let health_url = clauditor_core_health_url();
    if health_check(&health_url).await {
        return Ok(());
    }

    println!("clauditor-core is not healthy; starting the local stack...");
    start_stack(false).await
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

fn current_cli_path() -> String {
    std::env::current_exe()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "clauditor".to_string())
}

fn tmux_session_name() -> String {
    format!("clauditor-watch-{}", std::process::id())
}

fn watcher_args(core_url: String, tmux: bool, postmortem: bool) -> Vec<String> {
    let mut args = vec!["watch".to_string()];
    if tmux {
        args.push("--tmux".to_string());
    }
    args.extend(["--url".to_string(), core_url]);
    if postmortem {
        args.push("--postmortem".to_string());
    }
    args
}

fn start_watcher(postmortem: bool) -> Result<WatchHandle, String> {
    let cli_path = current_cli_path();
    let core_url = clauditor_core_url();
    if command_exists("tmux") {
        let session = tmux_session_name();
        let mut command_parts = vec![cli_path];
        command_parts.extend(watcher_args(core_url.clone(), true, postmortem));
        let command = shell_join(&command_parts);
        let status = Command::new("tmux")
            .args(["new-session", "-d", "-s", &session, &command])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .status()
            .map_err(|err| format!("failed to start tmux watcher: {err}"))?;
        if !status.success() {
            return Err("failed to start tmux watcher".to_string());
        }
        println!("Watch: tmux attach -t {session}");
        return Ok(WatchHandle::TmuxSession(session));
    }

    let args = watcher_args(core_url, false, postmortem);
    let child = Command::new(cli_path)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| format!("failed to start plain watcher: {err}"))?;
    println!("Watch: plain mode");
    Ok(WatchHandle::Plain(child))
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

fn run_command_with_env(
    command: &str,
    args: &[String],
    envs: &[(&str, &str)],
) -> Result<i32, String> {
    let mut child = Command::new(command);
    child.args(args);
    for (key, value) in envs {
        child.env(key, value);
    }
    let status = child
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| format!("failed to spawn {command}: {err}"))?
        .wait()
        .map_err(|err| format!("failed while waiting for {command}: {err}"))?;

    Ok(exit_code(status))
}

async fn run_child_command_with_deps<Ensure, EnsureFut, Start, Run, Render, RenderFut>(
    watch_flag: bool,
    command: Vec<String>,
    ensure_stack: Ensure,
    start_watcher_fn: Start,
    run_command_fn: Run,
    render_final_postmortem: Render,
) -> i32
where
    Ensure: FnOnce() -> EnsureFut,
    EnsureFut: std::future::Future<Output = Result<(), String>>,
    Start: FnOnce(bool) -> Result<WatchHandle, String>,
    Run: FnOnce(&str, &[String], &[(&str, &str)]) -> Result<i32, String>,
    Render: FnOnce(String) -> RenderFut,
    RenderFut: std::future::Future<Output = ()>,
{
    let (watch, child_command) = extract_run_watch(watch_flag, command);
    if child_command.is_empty() {
        eprintln!("Error: missing command after `clauditor run`");
        return 1;
    }

    if let Err(err) = ensure_stack().await {
        eprintln!("Error: {err}");
        return 1;
    }

    let mut watcher = if watch {
        match start_watcher_fn(false) {
            Ok(handle) => Some(handle),
            Err(err) => {
                eprintln!("Error: {err}");
                return 1;
            }
        }
    } else {
        None
    };

    let command_name = &child_command[0];
    let command_args = &child_command[1..];
    let proxy_url = envoy_proxy_url();
    let result = run_command_fn(
        command_name,
        command_args,
        &[("ANTHROPIC_BASE_URL", proxy_url.as_str())],
    );

    if let Some(handle) = watcher.as_mut() {
        handle.stop();
    }

    if watch && result.is_ok() {
        render_final_postmortem(clauditor_core_url()).await;
    }

    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("Error: {err}");
            1
        }
    }
}

async fn run_child_command(watch_flag: bool, command: Vec<String>) -> i32 {
    run_child_command_with_deps(
        watch_flag,
        command,
        ensure_stack_running,
        start_watcher,
        run_command_with_env,
        |base_url| async move {
            render_run_final_postmortem(&base_url).await;
        },
    )
    .await
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
/// events (Lagged, quota-burn status).
pub(crate) fn event_session_id(event: &WatchEvent) -> Option<&str> {
    match event {
        WatchEvent::ToolUse { session_id, .. }
        | WatchEvent::ToolResult { session_id, .. }
        | WatchEvent::SkillEvent { session_id, .. }
        | WatchEvent::McpEvent { session_id, .. }
        | WatchEvent::CacheEvent { session_id, .. }
        | WatchEvent::SessionStart { session_id, .. }
        | WatchEvent::SessionEnd { session_id, .. }
        | WatchEvent::PostmortemReady { session_id, .. }
        | WatchEvent::FrustrationSignal { session_id, .. }
        | WatchEvent::CompactionLoop { session_id, .. }
        | WatchEvent::Diagnosis { session_id, .. }
        | WatchEvent::CacheWarning { session_id, .. }
        | WatchEvent::RequestError { session_id, .. }
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

struct WatchPostmortemState {
    enabled: bool,
    base_url: String,
    analyze_with_claude: bool,
    rendered: HashSet<String>,
}

impl WatchPostmortemState {
    fn new(enabled: bool, base_url: &str, analyze_with_claude: bool) -> Self {
        Self {
            enabled,
            base_url: base_url.to_string(),
            analyze_with_claude,
            rendered: HashSet::new(),
        }
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

fn print_postmortem_progress(tag: &str, session_id: &str, analyze_with_claude: bool) {
    let work = if analyze_with_claude {
        "fetching redacted report + running Claude analysis"
    } else {
        "fetching redacted report"
    };
    print_tagged(
        tag,
        &format!("POSTMORTEM IN PROGRESS · {work} · {session_id}")
            .cyan()
            .bold()
            .to_string(),
    );
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

        WatchEvent::PostmortemReady {
            idle_secs,
            total_tokens,
            total_turns,
            ..
        } => {
            print_tagged(
                &tag,
                &format!(
                    "POSTMORTEM READY \u{00b7} idle {} \u{00b7} {} tokens \u{00b7} {} turns",
                    format_duration_coarse(*idle_secs),
                    format_tokens(*total_tokens),
                    total_turns
                )
                .dimmed()
                .to_string(),
            );
        }

        WatchEvent::ToolUse {
            session_id: _,
            timestamp,
            tool_name,
            summary,
        } => {
            let time = local_time_from_iso(timestamp);
            if let Some((server, tool)) = parse_mcp_tool_name(tool_name) {
                let summary_display = truncate_for_box(summary, 80);
                let line = if summary_display.is_empty() {
                    format!("{}  MCP     {}.{}", time, server, tool)
                } else {
                    format!("{}  MCP     {}.{}  {}", time, server, tool, summary_display)
                };
                print_tagged(&tag, &line.cyan().to_string());
                return;
            }
            let label = format!("{:<6}", tool_name.to_uppercase());
            let summary_display = truncate_for_box(summary, 80);

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
            cache_expires_at_latest_epoch,
            cache_ttl_source,
            cache_ttl_mixed,
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
                "miss_rebuild" => format!("{}  CACHE   \u{25cb} rebuild", time)
                    .yellow()
                    .dimmed()
                    .to_string(),
                "miss_ttl" => format!("{}  CACHE   \u{25cb} miss (TTL inferred)", time)
                    .yellow()
                    .to_string(),
                "miss_thrash" => format!("{}  CACHE   \u{25cb} miss (thrash inferred)", time)
                    .red()
                    .dimmed()
                    .to_string(),
                other => format!("{}  CACHE   {}", time, other).dimmed().to_string(),
            };
            // Append TTL and estimated rebuild details when we have them.
            let mut line = base;
            if let Some(exp) = cache_expires_at_epoch {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let remaining = exp.saturating_sub(now);
                let mins = remaining / 60;
                let secs = remaining % 60;
                let expiry_text = if cache_ttl_mixed.unwrap_or(false) {
                    if let Some(latest) = cache_expires_at_latest_epoch {
                        let latest_remaining = latest.saturating_sub(now);
                        format!(
                            "first expires in {}m{:02}s; latest {}m{:02}s",
                            mins,
                            secs,
                            latest_remaining / 60,
                            latest_remaining % 60
                        )
                    } else {
                        format!("first expires in {}m{:02}s", mins, secs)
                    }
                } else {
                    format!("expires in {}m{:02}s", mins, secs)
                };
                let source_note = cache_ttl_source
                    .as_deref()
                    .filter(|source| source.contains("mixed"))
                    .map(|_| " (mixed TTL)")
                    .unwrap_or("");
                let suffix = if let Some(c) = estimated_rebuild_cost_dollars {
                    format!(
                        "  \u{00b7} {}{} \u{00b7} est. rebuild ${:.2}",
                        expiry_text, source_note, c
                    )
                } else {
                    format!("  \u{00b7} {}{}", expiry_text, source_note)
                };
                line.push_str(&suffix.dimmed().to_string());
            }
            print_tagged(&tag, &line);
        }

        WatchEvent::RequestError {
            session_id: _,
            error_type,
            message,
        } => {
            let time = now_hms();
            print_tagged(
                &tag,
                &format!("{}  \u{2717} API ERROR  {}: {}", time, error_type, message)
                    .red()
                    .bold()
                    .to_string(),
            );
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
                    "{}  \u{26a0}  MODEL ROUTE  requested {}, got {}",
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
                Some(n) if *n == 0 => "inferred at auto-compaction threshold".to_string(),
                Some(n) => format!("inferred ~{} turns to auto-compact", n),
                None => "inferred trajectory unknown".to_string(),
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
        "api_error" => "\u{2717}",                            // ✗
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
            "SESSION COMPLETE \u{00b7} {} turns \u{00b7} {} tokens \u{00b7} cache reuse {:.0}% \u{00b7} {}",
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
            format!("  {}", "[heuristic]".dimmed())
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

fn json_str<'a>(value: &'a serde_json::Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(|value| value.as_str())
}

fn json_u64(value: &serde_json::Value, pointer: &str) -> Option<u64> {
    value.pointer(pointer).and_then(|value| value.as_u64())
}

fn json_f64(value: &serde_json::Value, pointer: &str) -> Option<f64> {
    value.pointer(pointer).and_then(|value| value.as_f64())
}

fn json_bool(value: &serde_json::Value, pointer: &str) -> Option<bool> {
    value.pointer(pointer).and_then(|value| value.as_bool())
}

fn format_money(value: Option<f64>) -> String {
    value
        .map(|amount| format!("${amount:.2}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn format_percent(value: Option<f64>) -> String {
    value
        .map(|amount| format!("{:.0}%", amount * 100.0))
        .unwrap_or_else(|| "unknown".to_string())
}

fn pluralize(count: u64, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("{count} {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn join_limited(items: &[String], limit: usize) -> String {
    if items.is_empty() {
        return "none".to_string();
    }
    let mut visible = items.iter().take(limit).cloned().collect::<Vec<_>>();
    if items.len() > limit {
        visible.push(format!("+{} more", items.len() - limit));
    }
    visible.join(", ")
}

fn is_no_cause(cause: &str) -> bool {
    let cause = cause.trim();
    cause.is_empty() || cause.eq_ignore_ascii_case("none")
}

fn human_cause(cause: &str, is_heuristic: bool) -> String {
    let label = match cause {
        cause if is_no_cause(cause) => return "No degradation detected".to_string(),
        "api_error" => "API error".to_string(),
        "cache_miss_ttl" => "Cache likely expired after idle time".to_string(),
        "cache_miss_thrash" => "Repeated cache rebuilds".to_string(),
        "compaction_suspected" => "Possible compaction loop".to_string(),
        "context_bloat" => "Context is getting large".to_string(),
        "harness_pressure" => "Harness pressure".to_string(),
        "model_fallback" => "Model route changed".to_string(),
        "near_compaction" => "Near auto-compaction".to_string(),
        "tool_failure_streak" => "Tool failure loop".to_string(),
        other => other.replace('_', " "),
    };
    if is_heuristic {
        format!("{label} [heuristic]")
    } else {
        label
    }
}

fn cache_status_label(ratio: Option<f64>) -> &'static str {
    match ratio {
        Some(value) if value >= 0.90 => "Healthy",
        Some(value) if value >= 0.60 => "Mixed",
        Some(_) => "Low",
        None => "Unknown",
    }
}

fn cache_signal(report: &serde_json::Value) -> String {
    let reusable = json_f64(report, "/signals/cache/cache_reusable_prefix_ratio")
        .or_else(|| json_f64(report, "/signals/cache/cache_hit_ratio"));
    let input_cache = json_f64(report, "/signals/cache/total_input_cache_rate");
    format!(
        "{}: {} reusable prompt cache; {} of input from cache",
        cache_status_label(reusable),
        format_percent(reusable),
        format_percent(input_cache)
    )
}

fn runway_text(turns_to_compact: Option<u64>) -> String {
    match turns_to_compact {
        Some(0) => "auto-compaction may start now".to_string(),
        Some(turns) => format!(
            "about {} before auto-compaction",
            pluralize(turns, "turn", "turns")
        ),
        None => "auto-compaction estimate unknown".to_string(),
    }
}

fn context_signal(report: &serde_json::Value) -> String {
    let fill = json_f64(report, "/signals/context/max_fill_percent").unwrap_or(0.0);
    let status = if fill >= 80.0 {
        "High"
    } else if fill >= 60.0 {
        "Growing"
    } else {
        "Plenty of room"
    };
    format!(
        "{status}: {:.0}% full; {}",
        fill,
        runway_text(json_u64(report, "/signals/context/turns_to_compact"))
    )
}

fn waste_signal(report: &serde_json::Value) -> String {
    let wasted_tokens = json_u64(report, "/impact/estimated_likely_wasted_tokens").unwrap_or(0);
    let wasted_cost = json_f64(report, "/impact/estimated_likely_wasted_cost_dollars");
    if wasted_tokens == 0 && wasted_cost.unwrap_or(0.0) == 0.0 {
        "No likely wasted tokens detected".to_string()
    } else {
        format!(
            "Likely waste: {} tokens, {}",
            format_tokens(wasted_tokens),
            format_money(wasted_cost)
        )
    }
}

fn tool_signal(report: &serde_json::Value) -> String {
    let Some(tools) = report
        .pointer("/signals/tools")
        .and_then(|value| value.as_array())
    else {
        return "No tool calls recorded".to_string();
    };
    if tools.is_empty() {
        return "No tool calls recorded".to_string();
    }

    let total_calls = tools
        .iter()
        .filter_map(|tool| tool.get("calls").and_then(|value| value.as_u64()))
        .sum::<u64>();
    let total_failures = tools
        .iter()
        .filter_map(|tool| tool.get("failures").and_then(|value| value.as_u64()))
        .sum::<u64>();
    let failing = tools
        .iter()
        .filter_map(|tool| {
            let failures = tool.get("failures").and_then(|value| value.as_u64())?;
            if failures == 0 {
                return None;
            }
            let name = tool.get("tool_name").and_then(|value| value.as_str())?;
            Some(format!("{name} ({failures})"))
        })
        .collect::<Vec<_>>();

    if total_failures > 0 {
        return format!(
            "{}, {}; failing: {}",
            pluralize(total_calls, "call", "calls"),
            pluralize(total_failures, "failure", "failures"),
            join_limited(&failing, 3)
        );
    }

    let repeated = tools
        .iter()
        .filter_map(|tool| {
            let calls = tool.get("calls").and_then(|value| value.as_u64())?;
            (calls >= 3).then(|| {
                tool.get("tool_name")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown")
                    .to_string()
            })
        })
        .collect::<Vec<_>>();
    if repeated.is_empty() {
        format!("{}, 0 failures", pluralize(total_calls, "call", "calls"))
    } else {
        format!(
            "{}, 0 failures; repeated: {}",
            pluralize(total_calls, "call", "calls"),
            join_limited(&repeated, 3)
        )
    }
}

fn failed_event_type(event_type: &str) -> bool {
    let lower = event_type.to_ascii_lowercase();
    lower.contains("fail") || lower.contains("error")
}

fn grouped_event_count(events: &[serde_json::Value]) -> u64 {
    events
        .iter()
        .filter_map(|event| event.get("count").and_then(|value| value.as_u64()))
        .sum()
}

fn skill_signal(report: &serde_json::Value) -> String {
    let Some(events) = report
        .pointer("/signals/skills")
        .and_then(|value| value.as_array())
    else {
        return "No failed skill events detected".to_string();
    };
    let failed = events
        .iter()
        .filter_map(|event| {
            let event_type = event.get("event_type").and_then(|value| value.as_str())?;
            if !failed_event_type(event_type) {
                return None;
            }
            let count = event
                .get("count")
                .and_then(|value| value.as_u64())
                .unwrap_or(1);
            let skill = event
                .get("skill_name")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            Some((count, format!("{skill} ({count})")))
        })
        .collect::<Vec<_>>();
    let failed_count = failed.iter().map(|(count, _)| *count).sum::<u64>();
    if failed_count > 0 {
        let names = failed.into_iter().map(|(_, name)| name).collect::<Vec<_>>();
        return format!(
            "{}; failing: {}",
            pluralize(failed_count, "failed skill event", "failed skill events"),
            join_limited(&names, 3)
        );
    }

    if events.is_empty() {
        "No failed skill events detected".to_string()
    } else {
        format!(
            "{}, 0 failures",
            pluralize(grouped_event_count(events), "skill event", "skill events")
        )
    }
}

fn mcp_event_name(event: &serde_json::Value) -> String {
    let server = event
        .get("server")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let tool = event
        .get("tool")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    format!("{server}.{tool}")
}

fn mcp_signal(report: &serde_json::Value) -> String {
    let Some(events) = report
        .pointer("/signals/mcp")
        .and_then(|value| value.as_array())
    else {
        return "No failed MCP calls detected".to_string();
    };
    let failed = events
        .iter()
        .filter_map(|event| {
            let event_type = event.get("event_type").and_then(|value| value.as_str())?;
            if !failed_event_type(event_type) {
                return None;
            }
            let count = event
                .get("count")
                .and_then(|value| value.as_u64())
                .unwrap_or(1);
            Some((count, format!("{} ({count})", mcp_event_name(event))))
        })
        .collect::<Vec<_>>();
    let failed_count = failed.iter().map(|(count, _)| *count).sum::<u64>();
    if failed_count > 0 {
        let names = failed.into_iter().map(|(_, name)| name).collect::<Vec<_>>();
        return format!(
            "{}; failing: {}",
            pluralize(failed_count, "failed MCP call", "failed MCP calls"),
            join_limited(&names, 3)
        );
    }

    if events.is_empty() {
        "No failed MCP calls detected".to_string()
    } else {
        format!(
            "{}, 0 failures",
            pluralize(grouped_event_count(events), "MCP call", "MCP calls")
        )
    }
}

fn table_cell(value: &str) -> String {
    let single_line = value.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_for_box(&single_line, 140)
}

fn column_widths_for(headers: &[&str]) -> Vec<usize> {
    match headers {
        ["Type", "Signal", "Turn", "Detail"] => vec![10, 12, 5, 0],
        ["Time", "Turn", "Event", "Detail"] => vec![20, 5, 18, 0],
        _ => headers
            .iter()
            .enumerate()
            .map(|(idx, header)| {
                if idx + 1 == headers.len() {
                    0
                } else {
                    header.len().max(8)
                }
            })
            .collect(),
    }
}

fn push_table_header(out: &mut String, headers: &[&str]) {
    let widths = column_widths_for(headers);
    out.push_str("  ");
    for (idx, header) in headers.iter().enumerate() {
        let width = widths.get(idx).copied().unwrap_or(0);
        if width == 0 {
            out.push_str(header);
        } else {
            out.push_str(&format!("{:<width$}", header, width = width));
            out.push_str("  ");
        }
    }
    out.push('\n');
    out.push_str("  ");
    for (idx, header) in headers.iter().enumerate() {
        let width = widths.get(idx).copied().unwrap_or(0);
        let underline_width = if width == 0 {
            header.len().max(6)
        } else {
            width
        };
        out.push_str(&"-".repeat(underline_width));
        if width != 0 {
            out.push_str("  ");
        }
    }
    out.push('\n');
}

fn push_table_row(out: &mut String, cells: &[String]) {
    let widths = match cells.len() {
        4 => {
            let first = cells.first().map(String::as_str).unwrap_or("");
            if first.contains("T") || first == "-" || first.starts_with("202") {
                vec![20, 5, 18, 0]
            } else {
                vec![10, 12, 5, 0]
            }
        }
        2 => vec![3, 0],
        _ => cells
            .iter()
            .enumerate()
            .map(|(idx, cell)| {
                if idx + 1 == cells.len() {
                    0
                } else {
                    cell.len().max(8)
                }
            })
            .collect(),
    };
    out.push_str("  ");
    for (idx, cell) in cells.iter().enumerate() {
        let width = widths.get(idx).copied().unwrap_or(0);
        let value = table_cell(cell);
        if width == 0 {
            out.push_str(&value);
        } else {
            out.push_str(&format!("{:<width$}", value, width = width));
            out.push_str("  ");
        }
    }
    out.push('\n');
}

fn push_key_value_table(out: &mut String, rows: Vec<(&str, String)>) {
    let label_width = rows
        .iter()
        .map(|(field, _)| field.len())
        .max()
        .unwrap_or(5)
        .clamp(10, 22);
    for (field, value) in rows {
        out.push_str(&format!(
            "  {:<label_width$}  {}\n",
            field,
            table_cell(&value),
            label_width = label_width
        ));
    }
    out.push('\n');
}

fn parse_percent_prefix(value: &str) -> Option<f64> {
    value.split_whitespace().find_map(|part| {
        part.trim_matches(|ch: char| !(ch.is_ascii_digit() || ch == '.' || ch == '%'))
            .strip_suffix('%')
            .and_then(|number| number.parse::<f64>().ok())
    })
}

fn colorize_postmortem_value(label: &str, value: &str) -> String {
    let lower = value.to_ascii_lowercase();
    match label {
        "State" => {
            if lower.contains("final") {
                value.green().bold().to_string()
            } else {
                value.yellow().bold().to_string()
            }
        }
        "Outcome" => {
            if lower.contains("degraded") || lower.contains("failed") || lower.contains("error") {
                value.red().bold().to_string()
            } else if lower.contains("complete") {
                value.green().bold().to_string()
            } else if lower.contains("progress") {
                value.yellow().bold().to_string()
            } else {
                value.to_string()
            }
        }
        "Cause" => {
            if lower == "none" || lower.contains("no degradation detected") {
                value.green().bold().to_string()
            } else {
                value.yellow().bold().to_string()
            }
        }
        "Cache" => value.green().to_string(),
        "Context" => match parse_percent_prefix(value) {
            Some(percent) if percent >= 80.0 => value.red().bold().to_string(),
            Some(percent) if percent >= 60.0 => value.yellow().bold().to_string(),
            Some(_) => value.green().to_string(),
            None => value.to_string(),
        },
        "Waste" => {
            if lower.starts_with("0 tokens") || lower.contains("no likely wasted tokens") {
                value.green().to_string()
            } else {
                value.red().bold().to_string()
            }
        }
        "Tools" | "Skills" | "MCP" => {
            if lower.contains("fail")
                && !lower.contains("0 failures")
                && !lower.starts_with("no failed")
            {
                value.red().bold().to_string()
            } else {
                value.green().to_string()
            }
        }
        "Cost" => value.magenta().to_string(),
        "Risk" => {
            if lower.contains("high") {
                value.red().bold().to_string()
            } else if lower.contains("medium") {
                value.yellow().bold().to_string()
            } else if lower.contains("low") {
                value.green().bold().to_string()
            } else {
                value.to_string()
            }
        }
        "Next" | "Next action" => value.bright_white().to_string(),
        _ => value.to_string(),
    }
}

fn colorize_aligned_key_value_line(line: &str) -> Option<String> {
    let indent_len = line.len() - line.trim_start_matches(' ').len();
    if indent_len == 0 {
        return None;
    }
    let indent = &line[..indent_len];
    let rest = &line[indent_len..];
    let split_at = rest.find("  ")?;
    let label_area = &rest[..split_at];
    let label = label_area.trim_end();
    if label.is_empty() {
        return None;
    }
    let value_area = &rest[split_at..];
    let value = value_area.trim_start();
    if value.is_empty() {
        return None;
    }

    let padding_len = (label_area.len() - label.len()) + (value_area.len() - value.len());
    Some(format!(
        "{}{}{}{}",
        indent,
        label.bright_blue().bold(),
        " ".repeat(padding_len),
        colorize_postmortem_value(label, value)
    ))
}

fn colorize_evidence_type(kind: &str) -> String {
    match kind {
        "direct" => kind.green().bold().to_string(),
        "heuristic" => kind.yellow().bold().to_string(),
        "derived" => kind.cyan().bold().to_string(),
        "-" => kind.bright_black().to_string(),
        _ => kind.bright_white().to_string(),
    }
}

fn colorize_evidence_line(line: &str) -> Option<String> {
    let indent_len = line.len() - line.trim_start_matches(' ').len();
    let indent = &line[..indent_len];
    let rest = &line[indent_len..];
    let kind_end = rest.find(char::is_whitespace)?;
    let kind = &rest[..kind_end];
    if !matches!(kind, "direct" | "heuristic" | "derived" | "-") {
        return None;
    }
    Some(format!(
        "{}{}{}",
        indent,
        colorize_evidence_type(kind),
        &rest[kind_end..]
    ))
}

fn colorize_postmortem_line(line: &str) -> String {
    let trimmed = line.trim_start();
    if line.starts_with("# ") {
        return line.bright_cyan().bold().to_string();
    }
    if line.starts_with("## ") {
        return line.cyan().bold().to_string();
    }
    if trimmed.starts_with("Type") && trimmed.contains("Signal") {
        return line.bright_black().bold().to_string();
    }
    if !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|ch| ch == '-' || ch.is_ascii_whitespace())
    {
        return line.bright_black().to_string();
    }
    if let Some(colored) = colorize_evidence_line(line) {
        return colored;
    }
    if let Some(colored) = colorize_aligned_key_value_line(line) {
        return colored;
    }
    line.to_string()
}

fn colorize_postmortem_for_terminal(markdown: &str) -> String {
    let mut colored = markdown
        .lines()
        .map(colorize_postmortem_line)
        .collect::<Vec<_>>()
        .join("\n");
    if markdown.ends_with('\n') {
        colored.push('\n');
    }
    colored
}

#[cfg(unix)]
fn terminal_width_from_stdout() -> Option<usize> {
    #[repr(C)]
    struct Winsize {
        ws_row: u16,
        ws_col: u16,
        ws_xpixel: u16,
        ws_ypixel: u16,
    }

    #[cfg(target_os = "linux")]
    const TIOCGWINSZ: std::os::raw::c_ulong = 0x5413;
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    const TIOCGWINSZ: std::os::raw::c_ulong = 0x40087468;
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    )))]
    return None;

    unsafe extern "C" {
        fn ioctl(
            fd: std::os::raw::c_int,
            request: std::os::raw::c_ulong,
            size: *mut Winsize,
        ) -> std::os::raw::c_int;
    }

    let mut size = Winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let rc = unsafe { ioctl(1, TIOCGWINSZ, &mut size) };
    (rc == 0 && size.ws_col >= 20).then_some(size.ws_col as usize)
}

#[cfg(not(unix))]
fn terminal_width_from_stdout() -> Option<usize> {
    None
}

fn terminal_width_from_columns_env() -> Option<usize> {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|width| *width >= 20)
}

fn terminal_width() -> usize {
    terminal_width_from_stdout()
        .or_else(terminal_width_from_columns_env)
        .unwrap_or(100)
}

fn postmortem_separator_line_for_width(width: usize) -> String {
    "\u{2501}".repeat(width.max(20))
}

fn postmortem_separator_line() -> String {
    postmortem_separator_line_for_width(terminal_width())
}

fn print_postmortem_terminal_block(markdown: &str) {
    println!();
    println!("{}", postmortem_separator_line().dimmed());
    print!("{}", colorize_postmortem_for_terminal(markdown));
    if !markdown.ends_with('\n') {
        println!();
    }
    println!("{}", postmortem_separator_line().dimmed());
}

fn render_postmortem_markdown(report: &serde_json::Value) -> String {
    render_watch_postmortem_markdown(report, None)
}

fn render_watch_postmortem_markdown(report: &serde_json::Value, analysis: Option<&str>) -> String {
    let session_id = json_str(report, "/session_id").unwrap_or("unknown");
    let partial = json_bool(report, "/partial").unwrap_or(false);
    let outcome = json_str(report, "/summary/outcome").unwrap_or("unknown");
    let model = json_str(report, "/summary/model").unwrap_or("unknown");
    let duration = json_u64(report, "/summary/duration_secs")
        .map(format_duration_coarse)
        .unwrap_or_else(|| "unknown".to_string());
    let turns = json_u64(report, "/summary/total_turns").unwrap_or(0);
    let tokens = json_u64(report, "/summary/total_tokens").unwrap_or(0);
    let cause = json_str(report, "/diagnosis/likely_cause").unwrap_or("none");
    let cause_is_heuristic =
        json_bool(report, "/diagnosis/likely_cause_is_heuristic").unwrap_or(false);
    let next_action = json_str(report, "/diagnosis/next_action").unwrap_or("No action recorded.");

    let mut out = String::new();
    out.push_str("# Clauditor Postmortem\n\n");
    out.push_str("## Snapshot\n");
    push_key_value_table(
        &mut out,
        vec![
            ("Session", format!("`{session_id}`")),
            (
                "State",
                if partial {
                    "partial snapshot"
                } else {
                    "final postmortem"
                }
                .to_string(),
            ),
            ("Outcome", outcome.to_string()),
            ("Model", model.to_string()),
            ("Duration", duration),
            (
                "Turns/tokens",
                format!("{turns} turns, {}", format_tokens(tokens)),
            ),
            (
                "Cost",
                format_money(json_f64(report, "/impact/estimated_total_cost_dollars")),
            ),
        ],
    );

    out.push_str("## Signals\n");
    push_key_value_table(
        &mut out,
        vec![
            ("Cause", human_cause(cause, cause_is_heuristic)),
            ("Cache", cache_signal(report)),
            ("Context", context_signal(report)),
            ("Waste", waste_signal(report)),
            ("Tools", tool_signal(report)),
            ("Skills", skill_signal(report)),
            ("MCP", mcp_signal(report)),
            ("Next", next_action.to_string()),
        ],
    );

    out.push_str("## Evidence\n");
    push_table_header(&mut out, &["Type", "Signal", "Turn", "Detail"]);
    let mut rendered = 0;
    if let Some(items) = report.get("evidence").and_then(|value| value.as_array()) {
        for item in items.iter().take(2) {
            let kind = item
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or("evidence");
            let label = item
                .get("label")
                .and_then(|value| value.as_str())
                .unwrap_or("signal");
            let detail = item
                .get("detail")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let turn = item
                .get("turn")
                .and_then(|value| value.as_u64())
                .map(|turn| turn.to_string())
                .unwrap_or_else(|| "-".to_string());
            push_table_row(
                &mut out,
                &[
                    kind.to_string(),
                    label.to_string(),
                    turn,
                    detail.to_string(),
                ],
            );
            rendered += 1;
        }
    }
    if rendered == 0 {
        push_table_row(
            &mut out,
            &[
                "-".to_string(),
                "-".to_string(),
                "-".to_string(),
                "No evidence rows recorded.".to_string(),
            ],
        );
    }
    out.push('\n');

    if let Some(analysis) = analysis {
        append_claude_analysis_section(&mut out, analysis);
    }

    out
}

fn claude_analysis_enabled(analyze_with_claude: bool, no_analyze_with_claude: bool) -> bool {
    analyze_with_claude || !no_analyze_with_claude
}

fn extract_labeled_value(line: &str, label: &str) -> Option<String> {
    let mut cleaned = line.trim().trim_start_matches("- ").trim().to_string();
    cleaned = cleaned.replace("**", "");
    cleaned = cleaned.replace('`', "");
    let cleaned = cleaned.trim();
    if !cleaned
        .to_ascii_lowercase()
        .starts_with(&label.to_ascii_lowercase())
    {
        return None;
    }
    let value = cleaned[label.len()..]
        .trim_start()
        .trim_start_matches(':')
        .trim_start_matches('-')
        .trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn render_claude_analysis_section(analysis: &str) -> String {
    let mut status = None;
    let mut main_signal = None;
    let mut risk = None;
    let mut next_action = None;
    let mut restart_lines = Vec::new();
    let mut in_restart_prompt = false;
    let mut fallback_lines = Vec::new();

    for raw_line in analysis.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("```") {
            continue;
        }
        if line.eq_ignore_ascii_case("## Claude Analysis") {
            in_restart_prompt = false;
            continue;
        }
        if line.eq_ignore_ascii_case("## Restart Prompt") {
            in_restart_prompt = true;
            continue;
        }
        if line.starts_with('#') {
            continue;
        }

        if in_restart_prompt {
            restart_lines.push(line.trim_start_matches('>').trim().to_string());
            continue;
        }

        if status.is_none() {
            status = extract_labeled_value(line, "Status");
        }
        if main_signal.is_none() {
            main_signal = extract_labeled_value(line, "Main signal");
        }
        if risk.is_none() {
            risk = extract_labeled_value(line, "Risk");
        }
        if next_action.is_none() {
            next_action = extract_labeled_value(line, "Next action")
                .or_else(|| extract_labeled_value(line, "Next"));
        }
        if fallback_lines.len() < 2 {
            fallback_lines.push(line.to_string());
        }
    }

    let restart_prompt = if restart_lines
        .iter()
        .any(|line| line.eq_ignore_ascii_case("No restart needed."))
    {
        Some("No restart needed.".to_string())
    } else if restart_lines.is_empty() {
        None
    } else {
        Some(restart_lines.join(" "))
    };

    if next_action.is_none() {
        next_action = restart_prompt.clone();
    }

    let mut rows = Vec::new();
    if let Some(value) = status {
        rows.push(("Status", value));
    }
    if let Some(value) = main_signal {
        rows.push(("Main signal", value));
    }
    if let Some(value) = risk {
        rows.push(("Risk", value));
    }
    if let Some(value) = next_action {
        rows.push(("Next action", value));
    }
    if rows.is_empty() && !fallback_lines.is_empty() {
        rows.push(("Summary", fallback_lines.join(" ")));
    }
    if rows.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    out.push_str("## Claude Analysis\n");
    push_key_value_table(&mut out, rows);
    out.push_str("## Restart Prompt\n");
    out.push_str(&format!(
        "  {}\n",
        table_cell(&restart_prompt.unwrap_or_else(|| "No restart needed.".to_string()))
    ));
    out
}

fn append_claude_analysis_section(markdown: &mut String, analysis: &str) {
    let trimmed = render_claude_analysis_section(analysis);
    if trimmed.is_empty() {
        return;
    }
    if !markdown.ends_with('\n') {
        markdown.push('\n');
    }
    markdown.push('\n');
    markdown.push_str(trimmed.trim_end());
    markdown.push('\n');
}

fn build_claude_analysis_prompt(report: &serde_json::Value) -> String {
    let bundle = serde_json::to_string_pretty(report).unwrap_or_else(|_| report.to_string());
    let bundle = truncate_for_box(&bundle, 24_000);
    format!(
        "You are Clauditor's postmortem analyst.\n\
Use only the redacted JSON evidence below. Do not invent facts, files, commands, or raw prompts.\n\
Write compact GitHub-flavored Markdown for a tmux pane. Keep it under 160 words.\n\
Use aligned key/value rows, not Markdown pipe tables. No code fences. No extra caveat block, no extra prose, no bullet lists.\n\
Preserve direct versus heuristic evidence labels. Do not turn heuristic, inferred, likely, or suspected causes into direct facts.\n\
When `is_heuristic` is true or evidence type is `heuristic`, mark causal wording with `[heuristic]`, likely, suspected, or inferred. Direct evidence can stay direct.\n\
Include exactly these sections:\n\
## Claude Analysis\n\
Status       partial or final, in plain language\n\
Main signal  the one signal that matters most\n\
Risk         should the user care now?\n\
Next action  the next concrete action\n\
## Restart Prompt\n\
One short prompt the user can paste into a fresh Claude Code session, or \"No restart needed.\".\n\
Preserve caveats briefly inside the key/value rows only: costs are estimates, context runway is heuristic, and redacted evidence may omit details.\n\n\
Redacted Clauditor postmortem JSON:\n```json\n{bundle}\n```"
    )
}

async fn run_claude_postmortem_analysis_with_command(
    command: &Path,
    report: &serde_json::Value,
    timeout: Duration,
) -> Result<String, String> {
    let prompt = build_claude_analysis_prompt(report);
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
        .env("CLAUDITOR_POSTMORTEM_ANALYSIS", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| format!("failed to start claude: {err}"))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "failed to open claude stdin".to_string())?;
    stdin
        .write_all(prompt.as_bytes())
        .await
        .map_err(|err| format!("failed to write claude prompt: {err}"))?;
    drop(stdin);

    let output = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_err(|_| "claude analysis timed out".to_string())?
        .map_err(|err| format!("failed to wait for claude: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return if stderr.is_empty() {
            Err(format!("claude exited with {}", output.status))
        } else {
            Err(format!("claude exited with {}: {stderr}", output.status))
        };
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        Err("claude returned empty analysis".to_string())
    } else {
        Ok(truncate_for_box(&stdout, 12_000))
    }
}

async fn run_claude_postmortem_analysis_with_lookup<Lookup>(
    report: &serde_json::Value,
    lookup: Lookup,
    timeout: Duration,
) -> Result<String, String>
where
    Lookup: FnOnce(&str) -> Option<PathBuf>,
{
    let claude = lookup("claude").ok_or_else(|| "claude command not found".to_string())?;
    run_claude_postmortem_analysis_with_command(&claude, report, timeout).await
}

async fn run_claude_postmortem_analysis(report: &serde_json::Value) -> Result<String, String> {
    run_claude_postmortem_analysis_with_lookup(report, command_path, Duration::from_secs(120)).await
}

async fn render_postmortem_markdown_with_optional_analysis(
    base_url: &str,
    target: &str,
    report: &serde_json::Value,
    analyze_with_claude: bool,
) -> (String, Option<String>) {
    let mut markdown = render_postmortem_markdown(report);
    if !analyze_with_claude {
        return (markdown, None);
    }

    let redacted_report = if json_bool(report, "/redacted").unwrap_or(false) {
        report.clone()
    } else {
        match fetch_postmortem_json(base_url, target, true).await {
            Ok(value) => value,
            Err(err) => {
                return (
                    markdown,
                    Some(format!(
                        "Claude analysis skipped because redacted evidence could not be fetched: {err}"
                    )),
                );
            }
        }
    };

    match run_claude_postmortem_analysis(&redacted_report).await {
        Ok(analysis) => {
            append_claude_analysis_section(&mut markdown, &analysis);
            (markdown, None)
        }
        Err(err) => (
            markdown,
            Some(format!("Claude analysis unavailable: {err}")),
        ),
    }
}

async fn render_watch_postmortem_markdown_with_optional_analysis(
    base_url: &str,
    target: &str,
    report: &serde_json::Value,
    analyze_with_claude: bool,
) -> (String, Option<String>) {
    if !analyze_with_claude {
        return (render_watch_postmortem_markdown(report, None), None);
    }

    let redacted_report = if json_bool(report, "/redacted").unwrap_or(false) {
        report.clone()
    } else {
        match fetch_postmortem_json(base_url, target, true).await {
            Ok(value) => value,
            Err(err) => {
                return (
                    render_watch_postmortem_markdown(report, None),
                    Some(format!(
                        "Claude analysis skipped because redacted evidence could not be fetched: {err}"
                    )),
                );
            }
        }
    };

    match run_claude_postmortem_analysis(&redacted_report).await {
        Ok(analysis) => (
            render_watch_postmortem_markdown(report, Some(&analysis)),
            None,
        ),
        Err(err) => (
            render_watch_postmortem_markdown(report, None),
            Some(format!("Claude analysis unavailable: {err}")),
        ),
    }
}

async fn fetch_run_final_postmortem_markdown(
    base_url: &str,
    analyze_with_claude: bool,
) -> Result<(String, Option<String>), String> {
    fetch_run_final_postmortem_markdown_with_retry(
        base_url,
        analyze_with_claude,
        RUN_FINAL_POSTMORTEM_ATTEMPTS,
        Duration::from_millis(RUN_FINAL_POSTMORTEM_RETRY_DELAY_MS),
    )
    .await
}

async fn fetch_run_final_postmortem_markdown_with_retry(
    base_url: &str,
    analyze_with_claude: bool,
    attempts: usize,
    retry_delay: Duration,
) -> Result<(String, Option<String>), String> {
    let report =
        fetch_postmortem_json_with_retry(base_url, "last", true, attempts, retry_delay).await?;
    Ok(render_postmortem_markdown_with_optional_analysis(
        base_url,
        "last",
        &report,
        analyze_with_claude,
    )
    .await)
}

async fn render_run_final_postmortem(base_url: &str) {
    eprintln!(
        "{}",
        "Postmortem in progress: waiting for final report + running Claude analysis..."
            .cyan()
            .bold()
    );
    match fetch_run_final_postmortem_markdown(base_url, claude_analysis_enabled(false, false)).await
    {
        Ok((markdown, warning)) => {
            if let Some(warning) = warning {
                eprintln!("{}", warning.yellow());
            }
            print_postmortem_terminal_block(&markdown);
        }
        Err(err) => {
            eprintln!(
                "{}",
                format!(
                    "Final postmortem unavailable after waiting for clauditor-core: {err}\n\
Try again with: clauditor postmortem last\n\
If this run made no proxied Claude API request, no postmortem was recorded."
                )
                .yellow()
            );
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Doctor => {
            std::process::exit(run_doctor().await);
        }
        Commands::Up { no_grafana } => {
            std::process::exit(run_up(no_grafana).await);
        }
        Commands::Run { watch, command } => {
            std::process::exit(run_child_command(watch, command).await);
        }
        Commands::Watch {
            url,
            no_cache,
            no_signals,
            session,
            postmortem,
            no_postmortem,
            analyze_with_claude,
            no_analyze_with_claude,
            tmux,
            tmux_max_panes,
        } => {
            let auto_postmortem = postmortem && !no_postmortem;
            let analyze_postmortems =
                claude_analysis_enabled(analyze_with_claude, no_analyze_with_claude);
            if tmux {
                // Tmux orchestrator mode. Self-bootstrap into a tmux session
                // if we're not already inside one, so the user just runs
                // `clauditor watch --tmux` once.
                if let Err(e) = tmux::bootstrap_into_tmux(
                    &url,
                    no_cache,
                    no_signals,
                    auto_postmortem,
                    !analyze_postmortems,
                    tmux_max_panes,
                ) {
                    eprintln!("{}", e.red());
                    std::process::exit(1);
                }
                let orchestrator = match tmux::TmuxOrchestrator::new(
                    url.clone(),
                    no_cache,
                    no_signals,
                    auto_postmortem,
                    !analyze_postmortems,
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
                let mut postmortem_state =
                    WatchPostmortemState::new(auto_postmortem, &url, analyze_postmortems);

                loop {
                    match connect_and_stream(
                        &watch_url,
                        no_cache,
                        no_signals,
                        &session,
                        &mut active,
                        &mut postmortem_state,
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
        Commands::Postmortem {
            url,
            target,
            redact,
            no_redact,
            analyze_with_claude,
            no_analyze_with_claude,
            output,
        } => {
            let redact = redact || !no_redact;
            let analyze_postmortem =
                claude_analysis_enabled(analyze_with_claude, no_analyze_with_claude);
            std::process::exit(
                run_postmortem(&url, &target, redact, analyze_postmortem, output.as_deref()).await,
            );
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

async fn fetch_postmortem_json(
    base_url: &str,
    target: &str,
    redact: bool,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{}/api/postmortem/{}",
        base_url.trim_end_matches('/'),
        target
    );
    let resp = reqwest::Client::new()
        .get(&url)
        .query(&[("redact", redact.to_string())])
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let detail = body.trim();
        return if detail.is_empty() {
            Err(format!("HTTP {status}"))
        } else {
            Err(format!("HTTP {status}: {detail}"))
        };
    }
    resp.json::<serde_json::Value>()
        .await
        .map_err(|err| err.to_string())
}

async fn fetch_postmortem_json_with_retry(
    base_url: &str,
    target: &str,
    redact: bool,
    attempts: usize,
    retry_delay: Duration,
) -> Result<serde_json::Value, String> {
    let attempts = attempts.max(1);
    let mut last_error = String::new();
    for attempt in 0..attempts {
        match fetch_postmortem_json(base_url, target, redact).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let retryable = err.contains("HTTP 404") || err.contains("HTTP 503");
                last_error = err;
                if !retryable || attempt + 1 == attempts {
                    break;
                }
                tokio::time::sleep(retry_delay).await;
            }
        }
    }
    Err(last_error)
}

async fn run_postmortem(
    base_url: &str,
    target: &str,
    redact: bool,
    analyze_with_claude: bool,
    output: Option<&Path>,
) -> i32 {
    eprintln!(
        "{}",
        format!(
            "Postmortem in progress: fetching {} report for {target}...",
            if redact { "redacted" } else { "local" }
        )
        .cyan()
        .bold()
    );
    match fetch_postmortem_json(base_url, target, redact).await {
        Ok(report) => {
            if analyze_with_claude {
                eprintln!(
                    "{}",
                    "Postmortem in progress: running Claude analysis..."
                        .cyan()
                        .bold()
                );
            }
            let (markdown, warning) = render_postmortem_markdown_with_optional_analysis(
                base_url,
                target,
                &report,
                analyze_with_claude,
            )
            .await;
            if let Some(warning) = warning {
                eprintln!("{}", warning.yellow());
            }
            if let Some(path) = output {
                if let Some(parent) = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                {
                    if let Err(err) = fs::create_dir_all(parent) {
                        eprintln!(
                            "{}",
                            format!("Error: failed to create {}: {}", parent.display(), err).red()
                        );
                        return 1;
                    }
                }
                if let Err(err) = fs::write(path, markdown) {
                    eprintln!(
                        "{}",
                        format!("Error: failed to write {}: {}", path.display(), err).red()
                    );
                    return 1;
                }
                println!("Wrote postmortem to {}", path.display());
            } else {
                print_postmortem_terminal_block(&markdown);
            }
            0
        }
        Err(err) => {
            eprintln!("{}", format!("Error: {err}").red());
            1
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
        "SESSION", "MODEL", "TURNS", "OUTCOME", "ESTIMATED COST", "BILLED COST", "REUSE%"
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
    postmortem: &mut WatchPostmortemState,
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

    let mut line_buffer = Vec::new();
    let mut data_buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
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
                    let postmortem_tag = event_session_id(&event)
                        .map(|sid| active.tag_for(sid))
                        .unwrap_or_default();
                    render_event(&event, no_cache, no_signals, session_filter, active);
                    if postmortem.enabled {
                        if let Some((session_id, dedupe_key)) = auto_postmortem_target(&event) {
                            if postmortem.rendered.insert(dedupe_key) {
                                print_postmortem_progress(
                                    &postmortem_tag,
                                    &session_id,
                                    postmortem.analyze_with_claude,
                                );
                                match fetch_postmortem_json_with_retry(
                                    &postmortem.base_url,
                                    &session_id,
                                    true,
                                    5,
                                    Duration::from_millis(250),
                                )
                                .await
                                {
                                    Ok(report) => {
                                        let (markdown, warning) =
                                            render_watch_postmortem_markdown_with_optional_analysis(
                                                &postmortem.base_url,
                                                &session_id,
                                                &report,
                                                postmortem.analyze_with_claude,
                                            )
                                            .await;
                                        if let Some(warning) = warning {
                                            eprintln!("{}", warning.yellow());
                                        }
                                        print_postmortem_terminal_block(&markdown);
                                    }
                                    Err(err) => {
                                        eprintln!(
                                            "{}",
                                            format!(
                                                "Postmortem unavailable for {session_id}: {err}"
                                            )
                                            .yellow()
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                data_buffer.clear();
            }
        }
    }

    Ok(())
}

fn auto_postmortem_target(event: &WatchEvent) -> Option<(String, String)> {
    match event {
        WatchEvent::SessionEnd { session_id, .. } => {
            Some((session_id.clone(), format!("final:{session_id}")))
        }
        WatchEvent::PostmortemReady {
            session_id,
            total_turns,
            ..
        } => Some((
            session_id.clone(),
            format!("idle:{session_id}:{total_turns}"),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        append_claude_analysis_section, auto_postmortem_target, build_claude_analysis_prompt,
        claude_analysis_enabled, colorize_postmortem_for_terminal, compact_datetime_from_iso,
        event_session_id, extract_run_watch, fetch_run_final_postmortem_markdown,
        fetch_run_final_postmortem_markdown_with_retry, format_duration_coarse, format_tokens,
        local_time_from_iso, parse_mcp_tool_name, postmortem_separator_line_for_width, push_unique,
        render_postmortem_markdown, render_postmortem_markdown_with_optional_analysis,
        run_child_command_with_deps, run_claude_postmortem_analysis_with_command,
        run_claude_postmortem_analysis_with_lookup, shell_join, shell_quote, truncate_for_box,
        watcher_args, yaml_quote, ActiveSessions, Cli, Commands, WatchEvent, WatchPostmortemState,
    };
    use chrono::{DateTime, Local};
    use clap::Parser;
    use std::collections::HashSet;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_test_dir(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "clauditor-cli-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create test dir");
        path
    }

    fn serve_sse_chunks_once(chunks: Vec<String>) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind sse server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept sse request");
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let n = stream.read(&mut buffer).expect("read request");
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            tx.send(String::from_utf8_lossy(&request).into_owned())
                .expect("send captured request");

            let content_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                content_len
            )
            .expect("write sse response headers");

            for chunk in chunks {
                stream.write_all(chunk.as_bytes()).expect("write sse chunk");
                stream.flush().expect("flush sse chunk");
                thread::sleep(Duration::from_millis(5));
            }
        });

        (url, rx)
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let n = stream.read(&mut buffer).expect("read request");
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..n]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn serve_watch_and_postmortem_once(
        watch_chunks: Vec<String>,
        postmortem_status: &str,
        postmortem_body: &str,
    ) -> (String, mpsc::Receiver<Vec<String>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind watch server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let status = postmortem_status.to_string();
        let body = postmortem_body.to_string();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let mut requests = Vec::new();

            let (mut stream, _) = listener.accept().expect("accept watch request");
            requests.push(read_http_request(&mut stream));
            let content_len: usize = watch_chunks.iter().map(|chunk| chunk.len()).sum();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                content_len
            )
            .expect("write watch response headers");
            for chunk in watch_chunks {
                stream
                    .write_all(chunk.as_bytes())
                    .expect("write watch chunk");
                stream.flush().expect("flush watch chunk");
            }

            let (mut stream, _) = listener.accept().expect("accept postmortem request");
            requests.push(read_http_request(&mut stream));
            write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write postmortem response");

            tx.send(requests).expect("send captured requests");
        });

        (url, rx)
    }

    fn serve_watch_and_postmortems(
        watch_chunks: Vec<String>,
        postmortems: Vec<(String, String)>,
    ) -> (String, mpsc::Receiver<Vec<String>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind watch server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let mut requests = Vec::new();

            let (mut stream, _) = listener.accept().expect("accept watch request");
            requests.push(read_http_request(&mut stream));
            let content_len: usize = watch_chunks.iter().map(|chunk| chunk.len()).sum();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                content_len
            )
            .expect("write watch response headers");
            for chunk in watch_chunks {
                stream
                    .write_all(chunk.as_bytes())
                    .expect("write watch chunk");
                stream.flush().expect("flush watch chunk");
            }

            for (status, body) in postmortems {
                let (mut stream, _) = listener.accept().expect("accept postmortem request");
                requests.push(read_http_request(&mut stream));
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .expect("write postmortem response");
            }

            tx.send(requests).expect("send captured requests");
        });

        (url, rx)
    }

    fn serve_postmortem_last_once(body: &str) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind postmortem server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let body = body.to_string();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept postmortem request");
            let request = read_http_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write postmortem response");
            tx.send(request).expect("send captured request");
        });

        (url, rx)
    }

    fn serve_postmortem_response_once(
        status: &str,
        body: &str,
    ) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind postmortem server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let status = status.to_string();
        let body = body.to_string();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept postmortem request");
            let request = read_http_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write postmortem response");
            tx.send(request).expect("send captured request");
        });

        (url, rx)
    }

    fn serve_postmortem_responses(
        responses: Vec<(String, String)>,
    ) -> (String, mpsc::Receiver<Vec<String>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind postmortem server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().expect("accept postmortem request");
                requests.push(read_http_request(&mut stream));
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .expect("write postmortem response");
            }
            tx.send(requests).expect("send captured requests");
        });

        (url, rx)
    }

    fn fake_claude_script(
        label: &str,
        script_body: &str,
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let dir = unique_test_dir(label);
        let executable = dir.join("claude");
        {
            let mut file = fs::File::create(&executable).expect("create fake claude");
            writeln!(file, "#!/bin/sh").expect("write fake claude");
            write!(file, "{script_body}").expect("write fake claude");
        }
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&executable).expect("metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&executable, permissions).expect("chmod fake claude");
        }
        (dir, executable)
    }

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

    #[test]
    fn formatting_helpers_render_compact_user_text() {
        assert_eq!(format_tokens(999), "999");
        assert_eq!(format_tokens(12_345), "12K");
        assert_eq!(format_tokens(3_400_000), "3.4M");

        assert_eq!(format_duration_coarse(45), "45s");
        assert_eq!(format_duration_coarse(12 * 60), "12m");
        assert_eq!(format_duration_coarse(3 * 60 * 60 + 20 * 60), "3h 20m");
        assert_eq!(
            format_duration_coarse(2 * 24 * 60 * 60 + 5 * 60 * 60),
            "2d 5h"
        );

        assert_eq!(truncate_for_box("short", 10), "short");
        assert_eq!(truncate_for_box("abcdef", 4), "abc\u{2026}");
        assert_eq!(truncate_for_box("åäöabc", 4), "åäö\u{2026}");
    }

    #[test]
    fn shell_and_yaml_quoting_preserve_command_arguments() {
        assert_eq!(shell_quote("abc/def-123"), "abc/def-123");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("hello world"), "'hello world'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");

        assert_eq!(
            shell_join(&["clauditor".to_string(), "hello world".to_string()]),
            "clauditor 'hello world'"
        );
        assert_eq!(yaml_quote(r#"a\b"c"#), r#""a\\b\"c""#);
    }

    #[test]
    fn side_watcher_args_can_enable_auto_postmortem() {
        assert_eq!(
            watcher_args("http://core".to_string(), false, false),
            vec!["watch", "--url", "http://core"]
        );
        assert_eq!(
            watcher_args("http://core".to_string(), false, true),
            vec!["watch", "--url", "http://core", "--postmortem"]
        );
        assert_eq!(
            watcher_args("http://core".to_string(), true, true),
            vec!["watch", "--tmux", "--url", "http://core", "--postmortem"]
        );
    }

    #[test]
    fn command_path_accepts_explicit_executable_paths() {
        let dir = unique_test_dir("command-path");
        let executable = dir.join("fake-command");
        {
            let mut file = fs::File::create(&executable).expect("create executable");
            writeln!(file, "#!/bin/sh").expect("write executable");
        }
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&executable).expect("metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&executable, permissions).expect("chmod executable");
        }

        assert_eq!(
            super::command_path(executable.to_str().expect("utf8 path")),
            Some(executable.clone())
        );
        assert!(super::command_exists(
            executable.to_str().expect("utf8 path")
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn write_if_changed_creates_parent_dirs_and_preserves_identical_files() {
        let dir = unique_test_dir("write-if-changed");
        let path = dir.join("nested/config.yml");

        super::write_if_changed(&path, "one").expect("first write");
        let modified = fs::metadata(&path).expect("metadata").modified().ok();
        super::write_if_changed(&path, "one").expect("same write");
        assert_eq!(fs::read_to_string(&path).expect("read"), "one");
        if let Some(modified) = modified {
            assert_eq!(
                fs::metadata(&path).expect("metadata").modified().ok(),
                Some(modified)
            );
        }

        super::write_if_changed(&path, "two").expect("changed write");
        assert_eq!(fs::read_to_string(&path).expect("read"), "two");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn run_watch_after_child_command_is_clauditor_flag() {
        let cli = Cli::try_parse_from(["clauditor", "run", "claude", "--watch"])
            .expect("run command parses");
        let Commands::Run { watch, command } = cli.command else {
            panic!("expected run command");
        };
        let (watch, command) = extract_run_watch(watch, command);
        assert!(watch);
        assert_eq!(command, vec!["claude"]);
    }

    #[test]
    fn run_watch_before_child_command_is_clauditor_flag() {
        let cli = Cli::try_parse_from(["clauditor", "run", "--watch", "claude"])
            .expect("run command parses");
        let Commands::Run { watch, command } = cli.command else {
            panic!("expected run command");
        };
        let (watch, command) = extract_run_watch(watch, command);
        assert!(watch);
        assert_eq!(command, vec!["claude"]);
    }

    #[test]
    fn run_preserves_child_flags() {
        let cli = Cli::try_parse_from([
            "clauditor",
            "run",
            "claude",
            "--dangerously-skip-permissions",
            "--model",
            "opus",
        ])
        .expect("run command parses");
        let Commands::Run { watch, command } = cli.command else {
            panic!("expected run command");
        };
        let (watch, command) = extract_run_watch(watch, command);
        assert!(!watch);
        assert_eq!(
            command,
            vec![
                "claude",
                "--dangerously-skip-permissions",
                "--model",
                "opus"
            ]
        );
    }

    #[tokio::test]
    async fn run_watch_preserves_child_exit_code_and_renders_final_postmortem() {
        let events = Arc::new(Mutex::new(Vec::new()));

        let exit_code = run_child_command_with_deps(
            true,
            vec!["fake-child".to_string(), "--flag".to_string()],
            || async { Ok(()) },
            {
                let events = events.clone();
                move |postmortem| {
                    assert!(!postmortem);
                    events
                        .lock()
                        .expect("test events lock")
                        .push("start".to_string());
                    Ok(super::WatchHandle::Test(events.clone()))
                }
            },
            {
                let events = events.clone();
                move |command, args, envs| {
                    assert_eq!(command, "fake-child");
                    assert_eq!(args, &[String::from("--flag")]);
                    assert!(envs
                        .iter()
                        .any(|(key, value)| *key == "ANTHROPIC_BASE_URL" && !value.is_empty()));
                    events
                        .lock()
                        .expect("test events lock")
                        .push("run".to_string());
                    Ok(23)
                }
            },
            {
                let events = events.clone();
                move |_base_url| async move {
                    events
                        .lock()
                        .expect("test events lock")
                        .push("render".to_string());
                }
            },
        )
        .await;

        assert_eq!(exit_code, 23);
        assert_eq!(
            *events.lock().expect("test events lock"),
            vec!["start", "run", "stop", "render"]
        );
    }

    #[test]
    fn parser_applies_command_defaults() {
        let cli = Cli::try_parse_from(["clauditor", "watch"]).expect("watch parses");
        let Commands::Watch {
            url,
            no_cache,
            no_signals,
            session,
            postmortem,
            no_postmortem,
            analyze_with_claude,
            no_analyze_with_claude,
            tmux,
            tmux_max_panes,
        } = cli.command
        else {
            panic!("expected watch command");
        };
        assert_eq!(url, "http://localhost:9091");
        assert!(!no_cache);
        assert!(!no_signals);
        assert_eq!(session, None);
        assert!(!postmortem);
        assert!(!no_postmortem);
        assert!(claude_analysis_enabled(
            analyze_with_claude,
            no_analyze_with_claude
        ));
        assert!(!tmux);
        assert_eq!(tmux_max_panes, 4);

        let cli = Cli::try_parse_from(["clauditor", "watch", "--postmortem"])
            .expect("watch postmortem parses");
        let Commands::Watch { postmortem, .. } = cli.command else {
            panic!("expected watch command");
        };
        assert!(postmortem);

        let cli = Cli::try_parse_from(["clauditor", "watch", "--no-postmortem"])
            .expect("watch legacy no-postmortem parses");
        let Commands::Watch {
            postmortem,
            no_postmortem,
            ..
        } = cli.command
        else {
            panic!("expected watch command");
        };
        assert!(!postmortem);
        assert!(no_postmortem);

        let cli = Cli::try_parse_from(["clauditor", "sessions"]).expect("sessions parses");
        let Commands::Sessions { url, limit, days } = cli.command else {
            panic!("expected sessions command");
        };
        assert_eq!(url, "http://localhost:9091");
        assert_eq!(limit, 20);
        assert_eq!(days, 7);

        let cli = Cli::try_parse_from(["clauditor", "recall", "auth"]).expect("recall parses");
        let Commands::Recall {
            url,
            limit,
            days,
            query,
        } = cli.command
        else {
            panic!("expected recall command");
        };
        assert_eq!(url, "http://localhost:9091");
        assert_eq!(limit, 5);
        assert_eq!(days, 30);
        assert_eq!(query, vec!["auth"]);

        let cli = Cli::try_parse_from(["clauditor", "postmortem", "last"])
            .expect("postmortem last parses");
        let Commands::Postmortem {
            url,
            target,
            redact,
            no_redact,
            analyze_with_claude,
            no_analyze_with_claude,
            output,
        } = cli.command
        else {
            panic!("expected postmortem command");
        };
        assert_eq!(url, "http://localhost:9091");
        assert_eq!(target, "last");
        assert!(!redact);
        assert!(!no_redact);
        assert!(claude_analysis_enabled(
            analyze_with_claude,
            no_analyze_with_claude
        ));
        assert_eq!(output, None);

        let cli = Cli::try_parse_from(["clauditor", "postmortem", "last", "--no-redact"])
            .expect("postmortem no-redact parses");
        let Commands::Postmortem { no_redact, .. } = cli.command else {
            panic!("expected postmortem command");
        };
        assert!(no_redact);

        let cli = Cli::try_parse_from(["clauditor", "postmortem", "last", "--redact"])
            .expect("legacy postmortem redact parses");
        let Commands::Postmortem { redact, .. } = cli.command else {
            panic!("expected postmortem command");
        };
        assert!(redact);

        let cli = Cli::try_parse_from([
            "clauditor",
            "postmortem",
            "last",
            "--no-analyze-with-claude",
        ])
        .expect("postmortem no-analyze parses");
        let Commands::Postmortem {
            analyze_with_claude,
            no_analyze_with_claude,
            ..
        } = cli.command
        else {
            panic!("expected postmortem command");
        };
        assert!(!claude_analysis_enabled(
            analyze_with_claude,
            no_analyze_with_claude
        ));

        let cli = Cli::try_parse_from([
            "clauditor",
            "postmortem",
            "session_1234_abcd",
            "--output",
            "postmortem.md",
        ])
        .expect("postmortem session parses");
        let Commands::Postmortem {
            target,
            redact,
            no_redact,
            output,
            ..
        } = cli.command
        else {
            panic!("expected postmortem command");
        };
        assert_eq!(target, "session_1234_abcd");
        assert!(!redact);
        assert!(!no_redact);
        assert_eq!(output, Some(std::path::PathBuf::from("postmortem.md")));
    }

    #[test]
    fn active_sessions_tags_only_when_multiple_sessions_exist() {
        let mut active = ActiveSessions::new();
        active.add("session_a", "api");
        assert!(!active.is_multi());
        assert_eq!(active.tag_for("session_a"), "");

        active.add("session_b", "worker-long");
        assert!(active.is_multi());
        assert_eq!(active.tag_for("session_a"), "[api        ]  ");
        assert_eq!(active.tag_for("missing"), "[?          ]  ");

        active.remove("session_b");
        assert!(!active.is_multi());
    }

    #[tokio::test]
    async fn watch_stream_consumes_sse_chunks_and_applies_session_filter() {
        let chunks = vec![
            ": keepalive\n\n".to_string(),
            concat!(
                "data: {\"type\":\"session_start\",\"session_id\":\"session_other\",",
                "\"display_name\":\"other\",\"model\":\"opus\"}\n\n"
            )
            .to_string(),
            concat!(
                "data: {\"type\":\"session_start\",\"session_id\":\"session_target\",",
                "\"display_name\":\"api\",\"model\":\"sonnet\",",
                "\"initial_prompt\":\"investigate auth\"}\n\n"
            )
            .to_string(),
            concat!(
                "data: {\"type\":\"tool_use\",\"session_id\":\"session_target\",",
                "\"timestamp\":\"2026-04-28T00:00:00Z\",",
                "\"tool_name\":\"Read\",\"summary\":\"src/main.rs\"}\n\n"
            )
            .to_string(),
            concat!(
                "data: {\"type\":\"session_end\",\"session_id\":\"session_target\",",
                "\"outcome\":\"Likely Completed\",\"total_tokens\":1234,\"total_turns\":3}\n\n"
            )
            .to_string(),
        ];
        let (url, request_rx) = serve_sse_chunks_once(chunks);
        let mut active = ActiveSessions::new();
        let filter = Some("session_target".to_string());
        let mut postmortem_state = WatchPostmortemState::new(false, &url, false);

        super::connect_and_stream(
            &url,
            false,
            false,
            &filter,
            &mut active,
            &mut postmortem_state,
        )
        .await
        .expect("watch stream closes cleanly");

        let request = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured watch request");
        assert!(
            request.starts_with("GET / "),
            "unexpected request:\n{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("accept: text/event-stream"),
            "missing SSE accept header:\n{request}"
        );
        assert!(
            active.sessions.is_empty(),
            "target session should be removed after session_end"
        );
    }

    #[test]
    fn postmortem_markdown_renderer_includes_expected_sections() {
        let report = serde_json::json!({
            "session_id": "session_target",
            "redacted": true,
            "partial": false,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": "2026-01-01T00:05:00Z",
                "duration_secs": 300,
                "model": "claude-sonnet",
                "outcome": "Likely Partially Completed",
                "total_turns": 4,
                "total_tokens": 123456,
                "initial_prompt_summary": "Initial prompt captured (redacted, 8 words).",
                "final_response_summary": "Final response summary captured (redacted, 6 words)."
            },
            "diagnosis": {
                "likely_cause": "tool_failure_streak",
                "detail": "Bash failed repeatedly.",
                "confidence": "high",
                "next_action": "Fix the failing command, then restart."
            },
            "impact": {
                "input_tokens": 100000,
                "output_tokens": 1000,
                "cache_read_tokens": 20000,
                "cache_creation_tokens": 2456,
                "total_tokens": 123456,
                "estimated_total_cost_dollars": 1.23,
                "estimated_likely_wasted_tokens": 2456,
                "estimated_likely_wasted_cost_dollars": 0.12,
                "cost_source": "builtin_model_family_pricing",
                "cost_caveat": "Estimated costs are local calculations, not authoritative billing records."
            },
            "signals": {
                "cache": { "cache_hit_ratio": 0.75 },
                "context": { "max_fill_percent": 82.0, "turns_to_compact": 1 },
                "tools": [
                    { "tool_name": "Bash", "calls": 5, "failures": 3 },
                    { "tool_name": "Read", "calls": 2, "failures": 0 }
                ],
                "skills": [
                    { "skill_name": "qa", "event_type": "failed", "source": "hook", "count": 1 }
                ],
                "mcp": [
                    { "server": "github", "tool": "get_issue", "event_type": "failed", "source": "hook", "count": 2 }
                ]
            },
            "evidence": [
                { "type": "direct", "label": "tools", "detail": "3 tool failures", "turn": 2 }
            ],
            "timeline": [
                { "timestamp": "2026-01-01T00:00:00Z", "label": "session_started", "detail": "Session row created." },
                { "timestamp": "2026-01-01T00:05:00Z", "label": "session_ended", "detail": "Likely Partially Completed" }
            ],
            "recommendations": ["Fix the failing command, then restart."],
            "caveats": ["This report is deterministic and generated from local Clauditor SQLite data."]
        });

        let markdown = render_postmortem_markdown(&report);
        assert!(markdown.contains("# Clauditor Postmortem"));
        assert!(markdown.contains("## Snapshot"));
        assert!(markdown.contains("## Signals"));
        assert!(markdown.contains("## Evidence"));
        assert!(markdown.contains("Session"));
        assert!(markdown.contains("`session_target`"));
        assert!(markdown.contains("Tool failure loop"));
        assert!(markdown.contains("Fix the failing command, then restart."));
        assert!(markdown.contains("75% reusable prompt cache"));
        assert!(markdown.contains("7 calls, 3 failures; failing: Bash (3)"));
        assert!(markdown.contains("1 failed skill event; failing: qa (1)"));
        assert!(markdown.contains("2 failed MCP calls; failing: github.get_issue (2)"));
        assert!(markdown.contains("$1.23"));
        assert!(!markdown.contains("|"));
        assert!(!markdown.contains("Initial prompt captured (redacted"));
        assert!(!markdown.contains("Estimated costs are local calculations"));
        assert!(!markdown.contains("## Timeline Highlights"));
        assert!(!markdown.contains("## Caveats"));
    }

    #[test]
    fn postmortem_markdown_marks_heuristic_output_but_keeps_direct_evidence_direct() {
        let report = serde_json::json!({
            "session_id": "session_heuristic",
            "redacted": true,
            "partial": false,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": "2026-01-01T00:05:00Z",
                "duration_secs": 300,
                "model": "claude-sonnet",
                "outcome": "Compaction Suspected",
                "total_turns": 4,
                "total_tokens": 123456
            },
            "diagnosis": {
                "likely_cause": "near_compaction",
                "likely_cause_is_heuristic": true,
                "detail": "[heuristic] Context reached 84% full.",
                "confidence": "medium",
                "next_action": "Start fresh with a short state summary."
            },
            "impact": {
                "input_tokens": 100000,
                "output_tokens": 1000,
                "cache_read_tokens": 20000,
                "cache_creation_tokens": 2456,
                "total_tokens": 123456,
                "estimated_total_cost_dollars": 1.23,
                "estimated_likely_wasted_tokens": 2456,
                "estimated_likely_wasted_cost_dollars": 0.12,
                "cost_source": "builtin_model_family_pricing"
            },
            "signals": {
                "cache": { "cache_hit_ratio": 0.75 },
                "context": { "max_fill_percent": 84.0, "turns_to_compact": 1, "heuristic": true }
            },
            "evidence": [
                { "type": "heuristic", "label": "context", "detail": "inferred 1 turn to compact", "turn": 2 },
                { "type": "direct", "label": "model", "detail": "requested opus, response reported sonnet", "turn": 3 }
            ],
            "timeline": [],
            "recommendations": ["Start fresh with a short state summary."],
            "caveats": []
        });

        let markdown = render_postmortem_markdown(&report);
        assert!(markdown.contains("Cause"));
        assert!(markdown.contains("Near auto-compaction [heuristic]"));
        assert!(markdown.contains("heuristic"));
        assert!(markdown.contains("context"));
        assert!(markdown.contains("inferred 1 turn to compact"));
        assert!(
            markdown.contains("direct")
                && markdown.contains("model")
                && markdown.contains("requested opus, response reported sonnet")
        );
        assert!(markdown.contains("Context"));
        assert!(markdown.contains("High: 84% full; about 1 turn before auto-compaction"));
        assert!(markdown.contains("Start fresh with a short state summary."));
        assert!(!markdown.contains("[heuristic] Context reached 84% full."));
    }

    #[test]
    fn postmortem_markdown_simplifies_clean_partial_session() {
        let report = serde_json::json!({
            "session_id": "session_clean",
            "redacted": true,
            "partial": true,
            "summary": {
                "duration_secs": 630,
                "model": "claude-sonnet-4-6",
                "outcome": "In Progress",
                "total_turns": 18,
                "total_tokens": 1195867
            },
            "diagnosis": {
                "likely_cause": "none",
                "likely_cause_is_heuristic": false,
                "next_action": "Session is still partial; use this as a progress snapshot, not a final diagnosis."
            },
            "impact": {
                "estimated_total_cost_dollars": 0.47,
                "estimated_likely_wasted_tokens": 0,
                "estimated_likely_wasted_cost_dollars": 0.0
            },
            "signals": {
                "cache": {
                    "cache_reusable_prefix_ratio": 0.983,
                    "total_input_cache_rate": 0.981
                },
                "context": {
                    "max_fill_percent": 38.0,
                    "turns_to_compact": 167
                },
                "tools": [
                    { "tool_name": "Bash", "calls": 4, "failures": 0 },
                    { "tool_name": "Read", "calls": 3, "failures": 0 }
                ],
                "skills": [],
                "mcp": []
            },
            "evidence": []
        });

        let markdown = render_postmortem_markdown(&report);

        assert!(markdown.contains("No degradation detected"));
        assert!(markdown.contains("Healthy: 98% reusable prompt cache; 98% of input from cache"));
        assert!(
            markdown.contains("Plenty of room: 38% full; about 167 turns before auto-compaction")
        );
        assert!(markdown.contains("No likely wasted tokens detected"));
        assert!(markdown.contains("7 calls, 0 failures; repeated: Bash, Read"));
        assert!(markdown.contains("No failed skill events detected"));
        assert!(markdown.contains("No failed MCP calls detected"));
        assert!(!markdown.contains("compaction runway"));
    }

    #[test]
    fn postmortem_markdown_appends_claude_analysis_section() {
        let mut markdown = "# Clauditor Postmortem\n".to_string();
        append_claude_analysis_section(
            &mut markdown,
            "## Claude Analysis\n\
```\n\
Status       Partial - no degradation detected\n\
Main signal  Cache hit ratio stayed high\n\
Risk         Low — context runway is heuristic\n\
Cost         $1.25 estimated\n\
```\n\
## Restart Prompt\n\
```\n\
No restart needed.\n\
If failures recur, restart with a shorter prompt.\n\
```",
        );
        assert!(markdown.contains("## Claude Analysis"));
        assert!(markdown.contains("Status       Partial"));
        assert!(markdown.contains("Main signal  Cache hit ratio stayed high"));
        assert!(markdown.contains("Risk         Low"));
        assert!(markdown.contains("Next action  No restart needed."));
        assert!(markdown.contains("## Restart Prompt"));
        assert!(markdown.contains("No restart needed."));
        assert!(!markdown.contains("```"));
        assert!(!markdown.contains("Cost         $1.25"));
    }

    #[test]
    fn postmortem_terminal_colorization_adds_ansi_without_touching_markdown() {
        let markdown = "# Clauditor Postmortem\n\
\n\
## Signals\n\
  Cause   none\n\
  Context 84% full; compaction runway 1\n\
  Risk    High - restart is cheaper\n\
\n\
## Evidence\n\
  Type        Signal        Turn   Detail\n\
  ----------  ------------  -----  ------\n\
  heuristic   context       2      inferred 1 turn to compact\n";

        colored::control::set_override(true);
        let terminal = colorize_postmortem_for_terminal(markdown);
        colored::control::unset_override();

        assert!(terminal.contains("\u{1b}["));
        assert!(terminal.contains("Clauditor Postmortem"));
        assert!(terminal.contains("heuristic"));
        assert!(!markdown.contains("\u{1b}["));
    }

    #[test]
    fn postmortem_separator_spans_terminal_width_with_minimum() {
        assert_eq!(postmortem_separator_line_for_width(72).chars().count(), 72);
        assert_eq!(postmortem_separator_line_for_width(8).chars().count(), 20);
    }

    #[test]
    fn claude_analysis_prompt_uses_redacted_evidence_contract() {
        let report = serde_json::json!({
            "session_id": "session_target",
            "redacted": true,
            "summary": {
                "initial_prompt_summary": "Initial prompt captured (redacted, 8 words)."
            }
        });
        let prompt = build_claude_analysis_prompt(&report);
        assert!(prompt.contains("Use only the redacted JSON evidence"));
        assert!(prompt.contains("Preserve direct versus heuristic evidence labels"));
        assert!(prompt.contains("Do not turn heuristic"));
        assert!(prompt.contains("Write compact GitHub-flavored Markdown for a tmux pane"));
        assert!(prompt.contains("Use aligned key/value rows"));
        assert!(prompt.contains("No code fences"));
        assert!(prompt.contains("Status       partial or final"));
        assert!(prompt.contains("Main signal  the one signal that matters most"));
        assert!(prompt.contains("## Claude Analysis"));
        assert!(prompt.contains("## Restart Prompt"));
        assert!(prompt.contains("\"redacted\": true"));
        assert!(prompt.contains("Initial prompt captured"));
    }

    #[tokio::test]
    async fn claude_analysis_runner_accepts_fake_claude_command() {
        let (dir, executable) = fake_claude_script(
            "fake-claude",
            concat!(
                "cat >/dev/null\n",
                "printf '%s\\n' '## Claude Analysis'\n",
                "printf '%s\\n' '- Likely cause: cache rebuild'\n",
                "printf '%s\\n' '## Restart Prompt'\n",
                "printf '%s\\n' 'Start fresh with a summary.'\n",
            ),
        );

        let report = serde_json::json!({
            "session_id": "session_target",
            "redacted": true,
            "summary": {"outcome": "Likely Completed"}
        });
        let analysis = run_claude_postmortem_analysis_with_command(
            &executable,
            &report,
            Duration::from_secs(2),
        )
        .await
        .expect("fake claude analysis");

        assert!(analysis.contains("## Claude Analysis"));
        assert!(analysis.contains("cache rebuild"));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn claude_analysis_reports_missing_claude_command() {
        let report = serde_json::json!({
            "session_id": "session_target",
            "redacted": true
        });
        let err = run_claude_postmortem_analysis_with_lookup(
            &report,
            |_| None,
            Duration::from_millis(50),
        )
        .await
        .expect_err("missing claude should fail");

        assert!(err.contains("claude command not found"));
    }

    #[tokio::test]
    async fn claude_analysis_reports_nonzero_exit() {
        let (dir, executable) = fake_claude_script(
            "fake-claude-nonzero",
            "cat >/dev/null\necho analysis failed >&2\nexit 42\n",
        );
        let report = serde_json::json!({
            "session_id": "session_target",
            "redacted": true
        });

        let err = run_claude_postmortem_analysis_with_command(
            &executable,
            &report,
            Duration::from_secs(2),
        )
        .await
        .expect_err("nonzero claude should fail");

        assert!(err.contains("claude exited"));
        assert!(err.contains("analysis failed"));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn claude_analysis_reports_timeout() {
        let (dir, executable) = fake_claude_script("fake-claude-timeout", "sleep 5\n");
        let report = serde_json::json!({
            "session_id": "session_target",
            "redacted": true
        });

        let err = run_claude_postmortem_analysis_with_command(
            &executable,
            &report,
            Duration::from_millis(50),
        )
        .await
        .expect_err("timed out claude should fail");

        assert!(err.contains("claude analysis timed out"));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn claude_analysis_reports_empty_output() {
        let (dir, executable) = fake_claude_script("fake-claude-empty", "cat >/dev/null\nexit 0\n");
        let report = serde_json::json!({
            "session_id": "session_target",
            "redacted": true
        });

        let err = run_claude_postmortem_analysis_with_command(
            &executable,
            &report,
            Duration::from_secs(2),
        )
        .await
        .expect_err("empty claude output should fail");

        assert!(err.contains("claude returned empty analysis"));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn optional_claude_analysis_warns_when_redacted_refetch_fails() {
        let report = serde_json::json!({
            "session_id": "session_unredacted",
            "redacted": false,
            "summary": {
                "outcome": "Likely Completed"
            }
        });
        let (url, request_rx) =
            serve_postmortem_response_once("500 Internal Server Error", "broken");

        let (markdown, warning) = render_postmortem_markdown_with_optional_analysis(
            &url,
            "session_unredacted",
            &report,
            true,
        )
        .await;

        assert!(markdown.contains("session_unredacted"));
        let warning = warning.expect("redacted refetch warning");
        assert!(warning.contains("redacted evidence could not be fetched"));
        let request = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured redacted refetch");
        assert!(request.starts_with("GET /api/postmortem/session_unredacted?redact=true "));
    }

    #[tokio::test]
    async fn watch_stream_fetches_redacted_postmortem_on_session_end() {
        let chunks = vec![concat!(
            "data: {\"type\":\"session_end\",\"session_id\":\"session_target\",",
            "\"outcome\":\"Likely Completed\",\"total_tokens\":1234,\"total_turns\":3}\n\n"
        )
        .to_string()];
        let body = serde_json::json!({
            "session_id": "session_target",
            "redacted": true,
            "partial": false,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": "2026-01-01T00:01:00Z",
                "duration_secs": 60,
                "model": "claude-sonnet",
                "outcome": "Likely Completed",
                "total_turns": 3,
                "total_tokens": 1234
            },
            "diagnosis": {
                "likely_cause": "none",
                "detail": "No primary degradation cause was recorded.",
                "confidence": "low",
                "next_action": "Continue."
            },
            "impact": {
                "input_tokens": 1000,
                "output_tokens": 234,
                "cache_read_tokens": 0,
                "cache_creation_tokens": 0,
                "total_tokens": 1234,
                "estimated_total_cost_dollars": 0.01,
                "estimated_likely_wasted_tokens": 0,
                "estimated_likely_wasted_cost_dollars": 0.0,
                "cost_source": "builtin_model_family_pricing",
                "cost_caveat": "Estimated costs are local calculations, not authoritative billing records."
            },
            "signals": {
                "cache": { "cache_hit_ratio": 0.0 },
                "context": { "max_fill_percent": 1.0, "turns_to_compact": null }
            },
            "evidence": [],
            "timeline": [],
            "recommendations": ["Continue."],
            "caveats": []
        })
        .to_string();
        let (url, request_rx) = serve_watch_and_postmortem_once(chunks, "200 OK", &body);
        let mut active = ActiveSessions::new();
        let filter = None;
        let mut postmortem_state = WatchPostmortemState::new(true, &url, false);

        super::connect_and_stream(
            &url,
            false,
            false,
            &filter,
            &mut active,
            &mut postmortem_state,
        )
        .await
        .expect("watch stream closes cleanly");

        let requests = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured requests");
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("GET /api/postmortem/session_target?redact=true "));
        assert!(postmortem_state.rendered.contains("final:session_target"));
        assert_eq!(
            auto_postmortem_target(&WatchEvent::SessionEnd {
                session_id: "session_target".to_string(),
                outcome: "Likely Completed".to_string(),
                total_tokens: 1,
                total_turns: 1,
            }),
            Some((
                "session_target".to_string(),
                "final:session_target".to_string()
            ))
        );
    }

    #[test]
    fn auto_postmortem_dedupe_separates_idle_and_final_reports() {
        let idle = auto_postmortem_target(&WatchEvent::PostmortemReady {
            session_id: "session_same_turns".to_string(),
            idle_secs: 90,
            total_tokens: 123,
            total_turns: 4,
        });
        let final_report = auto_postmortem_target(&WatchEvent::SessionEnd {
            session_id: "session_same_turns".to_string(),
            outcome: "Likely Completed".to_string(),
            total_tokens: 123,
            total_turns: 4,
        });

        assert_eq!(
            idle,
            Some((
                "session_same_turns".to_string(),
                "idle:session_same_turns:4".to_string()
            ))
        );
        assert_eq!(
            final_report,
            Some((
                "session_same_turns".to_string(),
                "final:session_same_turns".to_string()
            ))
        );

        let mut rendered = HashSet::new();
        assert!(rendered.insert(idle.expect("idle target").1));
        assert!(rendered.insert(final_report.expect("final target").1));
    }

    #[tokio::test]
    async fn watch_stream_renders_idle_then_final_postmortems_with_same_turn_count() {
        let chunks = vec![
            concat!(
                "data: {\"type\":\"postmortem_ready\",\"session_id\":\"session_same_turns\",",
                "\"idle_secs\":90,\"total_tokens\":1234,\"total_turns\":4}\n\n"
            )
            .to_string(),
            concat!(
                "data: {\"type\":\"session_end\",\"session_id\":\"session_same_turns\",",
                "\"outcome\":\"Likely Completed\",\"total_tokens\":1234,\"total_turns\":4}\n\n"
            )
            .to_string(),
        ];
        let partial_body = serde_json::json!({
            "session_id": "session_same_turns",
            "redacted": true,
            "partial": true,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": null,
                "duration_secs": 60,
                "model": "claude-sonnet",
                "outcome": "In Progress",
                "total_turns": 4,
                "total_tokens": 1234
            },
            "diagnosis": {
                "likely_cause": "none",
                "detail": "Session is still active.",
                "confidence": "low",
                "next_action": "Continue."
            },
            "impact": { "total_tokens": 1234 },
            "signals": { "cache": {}, "context": {} },
            "evidence": [],
            "timeline": [],
            "recommendations": ["Continue."],
            "caveats": []
        })
        .to_string();
        let final_body = serde_json::json!({
            "session_id": "session_same_turns",
            "redacted": true,
            "partial": false,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": "2026-01-01T00:01:00Z",
                "duration_secs": 60,
                "model": "claude-sonnet",
                "outcome": "Likely Completed",
                "total_turns": 4,
                "total_tokens": 1234
            },
            "diagnosis": {
                "likely_cause": "none",
                "detail": "No primary degradation cause was recorded.",
                "confidence": "low",
                "next_action": "Continue."
            },
            "impact": { "total_tokens": 1234 },
            "signals": { "cache": {}, "context": {} },
            "evidence": [],
            "timeline": [],
            "recommendations": ["Continue."],
            "caveats": []
        })
        .to_string();
        let (url, request_rx) = serve_watch_and_postmortems(
            chunks,
            vec![
                ("200 OK".to_string(), partial_body),
                ("200 OK".to_string(), final_body),
            ],
        );
        let mut active = ActiveSessions::new();
        let filter = None;
        let mut postmortem_state = WatchPostmortemState::new(true, &url, false);

        super::connect_and_stream(
            &url,
            false,
            false,
            &filter,
            &mut active,
            &mut postmortem_state,
        )
        .await
        .expect("watch stream closes cleanly");

        let requests = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured requests");
        assert_eq!(requests.len(), 3);
        assert!(postmortem_state
            .rendered
            .contains("idle:session_same_turns:4"));
        assert!(postmortem_state
            .rendered
            .contains("final:session_same_turns"));
    }

    #[tokio::test]
    async fn run_final_postmortem_fetches_last_without_watch_event() {
        let body = serde_json::json!({
            "session_id": "session_last",
            "redacted": true,
            "partial": false,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": "2026-01-01T00:01:00Z",
                "duration_secs": 60,
                "model": "claude-sonnet",
                "outcome": "Likely Completed",
                "total_turns": 2,
                "total_tokens": 3456
            },
            "diagnosis": {
                "likely_cause": "none",
                "detail": "No primary degradation cause was recorded.",
                "confidence": "low",
                "next_action": "Continue."
            },
            "impact": {
                "input_tokens": 3000,
                "output_tokens": 456,
                "cache_read_tokens": 0,
                "cache_creation_tokens": 0,
                "total_tokens": 3456,
                "estimated_total_cost_dollars": 0.01,
                "estimated_likely_wasted_tokens": 0,
                "estimated_likely_wasted_cost_dollars": 0.0,
                "cost_source": "builtin_model_family_pricing",
                "cost_caveat": "Estimated costs are local calculations, not authoritative billing records."
            },
            "signals": {
                "cache": { "cache_hit_ratio": 0.0 },
                "context": {
                    "latest_fill_percent": 2.0,
                    "max_fill_percent": 2.0,
                    "turns_to_compact": null,
                    "context_window_tokens": 200000,
                    "heuristic": true
                }
            },
            "evidence": [],
            "timeline": [],
            "recommendations": ["Continue."],
            "caveats": []
        })
        .to_string();
        let (url, request_rx) = serve_postmortem_last_once(&body);

        let (markdown, warning) = fetch_run_final_postmortem_markdown(&url, false)
            .await
            .expect("final run postmortem");

        assert!(warning.is_none());
        assert!(markdown.contains("session_last"));
        let request = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured final postmortem request");
        assert!(request.starts_with("GET /api/postmortem/last?redact=true "));
    }

    #[tokio::test]
    async fn run_final_postmortem_retries_while_last_session_flushes() {
        let body = serde_json::json!({
            "session_id": "session_last_after_retry",
            "redacted": true,
            "partial": false,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": "2026-01-01T00:01:00Z",
                "duration_secs": 60,
                "model": "claude-sonnet",
                "outcome": "Likely Completed",
                "total_turns": 2,
                "total_tokens": 3456
            },
            "diagnosis": {
                "likely_cause": "none",
                "detail": "No primary degradation cause was recorded.",
                "confidence": "low",
                "next_action": "Continue."
            },
            "impact": {
                "input_tokens": 3000,
                "output_tokens": 456,
                "cache_read_tokens": 0,
                "cache_creation_tokens": 0,
                "total_tokens": 3456,
                "estimated_total_cost_dollars": 0.01,
                "estimated_likely_wasted_tokens": 0,
                "estimated_likely_wasted_cost_dollars": 0.0,
                "cost_source": "builtin_model_family_pricing"
            },
            "signals": {
                "cache": { "cache_hit_ratio": 0.0 },
                "context": {
                    "latest_fill_percent": 2.0,
                    "max_fill_percent": 2.0,
                    "turns_to_compact": null,
                    "context_window_tokens": 200000,
                    "heuristic": true
                }
            },
            "evidence": [],
            "timeline": [],
            "recommendations": ["Continue."],
            "caveats": []
        })
        .to_string();
        let (url, request_rx) = serve_postmortem_responses(vec![
            ("404 Not Found".to_string(), "no sessions found".to_string()),
            ("200 OK".to_string(), body),
        ]);

        let (markdown, warning) = fetch_run_final_postmortem_markdown_with_retry(
            &url,
            false,
            2,
            Duration::from_millis(1),
        )
        .await
        .expect("final run postmortem after retry");

        assert!(warning.is_none());
        assert!(markdown.contains("session_last_after_retry"));
        let requests = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured final postmortem requests");
        assert_eq!(requests.len(), 2);
        assert!(requests
            .iter()
            .all(|request| request.starts_with("GET /api/postmortem/last?redact=true ")));
    }

    #[tokio::test]
    async fn watch_stream_fetches_redacted_postmortem_on_idle_checkpoint() {
        let chunks = vec![concat!(
            "data: {\"type\":\"postmortem_ready\",\"session_id\":\"session_target\",",
            "\"idle_secs\":90,\"total_tokens\":1234,\"total_turns\":3}\n\n"
        )
        .to_string()];
        let body = serde_json::json!({
            "session_id": "session_target",
            "redacted": true,
            "partial": true,
            "summary": {
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": null,
                "duration_secs": 60,
                "model": "claude-sonnet",
                "outcome": "In Progress",
                "total_turns": 3,
                "total_tokens": 1234
            },
            "diagnosis": {
                "likely_cause": "none",
                "detail": "No primary degradation cause was recorded.",
                "confidence": "low",
                "next_action": "Continue."
            },
            "impact": {
                "input_tokens": 1000,
                "output_tokens": 234,
                "cache_read_tokens": 0,
                "cache_creation_tokens": 0,
                "total_tokens": 1234,
                "estimated_total_cost_dollars": 0.01,
                "estimated_likely_wasted_tokens": 0,
                "estimated_likely_wasted_cost_dollars": 0.0,
                "cost_source": "builtin_model_family_pricing",
                "cost_caveat": "Estimated costs are local calculations, not authoritative billing records."
            },
            "signals": {
                "cache": { "cache_hit_ratio": 0.0 },
                "context": { "max_fill_percent": 1.0, "turns_to_compact": null }
            },
            "evidence": [],
            "timeline": [],
            "recommendations": ["Continue."],
            "caveats": []
        })
        .to_string();
        let (url, request_rx) = serve_watch_and_postmortem_once(chunks, "200 OK", &body);
        let mut active = ActiveSessions::new();
        let filter = None;
        let mut postmortem_state = WatchPostmortemState::new(true, &url, false);

        super::connect_and_stream(
            &url,
            false,
            false,
            &filter,
            &mut active,
            &mut postmortem_state,
        )
        .await
        .expect("watch stream closes cleanly");

        let requests = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured requests");
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("GET /api/postmortem/session_target?redact=true "));
        assert!(postmortem_state.rendered.contains("idle:session_target:3"));
        assert_eq!(
            auto_postmortem_target(&WatchEvent::PostmortemReady {
                session_id: "session_target".to_string(),
                idle_secs: 90,
                total_tokens: 1,
                total_turns: 1,
            }),
            Some((
                "session_target".to_string(),
                "idle:session_target:1".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn watch_stream_postmortem_failure_does_not_terminate_stream() {
        let chunks = vec![concat!(
            "data: {\"type\":\"session_end\",\"session_id\":\"session_target\",",
            "\"outcome\":\"Likely Completed\",\"total_tokens\":1234,\"total_turns\":3}\n\n"
        )
        .to_string()];
        let (url, request_rx) =
            serve_watch_and_postmortem_once(chunks, "500 Internal Server Error", "broken");
        let mut active = ActiveSessions::new();
        let filter = None;
        let mut postmortem_state = WatchPostmortemState::new(true, &url, false);

        super::connect_and_stream(
            &url,
            false,
            false,
            &filter,
            &mut active,
            &mut postmortem_state,
        )
        .await
        .expect("postmortem failure should not close watch with an error");

        let requests = request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("captured requests");
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("GET /api/postmortem/session_target?redact=true "));
        assert!(postmortem_state.rendered.contains("final:session_target"));
    }

    #[test]
    fn watch_event_session_ids_skip_global_events() {
        let event = WatchEvent::ToolUse {
            session_id: "session_a".to_string(),
            timestamp: "2026-04-28T00:00:00Z".to_string(),
            tool_name: "Read".to_string(),
            summary: "src/main.rs".to_string(),
        };
        assert_eq!(event_session_id(&event), Some("session_a"));

        assert_eq!(
            event_session_id(&WatchEvent::RateLimitStatus {
                seconds_to_reset: Some(60),
                requests_remaining: None,
                requests_limit: None,
                input_tokens_remaining: None,
                output_tokens_remaining: None,
                tokens_used_this_week: Some(10),
                tokens_limit: Some(100),
                tokens_remaining: Some(90),
                budget_source: Some("env".to_string()),
                projected_exhaustion_secs: None,
            }),
            None
        );
        assert_eq!(event_session_id(&WatchEvent::Lagged { missed: 3 }), None);
    }

    #[test]
    fn watch_event_accepts_quota_burn_status_and_legacy_rate_limit_alias() {
        let current: WatchEvent = serde_json::from_str(
            r#"{"type":"quota_burn_status","tokens_used_this_week":10,"tokens_limit":100,"tokens_remaining":90}"#,
        )
        .expect("parse current quota event");
        assert!(matches!(
            current,
            WatchEvent::RateLimitStatus {
                tokens_used_this_week: Some(10),
                tokens_limit: Some(100),
                tokens_remaining: Some(90),
                ..
            }
        ));

        let legacy: WatchEvent =
            serde_json::from_str(r#"{"type":"rate_limit_status","tokens_used_this_week":10}"#)
                .expect("parse legacy quota event");
        assert!(matches!(
            legacy,
            WatchEvent::RateLimitStatus {
                tokens_used_this_week: Some(10),
                ..
            }
        ));
    }

    #[test]
    fn watch_event_schema_accepts_all_core_event_variants() {
        let fixtures = [
            r#"{"type":"tool_use","session_id":"s","timestamp":"2026-01-01T00:00:00Z","tool_name":"Read","summary":"src/main.rs"}"#,
            r#"{"type":"tool_result","session_id":"s","tool_name":"Bash","outcome":"success","duration_ms":12}"#,
            r#"{"type":"skill_event","session_id":"s","timestamp":"2026-01-01T00:00:00Z","skill_name":"tdd","event_type":"fired","source":"proxy","confidence":0.9,"detail":"ok"}"#,
            r#"{"type":"mcp_event","session_id":"s","timestamp":"2026-01-01T00:00:00Z","server":"github","tool":"get_issue","event_type":"called","source":"hook","detail":"ok"}"#,
            r#"{"type":"cache_event","session_id":"s","event_type":"miss_rebuild","cache_expires_at_epoch":1770000000,"estimated_rebuild_cost_dollars":0.24}"#,
            r#"{"type":"session_start","session_id":"s","display_name":"demo","model":"claude-sonnet-4-6","initial_prompt":"ship it"}"#,
            r#"{"type":"session_end","session_id":"s","outcome":"Completed","total_tokens":123,"total_turns":4}"#,
            r#"{"type":"postmortem_ready","session_id":"s","idle_secs":90,"total_tokens":123,"total_turns":4}"#,
            r#"{"type":"frustration_signal","session_id":"s","signal_type":"context_pressure"}"#,
            r#"{"type":"compaction_loop","session_id":"s","consecutive":3,"wasted_tokens":12000}"#,
            r#"{"type":"diagnosis","session_id":"s","report":{"outcome":"Completed","total_turns":4,"total_tokens":123,"cache_hit_ratio":0.5,"degraded":false,"degradation_turn":null,"causes":[],"advice":[]}}"#,
            r#"{"type":"cache_warning","session_id":"s","idle_secs":240,"ttl_secs":300}"#,
            r#"{"type":"model_fallback","session_id":"s","requested":"claude-opus-4-7","actual":"claude-sonnet-4-6"}"#,
            r#"{"type":"context_status","session_id":"s","fill_percent":72.5,"context_window_tokens":1000000,"turns_to_compact":2}"#,
            r#"{"type":"quota_burn_status","seconds_to_reset":3600,"tokens_used_this_week":10,"tokens_limit":100,"tokens_remaining":90,"budget_source":"env","projected_exhaustion_secs":1800}"#,
            r#"{"type":"lagged","missed":2}"#,
        ];

        for fixture in fixtures {
            serde_json::from_str::<WatchEvent>(fixture).expect(fixture);
        }
    }

    #[test]
    fn mcp_tool_names_split_server_and_tool() {
        assert_eq!(
            parse_mcp_tool_name("mcp__github__get_issue"),
            Some(("github", "get_issue"))
        );
        assert_eq!(
            parse_mcp_tool_name(" mcp__server__tool__suffix "),
            Some(("server", "tool__suffix"))
        );
        assert_eq!(parse_mcp_tool_name("Read"), None);
        assert_eq!(parse_mcp_tool_name("mcp__github"), None);
        assert_eq!(parse_mcp_tool_name("mcp____tool"), None);
    }

    #[test]
    fn push_unique_preserves_first_occurrence_order() {
        let mut lines = Vec::new();
        push_unique(&mut lines, "one");
        push_unique(&mut lines, "two");
        push_unique(&mut lines, "one");
        assert_eq!(lines, vec!["one", "two"]);
    }

    #[test]
    fn bundled_compose_uses_release_image_and_quoted_volume_mounts() {
        let yaml = super::bundled_compose_yaml(Path::new("/tmp/clauditor test"));
        assert!(yaml.contains(super::DEFAULT_CORE_IMAGE));
        assert!(yaml.contains("\"/tmp/clauditor test/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro\""));
        assert!(yaml
            .contains("\"/tmp/clauditor test/grafana/dashboards:/var/lib/grafana/dashboards:ro\""));
    }
}
