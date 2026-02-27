use std::{env, net::SocketAddr};

use pd_controller::{ControllerConfig, ControllerState, build_controller_app};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if wants_version_flag() {
        println!("{}", binary_version_text());
        return Ok(());
    }

    init_logging();
    info!("{}", binary_version_text());

    let addr = parse_addr("CONTROLLER_ADDR", "0.0.0.0:9100")?;
    let config = ControllerConfig {
        default_poll_interval_ms: parse_u64("CONTROLLER_DEFAULT_POLL_MS", 1_000)?,
        max_result_history: parse_usize("CONTROLLER_MAX_RESULT_HISTORY", 200)?,
        state_path: parse_state_path("CONTROLLER_STATE_PATH", ".pd-controller/state.json"),
    };

    let state = ControllerState::new(config);
    let app = build_controller_app(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("controller listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_logging() {
    let env_filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}

fn parse_addr(key: &str, default: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let value = env::var(key).unwrap_or_else(|_| default.to_string());
    Ok(value.parse()?)
}

fn parse_u64(key: &str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    match env::var(key) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn parse_usize(key: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    match env::var(key) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn parse_state_path(key: &str, default: &str) -> Option<std::path::PathBuf> {
    let value = env::var(key).unwrap_or_else(|_| default.to_string());
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(trimmed))
    }
}

fn wants_version_flag() -> bool {
    env::args()
        .skip(1)
        .any(|arg| matches!(arg.as_str(), "-V" | "--version"))
}

fn binary_version_text() -> String {
    let binary = env!("CARGO_PKG_NAME");
    let git_tag = option_env!("PD_BUILD_GIT_TAG").unwrap_or("untagged");
    let git_commit = option_env!("PD_BUILD_GIT_COMMIT").unwrap_or("unknown");
    let git_dirty = option_env!("PD_BUILD_GIT_DIRTY").unwrap_or("false");
    let dirty = matches!(git_dirty, "true" | "1" | "yes" | "dirty");

    if dirty {
        format!("{binary} {git_tag} (dirty commit: {git_commit})")
    } else {
        format!("{binary} {git_tag}")
    }
}
