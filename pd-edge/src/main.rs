use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use edge::{
    ActiveControlPlaneConfig, SharedState, build_admin_app, build_data_app, init_logging,
    spawn_active_control_plane_client,
};
use tracing::{info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = match parse_cli_args() {
        Ok(CliAction::Run(cli)) => cli,
        Ok(CliAction::Help) => {
            print_cli_help();
            return Ok(());
        }
        Ok(CliAction::Version) => {
            println!("{}", binary_version_text());
            return Ok(());
        }
        Err(err) => {
            eprintln!("error: {err}\n");
            print_cli_help();
            return Err(err.into());
        }
    };

    init_logging()?;
    info!("{}", binary_version_text());

    let data_addr = parse_addr("DATA_ADDR", "0.0.0.0:8080")?;
    let admin_addr = parse_admin_addr("127.0.0.1:8081")?;
    let max_program_bytes = parse_max_program_bytes("MAX_PROGRAM_BYTES", 1024 * 1024)?;
    let active_control_url = cli.control_plane_url.clone();
    let edge_id_path = cli
        .edge_id_path
        .clone()
        .unwrap_or_else(|| PathBuf::from(".pd-edge/edge-id"));
    let has_partial_control_plane_flags = cli.edge_id.is_some()
        || cli.edge_id_path.is_some()
        || cli.control_plane_poll_interval_ms.is_some()
        || cli.control_plane_rpc_timeout_ms.is_some();

    if active_control_url.is_none() && has_partial_control_plane_flags {
        let err = "active control-plane flags require --control-plane-url".to_string();
        eprintln!("error: {err}\n");
        print_cli_help();
        return Err(err.into());
    }

    let poll_interval_ms = cli.control_plane_poll_interval_ms.unwrap_or(1_000);
    let request_timeout_ms = cli.control_plane_rpc_timeout_ms.unwrap_or(5_000);

    let state = SharedState::new(max_program_bytes);
    if let Some(control_plane_url) = active_control_url {
        let edge_name = cli.edge_name.clone().unwrap_or_else(default_edge_name);
        let edge_id = resolve_edge_id(cli.edge_id.as_deref(), edge_id_path.as_path())?;
        let config = ActiveControlPlaneConfig {
            control_plane_url,
            edge_id: edge_id.clone(),
            edge_name: edge_name.clone(),
            poll_interval_ms,
            request_timeout_ms,
        };
        spawn_active_control_plane_client(state.clone(), config);
        info!("active control-plane rpc enabled edge_id={edge_id} edge_name={edge_name}");
    }

    let data_app = build_data_app(state.clone());
    let admin_app = build_admin_app(state);

    let data_listener = tokio::net::TcpListener::bind(data_addr).await?;
    let admin_listener = tokio::net::TcpListener::bind(admin_addr).await?;

    info!(
        "data-plane listening on http://{}",
        data_listener.local_addr()?
    );
    info!(
        "admin endpoint listening on http://{}",
        admin_listener.local_addr()?
    );

    let data_server = axum::serve(data_listener, data_app);
    let admin_server = axum::serve(admin_listener, admin_app);

    tokio::select! {
        result = data_server => result?,
        result = admin_server => result?,
    }

    Ok(())
}

fn parse_addr(key: &str, default: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let value = env::var(key).unwrap_or_else(|_| default.to_string());
    Ok(value.parse()?)
}

fn parse_admin_addr(default: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    if let Ok(value) = env::var("ADMIN_ADDR") {
        return Ok(value.parse()?);
    }
    if let Ok(value) = env::var("CONTROL_ADDR") {
        warn!("CONTROL_ADDR is deprecated; use ADMIN_ADDR instead");
        return Ok(value.parse()?);
    }
    Ok(default.parse()?)
}

fn parse_max_program_bytes(key: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    match env::var(key) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

#[derive(Clone, Debug, Default)]
struct CliArgs {
    control_plane_url: Option<String>,
    edge_id: Option<String>,
    edge_name: Option<String>,
    edge_id_path: Option<PathBuf>,
    control_plane_poll_interval_ms: Option<u64>,
    control_plane_rpc_timeout_ms: Option<u64>,
}

enum CliAction {
    Run(CliArgs),
    Help,
    Version,
}

fn parse_cli_args() -> Result<CliAction, String> {
    let mut args = env::args().skip(1).peekable();
    let mut cli = CliArgs::default();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(CliAction::Help),
            "-V" | "--version" => return Ok(CliAction::Version),
            "--control-plane-url" => {
                cli.control_plane_url = Some(next_arg_value("--control-plane-url", &mut args)?);
            }
            "--edge-id" => {
                cli.edge_id = Some(next_arg_value("--edge-id", &mut args)?);
            }
            "--edge-name" => {
                cli.edge_name = Some(next_arg_value("--edge-name", &mut args)?);
            }
            "--edge-id-path" => {
                cli.edge_id_path =
                    Some(PathBuf::from(next_arg_value("--edge-id-path", &mut args)?));
            }
            "--control-plane-poll-interval-ms" => {
                let value = next_arg_value("--control-plane-poll-interval-ms", &mut args)?;
                cli.control_plane_poll_interval_ms =
                    Some(value.parse::<u64>().map_err(|_| {
                        format!("invalid --control-plane-poll-interval-ms: {value}")
                    })?);
            }
            "--control-plane-rpc-timeout-ms" => {
                let value = next_arg_value("--control-plane-rpc-timeout-ms", &mut args)?;
                cli.control_plane_rpc_timeout_ms = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid --control-plane-rpc-timeout-ms: {value}"))?,
                );
            }
            _ => {
                return Err(format!("unknown argument: {arg}"));
            }
        }
    }
    Ok(CliAction::Run(cli))
}

fn next_arg_value(
    flag: &str,
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
) -> Result<String, String> {
    let value = args
        .next()
        .ok_or_else(|| format!("missing value for {flag}"))?;
    if value.trim().is_empty() {
        return Err(format!("value for {flag} cannot be empty"));
    }
    Ok(value)
}

fn print_cli_help() {
    eprintln!(concat!(
        "Usage: pd-edge [options]\n\n",
        "Options:\n",
        "  --control-plane-url <URL>                 Enable active control-plane RPC client\n",
        "  --edge-id <UUID>                          Explicit edge UUID used by active control-plane client\n",
        "  --edge-name <NAME>                        Friendly edge name (default: hostname)\n",
        "  --edge-id-path <PATH>                     Edge UUID file path (default .pd-edge/edge-id)\n",
        "  --control-plane-poll-interval-ms <MS>     Poll interval for active control-plane client\n",
        "  --control-plane-rpc-timeout-ms <MS>       RPC timeout for active control-plane client\n",
        "  -V, --version                             Show version with git metadata\n",
        "  -h, --help                                Show this help\n\n",
        "Address and local admin settings still use env vars:\n",
        "  DATA_ADDR, ADMIN_ADDR, MAX_PROGRAM_BYTES\n",
        "  (legacy fallback: CONTROL_ADDR)\n"
    ));
}

fn binary_version_text() -> String {
    let binary = env!("CARGO_PKG_NAME");
    let pkg_version = env!("CARGO_PKG_VERSION");
    let git_tag = option_env!("PD_BUILD_GIT_TAG").unwrap_or("untagged");
    let git_commit = option_env!("PD_BUILD_GIT_COMMIT").unwrap_or("unknown");
    let git_dirty = option_env!("PD_BUILD_GIT_DIRTY").unwrap_or("false");
    let dirty = matches!(git_dirty, "true" | "1" | "yes" | "dirty");

    if dirty {
        format!("{binary} {pkg_version} (tag: {git_tag}, dirty commit: {git_commit})")
    } else if git_tag != "untagged" {
        format!("{binary} {pkg_version} (tag: {git_tag})")
    } else {
        format!("{binary} {pkg_version} (commit: {git_commit})")
    }
}

fn resolve_edge_id(
    explicit: Option<&str>,
    id_path: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(value) = explicit {
        let parsed = Uuid::parse_str(value)
            .map_err(|_| format!("--edge-id must be a valid UUID, got: {value}"))?;
        let id = parsed.to_string();
        persist_edge_id(id_path, &id)?;
        return Ok(id);
    }

    if id_path.exists() {
        let raw = fs::read_to_string(id_path)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(format!(
                "edge id file '{}' is empty; remove it or provide --edge-id",
                id_path.display()
            )
            .into());
        }
        match Uuid::parse_str(trimmed) {
            Ok(parsed) => {
                let normalized = parsed.to_string();
                if normalized != trimmed {
                    persist_edge_id(id_path, &normalized)?;
                }
                return Ok(normalized);
            }
            Err(_) => {
                warn!(
                    "invalid UUID in edge id file path={}, generating a new one",
                    id_path.display()
                );
            }
        }
    }

    let generated = Uuid::new_v4().to_string();
    persist_edge_id(id_path, &generated)?;
    Ok(generated)
}

fn persist_edge_id(path: &Path, edge_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{edge_id}\n"))?;
    Ok(())
}

fn default_edge_name() -> String {
    for key in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    "unknown-host".to_string()
}
