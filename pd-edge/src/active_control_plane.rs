use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use rand::RngCore;
use tracing::{info, warn};

use crate::{
    CommandResultPayload, ControlPlaneCommand, EdgeCommandResult, EdgePollRequest,
    EdgePollResponse, RemoteDebugCommandResponse, SharedState, StartDebugSessionRequest,
    apply_program_from_bytes, run_debug_command, start_debug_session, stop_debug_session,
};

const MIN_POLL_INTERVAL_MS: u64 = 100;
const MAX_POLL_INTERVAL_MS: u64 = 60_000;
const DEFAULT_DEBUG_NONCE_HEADER: &str = "x-pd-debug-nonce";

#[derive(Clone, Debug)]
pub struct ActiveControlPlaneConfig {
    pub control_plane_url: String,
    pub edge_id: String,
    pub edge_name: String,
    pub poll_interval_ms: u64,
    pub request_timeout_ms: u64,
}

pub fn spawn_active_control_plane_client(
    state: SharedState,
    config: ActiveControlPlaneConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_active_control_plane_client(state, config).await;
    })
}

pub async fn run_active_control_plane_client(state: SharedState, config: ActiveControlPlaneConfig) {
    let poll_url = format!(
        "{}/rpc/v1/edge/poll",
        normalize_base_url(&config.control_plane_url)
    );
    let result_url = format!(
        "{}/rpc/v1/edge/result",
        normalize_base_url(&config.control_plane_url)
    );

    let mut sleep_for = Duration::from_millis(sanitize_interval(config.poll_interval_ms));
    let request_timeout = Duration::from_millis(sanitize_interval(config.request_timeout_ms));

    info!(
        "active control-plane client enabled edge_id={} edge_name={} endpoint={}",
        config.edge_id, config.edge_name, poll_url
    );

    loop {
        let telemetry = state.telemetry_snapshot().await;
        let traffic_sample = state.traffic_sample();
        let poll_request = EdgePollRequest {
            edge_id: config.edge_id.clone(),
            edge_name: Some(config.edge_name.clone()),
            telemetry,
            traffic_sample: Some(traffic_sample),
        };

        let response = state
            .client
            .post(&poll_url)
            .timeout(request_timeout)
            .json(&poll_request)
            .send()
            .await;

        match response {
            Ok(response) => {
                if !response.status().is_success() {
                    state.record_control_rpc_poll_error();
                    warn!(
                        "control-plane poll failed with status {}",
                        response.status()
                    );
                    tokio::time::sleep(sleep_for).await;
                    continue;
                }

                let payload = match response.json::<EdgePollResponse>().await {
                    Ok(payload) => payload,
                    Err(err) => {
                        state.record_control_rpc_poll_error();
                        warn!("failed to decode control-plane poll payload: {err}");
                        tokio::time::sleep(sleep_for).await;
                        continue;
                    }
                };
                state.record_control_rpc_poll_success();
                sleep_for = Duration::from_millis(sanitize_interval(payload.poll_interval_ms));

                if let Some(command) = payload.command {
                    let result = execute_command(
                        &state,
                        &config.edge_id,
                        &config.edge_name,
                        command.clone(),
                    )
                    .await;
                    let result_ok = result.ok;

                    let send_result = state
                        .client
                        .post(&result_url)
                        .timeout(request_timeout)
                        .json(&result)
                        .send()
                        .await;

                    match send_result {
                        Ok(response) if response.status().is_success() => {
                            state.record_control_rpc_result_success();
                            info!(
                                "reported command result to control-plane command_id={} ok={}",
                                command.command_id(),
                                result_ok
                            );
                        }
                        Ok(response) => {
                            state.record_control_rpc_result_error();
                            warn!(
                                "control-plane result endpoint rejected command_id={} status={}",
                                command.command_id(),
                                response.status()
                            );
                        }
                        Err(err) => {
                            state.record_control_rpc_result_error();
                            warn!(
                                "failed to report command result command_id={} err={err}",
                                command.command_id()
                            );
                        }
                    }
                }
            }
            Err(err) => {
                state.record_control_rpc_poll_error();
                warn!("control-plane poll transport error: {err}");
            }
        }

        tokio::time::sleep(sleep_for).await;
    }
}

async fn execute_command(
    state: &SharedState,
    edge_id: &str,
    edge_name: &str,
    command: ControlPlaneCommand,
) -> EdgeCommandResult {
    let command_id = command.command_id().to_string();
    let mut ok = true;

    let result = match command {
        ControlPlaneCommand::ApplyProgram { program_base64, .. } => {
            match STANDARD.decode(program_base64.as_bytes()) {
                Ok(bytes) => {
                    let report = apply_program_from_bytes(state, &bytes).await;
                    ok = report.applied;
                    CommandResultPayload::ApplyProgram { report }
                }
                Err(err) => {
                    ok = false;
                    CommandResultPayload::Error {
                        message: format!("invalid base64 program payload: {err}"),
                    }
                }
            }
        }
        ControlPlaneCommand::StartDebugSession {
            header_name,
            stop_on_entry,
            ..
        } => {
            let header_name = header_name.unwrap_or_else(|| DEFAULT_DEBUG_NONCE_HEADER.to_string());
            let nonce = generate_debug_nonce();
            let request = StartDebugSessionRequest {
                header_name: header_name.clone(),
                header_value: nonce.clone(),
                tcp_addr: None,
                stop_on_entry: stop_on_entry.unwrap_or(true),
            };
            match start_debug_session(&state.debug_session, request) {
                Ok(status) => CommandResultPayload::StartDebugSession {
                    status: Some(status),
                    nonce_header_name: Some(header_name),
                    nonce_header_value: Some(nonce),
                    message: None,
                },
                Err(err) => {
                    ok = false;
                    CommandResultPayload::StartDebugSession {
                        status: None,
                        nonce_header_name: None,
                        nonce_header_value: None,
                        message: Some(err.to_string()),
                    }
                }
            }
        }
        ControlPlaneCommand::DebugCommand {
            session_id,
            command,
            ..
        } => match run_debug_command(&state.debug_session, command) {
            Ok(response) => CommandResultPayload::DebugCommand {
                session_id: Some(session_id),
                response: Some(RemoteDebugCommandResponse {
                    output: response.output,
                    current_line: response.current_line,
                    attached: response.attached,
                }),
                message: None,
            },
            Err(err) => {
                ok = false;
                CommandResultPayload::DebugCommand {
                    session_id: Some(session_id),
                    response: None,
                    message: Some(err.to_string()),
                }
            }
        },
        ControlPlaneCommand::StopDebugSession { .. } => {
            let stopped = stop_debug_session(&state.debug_session);
            CommandResultPayload::StopDebugSession { stopped }
        }
        ControlPlaneCommand::GetHealth { .. } => {
            let status = state.health_status().await;
            CommandResultPayload::Health { status }
        }
        ControlPlaneCommand::GetMetrics { .. } => {
            let text = state.metrics_text().await;
            CommandResultPayload::Metrics { text }
        }
        ControlPlaneCommand::GetTelemetry { .. } => {
            let snapshot = state.telemetry_snapshot().await;
            CommandResultPayload::Telemetry { snapshot }
        }
        ControlPlaneCommand::Ping { payload, .. } => CommandResultPayload::Pong { payload },
    };

    let telemetry = state.telemetry_snapshot().await;
    EdgeCommandResult {
        edge_id: edge_id.to_string(),
        edge_name: Some(edge_name.to_string()),
        command_id,
        ok,
        result,
        telemetry,
    }
}

fn normalize_base_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

fn sanitize_interval(value: u64) -> u64 {
    value.clamp(MIN_POLL_INTERVAL_MS, MAX_POLL_INTERVAL_MS)
}

fn generate_debug_nonce() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = std::fmt::Write::write_fmt(&mut out, format_args!("{byte:02x}"));
    }
    out
}
