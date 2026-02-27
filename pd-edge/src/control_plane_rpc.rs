use serde::{Deserialize, Serialize};

use crate::{DebugSessionStatus, HealthStatus, ProgramApplyReport, TelemetrySnapshot};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgePollRequest {
    pub edge_id: String,
    #[serde(default)]
    pub edge_name: Option<String>,
    pub telemetry: TelemetrySnapshot,
    #[serde(default)]
    pub traffic_sample: Option<EdgeTrafficSample>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeTrafficSample {
    pub requests_total: u64,
    pub status_2xx_total: u64,
    pub status_3xx_total: u64,
    pub status_4xx_total: u64,
    pub status_5xx_total: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgePollResponse {
    pub command: Option<ControlPlaneCommand>,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlPlaneCommand {
    ApplyProgram {
        command_id: String,
        program_base64: String,
    },
    StartDebugSession {
        command_id: String,
        tcp_addr: String,
        header_name: Option<String>,
        stop_on_entry: Option<bool>,
    },
    DebugCommand {
        command_id: String,
        session_id: String,
        command: RemoteDebugCommand,
    },
    StopDebugSession {
        command_id: String,
    },
    GetHealth {
        command_id: String,
    },
    GetMetrics {
        command_id: String,
    },
    GetTelemetry {
        command_id: String,
    },
    Ping {
        command_id: String,
        payload: Option<String>,
    },
}

impl ControlPlaneCommand {
    pub fn command_id(&self) -> &str {
        match self {
            ControlPlaneCommand::ApplyProgram { command_id, .. }
            | ControlPlaneCommand::StartDebugSession { command_id, .. }
            | ControlPlaneCommand::DebugCommand { command_id, .. }
            | ControlPlaneCommand::StopDebugSession { command_id }
            | ControlPlaneCommand::GetHealth { command_id }
            | ControlPlaneCommand::GetMetrics { command_id }
            | ControlPlaneCommand::GetTelemetry { command_id }
            | ControlPlaneCommand::Ping { command_id, .. } => command_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteDebugCommand {
    Where,
    Step,
    Next,
    Continue,
    Out,
    BreakLine { line: u32 },
    ClearLine { line: u32 },
    PrintVar { name: String },
    Locals,
    Stack,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteDebugCommandResponse {
    pub output: String,
    pub current_line: Option<u32>,
    pub attached: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeCommandResult {
    pub edge_id: String,
    #[serde(default)]
    pub edge_name: Option<String>,
    pub command_id: String,
    pub ok: bool,
    #[serde(flatten)]
    pub result: CommandResultPayload,
    pub telemetry: TelemetrySnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "result_type", rename_all = "snake_case")]
pub enum CommandResultPayload {
    ApplyProgram {
        report: ProgramApplyReport,
    },
    StartDebugSession {
        status: Option<DebugSessionStatus>,
        nonce_header_name: Option<String>,
        nonce_header_value: Option<String>,
        message: Option<String>,
    },
    DebugCommand {
        session_id: Option<String>,
        response: Option<RemoteDebugCommandResponse>,
        message: Option<String>,
    },
    StopDebugSession {
        stopped: bool,
    },
    Health {
        status: HealthStatus,
    },
    Metrics {
        text: String,
    },
    Telemetry {
        snapshot: TelemetrySnapshot,
    },
    Pong {
        payload: Option<String>,
    },
    Error {
        message: String,
    },
}

const fn default_poll_interval_ms() -> u64 {
    1_000
}
