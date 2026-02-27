use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use axum::http::{HeaderMap, HeaderName, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use vm::{
    DebugCommandBridge, DebugCommandBridgeError, Debugger, Vm, VmResult, VmStatus,
};

use crate::{
    control_plane_rpc::{RemoteDebugCommand, RemoteDebugCommandResponse},
    logging::category_debug,
};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(8);

pub struct DebugSessionStore {
    session: RwLock<Option<Arc<DebugSession>>>,
}

pub type SharedDebugSession = Arc<DebugSessionStore>;

#[derive(Clone, Debug, Deserialize)]
pub struct StartDebugSessionRequest {
    pub header_name: String,
    pub header_value: String,
    #[serde(default)]
    pub tcp_addr: Option<String>,
    #[serde(default = "default_stop_on_entry")]
    pub stop_on_entry: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugSessionStatus {
    pub active: bool,
    pub attached: bool,
    pub current_line: Option<u32>,
    pub header_name: Option<String>,
    pub header_value: Option<String>,
    pub tcp_addr: Option<String>,
    pub stop_on_entry: Option<bool>,
}

#[derive(Debug)]
pub enum DebugSessionError {
    AlreadyActive,
    InvalidHeaderName,
    EmptyHeaderValue,
    NotActive,
    NotAttached,
    CommandTimeout,
    BridgeClosed,
    InvalidCommand(String),
}

impl DebugSessionError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            DebugSessionError::AlreadyActive => StatusCode::CONFLICT,
            DebugSessionError::InvalidHeaderName
            | DebugSessionError::EmptyHeaderValue
            | DebugSessionError::CommandTimeout
            | DebugSessionError::InvalidCommand(_)
            | DebugSessionError::BridgeClosed => StatusCode::BAD_REQUEST,
            DebugSessionError::NotActive | DebugSessionError::NotAttached => StatusCode::CONFLICT,
        }
    }
}

impl std::fmt::Display for DebugSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DebugSessionError::AlreadyActive => write!(f, "debug session already active"),
            DebugSessionError::InvalidHeaderName => write!(f, "invalid debug header name"),
            DebugSessionError::EmptyHeaderValue => write!(f, "debug header value cannot be empty"),
            DebugSessionError::NotActive => write!(f, "debug session is not active"),
            DebugSessionError::NotAttached => {
                write!(f, "debugger is not attached to a matching request yet")
            }
            DebugSessionError::CommandTimeout => {
                write!(f, "timed out waiting for debugger command response")
            }
            DebugSessionError::BridgeClosed => write!(f, "debugger bridge closed"),
            DebugSessionError::InvalidCommand(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for DebugSessionError {}

pub fn new_debug_session_store() -> SharedDebugSession {
    Arc::new(DebugSessionStore {
        session: RwLock::new(None),
    })
}

pub fn start_debug_session(
    store: &SharedDebugSession,
    request: StartDebugSessionRequest,
) -> Result<DebugSessionStatus, DebugSessionError> {
    if request.header_value.trim().is_empty() {
        warn!(
            "{} rejected start request: empty header value",
            category_debug()
        );
        return Err(DebugSessionError::EmptyHeaderValue);
    }
    let header_name = HeaderName::from_bytes(request.header_name.as_bytes()).map_err(|_| {
        warn!(
            "{} rejected start request: invalid header name",
            category_debug()
        );
        DebugSessionError::InvalidHeaderName
    })?;

    let mut guard = store.session.write().expect("debug session lock poisoned");
    if guard.is_some() {
        warn!(
            "{} start requested while session already active",
            category_debug()
        );
        return Err(DebugSessionError::AlreadyActive);
    }

    let bridge = DebugCommandBridge::new();
    let mut debugger = Debugger::with_command_bridge(bridge.clone());
    if request.stop_on_entry {
        debugger.stop_on_entry();
    }

    let session = Arc::new(DebugSession {
        header_name,
        header_value: request.header_value,
        stop_on_entry: request.stop_on_entry,
        debugger: Mutex::new(debugger),
        bridge,
    });
    let status = DebugSessionStatus::from_session(&session);
    *guard = Some(session);
    info!(
        "{} started session header={} value={} stop_on_entry={}",
        category_debug(),
        status.header_name.as_deref().unwrap_or(""),
        status.header_value.as_deref().unwrap_or(""),
        status.stop_on_entry.unwrap_or(false)
    );
    Ok(status)
}

pub fn stop_debug_session(store: &SharedDebugSession) -> bool {
    let mut guard = store.session.write().expect("debug session lock poisoned");
    let stopped = guard.take();
    if let Some(session) = stopped {
        session.bridge.close();
        info!("{} session stopped", category_debug());
        true
    } else {
        info!("{} stop requested with no active session", category_debug());
        false
    }
}

pub fn run_debug_command(
    store: &SharedDebugSession,
    command: RemoteDebugCommand,
) -> Result<RemoteDebugCommandResponse, DebugSessionError> {
    let session = {
        let guard = store.session.read().expect("debug session lock poisoned");
        guard.clone().ok_or(DebugSessionError::NotActive)?
    };
    let (command_text, resume_mode) = debug_command_text(&command)?;
    let response = session
        .bridge
        .execute(command_text.clone(), COMMAND_TIMEOUT)
        .map_err(map_bridge_error)?;
    if resume_mode {
        return Ok(RemoteDebugCommandResponse {
            output: format!("sent '{command_text}'"),
            current_line: None,
            attached: false,
        });
    }
    Ok(RemoteDebugCommandResponse {
        output: response.output,
        current_line: response.current_line,
        attached: response.attached,
    })
}

pub fn debug_session_status(store: &SharedDebugSession) -> DebugSessionStatus {
    let guard = store.session.read().expect("debug session lock poisoned");
    if let Some(session) = guard.as_ref() {
        DebugSessionStatus::from_session(session)
    } else {
        DebugSessionStatus::inactive()
    }
}

pub fn run_vm_with_optional_debugger(
    store: &SharedDebugSession,
    request_headers: &HeaderMap,
    vm: &mut Vm,
) -> VmResult<VmStatus> {
    let session = {
        let guard = store.session.read().expect("debug session lock poisoned");
        guard.clone()
    };

    if let Some(session) = session
        && request_matches_session(request_headers, &session)
    {
        info!(
            "{} request matched debug session header={}, attaching pdb",
            category_debug(),
            session.header_name.as_str()
        );
        let mut debugger = session.debugger.lock().expect("debugger lock poisoned");
        let result = vm.run_with_debugger(&mut debugger);
        let detached = debugger.take_detach_event();
        drop(debugger);

        if detached {
            stop_debug_session_if_match(store, &session);
        }
        return result;
    }

    vm.run()
}

fn map_bridge_error(error: DebugCommandBridgeError) -> DebugSessionError {
    match error {
        DebugCommandBridgeError::NotAttached => DebugSessionError::NotAttached,
        DebugCommandBridgeError::Timeout => DebugSessionError::CommandTimeout,
        DebugCommandBridgeError::Closed => DebugSessionError::BridgeClosed,
    }
}

fn stop_debug_session_if_match(store: &SharedDebugSession, active: &Arc<DebugSession>) {
    let mut guard = store.session.write().expect("debug session lock poisoned");
    if let Some(current) = guard.as_ref()
        && Arc::ptr_eq(current, active)
    {
        current.bridge.close();
        *guard = None;
        info!(
            "{} session removed automatically after debugger detached",
            category_debug()
        );
    }
}

fn request_matches_session(request_headers: &HeaderMap, session: &DebugSession) -> bool {
    request_headers
        .get(&session.header_name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value == session.header_value)
        .unwrap_or(false)
}

fn default_stop_on_entry() -> bool {
    true
}

struct DebugSession {
    header_name: HeaderName,
    header_value: String,
    stop_on_entry: bool,
    debugger: Mutex<Debugger>,
    bridge: DebugCommandBridge,
}

fn debug_command_text(command: &RemoteDebugCommand) -> Result<(String, bool), DebugSessionError> {
    match command {
        RemoteDebugCommand::Where => Ok(("where".to_string(), false)),
        RemoteDebugCommand::Step => Ok(("step".to_string(), true)),
        RemoteDebugCommand::Next => Ok(("next".to_string(), true)),
        RemoteDebugCommand::Continue => Ok(("continue".to_string(), true)),
        RemoteDebugCommand::Out => Ok(("out".to_string(), true)),
        RemoteDebugCommand::BreakLine { line } => Ok((format!("break line {line}"), false)),
        RemoteDebugCommand::ClearLine { line } => Ok((format!("clear line {line}"), false)),
        RemoteDebugCommand::PrintVar { name } => {
            if name.trim().is_empty() {
                return Err(DebugSessionError::InvalidCommand(
                    "variable name cannot be empty".to_string(),
                ));
            }
            Ok((format!("print {}", name.trim()), false))
        }
        RemoteDebugCommand::Locals => Ok(("locals".to_string(), false)),
        RemoteDebugCommand::Stack => Ok(("stack".to_string(), false)),
    }
}

impl DebugSessionStatus {
    fn inactive() -> Self {
        Self {
            active: false,
            attached: false,
            current_line: None,
            header_name: None,
            header_value: None,
            tcp_addr: None,
            stop_on_entry: None,
        }
    }

    fn from_session(session: &DebugSession) -> Self {
        let bridge_status = session.bridge.status();
        Self {
            active: true,
            attached: bridge_status.attached,
            current_line: bridge_status.current_line,
            header_name: Some(session.header_name.as_str().to_string()),
            header_value: Some(session.header_value.clone()),
            tcp_addr: None,
            stop_on_entry: Some(session.stop_on_entry),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_is_inactive_by_default() {
        let store = new_debug_session_store();
        let status = debug_session_status(&store);
        assert!(!status.active);
        assert!(!status.attached);
    }

    #[test]
    fn stop_noop_returns_false_when_not_active() {
        let store = new_debug_session_store();
        assert!(!stop_debug_session(&store));
    }

    #[test]
    fn invalid_session_request_is_rejected() {
        let store = new_debug_session_store();
        let request = StartDebugSessionRequest {
            header_name: "bad header".to_string(),
            header_value: "x".to_string(),
            tcp_addr: None,
            stop_on_entry: true,
        };
        let err = start_debug_session(&store, request).expect_err("request should be invalid");
        assert!(matches!(err, DebugSessionError::InvalidHeaderName));
    }

    #[test]
    fn empty_header_value_is_rejected() {
        let store = new_debug_session_store();
        let request = StartDebugSessionRequest {
            header_name: "x-debug".to_string(),
            header_value: "".to_string(),
            tcp_addr: None,
            stop_on_entry: true,
        };
        let err = start_debug_session(&store, request).expect_err("request should be invalid");
        assert!(matches!(err, DebugSessionError::EmptyHeaderValue));
    }
}
