use std::{
    io,
    sync::{Arc, Mutex, RwLock},
};

use axum::http::{HeaderMap, HeaderName, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use vm::{Debugger, Vm, VmResult, VmStatus};

use crate::logging::category_debug;

pub struct DebugSessionStore {
    session: RwLock<Option<Arc<DebugSession>>>,
}

pub type SharedDebugSession = Arc<DebugSessionStore>;

#[derive(Clone, Debug, Deserialize)]
pub struct StartDebugSessionRequest {
    pub header_name: String,
    pub header_value: String,
    pub tcp_addr: String,
    #[serde(default = "default_stop_on_entry")]
    pub stop_on_entry: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugSessionStatus {
    pub active: bool,
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
    EmptyTcpAddr,
    Bind(io::Error),
}

impl DebugSessionError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            DebugSessionError::AlreadyActive => StatusCode::CONFLICT,
            DebugSessionError::InvalidHeaderName
            | DebugSessionError::EmptyHeaderValue
            | DebugSessionError::EmptyTcpAddr => StatusCode::BAD_REQUEST,
            DebugSessionError::Bind(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl std::fmt::Display for DebugSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DebugSessionError::AlreadyActive => write!(f, "debug session already active"),
            DebugSessionError::InvalidHeaderName => write!(f, "invalid debug header name"),
            DebugSessionError::EmptyHeaderValue => write!(f, "debug header value cannot be empty"),
            DebugSessionError::EmptyTcpAddr => write!(f, "tcp_addr cannot be empty"),
            DebugSessionError::Bind(err) => {
                write!(f, "failed to start debugger tcp listener: {err}")
            }
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
    if request.tcp_addr.trim().is_empty() {
        warn!(
            "{} rejected start request: empty tcp addr",
            category_debug()
        );
        return Err(DebugSessionError::EmptyTcpAddr);
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

    let mut debugger = Debugger::with_tcp(&request.tcp_addr).map_err(DebugSessionError::Bind)?;
    if request.stop_on_entry {
        debugger.stop_on_entry();
    }

    let session = Arc::new(DebugSession {
        header_name,
        header_value: request.header_value,
        tcp_addr: request.tcp_addr,
        stop_on_entry: request.stop_on_entry,
        debugger: Mutex::new(debugger),
    });
    let status = DebugSessionStatus::from_session(&session);
    *guard = Some(session);
    info!(
        "{} started session header={} value={} tcp_addr={} stop_on_entry={}",
        category_debug(),
        status.header_name.as_deref().unwrap_or(""),
        status.header_value.as_deref().unwrap_or(""),
        status.tcp_addr.as_deref().unwrap_or(""),
        status.stop_on_entry.unwrap_or(false)
    );
    Ok(status)
}

pub fn stop_debug_session(store: &SharedDebugSession) -> bool {
    let mut guard = store.session.write().expect("debug session lock poisoned");
    let stopped = guard.take().is_some();
    if stopped {
        info!("{} session stopped", category_debug());
    } else {
        info!("{} stop requested with no active session", category_debug());
    }
    stopped
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

fn stop_debug_session_if_match(store: &SharedDebugSession, active: &Arc<DebugSession>) {
    let mut guard = store.session.write().expect("debug session lock poisoned");
    if let Some(current) = guard.as_ref()
        && Arc::ptr_eq(current, active)
    {
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
    tcp_addr: String,
    stop_on_entry: bool,
    debugger: Mutex<Debugger>,
}

impl DebugSessionStatus {
    fn inactive() -> Self {
        Self {
            active: false,
            header_name: None,
            header_value: None,
            tcp_addr: None,
            stop_on_entry: None,
        }
    }

    fn from_session(session: &DebugSession) -> Self {
        Self {
            active: true,
            header_name: Some(session.header_name.as_str().to_string()),
            header_value: Some(session.header_value.clone()),
            tcp_addr: Some(session.tcp_addr.clone()),
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
            tcp_addr: "127.0.0.1:9500".to_string(),
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
            tcp_addr: "127.0.0.1:9500".to_string(),
            stop_on_entry: true,
        };
        let err = start_debug_session(&store, request).expect_err("request should be invalid");
        assert!(matches!(err, DebugSessionError::EmptyHeaderValue));
    }
}
