use std::{sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, Uri,
        header::{CONTENT_TYPE, HOST},
    },
    middleware::{self, Next},
    response::IntoResponse,
    routing::{any, put},
};
use tokio::sync::RwLock;
use tracing::{info, warn};
use vm::{Program, Vm, VmStatus, decode_program, infer_local_count, validate_program};

use crate::{
    HOST_FUNCTION_COUNT,
    debug_session::{
        DebugSessionStatus, SharedDebugSession, StartDebugSessionRequest, debug_session_status,
        new_debug_session_store, run_vm_with_optional_debugger, start_debug_session,
        stop_debug_session,
    },
    host_abi::{
        ProxyVmContext, RateLimiterStore, SharedRateLimiter, register_host_module,
        snapshot_execution_outcome,
    },
    logging::{category_access, category_debug, category_program, method_label, status_label},
};

#[derive(Clone)]
pub struct SharedState {
    pub active_program: Arc<RwLock<Option<Arc<LoadedProgram>>>>,
    pub max_program_bytes: usize,
    pub client: reqwest::Client,
    pub rate_limiter: SharedRateLimiter,
    pub debug_session: SharedDebugSession,
}

#[derive(Clone)]
pub struct LoadedProgram {
    pub program: Arc<Program>,
    pub local_count: usize,
}

impl SharedState {
    pub fn new(max_program_bytes: usize) -> Self {
        Self {
            active_program: Arc::new(RwLock::new(None)),
            max_program_bytes,
            client: reqwest::Client::new(),
            rate_limiter: Arc::new(std::sync::Mutex::new(RateLimiterStore::new())),
            debug_session: new_debug_session_store(),
        }
    }
}

pub fn build_data_app(state: SharedState) -> Router {
    Router::new()
        .fallback(any(data_plane_handler))
        .layer(middleware::from_fn(access_log_middleware))
        .with_state(state)
}

pub fn build_control_app(state: SharedState) -> Router {
    Router::new()
        .route("/program", put(upload_program_handler))
        .route(
            "/debug/session",
            put(start_debug_session_handler)
                .delete(stop_debug_session_handler)
                .get(debug_session_status_handler),
        )
        .layer(middleware::from_fn(access_log_middleware))
        .with_state(state)
}

async fn data_plane_handler(State(state): State<SharedState>, request: Request) -> Response<Body> {
    let snapshot = {
        let guard = state.active_program.read().await;
        guard.clone()
    };

    let Some(program) = snapshot else {
        warn!("{} no program loaded; returning 404", category_program());
        return text_response(StatusCode::NOT_FOUND, "not found");
    };

    let (parts, body) = request.into_parts();
    let body_bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!("{} failed to read request body: {err}", category_program());
            return text_response(StatusCode::BAD_REQUEST, "invalid request body");
        }
    };

    let proxy_inputs = {
        let method = parts.method.clone();
        let uri = parts.uri.clone();
        let request_headers = parts.headers.clone();
        let vm_outcome = match execute_vm_for_request(&state, &program, request_headers.clone())
            .await
        {
            Ok(outcome) => outcome,
            Err(VmExecutionError::HostRegistration(err)) => {
                warn!(
                    "{} failed to register host module: {err}",
                    category_program()
                );
                return text_response(StatusCode::NOT_FOUND, "not found");
            }
            Err(VmExecutionError::Vm(err)) => {
                warn!("{} vm execution error: {err}", category_program());
                return text_response(StatusCode::NOT_FOUND, "not found");
            }
            Err(VmExecutionError::NotHalted(status)) => {
                warn!(
                    "{} vm returned non-halted status {:?}",
                    category_program(),
                    status
                );
                return text_response(StatusCode::NOT_FOUND, "not found");
            }
            Err(VmExecutionError::TaskJoin(err)) => {
                warn!("{} vm execution task failed: {err}", category_program());
                return text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error");
            }
        };

        if let Some(body) = vm_outcome.response_content {
            info!(
                "{} vm short-circuited response ({} bytes)",
                category_program(),
                body.len()
            );
            return short_circuit_response(body, vm_outcome.response_headers);
        }

        let Some(upstream) = vm_outcome.upstream else {
            warn!(
                "{} vm did not set upstream or response content; returning 404",
                category_program()
            );
            return text_response(StatusCode::NOT_FOUND, "not found");
        };

        (
            method,
            uri,
            parts.headers,
            body_bytes,
            upstream,
            vm_outcome.response_headers,
        )
    };

    proxy_to_upstream(
        &state,
        proxy_inputs.0,
        proxy_inputs.1,
        proxy_inputs.2,
        proxy_inputs.3,
        proxy_inputs.4,
        proxy_inputs.5,
    )
    .await
}

async fn upload_program_handler(
    State(state): State<SharedState>,
    request: Request,
) -> Response<Body> {
    if !is_octet_stream(request.headers().get(CONTENT_TYPE)) {
        warn!(
            "{} rejected program upload with invalid content-type",
            category_program()
        );
        return text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "content-type must be application/octet-stream",
        );
    }

    let body = match to_bytes(request.into_body(), state.max_program_bytes + 1).await {
        Ok(body) => body,
        Err(err) => {
            warn!(
                "{} failed reading upload body or exceeded limit: {err}",
                category_program()
            );
            return text_response(StatusCode::PAYLOAD_TOO_LARGE, "payload too large");
        }
    };

    if body.len() > state.max_program_bytes {
        warn!(
            "{} upload too large: {} bytes (limit {})",
            category_program(),
            body.len(),
            state.max_program_bytes
        );
        return text_response(StatusCode::PAYLOAD_TOO_LARGE, "payload too large");
    }

    let program = match decode_program(&body) {
        Ok(program) => program,
        Err(err) => {
            warn!("{} decode error: {err}", category_program());
            return text_response(StatusCode::BAD_REQUEST, &format!("invalid program: {err}"));
        }
    };
    if let Err(err) = validate_program(&program, HOST_FUNCTION_COUNT) {
        warn!("{} validation error: {err}", category_program());
        return text_response(StatusCode::BAD_REQUEST, &format!("invalid bytecode: {err}"));
    }

    let local_count = match infer_local_count(&program) {
        Ok(local_count) => local_count,
        Err(err) => {
            warn!("{} local inference error: {err}", category_program());
            return text_response(StatusCode::BAD_REQUEST, &format!("invalid bytecode: {err}"));
        }
    };

    let const_count = program.constants.len();
    let code_len = program.code.len();
    let mut guard = state.active_program.write().await;
    *guard = Some(Arc::new(LoadedProgram {
        program: Arc::new(program),
        local_count,
    }));
    info!(
        "{} loaded program successfully (constants={}, code_bytes={}, locals={})",
        category_program(),
        const_count,
        code_len,
        local_count
    );
    no_content_response()
}

async fn start_debug_session_handler(
    State(state): State<SharedState>,
    Json(request): Json<StartDebugSessionRequest>,
) -> impl IntoResponse {
    match start_debug_session(&state.debug_session, request) {
        Ok(status) => {
            info!(
                "{} debug session started via control endpoint",
                category_debug()
            );
            (StatusCode::CREATED, Json(status)).into_response()
        }
        Err(err) => {
            warn!("{} failed to start debug session: {err}", category_debug());
            (err.status_code(), err.to_string()).into_response()
        }
    }
}

async fn stop_debug_session_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let stopped = stop_debug_session(&state.debug_session);
    if stopped {
        info!("{} debug session stopped", category_debug());
    } else {
        info!(
            "{} stop requested but no session was active",
            category_debug()
        );
    }
    StatusCode::NO_CONTENT
}

async fn debug_session_status_handler(State(state): State<SharedState>) -> impl IntoResponse {
    let status: DebugSessionStatus = debug_session_status(&state.debug_session);
    (StatusCode::OK, Json(status))
}

async fn proxy_to_upstream(
    state: &SharedState,
    method: Method,
    uri: Uri,
    request_headers: HeaderMap,
    request_body: Bytes,
    upstream: String,
    vm_response_headers: HeaderMap,
) -> Response<Body> {
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let upstream_url = format!("http://{upstream}{path_and_query}");

    let mut outbound = state
        .client
        .request(method, upstream_url)
        .body(request_body.to_vec());
    for (name, value) in &request_headers {
        if name != HOST && !is_hop_by_hop(name) {
            outbound = outbound.header(name, value);
        }
    }
    outbound = outbound.header(HOST, upstream.as_str());

    let upstream_response = match outbound.send().await {
        Ok(response) => response,
        Err(err) => {
            warn!("{} upstream request failed: {err}", category_program());
            return text_response(StatusCode::BAD_GATEWAY, "bad gateway");
        }
    };

    let status = upstream_response.status();
    let upstream_headers = upstream_response.headers().clone();
    let body = match upstream_response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!(
                "{} failed reading upstream response body: {err}",
                category_program()
            );
            return text_response(StatusCode::BAD_GATEWAY, "bad gateway");
        }
    };

    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    for (name, value) in &upstream_headers {
        if !is_hop_by_hop(name) {
            response.headers_mut().insert(name, value.clone());
        }
    }

    merge_headers(response.headers_mut(), &vm_response_headers);
    response
}

fn short_circuit_response(body: String, headers: HeaderMap) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    merge_headers(response.headers_mut(), &headers);
    if !response.headers().contains_key(CONTENT_TYPE) {
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    }
    response
}

fn merge_headers(target: &mut HeaderMap, overlay: &HeaderMap) {
    for (name, value) in overlay {
        target.insert(name, value.clone());
    }
}

fn no_content_response() -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    response
}

fn text_response(status: StatusCode, text: &str) -> Response<Body> {
    let mut response = Response::new(Body::from(text.to_string()));
    *response.status_mut() = status;
    response
}

fn is_octet_stream(value: Option<&HeaderValue>) -> bool {
    let Some(value) = value else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    value
        .split(';')
        .next()
        .map(|value| {
            value
                .trim()
                .eq_ignore_ascii_case("application/octet-stream")
        })
        .unwrap_or(false)
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

async fn access_log_middleware(request: Request, next: Next) -> Response<Body> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let started = Instant::now();
    let response = next.run(request).await;
    let elapsed_ms = started.elapsed().as_millis();
    let status = response.status();

    info!(
        "{} {} {} {} {}ms",
        category_access(),
        method_label(method.as_str()),
        status_label(status.as_u16()),
        uri,
        elapsed_ms
    );

    response
}

#[derive(Debug)]
enum VmExecutionError {
    HostRegistration(vm::VmError),
    Vm(vm::VmError),
    NotHalted(VmStatus),
    TaskJoin(tokio::task::JoinError),
}

async fn execute_vm_for_request(
    state: &SharedState,
    program: &LoadedProgram,
    request_headers: HeaderMap,
) -> Result<crate::host_abi::VmExecutionOutcome, VmExecutionError> {
    let local_count = program.local_count;
    let program = program.program.clone();
    let rate_limiter = state.rate_limiter.clone();
    let debug_session = state.debug_session.clone();

    let task = tokio::task::spawn_blocking(move || {
        let vm_context = Arc::new(std::sync::Mutex::new(ProxyVmContext::from_request_headers(
            request_headers.clone(),
            rate_limiter,
        )));

        let mut vm = Vm::with_locals((*program).clone(), local_count);
        register_host_module(&mut vm, vm_context.clone())
            .map_err(VmExecutionError::HostRegistration)?;

        let status = run_vm_with_optional_debugger(&debug_session, &request_headers, &mut vm)
            .map_err(VmExecutionError::Vm)?;
        if status != VmStatus::Halted {
            return Err(VmExecutionError::NotHalted(status));
        }

        Ok(snapshot_execution_outcome(&vm_context))
    });

    task.await.map_err(VmExecutionError::TaskJoin)?
}
