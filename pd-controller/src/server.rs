use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::{get, post, put},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use edge::{
    CommandResultPayload, ControlPlaneCommand, EdgeCommandResult, EdgePollRequest,
    EdgePollResponse, EdgeTrafficSample, RemoteDebugCommand, TelemetrySnapshot,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::oneshot,
    time::{Duration, timeout},
};
use tracing::warn;
use uuid::Uuid;
use vm::{SourceFlavor, compile_source_with_flavor, encode_program};

const MAX_UPLOAD_BYTES: usize = 8 * 1024 * 1024;
const MAX_UI_BLOCKS: usize = 256;
const MAX_TRAFFIC_POINTS: usize = 720;
const PERSISTENCE_SCHEMA_VERSION: u32 = 1;
const TIMESERIES_BINARY_MAGIC: [u8; 4] = *b"PDTS";
const DEFAULT_REMOTE_DEBUGGER_TCP_ADDR: &str = "127.0.0.1:9002";
const DEBUG_RESUME_GRACE_MS: u64 = 1_500;

mod embedded_webui {
    include!(concat!(env!("OUT_DIR"), "/embedded_webui.rs"));
}

#[derive(Clone, Debug)]
pub struct ControllerConfig {
    pub default_poll_interval_ms: u64,
    pub max_result_history: usize,
    pub state_path: Option<PathBuf>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            default_poll_interval_ms: 1_000,
            max_result_history: 200,
            state_path: None,
        }
    }
}

#[derive(Clone)]
pub struct ControllerState {
    inner: Arc<tokio::sync::RwLock<ControllerStore>>,
    metrics: Arc<ControllerMetrics>,
    command_sequence: Arc<AtomicU64>,
    program_sequence: Arc<AtomicU64>,
    debug_sessions: Arc<tokio::sync::RwLock<HashMap<String, DebugSessionRecord>>>,
    debug_start_lookup: Arc<tokio::sync::RwLock<HashMap<String, String>>>,
    debug_command_waiters: Arc<
        tokio::sync::Mutex<HashMap<String, oneshot::Sender<Result<DebugCommandResponse, String>>>>,
    >,
    persist_lock: Arc<tokio::sync::Mutex<()>>,
    config: ControllerConfig,
}

#[derive(Default)]
struct ControllerStore {
    edges: HashMap<String, EdgeRecord>,
    edge_lookup: HashMap<String, String>,
    programs: HashMap<String, StoredProgram>,
}

#[derive(Default)]
struct EdgeRecord {
    edge_name: String,
    pending_commands: VecDeque<ControlPlaneCommand>,
    recent_results: VecDeque<EdgeCommandResult>,
    pending_apply_programs: HashMap<String, AppliedProgramRef>,
    applied_program: Option<AppliedProgramRef>,
    traffic_points: VecDeque<EdgeTrafficPoint>,
    last_traffic_cumulative: Option<EdgeTrafficSample>,
    last_poll_unix_ms: Option<u64>,
    last_result_unix_ms: Option<u64>,
    last_telemetry: Option<TelemetrySnapshot>,
    total_polls: u64,
    total_results: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DebugSessionPhase {
    Queued,
    WaitingForStartResult,
    WaitingForAttach,
    Attached,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugSessionSummary {
    pub session_id: String,
    pub edge_id: String,
    pub edge_name: String,
    pub phase: DebugSessionPhase,
    pub header_name: Option<String>,
    pub nonce_header_value: Option<String>,
    pub current_line: Option<u32>,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DebugSessionDetail {
    pub session_id: String,
    pub edge_id: String,
    pub edge_name: String,
    pub phase: DebugSessionPhase,
    pub header_name: Option<String>,
    pub nonce_header_value: Option<String>,
    pub tcp_addr: String,
    pub start_command_id: String,
    pub stop_command_id: Option<String>,
    pub current_line: Option<u32>,
    pub source_flavor: Option<String>,
    pub source_code: Option<String>,
    pub breakpoints: Vec<u32>,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
    pub attached_unix_ms: Option<u64>,
    pub message: Option<String>,
    pub last_output: Option<String>,
}

#[derive(Clone, Debug)]
struct DebugSessionRecord {
    session_id: String,
    edge_id: String,
    edge_name: String,
    phase: DebugSessionPhase,
    requested_header_name: Option<String>,
    header_name: Option<String>,
    nonce_header_value: Option<String>,
    tcp_addr: String,
    start_command_id: String,
    stop_command_id: Option<String>,
    current_line: Option<u32>,
    source_flavor: Option<String>,
    source_code: Option<String>,
    breakpoints: HashSet<u32>,
    created_unix_ms: u64,
    updated_unix_ms: u64,
    attached_unix_ms: Option<u64>,
    last_resume_command_unix_ms: Option<u64>,
    message: Option<String>,
    last_output: Option<String>,
}

impl DebugSessionRecord {
    fn to_summary(&self) -> DebugSessionSummary {
        DebugSessionSummary {
            session_id: self.session_id.clone(),
            edge_id: self.edge_id.clone(),
            edge_name: self.edge_name.clone(),
            phase: self.phase.clone(),
            header_name: self.header_name.clone(),
            nonce_header_value: self.nonce_header_value.clone(),
            current_line: self.current_line,
            created_unix_ms: self.created_unix_ms,
            updated_unix_ms: self.updated_unix_ms,
            message: self.message.clone(),
        }
    }

    fn to_detail(&self) -> DebugSessionDetail {
        let mut breakpoints = self.breakpoints.iter().copied().collect::<Vec<_>>();
        breakpoints.sort_unstable();
        DebugSessionDetail {
            session_id: self.session_id.clone(),
            edge_id: self.edge_id.clone(),
            edge_name: self.edge_name.clone(),
            phase: self.phase.clone(),
            header_name: self.header_name.clone(),
            nonce_header_value: self.nonce_header_value.clone(),
            tcp_addr: self.tcp_addr.clone(),
            start_command_id: self.start_command_id.clone(),
            stop_command_id: self.stop_command_id.clone(),
            current_line: self.current_line,
            source_flavor: self.source_flavor.clone(),
            source_code: self.source_code.clone(),
            breakpoints,
            created_unix_ms: self.created_unix_ms,
            updated_unix_ms: self.updated_unix_ms,
            attached_unix_ms: self.attached_unix_ms,
            message: self.message.clone(),
            last_output: self.last_output.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControllerCoreSnapshot {
    #[serde(default = "snapshot_schema_version")]
    schema_version: u32,
    #[serde(default)]
    command_sequence: u64,
    #[serde(default)]
    program_sequence: u64,
    #[serde(default)]
    edges: HashMap<String, PersistedEdgeCoreRecord>,
    #[serde(default)]
    edge_lookup: HashMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ControllerProgramsSnapshot {
    #[serde(default = "snapshot_schema_version")]
    schema_version: u32,
    #[serde(default)]
    programs: HashMap<String, StoredProgram>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ControllerTimeseriesSnapshot {
    #[serde(default = "snapshot_schema_version")]
    schema_version: u32,
    #[serde(default)]
    edges: HashMap<String, PersistedEdgeTimeseriesRecord>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PersistedControllerStore {
    #[serde(default)]
    edges: HashMap<String, PersistedEdgeMergedRecord>,
    #[serde(default)]
    edge_lookup: HashMap<String, String>,
    #[serde(default)]
    programs: HashMap<String, StoredProgram>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PersistedEdgeCoreRecord {
    #[serde(default)]
    edge_id: Option<String>,
    #[serde(default)]
    edge_name: Option<String>,
    #[serde(default)]
    applied_program: Option<AppliedProgramRef>,
    #[serde(default)]
    last_poll_unix_ms: Option<u64>,
    #[serde(default)]
    last_result_unix_ms: Option<u64>,
    #[serde(default)]
    last_telemetry: Option<TelemetrySnapshot>,
    #[serde(default)]
    total_polls: u64,
    #[serde(default)]
    total_results: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PersistedEdgeTimeseriesRecord {
    #[serde(default)]
    traffic_points: VecDeque<EdgeTrafficPoint>,
    #[serde(default)]
    last_traffic_cumulative: Option<EdgeTrafficSample>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PersistedEdgeMergedRecord {
    #[serde(default)]
    edge_id: Option<String>,
    #[serde(default)]
    edge_name: Option<String>,
    #[serde(default)]
    applied_program: Option<AppliedProgramRef>,
    #[serde(default)]
    traffic_points: VecDeque<EdgeTrafficPoint>,
    #[serde(default)]
    last_traffic_cumulative: Option<EdgeTrafficSample>,
    #[serde(default)]
    last_poll_unix_ms: Option<u64>,
    #[serde(default)]
    last_result_unix_ms: Option<u64>,
    #[serde(default)]
    last_telemetry: Option<TelemetrySnapshot>,
    #[serde(default)]
    total_polls: u64,
    #[serde(default)]
    total_results: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControllerSnapshotLegacy {
    #[serde(default = "snapshot_schema_version")]
    schema_version: u32,
    #[serde(default)]
    command_sequence: u64,
    #[serde(default)]
    program_sequence: u64,
    #[serde(default)]
    store: PersistedControllerStore,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppliedProgramRef {
    pub program_id: String,
    pub name: String,
    pub version: u32,
}

struct ControllerMetrics {
    started_at: Instant,
    poll_requests_total: AtomicU64,
    result_posts_total: AtomicU64,
    commands_enqueued_total: AtomicU64,
    commands_delivered_total: AtomicU64,
    command_results_ok_total: AtomicU64,
    command_results_error_total: AtomicU64,
}

impl Default for ControllerMetrics {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            poll_requests_total: AtomicU64::new(0),
            result_posts_total: AtomicU64::new(0),
            commands_enqueued_total: AtomicU64::new(0),
            commands_delivered_total: AtomicU64::new(0),
            command_results_ok_total: AtomicU64::new(0),
            command_results_error_total: AtomicU64::new(0),
        }
    }
}

impl ControllerState {
    pub fn new(config: ControllerConfig) -> Self {
        let (store, command_sequence, program_sequence) =
            load_snapshot_from_disk(config.state_path.as_deref(), config.max_result_history);
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(store)),
            metrics: Arc::new(ControllerMetrics::default()),
            command_sequence: Arc::new(AtomicU64::new(command_sequence)),
            program_sequence: Arc::new(AtomicU64::new(program_sequence)),
            debug_sessions: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            debug_start_lookup: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            debug_command_waiters: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            persist_lock: Arc::new(tokio::sync::Mutex::new(())),
            config,
        }
    }

    fn next_command_id(&self) -> String {
        let id = self.command_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        format!("cmd-{id}")
    }

    fn next_program_id(&self) -> String {
        Uuid::new_v4().to_string()
    }

    async fn enqueue_command(
        &self,
        edge_identifier: String,
        command: ControlPlaneCommand,
    ) -> EnqueueCommandResponse {
        self.enqueue_command_tracked(edge_identifier, command, None)
            .await
    }

    async fn enqueue_command_tracked(
        &self,
        edge_identifier: String,
        command: ControlPlaneCommand,
        apply_program: Option<AppliedProgramRef>,
    ) -> EnqueueCommandResponse {
        let command_id = command.command_id().to_string();
        let pending_commands = {
            let mut guard = self.inner.write().await;
            let edge_id = guard.resolve_or_create_edge_id(&edge_identifier);
            let record = guard.edges.entry(edge_id).or_default();
            if record.edge_name.is_empty() {
                record.edge_name = edge_identifier.clone();
            }
            record.pending_commands.push_back(command);
            if let Some(program_ref) = apply_program {
                record
                    .pending_apply_programs
                    .insert(command_id.clone(), program_ref);
            }
            record.pending_commands.len()
        };
        self.metrics
            .commands_enqueued_total
            .fetch_add(1, Ordering::Relaxed);
        EnqueueCommandResponse {
            command_id,
            pending_commands,
        }
    }

    async fn persist_snapshot(&self) -> Result<(), String> {
        let Some(path) = self.config.state_path.clone() else {
            return Ok(());
        };
        let (programs_path, timeseries_path) = sidecar_snapshot_paths(path.as_path());

        let _save_guard = self.persist_lock.lock().await;
        let (core_snapshot, programs_snapshot, timeseries_snapshot) = {
            let guard = self.inner.read().await;
            let persisted = guard.to_persisted();
            let core_edges = persisted
                .edges
                .iter()
                .map(|(edge_id, record)| {
                    (
                        edge_id.clone(),
                        PersistedEdgeCoreRecord {
                            edge_id: record.edge_id.clone(),
                            edge_name: record.edge_name.clone(),
                            applied_program: record.applied_program.clone(),
                            last_poll_unix_ms: record.last_poll_unix_ms,
                            last_result_unix_ms: record.last_result_unix_ms,
                            last_telemetry: record.last_telemetry.clone(),
                            total_polls: record.total_polls,
                            total_results: record.total_results,
                        },
                    )
                })
                .collect::<HashMap<_, _>>();
            let timeseries_edges = persisted
                .edges
                .iter()
                .map(|(edge_id, record)| {
                    (
                        edge_id.clone(),
                        PersistedEdgeTimeseriesRecord {
                            traffic_points: record.traffic_points.clone(),
                            last_traffic_cumulative: record.last_traffic_cumulative.clone(),
                        },
                    )
                })
                .collect::<HashMap<_, _>>();
            (
                ControllerCoreSnapshot {
                    schema_version: PERSISTENCE_SCHEMA_VERSION,
                    command_sequence: self.command_sequence.load(Ordering::Relaxed),
                    program_sequence: self.program_sequence.load(Ordering::Relaxed),
                    edges: core_edges,
                    edge_lookup: persisted.edge_lookup.clone(),
                },
                ControllerProgramsSnapshot {
                    schema_version: PERSISTENCE_SCHEMA_VERSION,
                    programs: persisted.programs.clone(),
                },
                ControllerTimeseriesSnapshot {
                    schema_version: PERSISTENCE_SCHEMA_VERSION,
                    edges: timeseries_edges,
                },
            )
        };
        write_snapshot_to_disk(path.as_path(), &core_snapshot)?;
        write_snapshot_to_disk(programs_path.as_path(), &programs_snapshot)?;
        write_timeseries_snapshot_to_disk(timeseries_path.as_path(), &timeseries_snapshot)?;
        Ok(())
    }
}

impl ControllerStore {
    fn resolve_edge_id(&self, identifier: &str) -> Option<String> {
        if self.edges.contains_key(identifier) {
            return Some(identifier.to_string());
        }
        self.edge_lookup.get(identifier).cloned()
    }

    fn resolve_or_create_edge_id(&mut self, identifier: &str) -> String {
        if let Some(existing) = self.resolve_edge_id(identifier) {
            return existing;
        }
        let edge_id = Uuid::new_v4().to_string();
        self.edge_lookup
            .insert(identifier.to_string(), edge_id.clone());
        self.edges.insert(
            edge_id.clone(),
            EdgeRecord {
                edge_name: identifier.to_string(),
                ..EdgeRecord::default()
            },
        );
        edge_id
    }

    fn to_persisted(&self) -> PersistedControllerStore {
        PersistedControllerStore {
            edges: self
                .edges
                .iter()
                .map(|(edge_id, record)| {
                    (edge_id.clone(), record.to_persisted(Some(edge_id.clone())))
                })
                .collect(),
            edge_lookup: self.edge_lookup.clone(),
            programs: self.programs.clone(),
        }
    }

    fn from_persisted(
        store: PersistedControllerStore,
        max_result_history: usize,
    ) -> ControllerStore {
        let mut edge_lookup = store.edge_lookup;
        let mut edges = HashMap::new();

        for (stored_key, record) in store.edges {
            let edge_id = record.edge_id.clone().unwrap_or_else(|| {
                if Uuid::parse_str(&stored_key).is_ok() {
                    stored_key.clone()
                } else {
                    Uuid::new_v4().to_string()
                }
            });

            let edge_name = record.edge_name.clone().unwrap_or_else(|| {
                if Uuid::parse_str(&stored_key).is_ok() {
                    edge_lookup
                        .iter()
                        .find_map(|(name, id)| (id == &edge_id).then(|| name.clone()))
                        .unwrap_or_else(|| stored_key.clone())
                } else {
                    stored_key.clone()
                }
            });

            edge_lookup.insert(edge_name.clone(), edge_id.clone());
            edges.insert(
                edge_id,
                EdgeRecord::from_persisted(record, max_result_history, edge_name),
            );
        }

        ControllerStore {
            edges,
            edge_lookup,
            programs: store.programs,
        }
    }
}

impl EdgeRecord {
    fn to_persisted(&self, edge_id: Option<String>) -> PersistedEdgeMergedRecord {
        PersistedEdgeMergedRecord {
            edge_id,
            edge_name: Some(self.edge_name.clone()),
            applied_program: self.applied_program.clone(),
            traffic_points: self.traffic_points.clone(),
            last_traffic_cumulative: self.last_traffic_cumulative.clone(),
            last_poll_unix_ms: self.last_poll_unix_ms,
            last_result_unix_ms: self.last_result_unix_ms,
            last_telemetry: self.last_telemetry.clone(),
            total_polls: self.total_polls,
            total_results: self.total_results,
        }
    }

    fn from_persisted(
        store: PersistedEdgeMergedRecord,
        max_result_history: usize,
        edge_name: String,
    ) -> EdgeRecord {
        let mut record = EdgeRecord {
            edge_name,
            applied_program: store.applied_program,
            traffic_points: store.traffic_points,
            last_traffic_cumulative: store.last_traffic_cumulative,
            last_poll_unix_ms: store.last_poll_unix_ms,
            last_result_unix_ms: store.last_result_unix_ms,
            last_telemetry: store.last_telemetry,
            total_polls: store.total_polls,
            total_results: store.total_results,
            ..EdgeRecord::default()
        };
        record.recent_results.truncate(max_result_history.max(1));
        record
    }
}

fn snapshot_schema_version() -> u32 {
    PERSISTENCE_SCHEMA_VERSION
}

fn default_true() -> bool {
    true
}

fn load_snapshot_from_disk(
    state_path: Option<&FsPath>,
    max_result_history: usize,
) -> (ControllerStore, u64, u64) {
    let Some(path) = state_path else {
        return (ControllerStore::default(), 0, 0);
    };
    if !path.exists() {
        return (ControllerStore::default(), 0, 0);
    }
    let (programs_path, timeseries_path) = sidecar_snapshot_paths(path);

    let data = match fs::read(path) {
        Ok(data) => data,
        Err(err) => {
            warn!(
                "failed to read controller snapshot path={} err={err}",
                path.display()
            );
            return (ControllerStore::default(), 0, 0);
        }
    };
    let snapshot = match serde_json::from_slice::<ControllerCoreSnapshot>(&data) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            // Backward compatibility with previous monolithic state file format.
            let legacy = match serde_json::from_slice::<ControllerSnapshotLegacy>(&data) {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    warn!(
                        "failed to parse controller snapshot path={} err={err}",
                        path.display()
                    );
                    return (ControllerStore::default(), 0, 0);
                }
            };
            if legacy.schema_version != PERSISTENCE_SCHEMA_VERSION {
                warn!(
                    "ignoring controller snapshot path={} unsupported schema_version={}",
                    path.display(),
                    legacy.schema_version
                );
                return (ControllerStore::default(), 0, 0);
            }
            return (
                ControllerStore::from_persisted(legacy.store, max_result_history),
                legacy.command_sequence,
                legacy.program_sequence,
            );
        }
    };
    if snapshot.schema_version != PERSISTENCE_SCHEMA_VERSION {
        warn!(
            "ignoring controller snapshot path={} unsupported schema_version={}",
            path.display(),
            snapshot.schema_version
        );
        return (ControllerStore::default(), 0, 0);
    }

    let programs = if programs_path.exists() {
        match fs::read(programs_path.as_path())
            .ok()
            .and_then(|bytes| serde_json::from_slice::<ControllerProgramsSnapshot>(&bytes).ok())
        {
            Some(parsed) if parsed.schema_version == PERSISTENCE_SCHEMA_VERSION => parsed.programs,
            Some(_) => {
                warn!(
                    "ignoring controller programs snapshot path={} unsupported schema_version",
                    programs_path.display()
                );
                HashMap::new()
            }
            None => HashMap::new(),
        }
    } else {
        HashMap::new()
    };

    let timeseries = load_timeseries_snapshot(timeseries_path.as_path());

    let mut merged_edges = HashMap::new();
    for (edge_id, core) in snapshot.edges {
        let traffic = timeseries.get(&edge_id);
        merged_edges.insert(
            edge_id.clone(),
            PersistedEdgeMergedRecord {
                edge_id: core.edge_id,
                edge_name: core.edge_name,
                applied_program: core.applied_program,
                traffic_points: traffic
                    .map(|item| item.traffic_points.clone())
                    .unwrap_or_default(),
                last_traffic_cumulative: traffic
                    .and_then(|item| item.last_traffic_cumulative.clone()),
                last_poll_unix_ms: core.last_poll_unix_ms,
                last_result_unix_ms: core.last_result_unix_ms,
                last_telemetry: core.last_telemetry,
                total_polls: core.total_polls,
                total_results: core.total_results,
            },
        );
    }

    let store = PersistedControllerStore {
        edges: merged_edges,
        edge_lookup: snapshot.edge_lookup,
        programs,
    };

    (
        ControllerStore::from_persisted(store, max_result_history),
        snapshot.command_sequence,
        snapshot.program_sequence,
    )
}

fn sidecar_snapshot_paths(state_path: &FsPath) -> (PathBuf, PathBuf) {
    let parent = state_path.parent().unwrap_or_else(|| FsPath::new(""));
    let stem = state_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("state");
    (
        parent.join(format!("{stem}.programs.json")),
        parent.join(format!("{stem}.timeseries.bin")),
    )
}

fn write_snapshot_to_disk<T: Serialize>(path: &FsPath, snapshot: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(snapshot)
        .map_err(|err| format!("failed to serialize controller snapshot: {err}"))?;
    write_bytes_to_disk(path, &bytes)
}

fn write_timeseries_snapshot_to_disk(
    path: &FsPath,
    snapshot: &ControllerTimeseriesSnapshot,
) -> Result<(), String> {
    let bytes = encode_timeseries_snapshot(snapshot)?;
    write_bytes_to_disk(path, &bytes)
}

fn write_bytes_to_disk(path: &FsPath, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create controller state directory {}: {err}",
                parent.display()
            )
        })?;
    }

    let mut temp_name = path.as_os_str().to_os_string();
    temp_name.push(".tmp");
    let temp_path = PathBuf::from(temp_name);
    fs::write(&temp_path, bytes).map_err(|err| {
        format!(
            "failed to write temporary controller snapshot {}: {err}",
            temp_path.display()
        )
    })?;

    if path.exists() {
        let _ = fs::remove_file(path);
    }
    fs::rename(&temp_path, path).map_err(|err| {
        format!(
            "failed to move controller snapshot {} => {}: {err}",
            temp_path.display(),
            path.display()
        )
    })
}

fn load_timeseries_snapshot(
    timeseries_path: &FsPath,
) -> HashMap<String, PersistedEdgeTimeseriesRecord> {
    if timeseries_path.exists() {
        return match fs::read(timeseries_path) {
            Ok(bytes) => match decode_timeseries_snapshot(&bytes) {
                Ok(snapshot) if snapshot.schema_version == PERSISTENCE_SCHEMA_VERSION => {
                    snapshot.edges
                }
                Ok(_) => {
                    warn!(
                        "ignoring controller timeseries snapshot path={} unsupported schema_version",
                        timeseries_path.display()
                    );
                    HashMap::new()
                }
                Err(err) => {
                    warn!(
                        "failed to parse controller timeseries snapshot path={} err={err}",
                        timeseries_path.display()
                    );
                    HashMap::new()
                }
            },
            Err(err) => {
                warn!(
                    "failed to read controller timeseries snapshot path={} err={err}",
                    timeseries_path.display()
                );
                HashMap::new()
            }
        };
    }

    HashMap::new()
}

fn encode_timeseries_snapshot(snapshot: &ControllerTimeseriesSnapshot) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TIMESERIES_BINARY_MAGIC);
    put_u32(&mut bytes, snapshot.schema_version);
    put_u32(
        &mut bytes,
        u32::try_from(snapshot.edges.len())
            .map_err(|_| "too many edges in timeseries snapshot".to_string())?,
    );

    for (edge_id, record) in &snapshot.edges {
        put_string(&mut bytes, edge_id)?;
        put_u32(
            &mut bytes,
            u32::try_from(record.traffic_points.len())
                .map_err(|_| format!("too many traffic points for edge {edge_id}"))?,
        );

        for point in &record.traffic_points {
            put_u64(&mut bytes, point.unix_ms);
            put_u64(&mut bytes, point.requests);
            put_u64(&mut bytes, point.status_2xx);
            put_u64(&mut bytes, point.status_3xx);
            put_u64(&mut bytes, point.status_4xx);
            put_u64(&mut bytes, point.status_5xx);
        }

        match &record.last_traffic_cumulative {
            Some(sample) => {
                put_u8(&mut bytes, 1);
                put_u64(&mut bytes, sample.requests_total);
                put_u64(&mut bytes, sample.status_2xx_total);
                put_u64(&mut bytes, sample.status_3xx_total);
                put_u64(&mut bytes, sample.status_4xx_total);
                put_u64(&mut bytes, sample.status_5xx_total);
            }
            None => put_u8(&mut bytes, 0),
        }
    }

    Ok(bytes)
}

fn decode_timeseries_snapshot(bytes: &[u8]) -> Result<ControllerTimeseriesSnapshot, String> {
    let mut cursor = BytesCursor::new(bytes);
    let magic = cursor.read_bytes(TIMESERIES_BINARY_MAGIC.len())?;
    if magic != TIMESERIES_BINARY_MAGIC {
        return Err("unexpected timeseries binary magic".to_string());
    }
    let schema_version = cursor.read_u32()?;
    let edge_count = cursor.read_u32()?;

    let mut edges = HashMap::with_capacity(edge_count as usize);
    for _ in 0..edge_count {
        let edge_id = cursor.read_string()?;
        let point_count = cursor.read_u32()?;
        let mut traffic_points = VecDeque::with_capacity(point_count as usize);
        for _ in 0..point_count {
            traffic_points.push_back(EdgeTrafficPoint {
                unix_ms: cursor.read_u64()?,
                requests: cursor.read_u64()?,
                status_2xx: cursor.read_u64()?,
                status_3xx: cursor.read_u64()?,
                status_4xx: cursor.read_u64()?,
                status_5xx: cursor.read_u64()?,
            });
        }

        let last_traffic_cumulative = match cursor.read_u8()? {
            0 => None,
            1 => Some(EdgeTrafficSample {
                requests_total: cursor.read_u64()?,
                status_2xx_total: cursor.read_u64()?,
                status_3xx_total: cursor.read_u64()?,
                status_4xx_total: cursor.read_u64()?,
                status_5xx_total: cursor.read_u64()?,
            }),
            value => {
                return Err(format!(
                    "invalid last_traffic_cumulative marker for edge {edge_id}: {value}"
                ));
            }
        };

        edges.insert(
            edge_id,
            PersistedEdgeTimeseriesRecord {
                traffic_points,
                last_traffic_cumulative,
            },
        );
    }

    if !cursor.is_eof() {
        return Err("unexpected trailing bytes in timeseries snapshot".to_string());
    }

    Ok(ControllerTimeseriesSnapshot {
        schema_version,
        edges,
    })
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), String> {
    let raw = value.as_bytes();
    put_u32(
        bytes,
        u32::try_from(raw.len()).map_err(|_| "timeseries string length overflow".to_string())?,
    );
    bytes.extend_from_slice(raw);
    Ok(())
}

struct BytesCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BytesCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_eof(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_u8(&mut self) -> Result<u8, String> {
        let chunk = self.read_bytes(1)?;
        Ok(chunk[0])
    }

    fn read_u32(&mut self) -> Result<u32, String> {
        let chunk = self.read_bytes(4)?;
        let mut raw = [0u8; 4];
        raw.copy_from_slice(chunk);
        Ok(u32::from_le_bytes(raw))
    }

    fn read_u64(&mut self) -> Result<u64, String> {
        let chunk = self.read_bytes(8)?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(chunk);
        Ok(u64::from_le_bytes(raw))
    }

    fn read_string(&mut self) -> Result<String, String> {
        let len = self.read_u32()? as usize;
        let raw = self.read_bytes(len)?;
        String::from_utf8(raw.to_vec())
            .map_err(|err| format!("invalid utf8 in timeseries snapshot: {err}"))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], String> {
        let next = self
            .offset
            .checked_add(len)
            .ok_or_else(|| "timeseries snapshot offset overflow".to_string())?;
        if next > self.bytes.len() {
            return Err("unexpected end of timeseries snapshot".to_string());
        }
        let slice = &self.bytes[self.offset..next];
        self.offset = next;
        Ok(slice)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnqueueCommandResponse {
    pub command_id: String,
    pub pending_commands: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeSummary {
    pub edge_id: String,
    pub edge_name: String,
    pub sync_status: String,
    pub last_seen_unix_ms: Option<u64>,
    pub pending_commands: usize,
    pub recent_results: usize,
    pub applied_program: Option<AppliedProgramRef>,
    pub last_poll_unix_ms: Option<u64>,
    pub last_result_unix_ms: Option<u64>,
    pub total_polls: u64,
    pub total_results: u64,
    pub last_telemetry: Option<TelemetrySnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeDetailResponse {
    pub summary: EdgeSummary,
    pub pending_command_types: Vec<String>,
    pub traffic_series: Vec<EdgeTrafficPoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeTrafficPoint {
    pub unix_ms: u64,
    pub requests: u64,
    pub status_2xx: u64,
    pub status_3xx: u64,
    pub status_4xx: u64,
    pub status_5xx: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EdgeListResponse {
    edges: Vec<EdgeSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EdgeResultsResponse {
    results: Vec<EdgeCommandResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DebugSessionListResponse {
    sessions: Vec<DebugSessionSummary>,
}

#[derive(Clone, Debug, Deserialize)]
struct CreateDebugSessionRequest {
    edge_id: String,
    #[serde(default)]
    tcp_addr: Option<String>,
    #[serde(default)]
    header_name: Option<String>,
    #[serde(default)]
    stop_on_entry: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DebugCommandResponse {
    phase: DebugSessionPhase,
    output: String,
    current_line: Option<u32>,
    attached: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DebugCommandRequest {
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
struct ProgramSummary {
    program_id: String,
    name: String,
    latest_version: u32,
    versions: usize,
    created_unix_ms: u64,
    updated_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgramVersionSummary {
    version: u32,
    created_unix_ms: u64,
    flavor: String,
    flow_synced: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgramVersionDetail {
    version: u32,
    created_unix_ms: u64,
    flavor: String,
    flow_synced: bool,
    nodes: Vec<UiGraphNode>,
    edges: Vec<UiGraphEdge>,
    source: UiSourceBundle,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgramDetailResponse {
    program_id: String,
    name: String,
    latest_version: u32,
    created_unix_ms: u64,
    updated_unix_ms: u64,
    versions: Vec<ProgramVersionSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgramVersionResponse {
    program_id: String,
    name: String,
    detail: ProgramVersionDetail,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgramListResponse {
    programs: Vec<ProgramSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StatusResponse {
    status: &'static str,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Clone, Debug, Deserialize)]
struct EnqueueApplyProgramRequest {
    command_id: Option<String>,
    program_base64: String,
}

#[derive(Clone, Debug, Deserialize)]
struct EnqueueStartDebugRequest {
    command_id: Option<String>,
    #[serde(default)]
    tcp_addr: Option<String>,
    header_name: Option<String>,
    stop_on_entry: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
struct EnqueuePingRequest {
    command_id: Option<String>,
    payload: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct OptionalCommandIdRequest {
    command_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ResultsQuery {
    limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
struct CreateProgramRequest {
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
struct RenameProgramRequest {
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
struct CreateProgramVersionRequest {
    #[serde(default)]
    flavor: Option<String>,
    #[serde(default)]
    nodes: Vec<UiGraphNode>,
    #[serde(default)]
    edges: Vec<UiGraphEdge>,
    #[serde(default)]
    blocks: Vec<UiBlockInstance>,
    #[serde(default)]
    source: Option<UiSourceBundle>,
    #[serde(default = "default_true")]
    flow_synced: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct ApplyProgramVersionRequest {
    program_id: String,
    #[serde(default)]
    version: Option<u32>,
    #[serde(default)]
    flavor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum UiInputType {
    Text,
    Number,
}

#[derive(Clone, Debug, Serialize)]
struct UiBlockInput {
    key: &'static str,
    label: &'static str,
    input_type: UiInputType,
    default_value: &'static str,
    placeholder: &'static str,
    connectable: bool,
}

#[derive(Clone, Debug, Serialize)]
struct UiBlockOutput {
    key: &'static str,
    label: &'static str,
    expr_from_input: Option<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
struct UiBlockDefinition {
    id: &'static str,
    title: &'static str,
    category: &'static str,
    description: &'static str,
    inputs: Vec<UiBlockInput>,
    outputs: Vec<UiBlockOutput>,
    accepts_flow: bool,
}

#[derive(Clone, Debug, Serialize)]
struct UiBlocksResponse {
    blocks: Vec<UiBlockDefinition>,
}

#[derive(Clone, Debug, Deserialize)]
struct UiBlockInstance {
    block_id: String,
    #[serde(default)]
    values: HashMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
struct UiRenderRequest {
    #[serde(default)]
    blocks: Vec<UiBlockInstance>,
    #[serde(default)]
    nodes: Vec<UiGraphNode>,
    #[serde(default)]
    edges: Vec<UiGraphEdge>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UiSourceBundle {
    rustscript: String,
    javascript: String,
    lua: String,
    scheme: String,
}

#[derive(Clone, Debug, Serialize)]
struct UiRenderResponse {
    source: UiSourceBundle,
}

#[derive(Clone, Debug, Deserialize)]
struct UiDeployRequest {
    edge_id: String,
    #[serde(default)]
    flavor: Option<String>,
    #[serde(default)]
    blocks: Vec<UiBlockInstance>,
    #[serde(default)]
    nodes: Vec<UiGraphNode>,
    #[serde(default)]
    edges: Vec<UiGraphEdge>,
}

#[derive(Clone, Debug, Serialize)]
struct UiDeployResponse {
    command_id: String,
    pending_commands: usize,
    flavor: String,
    source: UiSourceBundle,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UiGraphNode {
    id: String,
    block_id: String,
    #[serde(default)]
    values: HashMap<String, String>,
    #[serde(default)]
    position: Option<UiGraphNodePosition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UiGraphNodePosition {
    x: f64,
    y: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UiGraphEdge {
    source: String,
    source_output: String,
    target: String,
    target_input: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredProgramVersion {
    version: u32,
    created_unix_ms: u64,
    flavor: String,
    #[serde(default = "default_true")]
    flow_synced: bool,
    nodes: Vec<UiGraphNode>,
    edges: Vec<UiGraphEdge>,
    source: UiSourceBundle,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredProgram {
    program_id: String,
    name: String,
    created_unix_ms: u64,
    updated_unix_ms: u64,
    versions: Vec<StoredProgramVersion>,
}

pub fn build_controller_app(state: ControllerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/metrics", get(metrics_handler))
        .route("/ui", get(ui_index_handler))
        .route("/ui/", get(ui_index_handler))
        .route("/ui/{*path}", get(ui_asset_handler))
        .route("/rpc/v1/edge/poll", post(rpc_poll_handler))
        .route("/rpc/v1/edge/result", post(rpc_result_handler))
        .route("/v1/ui/blocks", get(ui_blocks_handler))
        .route("/v1/ui/render", post(ui_render_handler))
        .route("/v1/ui/deploy", post(ui_deploy_handler))
        .route(
            "/v1/programs",
            get(list_programs_handler).post(create_program_handler),
        )
        .route(
            "/v1/programs/{program_id}",
            get(get_program_handler)
                .patch(rename_program_handler)
                .delete(delete_program_handler),
        )
        .route(
            "/v1/programs/{program_id}/versions",
            post(create_program_version_handler),
        )
        .route(
            "/v1/programs/{program_id}/versions/{version}",
            get(get_program_version_handler),
        )
        .route("/v1/edges", get(list_edges_handler))
        .route("/v1/edges/{edge_id}", get(get_edge_handler))
        .route("/v1/edges/{edge_id}/results", get(get_edge_results_handler))
        .route(
            "/v1/edges/{edge_id}/program",
            put(enqueue_program_binary_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/apply-program",
            post(enqueue_apply_program_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/apply-program-version",
            post(enqueue_apply_program_version_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/start-debug",
            post(enqueue_start_debug_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/stop-debug",
            post(enqueue_stop_debug_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/get-health",
            post(enqueue_get_health_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/get-metrics",
            post(enqueue_get_metrics_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/get-telemetry",
            post(enqueue_get_telemetry_handler),
        )
        .route(
            "/v1/edges/{edge_id}/commands/ping",
            post(enqueue_ping_handler),
        )
        .route(
            "/v1/debug-sessions",
            get(list_debug_sessions_handler).post(create_debug_session_handler),
        )
        .route(
            "/v1/debug-sessions/{session_id}",
            get(get_debug_session_handler).delete(stop_debug_session_handler),
        )
        .route(
            "/v1/debug-sessions/{session_id}/command",
            post(run_debug_command_handler),
        )
        .with_state(state)
}

async fn healthz_handler() -> Json<StatusResponse> {
    Json(StatusResponse { status: "ok" })
}

async fn metrics_handler(State(state): State<ControllerState>) -> impl IntoResponse {
    let (connected_edges, pending_commands) = {
        let guard = state.inner.read().await;
        let connected_edges = guard.edges.len();
        let pending_commands = guard
            .edges
            .values()
            .map(|record| record.pending_commands.len())
            .sum::<usize>();
        (connected_edges, pending_commands)
    };
    let metrics = format!(
        concat!(
            "pd_controller_uptime_seconds {}\n",
            "pd_controller_connected_edges {}\n",
            "pd_controller_pending_commands {}\n",
            "pd_controller_poll_requests_total {}\n",
            "pd_controller_result_posts_total {}\n",
            "pd_controller_commands_enqueued_total {}\n",
            "pd_controller_commands_delivered_total {}\n",
            "pd_controller_command_results_ok_total {}\n",
            "pd_controller_command_results_error_total {}\n"
        ),
        state.metrics.started_at.elapsed().as_secs(),
        connected_edges,
        pending_commands,
        state.metrics.poll_requests_total.load(Ordering::Relaxed),
        state.metrics.result_posts_total.load(Ordering::Relaxed),
        state
            .metrics
            .commands_enqueued_total
            .load(Ordering::Relaxed),
        state
            .metrics
            .commands_delivered_total
            .load(Ordering::Relaxed),
        state
            .metrics
            .command_results_ok_total
            .load(Ordering::Relaxed),
        state
            .metrics
            .command_results_error_total
            .load(Ordering::Relaxed),
    );
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics,
    )
}

async fn ui_index_handler() -> impl IntoResponse {
    ui_asset_response("index.html")
}

async fn ui_asset_handler(Path(path): Path<String>) -> impl IntoResponse {
    let normalized = path.trim_start_matches('/');
    if normalized.is_empty() {
        return ui_asset_response("index.html");
    }
    ui_asset_response(normalized)
}

fn ui_asset_response(path: &str) -> axum::response::Response {
    if let Some(bytes) = embedded_webui::get_asset(path) {
        return (
            StatusCode::OK,
            [(CONTENT_TYPE, webui_content_type(path))],
            bytes.to_vec(),
        )
            .into_response();
    }

    if let Some(index) = embedded_webui::get_asset("index.html") {
        return (
            StatusCode::OK,
            [(CONTENT_TYPE, "text/html; charset=utf-8")],
            index.to_vec(),
        )
            .into_response();
    }

    let message = if embedded_webui::has_assets() {
        "webui asset not found"
    } else {
        "webui assets are not embedded; build pd-controller/webui before compiling pd-controller"
    };
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response()
}

async fn ui_blocks_handler() -> Json<UiBlocksResponse> {
    Json(UiBlocksResponse {
        blocks: ui_block_catalog(),
    })
}

async fn ui_render_handler(
    Json(request): Json<UiRenderRequest>,
) -> Result<Json<UiRenderResponse>, (StatusCode, Json<ErrorResponse>)> {
    let source = render_ui_sources(&request.blocks, &request.nodes, &request.edges)?;
    Ok(Json(UiRenderResponse { source }))
}

async fn ui_deploy_handler(
    State(state): State<ControllerState>,
    Json(request): Json<UiDeployRequest>,
) -> Result<(StatusCode, Json<UiDeployResponse>), (StatusCode, Json<ErrorResponse>)> {
    if request.edge_id.trim().is_empty() {
        return Err(bad_request("edge_id cannot be empty"));
    }

    let source = render_ui_sources(&request.blocks, &request.nodes, &request.edges)?;
    let (flavor, flavor_label) = parse_ui_flavor(request.flavor.as_deref())?;
    let source_text = source_for_flavor(&source, flavor);
    let compiled = compile_source_with_flavor(&source_text, flavor)
        .map_err(|err| bad_request(&format!("source compile failed: {err}")))?;
    let program_bytes = encode_program(&compiled.program)
        .map_err(|err| bad_request(&format!("bytecode encode failed: {err}")))?;

    let command = ControlPlaneCommand::ApplyProgram {
        command_id: state.next_command_id(),
        program_base64: STANDARD.encode(program_bytes),
    };
    let queued = state.enqueue_command(request.edge_id, command).await;
    let response = UiDeployResponse {
        command_id: queued.command_id,
        pending_commands: queued.pending_commands,
        flavor: flavor_label.to_string(),
        source,
    };
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn list_programs_handler(State(state): State<ControllerState>) -> Json<ProgramListResponse> {
    let mut programs = {
        let guard = state.inner.read().await;
        guard
            .programs
            .values()
            .map(map_program_summary)
            .collect::<Vec<_>>()
    };
    programs.sort_by(|lhs, rhs| lhs.name.cmp(&rhs.name));
    Json(ProgramListResponse { programs })
}

async fn create_program_handler(
    State(state): State<ControllerState>,
    Json(request): Json<CreateProgramRequest>,
) -> Result<(StatusCode, Json<ProgramDetailResponse>), (StatusCode, Json<ErrorResponse>)> {
    if request.name.trim().is_empty() {
        return Err(bad_request("program name cannot be empty"));
    }
    let now = now_unix_ms();
    let program = StoredProgram {
        program_id: state.next_program_id(),
        name: request.name.trim().to_string(),
        created_unix_ms: now,
        updated_unix_ms: now,
        versions: Vec::new(),
    };
    let response = {
        let mut guard = state.inner.write().await;
        let detail = map_program_detail(&program);
        guard.programs.insert(program.program_id.clone(), program);
        detail
    };
    state.persist_snapshot().await.map_err(internal_error)?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_program_handler(
    State(state): State<ControllerState>,
    Path(program_id): Path<String>,
) -> Result<Json<ProgramDetailResponse>, (StatusCode, Json<ErrorResponse>)> {
    let detail = {
        let guard = state.inner.read().await;
        let Some(program) = guard.programs.get(&program_id) else {
            return Err(not_found("program not found"));
        };
        map_program_detail(program)
    };
    Ok(Json(detail))
}

async fn rename_program_handler(
    State(state): State<ControllerState>,
    Path(program_id): Path<String>,
    Json(request): Json<RenameProgramRequest>,
) -> Result<Json<ProgramDetailResponse>, (StatusCode, Json<ErrorResponse>)> {
    if request.name.trim().is_empty() {
        return Err(bad_request("program name cannot be empty"));
    }
    let detail = {
        let mut guard = state.inner.write().await;
        let Some(program) = guard.programs.get_mut(&program_id) else {
            return Err(not_found("program not found"));
        };
        program.name = request.name.trim().to_string();
        program.updated_unix_ms = now_unix_ms();
        map_program_detail(program)
    };
    state.persist_snapshot().await.map_err(internal_error)?;
    Ok(Json(detail))
}

async fn delete_program_handler(
    State(state): State<ControllerState>,
    Path(program_id): Path<String>,
) -> Result<Json<StatusResponse>, (StatusCode, Json<ErrorResponse>)> {
    {
        let mut guard = state.inner.write().await;
        if guard.programs.remove(&program_id).is_none() {
            return Err(not_found("program not found"));
        }
        for record in guard.edges.values_mut() {
            if record
                .applied_program
                .as_ref()
                .map(|applied| applied.program_id == program_id)
                .unwrap_or(false)
            {
                record.applied_program = None;
            }
            record
                .pending_apply_programs
                .retain(|_, applied| applied.program_id != program_id);
        }
    }
    state.persist_snapshot().await.map_err(internal_error)?;
    Ok(Json(StatusResponse { status: "deleted" }))
}

async fn create_program_version_handler(
    State(state): State<ControllerState>,
    Path(program_id): Path<String>,
    Json(request): Json<CreateProgramVersionRequest>,
) -> Result<(StatusCode, Json<ProgramVersionResponse>), (StatusCode, Json<ErrorResponse>)> {
    // Backward/compat guard: if client sends source-only payload with no graph,
    // accept it as code-only save even when flow_synced is absent/stale.
    let inferred_code_only =
        request.source.is_some() && request.nodes.is_empty() && request.blocks.is_empty();
    let flow_synced = if inferred_code_only {
        false
    } else {
        request.flow_synced
    };
    let (nodes, edges, source) = if flow_synced {
        let nodes = if !request.nodes.is_empty() {
            request.nodes.clone()
        } else {
            request
                .blocks
                .iter()
                .enumerate()
                .map(|(index, block)| UiGraphNode {
                    id: format!("b{}", index + 1),
                    block_id: block.block_id.clone(),
                    values: block.values.clone(),
                    position: None,
                })
                .collect::<Vec<_>>()
        };
        let edges = request.edges.clone();
        if nodes.is_empty() {
            return Err(bad_request(
                "program version must include at least one node",
            ));
        }
        let source = render_ui_sources(&request.blocks, &nodes, &edges)?;
        (nodes, edges, source)
    } else {
        let Some(source) = request.source.clone() else {
            return Err(bad_request(
                "source is required when flow_synced is false",
            ));
        };
        (Vec::new(), Vec::new(), source)
    };
    let (_, flavor_label) = parse_ui_flavor(request.flavor.as_deref())?;
    let detail = {
        let mut guard = state.inner.write().await;
        let Some(program) = guard.programs.get_mut(&program_id) else {
            return Err(not_found("program not found"));
        };
        let version = (program.versions.len() as u32) + 1;
        let created_unix_ms = now_unix_ms();
        let stored_version = StoredProgramVersion {
            version,
            created_unix_ms,
            flavor: flavor_label.to_string(),
            flow_synced,
            nodes: nodes.clone(),
            edges: edges.clone(),
            source: source.clone(),
        };
        program.versions.push(stored_version.clone());
        program.updated_unix_ms = created_unix_ms;
        ProgramVersionResponse {
            program_id: program.program_id.clone(),
            name: program.name.clone(),
            detail: map_program_version_detail(&stored_version),
        }
    };
    state.persist_snapshot().await.map_err(internal_error)?;
    Ok((StatusCode::CREATED, Json(detail)))
}

async fn get_program_version_handler(
    State(state): State<ControllerState>,
    Path((program_id, version)): Path<(String, u32)>,
) -> Result<Json<ProgramVersionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = {
        let guard = state.inner.read().await;
        let Some(program) = guard.programs.get(&program_id) else {
            return Err(not_found("program not found"));
        };
        let Some(stored_version) = program.versions.iter().find(|item| item.version == version)
        else {
            return Err(not_found("program version not found"));
        };
        ProgramVersionResponse {
            program_id: program.program_id.clone(),
            name: program.name.clone(),
            detail: map_program_version_detail(stored_version),
        }
    };
    Ok(Json(response))
}

async fn rpc_poll_handler(
    State(state): State<ControllerState>,
    Json(request): Json<EdgePollRequest>,
) -> Json<EdgePollResponse> {
    state
        .metrics
        .poll_requests_total
        .fetch_add(1, Ordering::Relaxed);

    let debug_session_active = request.telemetry.debug_session_active;
    let debug_session_attached = request.telemetry.debug_session_attached;
    let debug_session_current_line = request.telemetry.debug_session_current_line;
    let (resolved_edge_id, command) = {
        let mut guard = state.inner.write().await;
        let edge_id = guard.resolve_or_create_edge_id(&request.edge_id);
        let record = guard.edges.entry(edge_id.clone()).or_default();
        let reported_name = request
            .edge_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(name) = reported_name {
            record.edge_name = name.to_string();
        } else if record.edge_name.is_empty() {
            record.edge_name = request.edge_id.clone();
        }
        let now = now_unix_ms();
        record.last_poll_unix_ms = Some(now);
        record.last_telemetry = Some(request.telemetry);
        if let Some(sample) = request.traffic_sample {
            append_traffic_sample(record, sample, now);
        }
        record.total_polls += 1;
        let command = record.pending_commands.pop_front();
        (edge_id, command)
    };

    {
        let mut sessions = state.debug_sessions.write().await;
        for session in sessions.values_mut().filter(|item| {
            item.edge_id == resolved_edge_id || item.edge_id == request.edge_id
        }) {
            if matches!(
                session.phase,
                DebugSessionPhase::Stopped | DebugSessionPhase::Failed
            ) {
                continue;
            }
            if session.edge_id != resolved_edge_id {
                session.edge_id = resolved_edge_id.clone();
            }
            if !debug_session_active {
                session.phase = DebugSessionPhase::Stopped;
                session.current_line = None;
                session.last_resume_command_unix_ms = None;
                session.updated_unix_ms = now_unix_ms();
                session.message = Some("debug session is no longer active on edge".to_string());
                continue;
            }
            if debug_session_attached {
                session.phase = DebugSessionPhase::Attached;
                session.last_resume_command_unix_ms = None;
                if session.attached_unix_ms.is_none() {
                    session.attached_unix_ms = Some(now_unix_ms());
                }
                if let Some(line) = debug_session_current_line {
                    session.current_line = Some(line);
                }
                session.updated_unix_ms = now_unix_ms();
                session.message = Some("debugger attached".to_string());
            } else if session.phase != DebugSessionPhase::WaitingForStartResult {
                let now = now_unix_ms();
                if let Some(last_resume) = session.last_resume_command_unix_ms
                    && now.saturating_sub(last_resume) <= DEBUG_RESUME_GRACE_MS
                {
                    session.updated_unix_ms = now;
                    continue;
                }
                session.phase = DebugSessionPhase::WaitingForAttach;
                session.current_line = None;
                session.updated_unix_ms = now;
            }
        }
    }

    if command.is_some() {
        state
            .metrics
            .commands_delivered_total
            .fetch_add(1, Ordering::Relaxed);
    }

    if let Err(err) = state.persist_snapshot().await {
        warn!("failed to persist controller state after poll update: {err}");
    }

    let poll_interval_ms = if debug_session_active {
        200
    } else {
        state.config.default_poll_interval_ms
    };
    Json(EdgePollResponse {
        command,
        poll_interval_ms,
    })
}

async fn rpc_result_handler(
    State(state): State<ControllerState>,
    Json(result): Json<EdgeCommandResult>,
) -> StatusCode {
    state
        .metrics
        .result_posts_total
        .fetch_add(1, Ordering::Relaxed);

    let is_ok = result.ok;
    let command_id = result.command_id.clone();
    let result_payload = result.result.clone();
    let edge_name_for_debug = result.edge_name.clone();
    let resolved_edge_id_for_debug = {
        let mut guard = state.inner.write().await;
        let edge_id = guard.resolve_or_create_edge_id(&result.edge_id);
        let resolved_for_debug = edge_id.clone();
        let record = guard.edges.entry(edge_id).or_default();
        let reported_name = result
            .edge_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(name) = reported_name {
            record.edge_name = name.to_string();
        } else if record.edge_name.is_empty() {
            record.edge_name = result.edge_id.clone();
        }
        record.last_result_unix_ms = Some(now_unix_ms());
        record.last_telemetry = Some(result.telemetry.clone());
        record.total_results += 1;
        record.recent_results.push_back(result);
        while record.recent_results.len() > state.config.max_result_history {
            let _ = record.recent_results.pop_front();
        }
        if let Some(program_ref) = record.pending_apply_programs.remove(&command_id)
            && is_ok
        {
            record.applied_program = Some(program_ref);
        }
        resolved_for_debug
    };

    process_debug_session_result(
        state.clone(),
        &command_id,
        &resolved_edge_id_for_debug,
        edge_name_for_debug,
        is_ok,
        &result_payload,
    )
    .await;

    if let Err(err) = state.persist_snapshot().await {
        warn!("failed to persist controller state after command result: {err}");
    }

    if is_ok {
        state
            .metrics
            .command_results_ok_total
            .fetch_add(1, Ordering::Relaxed);
    } else {
        state
            .metrics
            .command_results_error_total
            .fetch_add(1, Ordering::Relaxed);
    }

    StatusCode::NO_CONTENT
}

async fn list_edges_handler(State(state): State<ControllerState>) -> Json<EdgeListResponse> {
    let mut edges = {
        let guard = state.inner.read().await;
        guard
            .edges
            .iter()
            .map(|(id, record)| map_summary(id, record))
            .collect::<Vec<_>>()
    };
    edges.sort_by(|lhs, rhs| lhs.edge_name.cmp(&rhs.edge_name));
    Json(EdgeListResponse { edges })
}

async fn get_edge_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
) -> Result<Json<EdgeDetailResponse>, (StatusCode, Json<ErrorResponse>)> {
    let detail = {
        let guard = state.inner.read().await;
        let Some(resolved_id) = guard.resolve_edge_id(&edge_id) else {
            return Err(not_found("edge not found"));
        };
        let Some(record) = guard.edges.get(&resolved_id) else {
            return Err(not_found("edge not found"));
        };
        EdgeDetailResponse {
            summary: map_summary(&resolved_id, record),
            pending_command_types: record
                .pending_commands
                .iter()
                .map(command_kind)
                .map(str::to_string)
                .collect(),
            traffic_series: record.traffic_points.iter().cloned().collect(),
        }
    };
    Ok(Json(detail))
}

async fn get_edge_results_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Query(query): Query<ResultsQuery>,
) -> Result<Json<EdgeResultsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit = query
        .limit
        .unwrap_or(state.config.max_result_history)
        .max(1);
    let response = {
        let guard = state.inner.read().await;
        let Some(resolved_id) = guard.resolve_edge_id(&edge_id) else {
            return Err(not_found("edge not found"));
        };
        let Some(record) = guard.edges.get(&resolved_id) else {
            return Err(not_found("edge not found"));
        };
        let results = record
            .recent_results
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        EdgeResultsResponse { results }
    };
    Ok(Json(response))
}

async fn list_debug_sessions_handler(
    State(state): State<ControllerState>,
) -> Json<DebugSessionListResponse> {
    let mut sessions = {
        let guard = state.debug_sessions.read().await;
        guard
            .values()
            .map(DebugSessionRecord::to_summary)
            .collect::<Vec<_>>()
    };
    sessions.sort_by(|lhs, rhs| rhs.updated_unix_ms.cmp(&lhs.updated_unix_ms));
    Json(DebugSessionListResponse { sessions })
}

async fn get_debug_session_handler(
    State(state): State<ControllerState>,
    Path(session_id): Path<String>,
) -> Result<Json<DebugSessionDetail>, (StatusCode, Json<ErrorResponse>)> {
    let detail = {
        let guard = state.debug_sessions.read().await;
        let Some(session) = guard.get(&session_id) else {
            return Err(not_found("debug session not found"));
        };
        session.to_detail()
    };
    Ok(Json(detail))
}

async fn create_debug_session_handler(
    State(state): State<ControllerState>,
    Json(request): Json<CreateDebugSessionRequest>,
) -> Result<(StatusCode, Json<DebugSessionDetail>), (StatusCode, Json<ErrorResponse>)> {
    let tcp_addr = request
        .tcp_addr
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_REMOTE_DEBUGGER_TCP_ADDR)
        .to_string();

    let (resolved_edge_id, edge_name, source_flavor, source_code) = {
        let guard = state.inner.read().await;
        let Some(resolved_edge_id) = guard.resolve_edge_id(&request.edge_id) else {
            return Err(not_found("edge not found"));
        };
        let Some(record) = guard.edges.get(&resolved_edge_id) else {
            return Err(not_found("edge not found"));
        };
        if !record
            .last_telemetry
            .as_ref()
            .map(|telemetry| telemetry.program_loaded)
            .unwrap_or(false)
        {
            return Err(bad_request(
                "edge has no loaded program yet; apply a program before starting a debug session",
            ));
        }
        let edge_name = if record.edge_name.trim().is_empty() {
            resolved_edge_id.clone()
        } else {
            record.edge_name.clone()
        };
        let (source_flavor, source_code) = resolve_edge_debug_source(&guard, &resolved_edge_id);
        (resolved_edge_id, edge_name, source_flavor, source_code)
    };

    let now = now_unix_ms();
    let command_id = state.next_command_id();
    let session_id = Uuid::new_v4().to_string();
    let stop_on_entry = request.stop_on_entry.unwrap_or(true);
    let command = ControlPlaneCommand::StartDebugSession {
        command_id: command_id.clone(),
        tcp_addr: tcp_addr.clone(),
        header_name: request.header_name.clone(),
        stop_on_entry: Some(stop_on_entry),
    };
    let _queued = state
        .enqueue_command(resolved_edge_id.clone(), command)
        .await;

    let record = DebugSessionRecord {
        session_id: session_id.clone(),
        edge_id: resolved_edge_id,
        edge_name,
        phase: DebugSessionPhase::WaitingForStartResult,
        requested_header_name: request.header_name,
        header_name: None,
        nonce_header_value: None,
        tcp_addr,
        start_command_id: command_id.clone(),
        stop_command_id: None,
        current_line: None,
        source_flavor,
        source_code,
        breakpoints: HashSet::new(),
        created_unix_ms: now,
        updated_unix_ms: now,
        attached_unix_ms: None,
        last_resume_command_unix_ms: None,
        message: Some("start-debug command queued".to_string()),
        last_output: None,
    };

    {
        let mut sessions = state.debug_sessions.write().await;
        sessions.insert(session_id.clone(), record.clone());
    }
    {
        let mut lookup = state.debug_start_lookup.write().await;
        lookup.insert(command_id, session_id);
    }

    Ok((StatusCode::ACCEPTED, Json(record.to_detail())))
}

async fn stop_debug_session_handler(
    State(state): State<ControllerState>,
    Path(session_id): Path<String>,
) -> Result<(StatusCode, Json<DebugSessionDetail>), (StatusCode, Json<ErrorResponse>)> {
    let edge_id = {
        let guard = state.debug_sessions.read().await;
        let Some(session) = guard.get(&session_id) else {
            return Err(not_found("debug session not found"));
        };
        session.edge_id.clone()
    };

    let command_id = state.next_command_id();
    let command = ControlPlaneCommand::StopDebugSession {
        command_id: command_id.clone(),
    };
    let _queued = state.enqueue_command(edge_id, command).await;

    let detail = {
        let mut sessions = state.debug_sessions.write().await;
        let Some(session) = sessions.get_mut(&session_id) else {
            return Err(not_found("debug session not found"));
        };
        session.phase = DebugSessionPhase::Stopped;
        session.updated_unix_ms = now_unix_ms();
        session.stop_command_id = Some(command_id);
        session.message = Some("stop-debug command queued".to_string());
        session.to_detail()
    };

    Ok((StatusCode::ACCEPTED, Json(detail)))
}

async fn run_debug_command_handler(
    State(state): State<ControllerState>,
    Path(session_id): Path<String>,
    Json(request): Json<DebugCommandRequest>,
) -> Result<Json<DebugCommandResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_for_state = request.clone();
    let (edge_id, phase) = {
        let guard = state.debug_sessions.read().await;
        let Some(session) = guard.get(&session_id) else {
            return Err(not_found("debug session not found"));
        };
        (session.edge_id.clone(), session.phase.clone())
    };
    if phase != DebugSessionPhase::Attached {
        return Err(bad_request(
            "debug session is not attached yet; wait for a matching request",
        ));
    }

    let rpc_command = match request {
        DebugCommandRequest::Where => RemoteDebugCommand::Where,
        DebugCommandRequest::Step => RemoteDebugCommand::Step,
        DebugCommandRequest::Next => RemoteDebugCommand::Next,
        DebugCommandRequest::Continue => RemoteDebugCommand::Continue,
        DebugCommandRequest::Out => RemoteDebugCommand::Out,
        DebugCommandRequest::BreakLine { line } => RemoteDebugCommand::BreakLine { line },
        DebugCommandRequest::ClearLine { line } => RemoteDebugCommand::ClearLine { line },
        DebugCommandRequest::PrintVar { name } => {
            if name.trim().is_empty() {
                return Err(bad_request("variable name cannot be empty"));
            }
            RemoteDebugCommand::PrintVar {
                name: name.trim().to_string(),
            }
        }
        DebugCommandRequest::Locals => RemoteDebugCommand::Locals,
        DebugCommandRequest::Stack => RemoteDebugCommand::Stack,
    };

    let command_id = state.next_command_id();
    let (response_tx, response_rx) = oneshot::channel();
    {
        let mut waiters = state.debug_command_waiters.lock().await;
        waiters.insert(command_id.clone(), response_tx);
    }

    let command = ControlPlaneCommand::DebugCommand {
        command_id: command_id.clone(),
        session_id: session_id.clone(),
        command: rpc_command,
    };
    let _queued = state.enqueue_command(edge_id, command).await;

    let response = match timeout(Duration::from_secs(20), response_rx).await {
        Ok(Ok(Ok(response))) => response,
        Ok(Ok(Err(message))) => {
            return Err(bad_request(&message));
        }
        Ok(Err(_)) => {
            return Err(bad_request("debug command response channel closed"));
        }
        Err(_) => {
            let mut waiters = state.debug_command_waiters.lock().await;
            waiters.remove(&command_id);
            return Err(bad_request(
                "debug command timed out waiting for edge result",
            ));
        }
    };

    {
        let mut sessions = state.debug_sessions.write().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            match request_for_state {
                DebugCommandRequest::BreakLine { line } => {
                    session.breakpoints.insert(line);
                }
                DebugCommandRequest::ClearLine { line } => {
                    session.breakpoints.remove(&line);
                }
                _ => {}
            }
        }
    }

    Ok(Json(response))
}

async fn enqueue_program_binary_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    request: Request,
) -> Result<(StatusCode, Json<EnqueueCommandResponse>), (StatusCode, Json<ErrorResponse>)> {
    if !is_octet_stream(request.headers().get(CONTENT_TYPE)) {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(ErrorResponse {
                error: "content-type must be application/octet-stream".to_string(),
            }),
        ));
    }

    let bytes = match to_bytes(request.into_body(), MAX_UPLOAD_BYTES + 1).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: "payload too large".to_string(),
                }),
            ));
        }
    };
    if bytes.len() > MAX_UPLOAD_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ErrorResponse {
                error: "payload too large".to_string(),
            }),
        ));
    }

    let command_id = state.next_command_id();
    let command = ControlPlaneCommand::ApplyProgram {
        command_id,
        program_base64: STANDARD.encode(bytes),
    };
    let queued = state.enqueue_command(edge_id, command).await;
    Ok((StatusCode::ACCEPTED, Json(queued)))
}

async fn enqueue_apply_program_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<EnqueueApplyProgramRequest>,
) -> (StatusCode, Json<EnqueueCommandResponse>) {
    let command = ControlPlaneCommand::ApplyProgram {
        command_id: request
            .command_id
            .unwrap_or_else(|| state.next_command_id()),
        program_base64: request.program_base64,
    };
    let queued = state.enqueue_command(edge_id, command).await;
    (StatusCode::ACCEPTED, Json(queued))
}

async fn enqueue_apply_program_version_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<ApplyProgramVersionRequest>,
) -> Result<(StatusCode, Json<EnqueueCommandResponse>), (StatusCode, Json<ErrorResponse>)> {
    let (source, flavor, program_name, selected_version) = {
        let guard = state.inner.read().await;
        let Some(program) = guard.programs.get(&request.program_id) else {
            return Err(not_found("program not found"));
        };
        let selected = if let Some(version) = request.version {
            program.versions.iter().find(|item| item.version == version)
        } else {
            program.versions.last()
        };
        let Some(version) = selected else {
            return Err(bad_request("program has no versions"));
        };
        let source = version.source.clone();
        let flavor = request
            .flavor
            .clone()
            .unwrap_or_else(|| version.flavor.clone());
        (source, flavor, program.name.clone(), version.version)
    };

    let (parsed_flavor, _) = parse_ui_flavor(Some(flavor.as_str()))?;
    let source_text = source_for_flavor(&source, parsed_flavor);
    let compiled = compile_source_with_flavor(&source_text, parsed_flavor)
        .map_err(|err| bad_request(&format!("source compile failed: {err}")))?;
    let program_bytes = encode_program(&compiled.program)
        .map_err(|err| bad_request(&format!("bytecode encode failed: {err}")))?;

    let command_id = state.next_command_id();
    let command = ControlPlaneCommand::ApplyProgram {
        command_id: command_id.clone(),
        program_base64: STANDARD.encode(program_bytes),
    };
    let queued = state
        .enqueue_command_tracked(
            edge_id,
            command,
            Some(AppliedProgramRef {
                program_id: request.program_id,
                name: program_name,
                version: selected_version,
            }),
        )
        .await;
    Ok((StatusCode::ACCEPTED, Json(queued)))
}

async fn enqueue_start_debug_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<EnqueueStartDebugRequest>,
) -> Result<(StatusCode, Json<EnqueueCommandResponse>), (StatusCode, Json<ErrorResponse>)> {
    let tcp_addr = request
        .tcp_addr
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_REMOTE_DEBUGGER_TCP_ADDR)
        .to_string();
    let command = ControlPlaneCommand::StartDebugSession {
        command_id: request
            .command_id
            .unwrap_or_else(|| state.next_command_id()),
        tcp_addr,
        header_name: request.header_name,
        stop_on_entry: request.stop_on_entry,
    };
    let queued = state.enqueue_command(edge_id, command).await;
    Ok((StatusCode::ACCEPTED, Json(queued)))
}

async fn enqueue_stop_debug_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<OptionalCommandIdRequest>,
) -> (StatusCode, Json<EnqueueCommandResponse>) {
    let command = ControlPlaneCommand::StopDebugSession {
        command_id: request
            .command_id
            .unwrap_or_else(|| state.next_command_id()),
    };
    let queued = state.enqueue_command(edge_id, command).await;
    (StatusCode::ACCEPTED, Json(queued))
}

async fn enqueue_get_health_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<OptionalCommandIdRequest>,
) -> (StatusCode, Json<EnqueueCommandResponse>) {
    let command = ControlPlaneCommand::GetHealth {
        command_id: request
            .command_id
            .unwrap_or_else(|| state.next_command_id()),
    };
    let queued = state.enqueue_command(edge_id, command).await;
    (StatusCode::ACCEPTED, Json(queued))
}

async fn enqueue_get_metrics_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<OptionalCommandIdRequest>,
) -> (StatusCode, Json<EnqueueCommandResponse>) {
    let command = ControlPlaneCommand::GetMetrics {
        command_id: request
            .command_id
            .unwrap_or_else(|| state.next_command_id()),
    };
    let queued = state.enqueue_command(edge_id, command).await;
    (StatusCode::ACCEPTED, Json(queued))
}

async fn enqueue_get_telemetry_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<OptionalCommandIdRequest>,
) -> (StatusCode, Json<EnqueueCommandResponse>) {
    let command = ControlPlaneCommand::GetTelemetry {
        command_id: request
            .command_id
            .unwrap_or_else(|| state.next_command_id()),
    };
    let queued = state.enqueue_command(edge_id, command).await;
    (StatusCode::ACCEPTED, Json(queued))
}

async fn enqueue_ping_handler(
    State(state): State<ControllerState>,
    Path(edge_id): Path<String>,
    Json(request): Json<EnqueuePingRequest>,
) -> (StatusCode, Json<EnqueueCommandResponse>) {
    let command = ControlPlaneCommand::Ping {
        command_id: request
            .command_id
            .unwrap_or_else(|| state.next_command_id()),
        payload: request.payload,
    };
    let queued = state.enqueue_command(edge_id, command).await;
    (StatusCode::ACCEPTED, Json(queued))
}

async fn process_debug_session_result(
    state: ControllerState,
    command_id: &str,
    edge_id: &str,
    edge_name: Option<String>,
    is_ok: bool,
    payload: &CommandResultPayload,
) {
    match payload {
        CommandResultPayload::StartDebugSession {
            status,
            nonce_header_name,
            nonce_header_value,
            message,
        } => {
            let session_id = {
                let mut lookup = state.debug_start_lookup.write().await;
                lookup.remove(command_id)
            };
            let Some(session_id) = session_id else {
                return;
            };

            {
                let mut sessions = state.debug_sessions.write().await;
                let Some(session) = sessions.get_mut(&session_id) else {
                    return;
                };
                session.edge_id = edge_id.to_string();
                if let Some(name) = edge_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                {
                    session.edge_name = name.to_string();
                }
                session.updated_unix_ms = now_unix_ms();
                if is_ok && status.is_some() {
                    let reported_status = status.as_ref();
                    session.phase = if reported_status.map(|item| item.attached).unwrap_or(false) {
                        DebugSessionPhase::Attached
                    } else {
                        DebugSessionPhase::WaitingForAttach
                    };
                    session.header_name = nonce_header_name
                        .clone()
                        .or_else(|| reported_status.and_then(|item| item.header_name.clone()))
                        .or_else(|| session.requested_header_name.clone());
                    session.nonce_header_value = nonce_header_value
                        .clone()
                        .or_else(|| reported_status.and_then(|item| item.header_value.clone()));
                    if let Some(addr) = reported_status
                        .and_then(|item| item.tcp_addr.clone())
                        .filter(|value| !value.trim().is_empty())
                    {
                        session.tcp_addr = addr;
                    }
                    session.current_line = reported_status.and_then(|item| item.current_line);
                    session.message = Some(
                        "debug session active on edge; waiting for a matching request to attach"
                            .to_string(),
                    );
                } else {
                    session.phase = DebugSessionPhase::Failed;
                    session.message =
                        Some(message.clone().unwrap_or_else(|| {
                            "failed to start debug session on edge".to_string()
                        }));
                }
            }
        }
        CommandResultPayload::DebugCommand {
            session_id,
            response,
            message,
        } => {
            let target_session_id = if let Some(session_id) = session_id.clone() {
                Some(session_id)
            } else {
                let sessions = state.debug_sessions.read().await;
                sessions
                    .values()
                    .find(|item| item.edge_id == edge_id)
                    .map(|item| item.session_id.clone())
            };
            let mut response_for_waiter: Result<DebugCommandResponse, String> = Err(message
                .clone()
                .unwrap_or_else(|| "debug command failed".to_string()));

            if let Some(target_session_id) = target_session_id {
                let mut sessions = state.debug_sessions.write().await;
                if let Some(session) = sessions.get_mut(&target_session_id) {
                    session.updated_unix_ms = now_unix_ms();
                    if let Some(name) = edge_name
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                    {
                        session.edge_name = name.to_string();
                    }
                    if is_ok {
                        if let Some(remote) = response {
                            if remote.attached {
                                session.phase = DebugSessionPhase::Attached;
                                session.last_resume_command_unix_ms = None;
                                if let Some(line) = remote.current_line {
                                    session.current_line = Some(line);
                                }
                                session.message = Some("debugger attached".to_string());
                            } else {
                                session.last_resume_command_unix_ms = Some(now_unix_ms());
                                if session.phase != DebugSessionPhase::Attached {
                                    session.phase = DebugSessionPhase::WaitingForAttach;
                                }
                                // Keep current line until we positively observe detached/not-attached.
                                session.message =
                                    Some("resume command sent; waiting for next stop".to_string());
                            }
                            session.last_output = Some(remote.output.clone());
                            response_for_waiter = Ok(DebugCommandResponse {
                                phase: session.phase.clone(),
                                output: remote.output.clone(),
                                current_line: session.current_line,
                                attached: remote.attached,
                            });
                        }
                    } else {
                        let error_message = message
                            .clone()
                            .unwrap_or_else(|| "debug command failed".to_string());
                        if error_message.contains("not attached") {
                            session.phase = DebugSessionPhase::WaitingForAttach;
                            session.last_resume_command_unix_ms = None;
                        } else {
                            session.phase = DebugSessionPhase::Failed;
                            session.last_resume_command_unix_ms = None;
                        }
                        session.message = Some(error_message.clone());
                        response_for_waiter = Err(error_message);
                    }
                }
            }

            let waiter = {
                let mut waiters = state.debug_command_waiters.lock().await;
                waiters.remove(command_id)
            };
            if let Some(waiter) = waiter {
                let _ = waiter.send(response_for_waiter);
            }
        }
        CommandResultPayload::StopDebugSession { .. } => {
            let mut sessions = state.debug_sessions.write().await;
            if let Some(session) = sessions
                .values_mut()
                .find(|session| session.stop_command_id.as_deref() == Some(command_id))
            {
                session.phase = DebugSessionPhase::Stopped;
                session.updated_unix_ms = now_unix_ms();
                session.message = Some("debug session stopped".to_string());
            }
        }
        _ => {}
    }
}

fn resolve_edge_debug_source(
    store: &ControllerStore,
    edge_id: &str,
) -> (Option<String>, Option<String>) {
    let Some(edge_record) = store.edges.get(edge_id) else {
        return (None, None);
    };
    let Some(applied) = edge_record.applied_program.as_ref() else {
        return (None, None);
    };
    let Some(program) = store.programs.get(&applied.program_id) else {
        return (None, None);
    };
    let Some(version) = program
        .versions
        .iter()
        .find(|item| item.version == applied.version)
    else {
        return (None, None);
    };
    let flavor = version.flavor.clone();
    let parsed_flavor = parse_ui_flavor(Some(flavor.as_str()))
        .map(|(item, _)| item)
        .unwrap_or(SourceFlavor::RustScript);
    (
        Some(flavor),
        Some(source_for_flavor(&version.source, parsed_flavor)),
    )
}

fn map_summary(edge_id: &str, record: &EdgeRecord) -> EdgeSummary {
    let has_pending_apply = record
        .pending_commands
        .iter()
        .any(|command| matches!(command, ControlPlaneCommand::ApplyProgram { .. }));
    let sync_status = if record.applied_program.is_none() {
        "not_synced"
    } else if has_pending_apply {
        "out_of_sync"
    } else {
        "synced"
    };
    EdgeSummary {
        edge_id: edge_id.to_string(),
        edge_name: if record.edge_name.trim().is_empty() {
            edge_id.to_string()
        } else {
            record.edge_name.clone()
        },
        sync_status: sync_status.to_string(),
        last_seen_unix_ms: record.last_poll_unix_ms,
        pending_commands: record.pending_commands.len(),
        recent_results: record.recent_results.len(),
        applied_program: record.applied_program.clone(),
        last_poll_unix_ms: record.last_poll_unix_ms,
        last_result_unix_ms: record.last_result_unix_ms,
        total_polls: record.total_polls,
        total_results: record.total_results,
        last_telemetry: record.last_telemetry.clone(),
    }
}

fn append_traffic_sample(record: &mut EdgeRecord, sample: EdgeTrafficSample, unix_ms: u64) {
    let previous = record.last_traffic_cumulative.as_ref();
    let point = EdgeTrafficPoint {
        unix_ms,
        requests: previous
            .map(|prev| sample.requests_total.saturating_sub(prev.requests_total))
            .unwrap_or(0),
        status_2xx: previous
            .map(|prev| {
                sample
                    .status_2xx_total
                    .saturating_sub(prev.status_2xx_total)
            })
            .unwrap_or(0),
        status_3xx: previous
            .map(|prev| {
                sample
                    .status_3xx_total
                    .saturating_sub(prev.status_3xx_total)
            })
            .unwrap_or(0),
        status_4xx: previous
            .map(|prev| {
                sample
                    .status_4xx_total
                    .saturating_sub(prev.status_4xx_total)
            })
            .unwrap_or(0),
        status_5xx: previous
            .map(|prev| {
                sample
                    .status_5xx_total
                    .saturating_sub(prev.status_5xx_total)
            })
            .unwrap_or(0),
    };
    record.traffic_points.push_back(point);
    while record.traffic_points.len() > MAX_TRAFFIC_POINTS {
        let _ = record.traffic_points.pop_front();
    }
    record.last_traffic_cumulative = Some(sample);
}

fn map_program_summary(program: &StoredProgram) -> ProgramSummary {
    ProgramSummary {
        program_id: program.program_id.clone(),
        name: program.name.clone(),
        latest_version: program
            .versions
            .last()
            .map(|item| item.version)
            .unwrap_or(0),
        versions: program.versions.len(),
        created_unix_ms: program.created_unix_ms,
        updated_unix_ms: program.updated_unix_ms,
    }
}

fn map_program_detail(program: &StoredProgram) -> ProgramDetailResponse {
    ProgramDetailResponse {
        program_id: program.program_id.clone(),
        name: program.name.clone(),
        latest_version: program
            .versions
            .last()
            .map(|item| item.version)
            .unwrap_or(0),
        created_unix_ms: program.created_unix_ms,
        updated_unix_ms: program.updated_unix_ms,
        versions: program
            .versions
            .iter()
            .map(|item| ProgramVersionSummary {
                version: item.version,
                created_unix_ms: item.created_unix_ms,
                flavor: item.flavor.clone(),
                flow_synced: item.flow_synced,
            })
            .collect(),
    }
}

fn map_program_version_detail(version: &StoredProgramVersion) -> ProgramVersionDetail {
    ProgramVersionDetail {
        version: version.version,
        created_unix_ms: version.created_unix_ms,
        flavor: version.flavor.clone(),
        flow_synced: version.flow_synced,
        nodes: version.nodes.clone(),
        edges: version.edges.clone(),
        source: version.source.clone(),
    }
}

fn command_kind(command: &ControlPlaneCommand) -> &'static str {
    match command {
        ControlPlaneCommand::ApplyProgram { .. } => "apply_program",
        ControlPlaneCommand::StartDebugSession { .. } => "start_debug_session",
        ControlPlaneCommand::DebugCommand { .. } => "debug_command",
        ControlPlaneCommand::StopDebugSession { .. } => "stop_debug_session",
        ControlPlaneCommand::GetHealth { .. } => "get_health",
        ControlPlaneCommand::GetMetrics { .. } => "get_metrics",
        ControlPlaneCommand::GetTelemetry { .. } => "get_telemetry",
        ControlPlaneCommand::Ping { .. } => "ping",
    }
}

fn ui_block_catalog() -> Vec<UiBlockDefinition> {
    vec![
        UiBlockDefinition {
            id: "const_string",
            title: "Const String",
            category: "value",
            description: "Create a string variable output for downstream blocks.",
            inputs: vec![
                UiBlockInput {
                    key: "var",
                    label: "Variable",
                    input_type: UiInputType::Text,
                    default_value: "text_value",
                    placeholder: "text_value",
                    connectable: false,
                },
                UiBlockInput {
                    key: "value",
                    label: "Value",
                    input_type: UiInputType::Text,
                    default_value: "hello",
                    placeholder: "hello",
                    connectable: false,
                },
            ],
            outputs: vec![UiBlockOutput {
                key: "value",
                label: "value",
                expr_from_input: Some("var"),
            }],
            accepts_flow: false,
        },
        UiBlockDefinition {
            id: "const_number",
            title: "Const Number",
            category: "value",
            description: "Create a number variable output for downstream blocks.",
            inputs: vec![
                UiBlockInput {
                    key: "var",
                    label: "Variable",
                    input_type: UiInputType::Text,
                    default_value: "num_value",
                    placeholder: "num_value",
                    connectable: false,
                },
                UiBlockInput {
                    key: "value",
                    label: "Value",
                    input_type: UiInputType::Number,
                    default_value: "1",
                    placeholder: "1",
                    connectable: false,
                },
            ],
            outputs: vec![UiBlockOutput {
                key: "value",
                label: "value",
                expr_from_input: Some("var"),
            }],
            accepts_flow: false,
        },
        UiBlockDefinition {
            id: "get_header",
            title: "Get Header",
            category: "http",
            description: "Read request header into a variable.",
            inputs: vec![
                UiBlockInput {
                    key: "var",
                    label: "Variable",
                    input_type: UiInputType::Text,
                    default_value: "header",
                    placeholder: "header",
                    connectable: false,
                },
                UiBlockInput {
                    key: "name",
                    label: "Header Name",
                    input_type: UiInputType::Text,
                    default_value: "x-client-id",
                    placeholder: "x-client-id",
                    connectable: false,
                },
            ],
            outputs: vec![UiBlockOutput {
                key: "value",
                label: "value",
                expr_from_input: Some("var"),
            }],
            accepts_flow: false,
        },
        UiBlockDefinition {
            id: "set_header",
            title: "Set Header",
            category: "edge_abi",
            description: "Set response header via vm.set_header.",
            inputs: vec![
                UiBlockInput {
                    key: "name",
                    label: "Header Name",
                    input_type: UiInputType::Text,
                    default_value: "x-vm",
                    placeholder: "x-vm",
                    connectable: false,
                },
                UiBlockInput {
                    key: "value",
                    label: "Value",
                    input_type: UiInputType::Text,
                    default_value: "ok",
                    placeholder: "ok or $var",
                    connectable: true,
                },
            ],
            outputs: vec![UiBlockOutput {
                key: "next",
                label: "next",
                expr_from_input: None,
            }],
            accepts_flow: true,
        },
        UiBlockDefinition {
            id: "set_response_content",
            title: "Set Response Content",
            category: "edge_abi",
            description: "Short-circuit request with response content.",
            inputs: vec![UiBlockInput {
                key: "value",
                label: "Body",
                input_type: UiInputType::Text,
                default_value: "okkk",
                placeholder: "request allowed or $var",
                connectable: true,
            }],
            outputs: vec![UiBlockOutput {
                key: "next",
                label: "next",
                expr_from_input: None,
            }],
            accepts_flow: true,
        },
        UiBlockDefinition {
            id: "set_response_status",
            title: "Set Response Status",
            category: "edge_abi",
            description: "Set response status code (used for short-circuit and upstream responses).",
            inputs: vec![UiBlockInput {
                key: "status",
                label: "Status Code",
                input_type: UiInputType::Number,
                default_value: "429",
                placeholder: "200-599",
                connectable: true,
            }],
            outputs: vec![UiBlockOutput {
                key: "next",
                label: "next",
                expr_from_input: None,
            }],
            accepts_flow: true,
        },
        UiBlockDefinition {
            id: "set_upstream",
            title: "Set Upstream",
            category: "edge_abi",
            description: "Forward request to upstream host:port.",
            inputs: vec![UiBlockInput {
                key: "upstream",
                label: "Upstream",
                input_type: UiInputType::Text,
                default_value: "127.0.0.1:8088",
                placeholder: "127.0.0.1:8088",
                connectable: true,
            }],
            outputs: vec![UiBlockOutput {
                key: "next",
                label: "next",
                expr_from_input: None,
            }],
            accepts_flow: true,
        },
        UiBlockDefinition {
            id: "rate_limit_if_else",
            title: "Rate Limit If/Else",
            category: "control",
            description: "Use vm.rate_limit_allow and branch to allowed/blocked flow outputs.",
            inputs: vec![
                UiBlockInput {
                    key: "key_expr",
                    label: "Key Expr",
                    input_type: UiInputType::Text,
                    default_value: "$header",
                    placeholder: "$header or literal",
                    connectable: true,
                },
                UiBlockInput {
                    key: "limit",
                    label: "Limit",
                    input_type: UiInputType::Number,
                    default_value: "3",
                    placeholder: "3",
                    connectable: false,
                },
                UiBlockInput {
                    key: "window_seconds",
                    label: "Window Seconds",
                    input_type: UiInputType::Number,
                    default_value: "60",
                    placeholder: "60",
                    connectable: false,
                },
            ],
            outputs: vec![
                UiBlockOutput {
                    key: "allowed",
                    label: "allowed",
                    expr_from_input: None,
                },
                UiBlockOutput {
                    key: "blocked",
                    label: "blocked",
                    expr_from_input: None,
                },
            ],
            accepts_flow: true,
        },
        UiBlockDefinition {
            id: "if",
            title: "If",
            category: "control",
            description: "Plain conditional compare with true/false flow outputs.",
            inputs: vec![
                UiBlockInput {
                    key: "lhs",
                    label: "LHS",
                    input_type: UiInputType::Text,
                    default_value: "left",
                    placeholder: "left or $var",
                    connectable: true,
                },
                UiBlockInput {
                    key: "rhs",
                    label: "RHS",
                    input_type: UiInputType::Text,
                    default_value: "right",
                    placeholder: "right or $var",
                    connectable: true,
                },
            ],
            outputs: vec![
                UiBlockOutput {
                    key: "true",
                    label: "true",
                    expr_from_input: None,
                },
                UiBlockOutput {
                    key: "false",
                    label: "false",
                    expr_from_input: None,
                },
            ],
            accepts_flow: true,
        },
        UiBlockDefinition {
            id: "loop",
            title: "Loop",
            category: "control",
            description: "Plain fixed-count loop with body/done flow outputs.",
            inputs: vec![UiBlockInput {
                key: "count",
                label: "Count",
                input_type: UiInputType::Number,
                default_value: "1",
                placeholder: "1 or $var",
                connectable: true,
            }],
            outputs: vec![
                UiBlockOutput {
                    key: "body",
                    label: "body",
                    expr_from_input: None,
                },
                UiBlockOutput {
                    key: "done",
                    label: "done",
                    expr_from_input: None,
                },
            ],
            accepts_flow: true,
        },
    ]
}

#[derive(Clone, Debug)]
struct ResolvedUiNode {
    id: String,
    block: UiBlockInstance,
}

#[derive(Clone, Debug)]
struct ResolvedFlowEdge {
    source_output: String,
    target: String,
}

#[derive(Clone, Debug)]
struct ResolvedUiGraph {
    ordered_nodes: Vec<ResolvedUiNode>,
    flow_outgoing: HashMap<String, Vec<ResolvedFlowEdge>>,
    flow_incoming_count: HashMap<String, usize>,
    has_flow_edges: bool,
}

fn render_ui_sources(
    blocks: &[UiBlockInstance],
    nodes: &[UiGraphNode],
    edges: &[UiGraphEdge],
) -> Result<UiSourceBundle, (StatusCode, Json<ErrorResponse>)> {
    if !blocks.is_empty() {
        return render_sources(blocks);
    }
    if nodes.is_empty() {
        return render_sources(&[]);
    }

    let resolved = resolve_ui_graph(nodes, edges)?;
    if !resolved.has_flow_edges {
        let ordered = resolved
            .ordered_nodes
            .into_iter()
            .map(|node| node.block)
            .collect::<Vec<_>>();
        return render_sources(&ordered);
    }
    render_sources_with_flow(&resolved)
}

fn resolve_ui_graph(
    nodes: &[UiGraphNode],
    edges: &[UiGraphEdge],
) -> Result<ResolvedUiGraph, (StatusCode, Json<ErrorResponse>)> {
    if nodes.len() > MAX_UI_BLOCKS {
        return Err(bad_request(&format!(
            "too many graph nodes: {} (limit {})",
            nodes.len(),
            MAX_UI_BLOCKS
        )));
    }

    let catalog = ui_block_catalog();
    let definition_map = catalog
        .iter()
        .map(|definition| (definition.id.to_string(), definition))
        .collect::<HashMap<_, _>>();
    let node_map = nodes
        .iter()
        .map(|node| (node.id.clone(), node))
        .collect::<HashMap<_, _>>();

    let mut data_incoming: HashMap<String, Vec<&UiGraphEdge>> = HashMap::new();
    let mut flow_outgoing: HashMap<String, Vec<ResolvedFlowEdge>> = HashMap::new();
    let mut flow_incoming_count: HashMap<String, usize> = HashMap::new();
    let mut indegree: HashMap<String, usize> = HashMap::new();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    let mut seen_data_targets: HashSet<(String, String)> = HashSet::new();
    let mut seen_flow_outputs: HashSet<(String, String)> = HashSet::new();
    let mut has_flow_edges = false;

    for node in nodes {
        indegree.insert(node.id.clone(), 0);
        flow_incoming_count.insert(node.id.clone(), 0);
    }

    for edge in edges {
        let Some(source_node) = node_map.get(&edge.source) else {
            return Err(bad_request(&format!(
                "edge references missing source node '{}'",
                edge.source
            )));
        };
        let Some(target_node) = node_map.get(&edge.target) else {
            return Err(bad_request(&format!(
                "edge references missing target node '{}'",
                edge.target
            )));
        };

        let Some(source_definition) = definition_map.get(&source_node.block_id) else {
            return Err(bad_request(&format!(
                "unknown block_id '{}' for source node '{}'",
                source_node.block_id, source_node.id
            )));
        };
        let Some(source_output) = source_definition
            .outputs
            .iter()
            .find(|output| output.key == edge.source_output)
        else {
            return Err(bad_request(&format!(
                "source output '{}' not found on block '{}'",
                edge.source_output, source_definition.id
            )));
        };

        let Some(target_definition) = definition_map.get(&target_node.block_id) else {
            return Err(bad_request(&format!(
                "unknown block_id '{}' for target node '{}'",
                target_node.block_id, target_node.id
            )));
        };

        match source_output.expr_from_input {
            Some(_) => {
                let Some(target_input) = target_definition
                    .inputs
                    .iter()
                    .find(|input| input.key == edge.target_input)
                else {
                    return Err(bad_request(&format!(
                        "target input '{}' not found on block '{}'",
                        edge.target_input, target_definition.id
                    )));
                };
                if !target_input.connectable {
                    return Err(bad_request(&format!(
                        "target input '{}' on block '{}' is not connectable",
                        edge.target_input, target_definition.id
                    )));
                }
                let data_target_key = (edge.target.clone(), edge.target_input.clone());
                if !seen_data_targets.insert(data_target_key) {
                    return Err(bad_request(&format!(
                        "target input '{}' on node '{}' has multiple data connections",
                        edge.target_input, edge.target
                    )));
                }
                data_incoming
                    .entry(edge.target.clone())
                    .or_default()
                    .push(edge);
            }
            None => {
                if edge.target_input != "__flow" {
                    return Err(bad_request(&format!(
                        "control output '{}' must connect to target_input='__flow'",
                        edge.source_output
                    )));
                }
                if !target_definition.accepts_flow {
                    return Err(bad_request(&format!(
                        "target block '{}' does not accept flow edges",
                        target_definition.id
                    )));
                }
                let flow_key = (edge.source.clone(), edge.source_output.clone());
                if !seen_flow_outputs.insert(flow_key) {
                    return Err(bad_request(&format!(
                        "source output '{}' on node '{}' already connected",
                        edge.source_output, edge.source
                    )));
                }
                flow_outgoing
                    .entry(edge.source.clone())
                    .or_default()
                    .push(ResolvedFlowEdge {
                        source_output: edge.source_output.clone(),
                        target: edge.target.clone(),
                    });
                let flow_incoming = flow_incoming_count.entry(edge.target.clone()).or_default();
                *flow_incoming += 1;
                if *flow_incoming > 1 {
                    return Err(bad_request(&format!(
                        "target node '{}' has multiple incoming flow edges",
                        edge.target
                    )));
                }
                has_flow_edges = true;
            }
        }

        *indegree.entry(edge.target.clone()).or_default() += 1;
        adjacency
            .entry(edge.source.clone())
            .or_default()
            .push(edge.target.clone());
    }

    let order_hint = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id.clone(), index))
        .collect::<HashMap<_, _>>();
    for targets in adjacency.values_mut() {
        targets.sort_by_key(|node_id| order_hint.get(node_id).copied().unwrap_or(usize::MAX));
    }

    let mut queue = nodes
        .iter()
        .filter(|node| indegree.get(&node.id).copied().unwrap_or(0) == 0)
        .map(|node| node.id.clone())
        .collect::<VecDeque<_>>();
    let mut ordered_ids = Vec::with_capacity(nodes.len());
    while let Some(node_id) = queue.pop_front() {
        ordered_ids.push(node_id.clone());
        if let Some(targets) = adjacency.get(&node_id) {
            for target_id in targets {
                if let Some(entry) = indegree.get_mut(target_id) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        queue.push_back(target_id.clone());
                    }
                }
            }
        }
    }
    if ordered_ids.len() != nodes.len() {
        return Err(bad_request(
            "graph contains a cycle; connect blocks as a directed acyclic graph",
        ));
    }

    for outgoing in flow_outgoing.values_mut() {
        outgoing.sort_by_key(|edge| order_hint.get(&edge.target).copied().unwrap_or(usize::MAX));
    }

    let mut ordered_nodes = Vec::with_capacity(nodes.len());
    for node_id in ordered_ids {
        let node = node_map
            .get(&node_id)
            .ok_or_else(|| bad_request("failed to resolve graph node"))?;
        let mut values = node.values.clone();

        if let Some(node_incoming) = data_incoming.get(&node_id) {
            for edge in node_incoming {
                let source_node = node_map
                    .get(&edge.source)
                    .ok_or_else(|| bad_request("failed to resolve edge source"))?;
                let source_definition = definition_map
                    .get(&source_node.block_id)
                    .ok_or_else(|| bad_request("failed to resolve source block definition"))?;
                let source_output = source_definition
                    .outputs
                    .iter()
                    .find(|output| output.key == edge.source_output)
                    .ok_or_else(|| bad_request("source output handle no longer exists"))?;
                let Some(expr_key) = source_output.expr_from_input else {
                    return Err(bad_request("source output does not expose an expression"));
                };
                let expr_name = source_node
                    .values
                    .get(expr_key)
                    .map(String::as_str)
                    .unwrap_or("value");
                let ident = sanitize_identifier(Some(&expr_name.to_string()), "value");
                values.insert(edge.target_input.clone(), format!("${ident}"));
            }
        }

        ordered_nodes.push(ResolvedUiNode {
            id: node.id.clone(),
            block: UiBlockInstance {
                block_id: node.block_id.clone(),
                values,
            },
        });
    }

    Ok(ResolvedUiGraph {
        ordered_nodes,
        flow_outgoing,
        flow_incoming_count,
        has_flow_edges,
    })
}

fn render_sources_with_flow(
    graph: &ResolvedUiGraph,
) -> Result<UiSourceBundle, (StatusCode, Json<ErrorResponse>)> {
    let mut rss_lines = vec!["use vm;".to_string(), String::new()];
    let mut js_lines = vec!["import * as vm from \"vm\";".to_string(), String::new()];
    let mut lua_lines = vec!["local vm = require(\"vm\")".to_string(), String::new()];
    let mut scm_lines = vec![
        "(require (prefix-in vm. \"vm\"))".to_string(),
        String::new(),
    ];

    let mut order_index = HashMap::new();
    let mut node_map = HashMap::new();
    for (index, node) in graph.ordered_nodes.iter().enumerate() {
        order_index.insert(node.id.clone(), index);
        node_map.insert(node.id.clone(), &node.block);
    }

    for node in &graph.ordered_nodes {
        if is_value_block(&node.block.block_id) {
            render_single_block(
                &node.block,
                &mut rss_lines,
                &mut js_lines,
                &mut lua_lines,
                &mut scm_lines,
            )?;
        }
    }

    for node in &graph.ordered_nodes {
        if is_value_block(&node.block.block_id) {
            continue;
        }
        if !is_flow_block(&node.block.block_id) {
            return Err(bad_request(&format!(
                "block '{}' is not flow-compatible when control edges are present",
                node.block.block_id
            )));
        }
    }

    let mut roots = graph
        .ordered_nodes
        .iter()
        .filter(|node| {
            !is_value_block(&node.block.block_id)
                && graph
                    .flow_incoming_count
                    .get(&node.id)
                    .copied()
                    .unwrap_or(0)
                    == 0
        })
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    roots.sort_by_key(|node_id| order_index.get(node_id).copied().unwrap_or(usize::MAX));

    let mut rendered = HashSet::new();
    let mut visiting = HashSet::new();
    for root in roots {
        let statements =
            render_flow_node(&root, graph, &node_map, &mut rendered, &mut visiting, 0)?;
        rss_lines.extend(statements.rustscript);
        js_lines.extend(statements.javascript);
        lua_lines.extend(statements.lua);
        scm_lines.extend(statements.scheme);
    }

    Ok(UiSourceBundle {
        rustscript: join_lines(&rss_lines),
        javascript: join_lines(&js_lines),
        lua: join_lines(&lua_lines),
        scheme: join_lines(&scm_lines),
    })
}

fn is_value_block(block_id: &str) -> bool {
    matches!(block_id, "const_string" | "const_number" | "get_header")
}

fn is_flow_block(block_id: &str) -> bool {
    matches!(
        block_id,
        "set_header"
            | "set_response_content"
            | "set_response_status"
            | "set_upstream"
            | "rate_limit_if_else"
            | "if"
            | "loop"
    )
}

#[derive(Default)]
struct FlowStatements {
    rustscript: Vec<String>,
    javascript: Vec<String>,
    lua: Vec<String>,
    scheme: Vec<String>,
}

impl FlowStatements {
    fn extend(&mut self, mut other: FlowStatements) {
        self.rustscript.append(&mut other.rustscript);
        self.javascript.append(&mut other.javascript);
        self.lua.append(&mut other.lua);
        self.scheme.append(&mut other.scheme);
    }
}

fn render_flow_node(
    node_id: &str,
    graph: &ResolvedUiGraph,
    node_map: &HashMap<String, &UiBlockInstance>,
    rendered: &mut HashSet<String>,
    visiting: &mut HashSet<String>,
    indent: usize,
) -> Result<FlowStatements, (StatusCode, Json<ErrorResponse>)> {
    if rendered.contains(node_id) {
        return Ok(FlowStatements::default());
    }

    if !visiting.insert(node_id.to_string()) {
        return Err(bad_request(
            "flow graph contains a cycle; use loop blocks instead of back edges",
        ));
    }

    let block = node_map
        .get(node_id)
        .ok_or_else(|| bad_request("flow path references missing node"))?;

    let result = match block.block_id.as_str() {
        "set_header" | "set_response_content" | "set_response_status" | "set_upstream" => {
            let mut statements = FlowStatements::default();
            let action = flow_action_statement(block)?;
            statements
                .rustscript
                .push(indent_line(indent, action.rustscript));
            statements
                .javascript
                .push(indent_line(indent, action.javascript));
            statements.lua.push(indent_line(indent, action.lua));
            statements.scheme.push(action.scheme);

            if let Some(next_target) = next_flow_target(node_id, graph)? {
                statements.extend(render_flow_node(
                    &next_target,
                    graph,
                    node_map,
                    rendered,
                    visiting,
                    indent,
                )?);
            }

            Ok(statements)
        }
        "rate_limit_if_else" => {
            let key_expr = block_value(block, "key_expr", "$header");
            let limit = sanitize_number(block.values.get("limit"), "3");
            let window = sanitize_number(block.values.get("window_seconds"), "60");
            let allowed_target =
                required_flow_target(node_id, graph, "allowed", "rate_limit_if_else")?;
            let blocked_target =
                required_flow_target(node_id, graph, "blocked", "rate_limit_if_else")?;

            let allowed_branch = render_flow_node(
                &allowed_target,
                graph,
                node_map,
                rendered,
                visiting,
                indent + 1,
            )?;
            let blocked_branch = render_flow_node(
                &blocked_target,
                graph,
                node_map,
                rendered,
                visiting,
                indent + 1,
            )?;

            let mut statements = FlowStatements::default();
            statements.rustscript.push(indent_line(
                indent,
                format!(
                    "if vm::rate_limit_allow({}, {}, {}) {{",
                    render_expr_rss(key_expr),
                    limit,
                    window
                ),
            ));
            statements
                .rustscript
                .extend(allowed_branch.rustscript.clone());
            statements
                .rustscript
                .push(indent_line(indent, "} else {".to_string()));
            statements
                .rustscript
                .extend(blocked_branch.rustscript.clone());
            statements
                .rustscript
                .push(indent_line(indent, "}".to_string()));

            statements.javascript.push(indent_line(
                indent,
                format!(
                    "if (vm.rate_limit_allow({}, {}, {})) {{",
                    render_expr_js(key_expr),
                    limit,
                    window
                ),
            ));
            statements
                .javascript
                .extend(allowed_branch.javascript.clone());
            statements
                .javascript
                .push(indent_line(indent, "} else {".to_string()));
            statements
                .javascript
                .extend(blocked_branch.javascript.clone());
            statements
                .javascript
                .push(indent_line(indent, "}".to_string()));

            statements.lua.push(indent_line(
                indent,
                format!(
                    "if vm.rate_limit_allow({}, {}, {}) then",
                    render_expr_lua(key_expr),
                    limit,
                    window
                ),
            ));
            statements.lua.extend(allowed_branch.lua.clone());
            statements.lua.push(indent_line(indent, "else".to_string()));
            statements.lua.extend(blocked_branch.lua.clone());
            statements.lua.push(indent_line(indent, "end".to_string()));

            statements.scheme.push(format!(
                "(if (vm.rate_limit_allow {} {} {}) {} {})",
                render_expr_scheme(key_expr),
                limit,
                window,
                scheme_branch_expr(&allowed_branch.scheme),
                scheme_branch_expr(&blocked_branch.scheme)
            ));
            Ok(statements)
        }
        "if" => {
            let lhs = block_value(block, "lhs", "left");
            let rhs = block_value(block, "rhs", "right");
            let true_target = required_flow_target(node_id, graph, "true", "if")?;
            let false_target = optional_flow_target(node_id, graph, "false");

            let true_branch = render_flow_node(
                &true_target,
                graph,
                node_map,
                rendered,
                visiting,
                indent + 1,
            )?;
            let false_branch = if let Some(target) = false_target {
                Some(render_flow_node(
                    &target,
                    graph,
                    node_map,
                    rendered,
                    visiting,
                    indent + 1,
                )?)
            } else {
                None
            };

            let mut statements = FlowStatements::default();
            statements.rustscript.push(indent_line(
                indent,
                format!("if {} == {} {{", render_expr_rss(lhs), render_expr_rss(rhs)),
            ));
            statements.rustscript.extend(true_branch.rustscript.clone());
            if let Some(false_branch) = &false_branch {
                statements
                    .rustscript
                    .push(indent_line(indent, "} else {".to_string()));
                statements
                    .rustscript
                    .extend(false_branch.rustscript.clone());
            }
            statements
                .rustscript
                .push(indent_line(indent, "}".to_string()));

            statements.javascript.push(indent_line(
                indent,
                format!(
                    "if ({} === {}) {{",
                    render_expr_js(lhs),
                    render_expr_js(rhs)
                ),
            ));
            statements.javascript.extend(true_branch.javascript.clone());
            if let Some(false_branch) = &false_branch {
                statements
                    .javascript
                    .push(indent_line(indent, "} else {".to_string()));
                statements
                    .javascript
                    .extend(false_branch.javascript.clone());
            }
            statements
                .javascript
                .push(indent_line(indent, "}".to_string()));

            statements.lua.push(indent_line(
                indent,
                format!(
                    "if {} == {} then",
                    render_expr_lua(lhs),
                    render_expr_lua(rhs)
                ),
            ));
            statements.lua.extend(true_branch.lua.clone());
            if let Some(false_branch) = &false_branch {
                statements.lua.push(indent_line(indent, "else".to_string()));
                statements.lua.extend(false_branch.lua.clone());
            }
            statements.lua.push(indent_line(indent, "end".to_string()));

            let scheme_false = false_branch
                .as_ref()
                .map(|branch| scheme_branch_expr(&branch.scheme))
                .unwrap_or_else(|| "null".to_string());
            statements.scheme.push(format!(
                "(if (== {} {}) {} {})",
                render_expr_scheme(lhs),
                render_expr_scheme(rhs),
                scheme_branch_expr(&true_branch.scheme),
                scheme_false
            ));
            Ok(statements)
        }
        "loop" => {
            let count = render_number_expr(block_value(block, "count", "1"), "1");
            let body_target = required_flow_target(node_id, graph, "body", "loop")?;
            let done_target = required_flow_target(node_id, graph, "done", "loop")?;

            let body_branch = render_flow_node(
                &body_target,
                graph,
                node_map,
                rendered,
                visiting,
                indent + 1,
            )?;
            let done_branch =
                render_flow_node(&done_target, graph, node_map, rendered, visiting, indent)?;

            let mut statements = FlowStatements::default();
            statements.rustscript.push(indent_line(
                indent,
                format!("for (let i = 0; i < {count}; i = i + 1) {{"),
            ));
            statements.rustscript.extend(body_branch.rustscript.clone());
            statements
                .rustscript
                .push(indent_line(indent, "}".to_string()));
            statements.rustscript.extend(done_branch.rustscript.clone());

            statements.javascript.push(indent_line(
                indent,
                format!("for (let i = 0; i < {count}; i = i + 1) {{"),
            ));
            statements.javascript.extend(body_branch.javascript.clone());
            statements
                .javascript
                .push(indent_line(indent, "}".to_string()));
            statements.javascript.extend(done_branch.javascript.clone());

            statements
                .lua
                .push(indent_line(indent, format!("for i = 1, {count} do")));
            statements.lua.extend(body_branch.lua.clone());
            statements.lua.push(indent_line(indent, "end".to_string()));
            statements.lua.extend(done_branch.lua.clone());

            statements.scheme.push(format!(
                "(let loop ((i 0)) (if (< i {count}) (begin {} (loop (+ i 1))) 'done))",
                scheme_branch_expr(&body_branch.scheme)
            ));
            statements.scheme.extend(done_branch.scheme.clone());
            Ok(statements)
        }
        other => Err(bad_request(&format!(
            "unsupported flow node block '{}'",
            other
        ))),
    };

    visiting.remove(node_id);
    if result.is_ok() {
        rendered.insert(node_id.to_string());
    }
    result
}

fn indent_line(level: usize, line: String) -> String {
    format!("{}{}", "    ".repeat(level), line)
}

fn next_flow_target(
    node_id: &str,
    graph: &ResolvedUiGraph,
) -> Result<Option<String>, (StatusCode, Json<ErrorResponse>)> {
    let outgoing = graph
        .flow_outgoing
        .get(node_id)
        .cloned()
        .unwrap_or_default();
    if outgoing.is_empty() {
        return Ok(None);
    }
    if outgoing.len() != 1 || outgoing[0].source_output != "next" {
        return Err(bad_request(
            "action blocks can only use a single 'next' outgoing flow edge",
        ));
    }
    Ok(Some(outgoing[0].target.clone()))
}

fn required_flow_target(
    node_id: &str,
    graph: &ResolvedUiGraph,
    output: &str,
    block_id: &str,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    graph
        .flow_outgoing
        .get(node_id)
        .and_then(|edges| {
            edges
                .iter()
                .find(|edge| edge.source_output == output)
                .map(|edge| edge.target.clone())
        })
        .ok_or_else(|| {
            bad_request(&format!(
                "{block_id} requires a '{output}' outgoing flow edge"
            ))
        })
}

fn optional_flow_target(node_id: &str, graph: &ResolvedUiGraph, output: &str) -> Option<String> {
    graph.flow_outgoing.get(node_id).and_then(|edges| {
        edges
            .iter()
            .find(|edge| edge.source_output == output)
            .map(|edge| edge.target.clone())
    })
}

#[derive(Clone, Debug)]
struct FlowActionStatement {
    rustscript: String,
    javascript: String,
    lua: String,
    scheme: String,
}

fn flow_action_statement(
    block: &UiBlockInstance,
) -> Result<FlowActionStatement, (StatusCode, Json<ErrorResponse>)> {
    match block.block_id.as_str() {
        "set_header" => {
            let name = block_value(block, "name", "x-vm");
            let value = block_value(block, "value", "ok");
            Ok(FlowActionStatement {
                rustscript: format!(
                    "vm::set_header({}, {});",
                    rust_string(name),
                    render_expr_rss(value)
                ),
                javascript: format!(
                    "vm.set_header({}, {});",
                    js_string(name),
                    render_expr_js(value)
                ),
                lua: format!(
                    "vm.set_header({}, {})",
                    lua_string(name),
                    render_expr_lua(value)
                ),
                scheme: format!(
                    "(vm.set_header {} {})",
                    scheme_string(name),
                    render_expr_scheme(value)
                ),
            })
        }
        "set_response_content" => {
            let value = block_value(block, "value", "request allowed");
            Ok(FlowActionStatement {
                rustscript: format!("vm::set_response_content({});", render_expr_rss(value)),
                javascript: format!("vm.set_response_content({});", render_expr_js(value)),
                lua: format!("vm.set_response_content({})", render_expr_lua(value)),
                scheme: format!("(vm.set_response_content {})", render_expr_scheme(value)),
            })
        }
        "set_response_status" => {
            let status = sanitize_status_code(block.values.get("status"), "429");
            Ok(FlowActionStatement {
                rustscript: format!("vm::set_response_status({status});"),
                javascript: format!("vm.set_response_status({status});"),
                lua: format!("vm.set_response_status({status})"),
                scheme: format!("(vm.set_response_status {status})"),
            })
        }
        "set_upstream" => {
            let upstream = block_value(block, "upstream", "127.0.0.1:8088");
            Ok(FlowActionStatement {
                rustscript: format!("vm::set_upstream({});", rust_string(upstream)),
                javascript: format!("vm.set_upstream({});", js_string(upstream)),
                lua: format!("vm.set_upstream({})", lua_string(upstream)),
                scheme: format!("(vm.set_upstream {})", scheme_string(upstream)),
            })
        }
        other => Err(bad_request(&format!(
            "unsupported flow action block '{}'",
            other
        ))),
    }
}

fn scheme_branch_expr(expressions: &[String]) -> String {
    if expressions.is_empty() {
        return "null".to_string();
    }
    if expressions.len() == 1 {
        return expressions[0].clone();
    }
    format!("(begin {})", expressions.join(" "))
}

fn render_number_expr(raw: &str, fallback: &str) -> String {
    if let Some(expr) = raw.strip_prefix('$') {
        return sanitize_identifier(Some(&expr.to_string()), "value");
    }
    let trimmed = raw.trim();
    if !trimmed.is_empty() && trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        trimmed.to_string()
    } else {
        fallback.to_string()
    }
}

fn render_sources(
    blocks: &[UiBlockInstance],
) -> Result<UiSourceBundle, (StatusCode, Json<ErrorResponse>)> {
    if blocks.len() > MAX_UI_BLOCKS {
        return Err(bad_request(&format!(
            "too many blocks: {} (limit {})",
            blocks.len(),
            MAX_UI_BLOCKS
        )));
    }

    let mut rss_lines = vec!["use vm;".to_string(), String::new()];
    let mut js_lines = vec!["import * as vm from \"vm\";".to_string(), String::new()];
    let mut lua_lines = vec!["local vm = require(\"vm\")".to_string(), String::new()];
    let mut scm_lines = vec![
        "(require (prefix-in vm. \"vm\"))".to_string(),
        String::new(),
    ];

    for block in blocks {
        render_single_block(
            block,
            &mut rss_lines,
            &mut js_lines,
            &mut lua_lines,
            &mut scm_lines,
        )?
    }

    Ok(UiSourceBundle {
        rustscript: join_lines(&rss_lines),
        javascript: join_lines(&js_lines),
        lua: join_lines(&lua_lines),
        scheme: join_lines(&scm_lines),
    })
}

fn render_single_block(
    block: &UiBlockInstance,
    rss: &mut Vec<String>,
    js: &mut Vec<String>,
    lua: &mut Vec<String>,
    scm: &mut Vec<String>,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    match block.block_id.as_str() {
        "const_string" => {
            let var = sanitize_identifier(block.values.get("var"), "text_value");
            let value = block_value(block, "value", "hello");
            rss.push(format!("let {var} = {};", rust_string(value)));
            js.push(format!("let {var} = {};", js_string(value)));
            lua.push(format!("local {var} = {}", lua_string(value)));
            scm.push(format!("(define {var} {})", scheme_string(value)));
        }
        "const_number" => {
            let var = sanitize_identifier(block.values.get("var"), "num_value");
            let value = sanitize_number(block.values.get("value"), "1");
            rss.push(format!("let {var} = {value};"));
            js.push(format!("let {var} = {value};"));
            lua.push(format!("local {var} = {value}"));
            scm.push(format!("(define {var} {value})"));
        }
        "get_header" => {
            let var = sanitize_identifier(block.values.get("var"), "header");
            let header_name = block_value(block, "name", "x-client-id");
            rss.push(format!(
                "let {var} = vm::get_header({});",
                rust_string(header_name)
            ));
            js.push(format!(
                "let {var} = vm.get_header({});",
                js_string(header_name)
            ));
            lua.push(format!(
                "local {var} = vm.get_header({})",
                lua_string(header_name)
            ));
            scm.push(format!(
                "(define {var} (vm.get_header {}))",
                scheme_string(header_name)
            ));
        }
        "set_header" => {
            let name = block_value(block, "name", "x-vm");
            let value = block_value(block, "value", "ok");
            rss.push(format!(
                "vm::set_header({}, {});",
                rust_string(name),
                render_expr_rss(value)
            ));
            js.push(format!(
                "vm.set_header({}, {});",
                js_string(name),
                render_expr_js(value)
            ));
            lua.push(format!(
                "vm.set_header({}, {})",
                lua_string(name),
                render_expr_lua(value)
            ));
            scm.push(format!(
                "(vm.set_header {} {})",
                scheme_string(name),
                render_expr_scheme(value)
            ));
        }
        "set_response_content" => {
            let value = block_value(block, "value", "request allowed");
            rss.push(format!(
                "vm::set_response_content({});",
                render_expr_rss(value)
            ));
            js.push(format!(
                "vm.set_response_content({});",
                render_expr_js(value)
            ));
            lua.push(format!(
                "vm.set_response_content({})",
                render_expr_lua(value)
            ));
            scm.push(format!(
                "(vm.set_response_content {})",
                render_expr_scheme(value)
            ));
        }
        "set_response_status" => {
            let status = sanitize_status_code(block.values.get("status"), "429");
            rss.push(format!("vm::set_response_status({status});"));
            js.push(format!("vm.set_response_status({status});"));
            lua.push(format!("vm.set_response_status({status})"));
            scm.push(format!("(vm.set_response_status {status})"));
        }
        "set_upstream" => {
            let upstream = block_value(block, "upstream", "127.0.0.1:8088");
            rss.push(format!("vm::set_upstream({});", rust_string(upstream)));
            js.push(format!("vm.set_upstream({});", js_string(upstream)));
            lua.push(format!("vm.set_upstream({})", lua_string(upstream)));
            scm.push(format!("(vm.set_upstream {})", scheme_string(upstream)));
        }
        "rate_limit_if_else" => {
            let key_expr = block_value(block, "key_expr", "$header");
            let limit = sanitize_number(block.values.get("limit"), "3");
            let window = sanitize_number(block.values.get("window_seconds"), "60");

            rss.push(format!(
                "if vm::rate_limit_allow({}, {}, {}) {{",
                render_expr_rss(key_expr),
                limit,
                window
            ));
            rss.push(format!(
                "    vm::set_response_content({});",
                rust_string("request allowed")
            ));
            rss.push("} else {".to_string());
            rss.push(format!(
                "    vm::set_response_content({});",
                rust_string("rate limit exceeded")
            ));
            rss.push("}".to_string());

            js.push(format!(
                "if (vm.rate_limit_allow({}, {}, {})) {{",
                render_expr_js(key_expr),
                limit,
                window
            ));
            js.push(format!(
                "    vm.set_response_content({});",
                js_string("request allowed")
            ));
            js.push("} else {".to_string());
            js.push(format!(
                "    vm.set_response_content({});",
                js_string("rate limit exceeded")
            ));
            js.push("}".to_string());

            lua.push(format!(
                "if vm.rate_limit_allow({}, {}, {}) then",
                render_expr_lua(key_expr),
                limit,
                window
            ));
            lua.push(format!(
                "    vm.set_response_content({})",
                lua_string("request allowed")
            ));
            lua.push("else".to_string());
            lua.push(format!(
                "    vm.set_response_content({})",
                lua_string("rate limit exceeded")
            ));
            lua.push("end".to_string());

            scm.push(format!(
                "(if (vm.rate_limit_allow {} {} {})",
                render_expr_scheme(key_expr),
                limit,
                window
            ));
            scm.push(format!(
                "    (vm.set_response_content {})",
                scheme_string("request allowed")
            ));
            scm.push(format!(
                "    (vm.set_response_content {}))",
                scheme_string("rate limit exceeded")
            ));
        }
        "if" => {
            let lhs = block_value(block, "lhs", "left");
            let rhs = block_value(block, "rhs", "right");

            rss.push(format!(
                "if {} == {} {{",
                render_expr_rss(lhs),
                render_expr_rss(rhs)
            ));
            rss.push("} else {".to_string());
            rss.push("}".to_string());

            js.push(format!(
                "if ({} === {}) {{",
                render_expr_js(lhs),
                render_expr_js(rhs)
            ));
            js.push("} else {".to_string());
            js.push("}".to_string());

            lua.push(format!(
                "if {} == {} then",
                render_expr_lua(lhs),
                render_expr_lua(rhs)
            ));
            lua.push("else".to_string());
            lua.push("end".to_string());

            scm.push(format!(
                "(if (== {} {}) null null)",
                render_expr_scheme(lhs),
                render_expr_scheme(rhs)
            ));
        }
        "loop" => {
            let count = render_number_expr(block_value(block, "count", "1"), "1");

            rss.push(format!("for (let i = 0; i < {count}; i = i + 1) {{"));
            rss.push("}".to_string());

            js.push(format!("for (let i = 0; i < {count}; i = i + 1) {{"));
            js.push("}".to_string());

            lua.push(format!("for i = 1, {count} do"));
            lua.push("end".to_string());

            scm.push(format!(
                "(let loop ((i 0)) (if (< i {count}) (loop (+ i 1)) 'done))"
            ));
        }
        "if_header_equals" => {
            let header_name = block_value(block, "header_name", "x-debug");
            let equals_value = block_value(block, "equals_value", "on");
            let then_body = block_value(block, "then_body", "debug mode");
            let else_body = block_value(block, "else_body", "normal mode");

            rss.push(format!(
                "let __header_check = vm::get_header({});",
                rust_string(header_name)
            ));
            rss.push(format!(
                "if __header_check == {} {{",
                rust_string(equals_value)
            ));
            rss.push(format!(
                "    vm::set_response_content({});",
                rust_string(then_body)
            ));
            rss.push("} else {".to_string());
            rss.push(format!(
                "    vm::set_response_content({});",
                rust_string(else_body)
            ));
            rss.push("}".to_string());

            js.push(format!(
                "let __header_check = vm.get_header({});",
                js_string(header_name)
            ));
            js.push(format!(
                "if (__header_check === {}) {{",
                js_string(equals_value)
            ));
            js.push(format!(
                "    vm.set_response_content({});",
                js_string(then_body)
            ));
            js.push("} else {".to_string());
            js.push(format!(
                "    vm.set_response_content({});",
                js_string(else_body)
            ));
            js.push("}".to_string());

            lua.push(format!(
                "local __header_check = vm.get_header({})",
                lua_string(header_name)
            ));
            lua.push(format!(
                "if __header_check == {} then",
                lua_string(equals_value)
            ));
            lua.push(format!(
                "    vm.set_response_content({})",
                lua_string(then_body)
            ));
            lua.push("else".to_string());
            lua.push(format!(
                "    vm.set_response_content({})",
                lua_string(else_body)
            ));
            lua.push("end".to_string());

            scm.push(format!(
                "(let ((__header_check (vm.get_header {})))",
                scheme_string(header_name)
            ));
            scm.push(format!(
                "  (if (== __header_check {})",
                scheme_string(equals_value)
            ));
            scm.push(format!(
                "      (vm.set_response_content {})",
                scheme_string(then_body)
            ));
            scm.push(format!(
                "      (vm.set_response_content {})))",
                scheme_string(else_body)
            ));
        }
        "repeat_set_header" => {
            let count = sanitize_number(block.values.get("count"), "3");
            let header_name = block_value(block, "header_name", "x-loop");
            let header_value = block_value(block, "header_value", "on");

            rss.push(format!("for (let i = 0; i < {count}; i = i + 1) {{"));
            rss.push(format!(
                "    vm::set_header({}, {});",
                rust_string(header_name),
                rust_string(header_value)
            ));
            rss.push("}".to_string());

            js.push(format!("for (let i = 0; i < {count}; i = i + 1) {{"));
            js.push(format!(
                "    vm.set_header({}, {});",
                js_string(header_name),
                js_string(header_value)
            ));
            js.push("}".to_string());

            lua.push(format!("for i = 1, {count} do"));
            lua.push(format!(
                "    vm.set_header({}, {})",
                lua_string(header_name),
                lua_string(header_value)
            ));
            lua.push("end".to_string());

            scm.push("(let loop ((i 0))".to_string());
            scm.push(format!("  (if (< i {count})"));
            scm.push(format!(
                "      (begin (vm.set_header {} {}) (loop (+ i 1)))",
                scheme_string(header_name),
                scheme_string(header_value)
            ));
            scm.push("      'done))".to_string());
        }
        other => return Err(bad_request(&format!("unknown block_id '{other}'"))),
    }
    Ok(())
}

fn parse_ui_flavor(
    value: Option<&str>,
) -> Result<(SourceFlavor, &'static str), (StatusCode, Json<ErrorResponse>)> {
    let raw = value.unwrap_or("rustscript").trim().to_ascii_lowercase();
    match raw.as_str() {
        "rustscript" | "rss" => Ok((SourceFlavor::RustScript, "rustscript")),
        "javascript" | "js" => Ok((SourceFlavor::JavaScript, "javascript")),
        "lua" => Ok((SourceFlavor::Lua, "lua")),
        "scheme" | "scm" => Ok((SourceFlavor::Scheme, "scheme")),
        _ => Err(bad_request(
            "flavor must be one of: rustscript, javascript, lua, scheme",
        )),
    }
}

fn source_for_flavor(bundle: &UiSourceBundle, flavor: SourceFlavor) -> String {
    match flavor {
        SourceFlavor::RustScript => bundle.rustscript.clone(),
        SourceFlavor::JavaScript => bundle.javascript.clone(),
        SourceFlavor::Lua => bundle.lua.clone(),
        SourceFlavor::Scheme => bundle.scheme.clone(),
    }
}

fn join_lines(lines: &[String]) -> String {
    lines.join("\n")
}

fn sanitize_identifier(value: Option<&String>, fallback: &str) -> String {
    let raw = value.map(|v| v.trim()).unwrap_or("");
    let candidate = if raw.is_empty() { fallback } else { raw };
    let mut output = String::with_capacity(candidate.len());
    for (index, ch) in candidate.chars().enumerate() {
        let valid = ch == '_' || ch.is_ascii_alphanumeric();
        if !valid {
            continue;
        }
        if index == 0 && ch.is_ascii_digit() {
            output.push('_');
        }
        output.push(ch);
    }
    if output.is_empty() {
        fallback.to_string()
    } else {
        output
    }
}

fn sanitize_number(value: Option<&String>, fallback: &str) -> String {
    let raw = value.map(|v| v.trim()).unwrap_or("");
    if !raw.is_empty() && raw.chars().all(|ch| ch.is_ascii_digit()) {
        raw.to_string()
    } else {
        fallback.to_string()
    }
}

fn sanitize_status_code(value: Option<&String>, fallback: &str) -> String {
    let raw = sanitize_number(value, fallback);
    match raw.parse::<u16>() {
        Ok(code) if (100..=599).contains(&code) => code.to_string(),
        _ => fallback.to_string(),
    }
}

fn block_value<'a>(block: &'a UiBlockInstance, key: &str, fallback: &'a str) -> &'a str {
    block
        .values
        .get(key)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
}

fn render_expr_rss(raw: &str) -> String {
    render_expr_common(raw, rust_string)
}

fn render_expr_js(raw: &str) -> String {
    render_expr_common(raw, js_string)
}

fn render_expr_lua(raw: &str) -> String {
    render_expr_common(raw, lua_string)
}

fn render_expr_scheme(raw: &str) -> String {
    render_expr_common(raw, scheme_string)
}

fn render_expr_common(raw: &str, literal_renderer: fn(&str) -> String) -> String {
    if let Some(expr) = raw.strip_prefix('$') {
        let ident = sanitize_identifier(Some(&expr.to_string()), "value");
        return ident;
    }
    literal_renderer(raw)
}

fn rust_string(value: &str) -> String {
    format!("\"{}\"", escape_double_quoted(value))
}

fn js_string(value: &str) -> String {
    format!("\"{}\"", escape_double_quoted(value))
}

fn lua_string(value: &str) -> String {
    format!("\"{}\"", escape_double_quoted(value))
}

fn scheme_string(value: &str) -> String {
    format!("\"{}\"", escape_double_quoted(value))
}

fn escape_double_quoted(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
}

fn webui_content_type(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".js") {
        "text/javascript; charset=utf-8"
    } else if path.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if path.ends_with(".json") {
        "application/json; charset=utf-8"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        "image/jpeg"
    } else if path.ends_with(".ico") {
        "image/x-icon"
    } else if path.ends_with(".webp") {
        "image/webp"
    } else if path.ends_with(".woff2") {
        "font/woff2"
    } else if path.ends_with(".woff") {
        "font/woff"
    } else if path.ends_with(".ttf") {
        "font/ttf"
    } else if path.ends_with(".map") {
        "application/json; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

fn is_octet_stream(value: Option<&axum::http::HeaderValue>) -> bool {
    let Some(value) = value else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    value
        .split(';')
        .next()
        .map(|item| item.trim().eq_ignore_ascii_case("application/octet-stream"))
        .unwrap_or(false)
}

fn bad_request(message: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
}

fn not_found(message: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
}

fn internal_error(message: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: message }),
    )
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
