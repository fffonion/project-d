# project-d

`project-d` is a Rust workspace for programmable edge data planes driven by a VM and a central controller.

## Workspace layout

- `pd-vm`: VM runtime, compiler (`.rss`, `.js`, `.lua`, `.scm`), debugger, and tools.
- `pd-edge-abi`: ABI contract shared by VM host functions and edge runtime.
- `pd-edge`: edge data plane runtime (data listener + local admin endpoint + active control-plane RPC client).
- `pd-controller`: control plane service (RPC endpoints, state persistence, program/version management, remote debug orchestration, and Web UI at `/ui`).

## Prerequisites

- Rust (edition 2024 compatible toolchain)
- Bun (for `pd-controller/webui`)

## Build and test

```powershell
cargo build --workspace
cargo test --workspace
```

## Local end-to-end quick start

1. Build Web UI assets (embedded into `pd-controller` binary):

```powershell
cd pd-controller/webui
bun install
bun run build
cd ../..
```

2. Start controller:

```powershell
cargo run -p pd-controller
```

3. Start one edge that actively connects to controller:

```powershell
cargo run -p pd-edge -- --control-plane-url "http://127.0.0.1:9100" --edge-name "edge-local-1"
```

4. Open controller Web UI:

```text
http://127.0.0.1:9100/ui
```

## Key runtime behavior

- Edge identity:
  - UUID is generated/persisted at `.pd-edge/edge-id` by default, or can be set with `--edge-id`.
  - Friendly name defaults to hostname, or can be set with `--edge-name`.
- Edge local listeners:
  - Data plane: `--data-addr` (default `0.0.0.0:8080`)
  - Admin endpoint: `--admin-addr` (default `127.0.0.1:8081`)
  - Program size limit: `--max-program-bytes` (default `1048576`)
- Controller persistence (default base path `.pd-controller/state.json`) is split as:
  - core: `state.json`
  - programs: `state.programs.json`
  - timeseries: `state.timeseries.bin`

## Useful docs

- [pd-controller README](pd-controller/README.md)
- [pd-edge README](pd-edge/README.md)
- [pd-vm README](pd-vm/README.md)
- ABI manifest: `pd-edge-abi/abi.json`
