# project-d

`project-d` is a Rust workspace for a programmable HTTP proxy powered by a stack-based VM.

## Workspace crates

- `pd-vm`: VM runtime, assembler, source compiler (`.rss`, `.js`, `.lua`, `.scm`), debugger, and trace JIT.
- `pd-edge`: HTTP proxy runtime with a local admin endpoint for uploading VM programs and optional debug sessions.
- `pd-controller`: control-plane server that queues commands for active edges and receives RPC results.
- `pd-edge-abi`: shared ABI contract (Rust constants + `abi.json`) used by the proxy host-call layer.

`pd-controller/webui` contains the React + shadcn web UI for visual block composition and deploy.

## Prerequisites

- Rust toolchain with 2024 edition support
- Bun (for `pd-controller/webui`)
- PowerShell (examples below use PowerShell syntax)

## Quick start

Build everything:

```powershell
cargo build --workspace
```

Run all tests:

```powershell
cargo test --workspace
```

Run VM example:

```powershell
cargo run -p pd-vm --bin pd-vm-run -- pd-vm/examples/example.rss
```

Start proxy:

```powershell
cargo run -p pd-edge
```

In another terminal, compile and upload the sample proxy program:

```powershell
cargo run -p pd-edge --example build_sample_program
```

Then send a request through the data plane:

```powershell
curl -i "http://127.0.0.1:8080/anything" -H "x-client-id: demo-client"
```

## More details

- VM docs and examples: `pd-vm/README.md`
- Proxy runtime and admin endpoint usage: `pd-edge/README.md`
- ABI manifest: `pd-edge-abi/abi.json`
