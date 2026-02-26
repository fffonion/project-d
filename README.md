# project-d

`project-d` is a Rust workspace for a programmable HTTP proxy powered by a stack-based VM.

## Workspace crates

- `pd-vm`: VM runtime, assembler, source compiler (`.rss`, `.js`, `.lua`, `.scm`), debugger, and trace JIT.
- `pd-proxy`: HTTP proxy runtime with control plane APIs for uploading VM programs and optional debug sessions.
- `pd-proxy-abi`: shared ABI contract (Rust constants + `abi.json`) used by the proxy host-call layer.

## Prerequisites

- Rust toolchain with 2024 edition support
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
cargo run -p pd-proxy
```

In another terminal, compile and upload the sample proxy program:

```powershell
cargo run -p pd-proxy --example build_sample_program
```

Then send a request through the data plane:

```powershell
curl -i "http://127.0.0.1:8080/anything" -H "x-client-id: demo-client"
```

## More details

- VM docs and examples: `pd-vm/README.md`
- Proxy runtime and control-plane usage: `pd-proxy/README.md`
- ABI manifest: `pd-proxy-abi/abi.json`
