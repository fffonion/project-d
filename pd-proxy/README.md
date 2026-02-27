# Proxy Quickstart

This proxy has two listeners:

- Data plane: `0.0.0.0:8080` (default)
- Control plane: `127.0.0.1:8081` (default)

The control plane accepts compiled VM bytecode via `PUT /program`.

## Codebase Layout

- `pd-proxy/src/runtime.rs`: proxy skeleton (data/control HTTP routes, upload, forwarding)
- `pd-proxy/src/host_abi.rs`: all VM host ABI functions and registration
- `pd-proxy/src/debug_session.rs`: on-demand debugger session lifecycle and VM attach logic

## Logging

Proxy now emits colored logs (via `tracing`) for:

- access logs (method, path, status, latency) on both data/control planes
- program load success/fail and validation/decode errors
- debug session start/stop and debugger attach events

Set log level with `RUST_LOG`, for example:

```powershell
$env:RUST_LOG="info"
cargo run -p pd-proxy
```

## ABI Source of Truth

Proxy host-call ABI is centralized in the `proxy_abi` crate:

- Rust constants + metadata: `proxy_abi::FUNCTIONS`
- ABI version: `proxy_abi::ABI_VERSION`
- Machine-readable manifest: [`pd-proxy-abi/abi.json`](../pd-proxy-abi/abi.json)

For Rust runtime embedding, use:

```rust
proxy::register_host_module(&mut vm, context)?;
```

instead of registering host functions one-by-one.

## Sample Program

`pd-proxy/examples/build_sample_program.rs` now:

1. Reads source from default path `examples/sample_proxy_program.rss` (resolved from the `pd-proxy` crate root)
2. Compiles it with `vm::compile_source_file` (extension-driven flavor detection)
3. Encodes bytecode and uploads to `http://127.0.0.1:8081/program`

The sample source (`sample_proxy_program.rss`) declares required proxy host
functions in fixed ABI order and then:

1. Reads `x-client-id`
2. Allows at most 3 requests per 60-second window per client id using `rate_limit_allow`
3. Short-circuits with:
- `x-vm: allowed` + body `request allowed` when under limit
- `x-vm: rate-limited` + body `rate limit exceeded` when over limit

## Run + Upload (PowerShell)

1. Start the proxy:

```powershell
cargo run -p pd-proxy
```

2. In another terminal, compile and upload sample source:

```powershell
cargo run -p pd-proxy --example build_sample_program
```

Alternative sample source flavors are also available:
- `pd-proxy/examples/sample_proxy_program.js`
- `pd-proxy/examples/sample_proxy_program.lua`

You can pass a relative sample path explicitly, for example:

```powershell
cargo run -p pd-proxy --example build_sample_program -- examples/sample_proxy_program.js
```

Expected output includes `control response: 204 No Content`.

3. Hit data plane to verify:

```powershell
curl -i "http://127.0.0.1:8080/anything" -H "x-client-id: demo-client"
```

First 3 responses for the same `x-client-id`:

- Status: `200 OK`
- Header: `x-vm: allowed`
- Body: `request allowed`

4th response within 60 seconds:

- Status: `200 OK`
- Header: `x-vm: rate-limited`
- Body: `rate limit exceeded`

## Optional Env Overrides

```powershell
$env:DATA_ADDR="0.0.0.0:9000"
$env:CONTROL_ADDR="127.0.0.1:9001"
$env:MAX_PROGRAM_BYTES="1048576"
cargo run -p pd-proxy
```

## On-Demand VM Debugging

Start a debugger session on control plane and target only requests that include a specific header.

1. Start debugger session:

```powershell
curl -X PUT "http://127.0.0.1:8081/debug/session" \
  -H "content-type: application/json" \
  -d "{\"header_name\":\"x-debug-vm\",\"header_value\":\"on\",\"tcp_addr\":\"127.0.0.1:9002\",\"stop_on_entry\":true}"
```

2. Connect a terminal client to debugger TCP port (example with netcat):

```bash
nc 127.0.0.1 9002
```

3. Send a matching request to data plane:

```powershell
curl -i "http://127.0.0.1:8080/anything" -H "x-debug-vm: on" -H "x-client-id: demo-client"
```

The VM for that request will attach to debugger and accept iterative `pdb` commands such as:
`break`, `break line`, `step`, `next`, `out`, `stack`, `locals`, `where`, `funcs`, `continue`.

4. Check session status:

```powershell
curl "http://127.0.0.1:8081/debug/session"
```

5. Stop debugger session:

```powershell
curl -X DELETE "http://127.0.0.1:8081/debug/session"
```
