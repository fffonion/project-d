use std::io;
use std::path::{Path, PathBuf};

use vm::{
    CallOutcome, Debugger, FunctionDecl, HostFunction, Value, Vm, VmError, VmStatus,
    compile_source_file,
};

const DEFAULT_SOURCE: &str = "examples/example.rss";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliConfig {
    source: Option<String>,
    debug: bool,
    tcp_addr: Option<String>,
    stop_on_entry: bool,
    jit_dump: bool,
    jit_hot_loop_threshold: Option<u32>,
    help: bool,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            source: None,
            debug: false,
            tcp_addr: None,
            stop_on_entry: true,
            jit_dump: false,
            jit_hot_loop_threshold: None,
            help: false,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cli = parse_cli_args(&args).map_err(io::Error::other)?;
    if cli.help {
        print_usage();
        return Ok(());
    }

    let source_path = resolve_source_path(cli.source.as_deref())?;
    let compiled = compile_source_file(&source_path)?;
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    if let Some(hot_loop) = cli.jit_hot_loop_threshold {
        let mut jit_config = vm.jit_config().clone();
        jit_config.hot_loop_threshold = hot_loop;
        vm.set_jit_config(jit_config);
    }
    register_functions(&mut vm, &compiled.functions)?;

    let mut debugger = if cli.debug {
        let mut debugger = if let Some(addr) = &cli.tcp_addr {
            println!("[debug] tcp debugger listening on {addr}");
            Debugger::with_tcp(addr)?
        } else {
            Debugger::new()
        };
        if cli.stop_on_entry {
            debugger.stop_on_entry();
        }
        Some(debugger)
    } else {
        None
    };

    loop {
        let status = if let Some(debugger) = debugger.as_mut() {
            vm.run_with_debugger(debugger)?
        } else {
            vm.run()?
        };
        match status {
            VmStatus::Halted => {
                println!("vm halted");
                println!("stack: {:?}", vm.stack());
                break;
            }
            VmStatus::Yielded => {
                println!("vm yielded, resuming...");
                continue;
            }
        }
    }
    if cli.jit_dump {
        println!("{}", vm.dump_jit_info());
    }
    Ok(())
}

fn parse_cli_args(args: &[String]) -> Result<CliConfig, String> {
    let mut cfg = CliConfig::default();
    let mut index = 0usize;

    if let Some(first) = args.first() {
        if first == "debug" {
            cfg.debug = true;
            index = 1;
        }
    }

    while index < args.len() {
        match args[index].as_str() {
            "-h" | "--help" => {
                cfg.help = true;
                index += 1;
            }
            "--debug" => {
                cfg.debug = true;
                index += 1;
            }
            "--tcp" => {
                cfg.debug = true;
                let addr = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --tcp".to_string())?
                    .clone();
                cfg.tcp_addr = Some(addr);
                index += 2;
            }
            "--stop-on-entry" => {
                cfg.debug = true;
                cfg.stop_on_entry = true;
                index += 1;
            }
            "--no-stop-on-entry" => {
                cfg.debug = true;
                cfg.stop_on_entry = false;
                index += 1;
            }
            "--jit-dump" => {
                cfg.jit_dump = true;
                index += 1;
            }
            "--jit-hot-loop" => {
                let raw = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --jit-hot-loop".to_string())?;
                let value = raw
                    .parse::<u32>()
                    .map_err(|_| format!("invalid --jit-hot-loop value '{raw}'"))?;
                cfg.jit_hot_loop_threshold = Some(value);
                index += 2;
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown flag '{value}'"));
            }
            path => {
                if cfg.source.is_some() {
                    return Err("multiple source paths provided".to_string());
                }
                cfg.source = Some(path.to_string());
                index += 1;
            }
        }
    }

    Ok(cfg)
}

fn resolve_source_path(arg: Option<&str>) -> Result<PathBuf, io::Error> {
    let rel = arg.unwrap_or(DEFAULT_SOURCE);
    let provided = PathBuf::from(rel);
    if provided.is_absolute() {
        return Ok(provided);
    }

    let cwd_path = std::env::current_dir()?.join(&provided);
    if cwd_path.exists() {
        return Ok(cwd_path);
    }

    Ok(Path::new(env!("CARGO_MANIFEST_DIR")).join(provided))
}

fn register_functions(vm: &mut Vm, functions: &[FunctionDecl]) -> Result<(), io::Error> {
    for decl in functions {
        let registered = match decl.name.as_str() {
            "print" => vm.register_function(Box::new(PrintFunction)),
            "add_one" => vm.register_function(Box::new(AddOneFunction)),
            "echo" => vm.register_function(Box::new(EchoFunction)),
            "get_header" => vm.register_function(Box::new(GetHeaderFunction)),
            "rate_limit_allow" => vm.register_function(Box::new(RateLimitAllowFunction)),
            "set_header" | "set_response_content" | "set_upstream" => {
                vm.register_function(Box::new(NoopFunction))
            }
            other => {
                return Err(io::Error::other(format!(
                    "no host binding for function '{other}' at index {}",
                    decl.index
                )));
            }
        };

        if registered != decl.index {
            return Err(io::Error::other(format!(
                "host function order mismatch for '{}': expected {}, got {}",
                decl.name, decl.index, registered
            )));
        }
    }
    Ok(())
}

fn print_usage() {
    println!("Usage:");
    println!("  pd-vm-run [source_path]");
    println!("  pd-vm-run --debug [--stop-on-entry|--no-stop-on-entry] [source_path]");
    println!("  pd-vm-run --debug --tcp <addr> [source_path]");
    println!("  pd-vm-run [--jit-hot-loop <n>] [--jit-dump] [source_path]");
    println!("  pd-vm-run debug [--tcp <addr>] [source_path]");
}

struct PrintFunction;

impl HostFunction for PrintFunction {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, VmError> {
        let rendered = args.iter().map(format_value).collect::<Vec<_>>().join(" ");
        println!("{rendered}");
        Ok(CallOutcome::Return(args.to_vec()))
    }
}

struct AddOneFunction;

impl HostFunction for AddOneFunction {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, VmError> {
        let value = match args.first() {
            Some(Value::Int(value)) => *value,
            _ => return Err(VmError::TypeMismatch("int")),
        };
        Ok(CallOutcome::Return(vec![Value::Int(value + 1)]))
    }
}

struct EchoFunction;

impl HostFunction for EchoFunction {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, VmError> {
        let value = args.first().cloned().ok_or(VmError::StackUnderflow)?;
        Ok(CallOutcome::Return(vec![value]))
    }
}

struct GetHeaderFunction;

impl HostFunction for GetHeaderFunction {
    fn call(&mut self, _vm: &mut Vm, _args: &[Value]) -> Result<CallOutcome, VmError> {
        Ok(CallOutcome::Return(vec![Value::String(String::new())]))
    }
}

struct RateLimitAllowFunction;

impl HostFunction for RateLimitAllowFunction {
    fn call(&mut self, _vm: &mut Vm, _args: &[Value]) -> Result<CallOutcome, VmError> {
        Ok(CallOutcome::Return(vec![Value::Bool(true)]))
    }
}

struct NoopFunction;

impl HostFunction for NoopFunction {
    fn call(&mut self, _vm: &mut Vm, _args: &[Value]) -> Result<CallOutcome, VmError> {
        Ok(CallOutcome::Return(vec![]))
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Int(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::String(value) => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_cli_args;

    fn s(value: &str) -> String {
        value.to_string()
    }

    #[test]
    fn parse_cli_defaults() {
        let cfg = parse_cli_args(&[]).expect("parse should succeed");
        assert!(!cfg.debug);
        assert!(cfg.tcp_addr.is_none());
        assert!(cfg.stop_on_entry);
        assert!(!cfg.jit_dump);
        assert!(cfg.jit_hot_loop_threshold.is_none());
        assert!(cfg.source.is_none());
    }

    #[test]
    fn parse_cli_debug_with_source_and_tcp() {
        let cfg = parse_cli_args(&[
            s("--debug"),
            s("--tcp"),
            s("127.0.0.1:9002"),
            s("examples/example.lua"),
        ])
        .expect("parse should succeed");
        assert!(cfg.debug);
        assert_eq!(cfg.tcp_addr.as_deref(), Some("127.0.0.1:9002"));
        assert_eq!(cfg.source.as_deref(), Some("examples/example.lua"));
    }

    #[test]
    fn parse_cli_legacy_debug_command() {
        let cfg =
            parse_cli_args(&[s("debug"), s("examples/example.rss")]).expect("parse should succeed");
        assert!(cfg.debug);
        assert_eq!(cfg.source.as_deref(), Some("examples/example.rss"));
    }

    #[test]
    fn parse_cli_rejects_multiple_sources() {
        let err = parse_cli_args(&[s("a.rss"), s("b.rss")]).expect_err("parse should fail");
        assert!(err.contains("multiple source paths"));
    }

    #[test]
    fn parse_cli_jit_flags() {
        let cfg = parse_cli_args(&[
            s("--jit-hot-loop"),
            s("2"),
            s("--jit-dump"),
            s("examples/example.rss"),
        ])
        .expect("parse should succeed");
        assert_eq!(cfg.jit_hot_loop_threshold, Some(2));
        assert!(cfg.jit_dump);
        assert_eq!(cfg.source.as_deref(), Some("examples/example.rss"));
    }
}
