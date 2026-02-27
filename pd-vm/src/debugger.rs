use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::net::{TcpListener, TcpStream};

use crate::debug::DebugInfo;
use crate::vm::Vm;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepMode {
    Running,
    Step,
    StepOver { depth: usize, ip: usize },
    StepOut { depth: usize },
}

pub struct Debugger {
    breakpoints: HashSet<usize>,
    line_breakpoints: HashSet<u32>,
    step_mode: StepMode,
    server: Option<DebugServer>,
    client_detached: bool,
}

impl Default for Debugger {
    fn default() -> Self {
        Self::new()
    }
}

impl Debugger {
    pub fn new() -> Self {
        Self {
            breakpoints: HashSet::new(),
            line_breakpoints: HashSet::new(),
            step_mode: StepMode::Running,
            server: None,
            client_detached: false,
        }
    }

    pub fn with_tcp(addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(false)?;
        Ok(Self {
            breakpoints: HashSet::new(),
            line_breakpoints: HashSet::new(),
            step_mode: StepMode::Running,
            server: Some(DebugServer::new(listener)),
            client_detached: false,
        })
    }

    pub fn stop_on_entry(&mut self) {
        self.step_mode = StepMode::Step;
    }

    pub fn add_breakpoint(&mut self, offset: usize) {
        self.breakpoints.insert(offset);
    }

    pub fn remove_breakpoint(&mut self, offset: usize) {
        self.breakpoints.remove(&offset);
    }

    pub fn on_instruction(&mut self, vm: &Vm) {
        let ip = vm.ip();
        let mut should_break = self.breakpoints.contains(&ip);

        if !should_break
            && let Some(line) = current_line(vm)
            && self.line_breakpoints.contains(&line)
        {
            should_break = true;
        }

        if !should_break {
            match self.step_mode {
                StepMode::Step => {
                    should_break = true;
                }
                StepMode::StepOver {
                    depth,
                    ip: start_ip,
                } => {
                    if vm.call_depth() <= depth && ip != start_ip {
                        should_break = true;
                    }
                }
                StepMode::StepOut { depth } => {
                    if vm.call_depth() < depth {
                        should_break = true;
                    }
                }
                StepMode::Running => {}
            }
        }
        if should_break {
            self.step_mode = StepMode::Running;
            self.client_detached = self.repl(vm);
        }
    }

    pub fn take_detach_event(&mut self) -> bool {
        std::mem::take(&mut self.client_detached)
    }

    fn repl(&mut self, vm: &Vm) -> bool {
        if let Some(server) = self.server.as_mut() {
            return server.repl(
                vm,
                &mut self.breakpoints,
                &mut self.line_breakpoints,
                &mut self.step_mode,
            );
        }
        repl_stdio(
            vm,
            &mut self.breakpoints,
            &mut self.line_breakpoints,
            &mut self.step_mode,
        );
        false
    }
}

struct DebugServer {
    listener: TcpListener,
    stream: Option<TcpStream>,
}

impl DebugServer {
    fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            stream: None,
        }
    }

    fn ensure_client(&mut self) -> io::Result<()> {
        if self.stream.is_none() {
            let (stream, _) = self.listener.accept()?;
            self.stream = Some(stream);
        }
        Ok(())
    }

    fn repl(
        &mut self,
        vm: &Vm,
        breakpoints: &mut HashSet<usize>,
        line_breakpoints: &mut HashSet<u32>,
        step: &mut StepMode,
    ) -> bool {
        if self.ensure_client().is_err() {
            return false;
        }
        let Some(stream) = self.stream.as_mut() else {
            return false;
        };
        let _ = writeln!(stream, "debugger attached. type 'help' for commands");
        let Ok(clone) = stream.try_clone() else {
            self.stream = None;
            return true;
        };
        let mut reader = io::BufReader::new(clone);
        loop {
            if write_prompt(stream).is_err() {
                self.stream = None;
                return true;
            }
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    self.stream = None;
                    return true;
                }
                Ok(_) => {}
                Err(_) => {
                    self.stream = None;
                    return true;
                }
            }
            if handle_command(&line, vm, breakpoints, line_breakpoints, step, stream).is_break() {
                return false;
            }
        }
    }
}

fn repl_stdio(
    vm: &Vm,
    breakpoints: &mut HashSet<usize>,
    line_breakpoints: &mut HashSet<u32>,
    step: &mut StepMode,
) {
    let stdin = io::stdin();
    let mut input = String::new();
    loop {
        input.clear();
        print!("(pdb) ");
        let _ = io::stdout().flush();
        if stdin.read_line(&mut input).is_err() {
            break;
        }
        if handle_command(
            &input,
            vm,
            breakpoints,
            line_breakpoints,
            step,
            &mut io::stdout(),
        )
        .is_break()
        {
            break;
        }
    }
}

fn write_prompt(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(b"(pdb) ")?;
    stream.flush()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplAction {
    Continue,
    Break,
}

impl ReplAction {
    fn is_break(self) -> bool {
        matches!(self, ReplAction::Break)
    }
}

fn handle_command(
    line: &str,
    vm: &Vm,
    breakpoints: &mut HashSet<usize>,
    line_breakpoints: &mut HashSet<u32>,
    step: &mut StepMode,
    out: &mut dyn Write,
) -> ReplAction {
    let mut parts = line.split_whitespace();
    let Some(cmd) = parts.next() else {
        return ReplAction::Continue;
    };
    match cmd {
        "c" | "continue" => return ReplAction::Break,
        "s" | "step" | "stepi" => {
            *step = StepMode::Step;
            return ReplAction::Break;
        }
        "n" | "next" => {
            *step = StepMode::StepOver {
                depth: vm.call_depth(),
                ip: vm.ip(),
            };
            return ReplAction::Break;
        }
        "finish" | "out" => {
            *step = StepMode::StepOut {
                depth: vm.call_depth(),
            };
            return ReplAction::Break;
        }
        "b" | "break" => {
            if let Some(arg) = parts.next() {
                if arg == "line" {
                    if let Some(line) = parse_u32(parts.next()) {
                        line_breakpoints.insert(line);
                        let _ = writeln!(out, "line breakpoint set at {line}");
                    } else {
                        let _ = writeln!(out, "usage: break line <number>");
                    }
                    return ReplAction::Continue;
                }
                if let Ok(offset) = arg.parse::<usize>() {
                    breakpoints.insert(offset);
                    let _ = writeln!(out, "breakpoint set at {offset}");
                } else {
                    let _ = writeln!(out, "expected instruction offset");
                }
            } else {
                let _ = writeln!(out, "usage: break <offset>");
            }
        }
        "bl" => {
            if let Some(line) = parse_u32(parts.next()) {
                line_breakpoints.insert(line);
                let _ = writeln!(out, "line breakpoint set at {line}");
            } else {
                let _ = writeln!(out, "usage: bl <line>");
            }
        }
        "clear" => {
            if let Some(arg) = parts.next() {
                if arg == "line" {
                    if let Some(line) = parse_u32(parts.next()) {
                        line_breakpoints.remove(&line);
                        let _ = writeln!(out, "line breakpoint cleared at {line}");
                    } else {
                        let _ = writeln!(out, "usage: clear line <number>");
                    }
                    return ReplAction::Continue;
                }
                if let Ok(offset) = arg.parse::<usize>() {
                    breakpoints.remove(&offset);
                    let _ = writeln!(out, "breakpoint cleared at {offset}");
                } else {
                    let _ = writeln!(out, "expected instruction offset");
                }
            } else {
                let _ = writeln!(out, "usage: clear <offset>");
            }
        }
        "cl" => {
            if let Some(line) = parse_u32(parts.next()) {
                line_breakpoints.remove(&line);
                let _ = writeln!(out, "line breakpoint cleared at {line}");
            } else {
                let _ = writeln!(out, "usage: cl <line>");
            }
        }
        "breaks" => {
            let _ = writeln!(out, "breakpoints: {:?}", breakpoints);
            let _ = writeln!(out, "line breakpoints: {:?}", line_breakpoints);
        }
        "stack" => {
            let _ = writeln!(out, "stack: {:?}", vm.stack());
        }
        "locals" => {
            print_locals(vm, out);
        }
        "p" | "print" => {
            if let Some(name) = parts.next() {
                print_local_by_name(vm, name, out);
            } else {
                let _ = writeln!(out, "usage: print <local_name>");
            }
        }
        "ip" => {
            let _ = writeln!(out, "ip: {}", vm.ip());
        }
        "where" => {
            if let Some(info) = vm.debug_info() {
                let line = info.line_for_offset(vm.ip());
                if let Some(line) = line {
                    if let Some(text) = info.source_line(line) {
                        let _ = writeln!(out, "line {line}: {text}");
                    } else {
                        let _ = writeln!(out, "line: {line}");
                    }
                } else {
                    let _ = writeln!(out, "line: unknown");
                }
            } else {
                let _ = writeln!(out, "no debug info");
            }
        }
        "funcs" => {
            if let Some(info) = vm.debug_info() {
                for func in &info.functions {
                    let _ = writeln!(out, "fn {}({})", func.name, format_args_list(func));
                }
            } else {
                let _ = writeln!(out, "no debug info");
            }
        }
        "help" => {
            let _ = writeln!(
                out,
                "commands: break, break line, bl, clear, clear line, cl, breaks, continue, step, next, out, stack, locals, print, ip, where, funcs, help"
            );
        }
        _ => {
            let _ = writeln!(out, "unknown command");
        }
    }
    ReplAction::Continue
}

fn format_args_list(func: &crate::debug::DebugFunction) -> String {
    let mut parts = Vec::new();
    for arg in &func.args {
        parts.push(format!("{}:{}", arg.position, arg.name));
    }
    parts.join(", ")
}

fn print_locals(vm: &Vm, out: &mut dyn Write) {
    let Some(info) = vm.debug_info() else {
        let _ = writeln!(out, "locals: {:?}", vm.locals());
        return;
    };

    if info.locals.is_empty() {
        let _ = writeln!(out, "locals: {:?}", vm.locals());
        return;
    }

    for local in &info.locals {
        match vm.locals().get(local.index as usize) {
            Some(value) => {
                let _ = writeln!(out, "{} = {:?}", local.name, value);
            }
            None => {
                let _ = writeln!(out, "{} = <unavailable>", local.name);
            }
        }
    }
}

fn print_local_by_name(vm: &Vm, name: &str, out: &mut dyn Write) {
    let Some(info) = vm.debug_info() else {
        let _ = writeln!(out, "no debug info");
        return;
    };

    let Some(index) = info.local_index(name) else {
        let _ = writeln!(out, "unknown local '{name}'");
        return;
    };

    match vm.locals().get(index as usize) {
        Some(value) => {
            let _ = writeln!(out, "{name} = {:?}", value);
        }
        None => {
            let _ = writeln!(out, "local '{name}' is out of range for this VM instance");
        }
    }
}

pub fn attach_with_debugger(vm: &mut Vm, debugger: &mut Debugger) {
    debugger.on_instruction(vm);
}

pub fn debug_info_from_vm(vm: &Vm) -> Option<&DebugInfo> {
    vm.debug_info()
}

fn current_line(vm: &Vm) -> Option<u32> {
    vm.debug_info()
        .and_then(|info| info.line_for_offset(vm.ip()))
}

fn parse_u32(token: Option<&str>) -> Option<u32> {
    token.and_then(|value| value.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::debug::{DebugInfo, LocalInfo};
    use crate::vm::{Program, Value, Vm};

    use super::{ReplAction, StepMode, handle_command};

    fn vm_with_named_local(name: &str, value: Value) -> Vm {
        let program = Program::with_debug(
            vec![value],
            vec![
                crate::vm::OpCode::Ldc as u8,
                0,
                0,
                0,
                0,
                crate::vm::OpCode::Stloc as u8,
                0,
                crate::vm::OpCode::Ret as u8,
            ],
            Some(DebugInfo {
                source: None,
                lines: vec![],
                functions: vec![],
                locals: vec![LocalInfo {
                    name: name.to_string(),
                    index: 0,
                }],
            }),
        );
        let mut vm = Vm::with_locals(program, 1);
        let status = vm.run().expect("vm should run");
        assert_eq!(status, crate::vm::VmStatus::Halted);
        vm
    }

    #[test]
    fn print_local_by_name_uses_debug_name() {
        let vm = vm_with_named_local("counter", Value::Int(42));
        let mut out = Vec::<u8>::new();
        let mut breakpoints = HashSet::new();
        let mut line_breakpoints = HashSet::new();
        let mut step_mode = StepMode::Running;

        let action = handle_command(
            "print counter",
            &vm,
            &mut breakpoints,
            &mut line_breakpoints,
            &mut step_mode,
            &mut out,
        );
        assert_eq!(action, ReplAction::Continue);
        let text = String::from_utf8(out).expect("output should be utf-8");
        assert!(text.contains("counter = Int(42)"));
    }

    #[test]
    fn print_local_by_name_reports_unknown_local() {
        let vm = vm_with_named_local("counter", Value::Int(42));
        let mut out = Vec::<u8>::new();
        let mut breakpoints = HashSet::new();
        let mut line_breakpoints = HashSet::new();
        let mut step_mode = StepMode::Running;

        handle_command(
            "p missing",
            &vm,
            &mut breakpoints,
            &mut line_breakpoints,
            &mut step_mode,
            &mut out,
        );
        let text = String::from_utf8(out).expect("output should be utf-8");
        assert!(text.contains("unknown local 'missing'"));
    }
}
