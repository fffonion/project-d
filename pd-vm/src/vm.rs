use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Bool(bool),
    String(String),
}

impl Value {
    fn as_int(&self) -> Result<i64, VmError> {
        match self {
            Value::Int(value) => Ok(*value),
            _ => Err(VmError::TypeMismatch("int")),
        }
    }

    fn as_bool(&self) -> Result<bool, VmError> {
        match self {
            Value::Bool(value) => Ok(*value),
            _ => Err(VmError::TypeMismatch("bool")),
        }
    }
}

#[derive(Debug)]
pub enum VmError {
    StackUnderflow,
    TypeMismatch(&'static str),
    DivisionByZero,
    InvalidShift(i64),
    InvalidConstant(u32),
    InvalidLocal(u8),
    InvalidCall(u16),
    InvalidOpcode(u8),
    BytecodeBounds,
    HostError(String),
    JitNative(String),
}

impl std::fmt::Display for VmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VmError::StackUnderflow => write!(f, "stack underflow"),
            VmError::TypeMismatch(expected) => write!(f, "type mismatch: expected {expected}"),
            VmError::DivisionByZero => write!(f, "division by zero"),
            VmError::InvalidShift(value) => {
                write!(f, "invalid shift amount {value}, expected 0..63")
            }
            VmError::InvalidConstant(index) => write!(f, "invalid constant {index}"),
            VmError::InvalidLocal(index) => write!(f, "invalid local {index}"),
            VmError::InvalidCall(index) => write!(f, "invalid call target {index}"),
            VmError::InvalidOpcode(opcode) => write!(f, "invalid opcode {opcode}"),
            VmError::BytecodeBounds => write!(f, "bytecode bounds"),
            VmError::HostError(message) => write!(f, "host error: {message}"),
            VmError::JitNative(message) => write!(f, "jit native error: {message}"),
        }
    }
}

impl std::error::Error for VmError {}

pub type VmResult<T> = Result<T, VmError>;

#[derive(Clone, Debug)]
pub struct Program {
    pub constants: Vec<Value>,
    pub code: Vec<u8>,
    pub debug: Option<crate::debug::DebugInfo>,
}

impl Program {
    pub fn new(constants: Vec<Value>, code: Vec<u8>) -> Self {
        Self {
            constants,
            code,
            debug: None,
        }
    }

    pub fn with_debug(
        constants: Vec<Value>,
        code: Vec<u8>,
        debug: Option<crate::debug::DebugInfo>,
    ) -> Self {
        Self {
            constants,
            code,
            debug,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    Nop = 0x00,
    Ret = 0x01,
    Ldc = 0x02,
    Add = 0x03,
    Sub = 0x04,
    Mul = 0x05,
    Div = 0x06,
    Neg = 0x07,
    Ceq = 0x08,
    Clt = 0x09,
    Cgt = 0x0A,
    Br = 0x0B,
    Brfalse = 0x0C,
    Pop = 0x0D,
    Dup = 0x0E,
    Ldloc = 0x0F,
    Stloc = 0x10,
    Call = 0x11,
    Shl = 0x12,
    Shr = 0x13,
}

impl OpCode {
    pub fn mnemonic(self) -> &'static str {
        match self {
            OpCode::Nop => "nop",
            OpCode::Ret => "ret",
            OpCode::Ldc => "ldc",
            OpCode::Add => "add",
            OpCode::Sub => "sub",
            OpCode::Mul => "mul",
            OpCode::Div => "div",
            OpCode::Neg => "neg",
            OpCode::Ceq => "ceq",
            OpCode::Clt => "clt",
            OpCode::Cgt => "cgt",
            OpCode::Br => "br",
            OpCode::Brfalse => "brfalse",
            OpCode::Pop => "pop",
            OpCode::Dup => "dup",
            OpCode::Ldloc => "ldloc",
            OpCode::Stloc => "stloc",
            OpCode::Call => "call",
            OpCode::Shl => "shl",
            OpCode::Shr => "shr",
        }
    }

    pub fn parse_mnemonic(op: &str) -> Option<Self> {
        match op {
            "nop" => Some(OpCode::Nop),
            "ret" => Some(OpCode::Ret),
            "ldc" => Some(OpCode::Ldc),
            "add" => Some(OpCode::Add),
            "sub" => Some(OpCode::Sub),
            "mul" => Some(OpCode::Mul),
            "div" => Some(OpCode::Div),
            "neg" => Some(OpCode::Neg),
            "ceq" => Some(OpCode::Ceq),
            "clt" => Some(OpCode::Clt),
            "cgt" => Some(OpCode::Cgt),
            "br" => Some(OpCode::Br),
            "brfalse" => Some(OpCode::Brfalse),
            "pop" => Some(OpCode::Pop),
            "dup" => Some(OpCode::Dup),
            "ldloc" => Some(OpCode::Ldloc),
            "stloc" => Some(OpCode::Stloc),
            "call" => Some(OpCode::Call),
            "shl" => Some(OpCode::Shl),
            "shr" => Some(OpCode::Shr),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum VmStatus {
    Halted,
    Yielded,
}

#[derive(Debug, PartialEq)]
pub enum CallOutcome {
    Return(Vec<Value>),
    Yield,
}

pub trait HostFunction {
    fn call(&mut self, vm: &mut Vm, args: &[Value]) -> VmResult<CallOutcome>;
}

pub struct Vm {
    program: Program,
    ip: usize,
    stack: Vec<Value>,
    locals: Vec<Value>,
    host_functions: Vec<Box<dyn HostFunction>>,
    call_depth: usize,
    jit: crate::jit::TraceJitEngine,
    native_traces: HashMap<usize, NativeTrace>,
    native_trace_exec_count: u64,
}

enum StepExecOutcome {
    Continue,
    Halted,
    Yielded,
}

enum TraceExecOutcome {
    Continue,
    Halted,
}

#[cfg(target_arch = "x86_64")]
type NativeTraceEntry = unsafe extern "C" fn(*mut Vm) -> i32;

#[cfg(not(target_arch = "x86_64"))]
type NativeTraceEntry = fn(*mut Vm) -> i32;

struct NativeTrace {
    _memory: ExecutableMemory,
    entry: NativeTraceEntry,
    code: Vec<u8>,
}

struct ExecutableMemory {
    ptr: *mut u8,
    len: usize,
}

impl Vm {
    pub fn new(program: Program) -> Self {
        Self {
            program,
            ip: 0,
            stack: Vec::new(),
            locals: Vec::new(),
            host_functions: Vec::new(),
            call_depth: 0,
            jit: crate::jit::TraceJitEngine::default(),
            native_traces: HashMap::new(),
            native_trace_exec_count: 0,
        }
    }

    pub fn with_locals(program: Program, local_count: usize) -> Self {
        Self {
            program,
            ip: 0,
            stack: Vec::new(),
            locals: vec![Value::Int(0); local_count],
            host_functions: Vec::new(),
            call_depth: 0,
            jit: crate::jit::TraceJitEngine::default(),
            native_traces: HashMap::new(),
            native_trace_exec_count: 0,
        }
    }

    pub fn register_function(&mut self, function: Box<dyn HostFunction>) -> u16 {
        let index = self.host_functions.len() as u16;
        self.host_functions.push(function);
        index
    }

    pub fn run(&mut self) -> VmResult<VmStatus> {
        self.run_internal(None, true)
    }

    pub fn run_with_debugger(
        &mut self,
        debugger: &mut crate::debugger::Debugger,
    ) -> VmResult<VmStatus> {
        self.run_internal(Some(debugger), false)
    }

    pub fn set_jit_config(&mut self, config: crate::jit::JitConfig) {
        self.jit.set_config(config);
    }

    pub fn jit_config(&self) -> &crate::jit::JitConfig {
        self.jit.config()
    }

    pub fn jit_snapshot(&self) -> crate::jit::JitSnapshot {
        self.jit.snapshot()
    }

    pub fn dump_jit_info(&self) -> String {
        let mut out = self.jit.dump_text(self.program.debug.as_ref());
        out.push_str(&format!(
            "  native trace executions: {}\n",
            self.native_trace_exec_count
        ));
        if self.native_traces.is_empty() {
            out.push_str("  native traces: 0\n");
            return out;
        }

        out.push_str(&format!("  native traces: {}\n", self.native_traces.len()));
        let mut ids: Vec<usize> = self.native_traces.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            if let Some(native) = self.native_traces.get(&id) {
                out.push_str(&format!(
                    "  native trace#{} entry=0x{:X} code_bytes={}\n",
                    id,
                    native.entry as usize,
                    native.code.len()
                ));
                out.push_str("    code:");
                for byte in &native.code {
                    out.push_str(&format!(" {:02X}", byte));
                }
                out.push('\n');
            }
        }
        out
    }

    fn run_internal(
        &mut self,
        mut debugger: Option<&mut crate::debugger::Debugger>,
        allow_jit: bool,
    ) -> VmResult<VmStatus> {
        loop {
            if let Some(active_debugger) = debugger.as_deref_mut() {
                active_debugger.on_instruction(self);
            }

            if allow_jit {
                let trace_id = {
                    let program = &self.program;
                    self.jit.observe_hot_ip(self.ip, program)
                };
                if let Some(trace_id) = trace_id {
                    match self.execute_jit_entry(trace_id)? {
                        TraceExecOutcome::Continue => continue,
                        TraceExecOutcome::Halted => return Ok(VmStatus::Halted),
                    }
                }
            }

            if self.ip >= self.program.code.len() {
                return Err(VmError::BytecodeBounds);
            }

            let opcode = self.read_u8()?;
            match self.execute_interpreter_instruction(opcode)? {
                StepExecOutcome::Continue => {}
                StepExecOutcome::Halted => return Ok(VmStatus::Halted),
                StepExecOutcome::Yielded => return Ok(VmStatus::Yielded),
            }
        }
    }

    fn execute_interpreter_instruction(&mut self, opcode: u8) -> VmResult<StepExecOutcome> {
        match opcode {
            x if x == OpCode::Nop as u8 => {}
            x if x == OpCode::Ret as u8 => return Ok(StepExecOutcome::Halted),
            x if x == OpCode::Ldc as u8 => {
                let index = self.read_u32()?;
                let value = self
                    .program
                    .constants
                    .get(index as usize)
                    .cloned()
                    .ok_or(VmError::InvalidConstant(index))?;
                self.stack.push(value);
            }
            x if x == OpCode::Add as u8 => {
                let rhs = self.pop_int()?;
                let lhs = self.pop_int()?;
                self.stack.push(Value::Int(lhs + rhs));
            }
            x if x == OpCode::Sub as u8 => {
                let rhs = self.pop_int()?;
                let lhs = self.pop_int()?;
                self.stack.push(Value::Int(lhs - rhs));
            }
            x if x == OpCode::Mul as u8 => {
                let rhs = self.pop_int()?;
                let lhs = self.pop_int()?;
                self.stack.push(Value::Int(lhs * rhs));
            }
            x if x == OpCode::Div as u8 => {
                let rhs = self.pop_int()?;
                let lhs = self.pop_int()?;
                if rhs == 0 {
                    return Err(VmError::DivisionByZero);
                }
                self.stack.push(Value::Int(lhs / rhs));
            }
            x if x == OpCode::Shl as u8 => {
                let rhs = self.pop_shift_amount()?;
                let lhs = self.pop_int()?;
                self.stack.push(Value::Int(lhs << rhs));
            }
            x if x == OpCode::Shr as u8 => {
                let rhs = self.pop_shift_amount()?;
                let lhs = self.pop_int()?;
                self.stack.push(Value::Int(lhs >> rhs));
            }
            x if x == OpCode::Neg as u8 => {
                let value = self.pop_int()?;
                self.stack.push(Value::Int(-value));
            }
            x if x == OpCode::Ceq as u8 => {
                let rhs = self.pop_value()?;
                let lhs = self.pop_value()?;
                self.stack.push(Value::Bool(lhs == rhs));
            }
            x if x == OpCode::Clt as u8 => {
                let rhs = self.pop_int()?;
                let lhs = self.pop_int()?;
                self.stack.push(Value::Bool(lhs < rhs));
            }
            x if x == OpCode::Cgt as u8 => {
                let rhs = self.pop_int()?;
                let lhs = self.pop_int()?;
                self.stack.push(Value::Bool(lhs > rhs));
            }
            x if x == OpCode::Br as u8 => {
                let target = self.read_u32()? as usize;
                self.jump_to(target)?;
            }
            x if x == OpCode::Brfalse as u8 => {
                let target = self.read_u32()? as usize;
                let condition = self.pop_bool()?;
                if !condition {
                    self.jump_to(target)?;
                }
            }
            x if x == OpCode::Pop as u8 => {
                self.pop_value()?;
            }
            x if x == OpCode::Dup as u8 => {
                let value = self.peek_value()?.clone();
                self.stack.push(value);
            }
            x if x == OpCode::Ldloc as u8 => {
                let index = self.read_u8()?;
                let value = self
                    .locals
                    .get(index as usize)
                    .cloned()
                    .ok_or(VmError::InvalidLocal(index))?;
                self.stack.push(value);
            }
            x if x == OpCode::Stloc as u8 => {
                let index = self.read_u8()?;
                let value = self.pop_value()?;
                let slot = self
                    .locals
                    .get_mut(index as usize)
                    .ok_or(VmError::InvalidLocal(index))?;
                *slot = value;
            }
            x if x == OpCode::Call as u8 => {
                let call_ip = self.ip - 1;
                let index = self.read_u16()?;
                let argc = self.read_u8()? as usize;
                let mut args = Vec::with_capacity(argc);
                for _ in 0..argc {
                    args.push(self.pop_value()?);
                }
                args.reverse();

                self.call_depth += 1;

                let function_ptr = self
                    .host_functions
                    .get_mut(index as usize)
                    .ok_or(VmError::InvalidCall(index))?
                    as *mut Box<dyn HostFunction>;

                let outcome = unsafe { (*function_ptr).call(self, &args)? };
                self.call_depth = self.call_depth.saturating_sub(1);

                match outcome {
                    CallOutcome::Return(values) => {
                        for value in values {
                            self.stack.push(value);
                        }
                    }
                    CallOutcome::Yield => {
                        for value in args {
                            self.stack.push(value);
                        }
                        self.ip = call_ip;
                        return Ok(StepExecOutcome::Yielded);
                    }
                }
            }
            other => return Err(VmError::InvalidOpcode(other)),
        }
        Ok(StepExecOutcome::Continue)
    }

    fn execute_jit_trace(&mut self, trace_id: usize) -> VmResult<TraceExecOutcome> {
        let Some(trace) = self.jit.trace_clone(trace_id) else {
            return Ok(TraceExecOutcome::Continue);
        };
        for step in &trace.steps {
            match step {
                crate::jit::TraceStep::Nop => {}
                crate::jit::TraceStep::Ldc(index) => {
                    let value = self
                        .program
                        .constants
                        .get(*index as usize)
                        .cloned()
                        .ok_or(VmError::InvalidConstant(*index))?;
                    self.stack.push(value);
                }
                crate::jit::TraceStep::Add => {
                    let rhs = self.pop_int()?;
                    let lhs = self.pop_int()?;
                    self.stack.push(Value::Int(lhs + rhs));
                }
                crate::jit::TraceStep::Sub => {
                    let rhs = self.pop_int()?;
                    let lhs = self.pop_int()?;
                    self.stack.push(Value::Int(lhs - rhs));
                }
                crate::jit::TraceStep::Mul => {
                    let rhs = self.pop_int()?;
                    let lhs = self.pop_int()?;
                    self.stack.push(Value::Int(lhs * rhs));
                }
                crate::jit::TraceStep::Div => {
                    let rhs = self.pop_int()?;
                    let lhs = self.pop_int()?;
                    if rhs == 0 {
                        return Err(VmError::DivisionByZero);
                    }
                    self.stack.push(Value::Int(lhs / rhs));
                }
                crate::jit::TraceStep::Shl => {
                    let rhs = self.pop_shift_amount()?;
                    let lhs = self.pop_int()?;
                    self.stack.push(Value::Int(lhs << rhs));
                }
                crate::jit::TraceStep::Shr => {
                    let rhs = self.pop_shift_amount()?;
                    let lhs = self.pop_int()?;
                    self.stack.push(Value::Int(lhs >> rhs));
                }
                crate::jit::TraceStep::Neg => {
                    let value = self.pop_int()?;
                    self.stack.push(Value::Int(-value));
                }
                crate::jit::TraceStep::Ceq => {
                    let rhs = self.pop_value()?;
                    let lhs = self.pop_value()?;
                    self.stack.push(Value::Bool(lhs == rhs));
                }
                crate::jit::TraceStep::Clt => {
                    let rhs = self.pop_int()?;
                    let lhs = self.pop_int()?;
                    self.stack.push(Value::Bool(lhs < rhs));
                }
                crate::jit::TraceStep::Cgt => {
                    let rhs = self.pop_int()?;
                    let lhs = self.pop_int()?;
                    self.stack.push(Value::Bool(lhs > rhs));
                }
                crate::jit::TraceStep::Pop => {
                    self.pop_value()?;
                }
                crate::jit::TraceStep::Dup => {
                    let value = self.peek_value()?.clone();
                    self.stack.push(value);
                }
                crate::jit::TraceStep::Ldloc(index) => {
                    let value = self
                        .locals
                        .get(*index as usize)
                        .cloned()
                        .ok_or(VmError::InvalidLocal(*index))?;
                    self.stack.push(value);
                }
                crate::jit::TraceStep::Stloc(index) => {
                    let value = self.pop_value()?;
                    let slot = self
                        .locals
                        .get_mut(*index as usize)
                        .ok_or(VmError::InvalidLocal(*index))?;
                    *slot = value;
                }
                crate::jit::TraceStep::GuardFalse { exit_ip } => {
                    let condition = self.pop_bool()?;
                    if !condition {
                        self.jump_to(*exit_ip)?;
                        self.jit.mark_trace_executed(trace_id);
                        return Ok(TraceExecOutcome::Continue);
                    }
                }
                crate::jit::TraceStep::JumpToRoot => {
                    self.jump_to(trace.root_ip)?;
                    self.jit.mark_trace_executed(trace_id);
                    return Ok(TraceExecOutcome::Continue);
                }
                crate::jit::TraceStep::Ret => {
                    self.jit.mark_trace_executed(trace_id);
                    return Ok(TraceExecOutcome::Halted);
                }
            }
        }
        self.jit.mark_trace_executed(trace_id);
        Ok(TraceExecOutcome::Continue)
    }

    fn execute_jit_entry(&mut self, trace_id: usize) -> VmResult<TraceExecOutcome> {
        #[cfg(target_arch = "x86_64")]
        {
            self.execute_jit_native(trace_id)
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            self.execute_jit_trace(trace_id)
        }
    }

    #[cfg(target_arch = "x86_64")]
    fn execute_jit_native(&mut self, trace_id: usize) -> VmResult<TraceExecOutcome> {
        self.ensure_native_trace(trace_id)?;
        let entry = {
            let native = self.native_traces.get(&trace_id).ok_or_else(|| {
                VmError::JitNative(format!("native trace entry for id {} missing", trace_id))
            })?;
            native.entry
        };

        clear_jit_bridge_error();
        let status = unsafe { entry(self as *mut Vm) };
        self.native_trace_exec_count = self.native_trace_exec_count.saturating_add(1);
        match status {
            0 => Ok(TraceExecOutcome::Continue),
            1 => Ok(TraceExecOutcome::Halted),
            -1 => {
                let err = take_jit_bridge_error().unwrap_or_else(|| {
                    VmError::JitNative("jit bridge reported failure without VmError".to_string())
                });
                Err(err)
            }
            other => Err(VmError::JitNative(format!(
                "unexpected native trace return status {}",
                other
            ))),
        }
    }

    #[cfg(target_arch = "x86_64")]
    fn ensure_native_trace(&mut self, trace_id: usize) -> VmResult<()> {
        if self.native_traces.contains_key(&trace_id) {
            return Ok(());
        }

        let code = emit_trace_stub_bytes(trace_id);
        let memory = ExecutableMemory::from_code(&code)?;
        let entry = unsafe { std::mem::transmute::<*const u8, NativeTraceEntry>(memory.ptr) };
        self.native_traces.insert(
            trace_id,
            NativeTrace {
                _memory: memory,
                entry,
                code,
            },
        );
        Ok(())
    }

    pub fn resume(&mut self) -> VmResult<VmStatus> {
        self.run()
    }

    pub fn stack(&self) -> &[Value] {
        &self.stack
    }

    pub fn locals(&self) -> &[Value] {
        &self.locals
    }

    pub fn ip(&self) -> usize {
        self.ip
    }

    pub fn debug_info(&self) -> Option<&crate::debug::DebugInfo> {
        self.program.debug.as_ref()
    }

    pub fn call_depth(&self) -> usize {
        self.call_depth
    }

    pub fn jit_native_trace_count(&self) -> usize {
        self.native_traces.len()
    }

    pub fn jit_native_exec_count(&self) -> u64 {
        self.native_trace_exec_count
    }

    fn pop_value(&mut self) -> VmResult<Value> {
        self.stack.pop().ok_or(VmError::StackUnderflow)
    }

    fn peek_value(&self) -> VmResult<&Value> {
        self.stack.last().ok_or(VmError::StackUnderflow)
    }

    fn pop_int(&mut self) -> VmResult<i64> {
        self.pop_value()?.as_int()
    }

    fn pop_bool(&mut self) -> VmResult<bool> {
        self.pop_value()?.as_bool()
    }

    fn pop_shift_amount(&mut self) -> VmResult<u32> {
        let value = self.pop_int()?;
        if !(0..=63).contains(&value) {
            return Err(VmError::InvalidShift(value));
        }
        Ok(value as u32)
    }

    fn read_u8(&mut self) -> VmResult<u8> {
        if self.ip >= self.program.code.len() {
            return Err(VmError::BytecodeBounds);
        }
        let value = self.program.code[self.ip];
        self.ip += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> VmResult<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> VmResult<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_bytes(&mut self, count: usize) -> VmResult<[u8; 4]> {
        if self.ip + count > self.program.code.len() {
            return Err(VmError::BytecodeBounds);
        }
        let mut buf = [0u8; 4];
        buf[..count].copy_from_slice(&self.program.code[self.ip..self.ip + count]);
        self.ip += count;
        Ok(buf)
    }

    fn jump_to(&mut self, target: usize) -> VmResult<()> {
        if target >= self.program.code.len() {
            return Err(VmError::BytecodeBounds);
        }
        self.ip = target;
        Ok(())
    }
}

#[cfg(target_arch = "x86_64")]
fn emit_trace_stub_bytes(trace_id: usize) -> Vec<u8> {
    let mut code = Vec::with_capacity(24);

    #[cfg(all(target_arch = "x86_64", target_os = "windows"))]
    {
        // mov rdx, imm64 ; second arg = trace_id on Win64
        code.push(0x48);
        code.push(0xBA);
        code.extend_from_slice(&(trace_id as u64).to_le_bytes());
    }

    #[cfg(all(target_arch = "x86_64", not(target_os = "windows")))]
    {
        // mov rsi, imm64 ; second arg = trace_id on SysV x86_64
        code.push(0x48);
        code.push(0xBE);
        code.extend_from_slice(&(trace_id as u64).to_le_bytes());
    }

    // mov rax, imm64 ; helper address
    code.push(0x48);
    code.push(0xB8);
    code.extend_from_slice(&(jit_bridge_entry as *const () as usize as u64).to_le_bytes());

    // jmp rax ; tail-call helper (keeps caller's ABI stack shape)
    code.push(0xFF);
    code.push(0xE0);
    code
}

#[cfg(target_arch = "x86_64")]
thread_local! {
    static JIT_BRIDGE_ERROR: std::cell::RefCell<Option<VmError>> = const { std::cell::RefCell::new(None) };
}

#[cfg(target_arch = "x86_64")]
fn clear_jit_bridge_error() {
    JIT_BRIDGE_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[cfg(target_arch = "x86_64")]
fn set_jit_bridge_error(error: VmError) {
    JIT_BRIDGE_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(error);
    });
}

#[cfg(target_arch = "x86_64")]
fn take_jit_bridge_error() -> Option<VmError> {
    JIT_BRIDGE_ERROR.with(|slot| slot.borrow_mut().take())
}

#[cfg(target_arch = "x86_64")]
extern "C" fn jit_bridge_entry(vm_ptr: *mut Vm, trace_id: usize) -> i32 {
    if vm_ptr.is_null() {
        set_jit_bridge_error(VmError::JitNative(
            "native bridge received null vm pointer".to_string(),
        ));
        return -1;
    }

    let vm = unsafe { &mut *vm_ptr };
    match vm.execute_jit_trace(trace_id) {
        Ok(TraceExecOutcome::Continue) => 0,
        Ok(TraceExecOutcome::Halted) => 1,
        Err(err) => {
            set_jit_bridge_error(err);
            -1
        }
    }
}

impl ExecutableMemory {
    fn from_code(code: &[u8]) -> VmResult<Self> {
        let len = code.len();
        if len == 0 {
            return Err(VmError::JitNative(
                "cannot create executable region for empty code".to_string(),
            ));
        }
        let ptr = alloc_executable_region(len)?;
        unsafe {
            std::ptr::copy_nonoverlapping(code.as_ptr(), ptr, len);
        }
        Ok(Self { ptr, len })
    }
}

impl Drop for ExecutableMemory {
    fn drop(&mut self) {
        let _ = free_executable_region(self.ptr, self.len);
    }
}

#[cfg(target_os = "windows")]
fn alloc_executable_region(len: usize) -> VmResult<*mut u8> {
    use windows_sys::Win32::System::Memory::{
        MEM_COMMIT, MEM_RESERVE, PAGE_EXECUTE_READWRITE, VirtualAlloc,
    };

    let ptr = unsafe {
        VirtualAlloc(
            std::ptr::null_mut(),
            len,
            MEM_COMMIT | MEM_RESERVE,
            PAGE_EXECUTE_READWRITE,
        ) as *mut u8
    };
    if ptr.is_null() {
        return Err(VmError::JitNative(format!(
            "VirtualAlloc failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(ptr)
}

#[cfg(target_os = "windows")]
fn free_executable_region(ptr: *mut u8, _len: usize) -> VmResult<()> {
    use windows_sys::Win32::System::Memory::{MEM_RELEASE, VirtualFree};

    if ptr.is_null() {
        return Ok(());
    }
    let ok = unsafe { VirtualFree(ptr as *mut _, 0, MEM_RELEASE) };
    if ok == 0 {
        return Err(VmError::JitNative(format!(
            "VirtualFree failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn alloc_executable_region(len: usize) -> VmResult<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE | libc::PROT_EXEC,
            libc::MAP_ANON | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(VmError::JitNative(format!(
            "mmap failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(ptr as *mut u8)
}

#[cfg(target_os = "macos")]
fn alloc_executable_region(len: usize) -> VmResult<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE | libc::PROT_EXEC,
            libc::MAP_ANON | libc::MAP_PRIVATE | libc::MAP_JIT,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(VmError::JitNative(format!(
            "mmap(MAP_JIT) failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(ptr as *mut u8)
}

#[cfg(unix)]
fn free_executable_region(ptr: *mut u8, len: usize) -> VmResult<()> {
    if ptr.is_null() {
        return Ok(());
    }
    let rc = unsafe { libc::munmap(ptr as *mut _, len) };
    if rc != 0 {
        return Err(VmError::JitNative(format!(
            "munmap failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(not(any(unix, target_os = "windows")))]
fn alloc_executable_region(_len: usize) -> VmResult<*mut u8> {
    Err(VmError::JitNative(
        "executable memory allocation not implemented for this platform".to_string(),
    ))
}

#[cfg(not(any(unix, target_os = "windows")))]
fn free_executable_region(_ptr: *mut u8, _len: usize) -> VmResult<()> {
    Ok(())
}
