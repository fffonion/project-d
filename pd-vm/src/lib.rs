mod builtins;

pub mod assembler;
pub mod compiler;
pub mod debug;
pub mod debugger;
pub mod jit;
pub mod vm;
pub mod wire;

pub use assembler::{AsmParseError, Assembler, AssemblerError, BytecodeBuilder, assemble};
pub use compiler::{
    CompileError, CompiledProgram, Compiler, Expr, FunctionDecl, ParseError, SourceError,
    SourceFlavor, SourcePathError, Stmt, compile_source, compile_source_file,
    compile_source_with_flavor,
};
pub use debug::{ArgInfo, DebugFunction, DebugInfo, LineInfo, LocalInfo};
pub use debugger::{
    DebugCommandBridge, DebugCommandBridgeError, DebugCommandBridgeResponse,
    DebugCommandBridgeStatus, Debugger, StepMode,
};
pub use jit::{
    JitAttempt, JitConfig, JitNyiDoc, JitNyiReason, JitSnapshot, JitTrace, JitTraceTerminal,
    TraceJitEngine,
};
pub use vm::{
    CallOutcome, HostBindingPlan, HostFunction, HostFunctionRegistry, HostImport, OpCode, Program,
    StaticHostFunction, Value, Vm, VmError, VmResult, VmStatus,
};
pub use wire::{
    DisassembleOptions, ValidationError, WireError, decode_program, disassemble_program,
    disassemble_program_with_options, disassemble_vmbc, disassemble_vmbc_with_options,
    encode_program, infer_local_count, validate_program,
};
