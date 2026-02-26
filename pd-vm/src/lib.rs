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
pub use debugger::{Debugger, StepMode};
pub use jit::{
    JitAttempt, JitConfig, JitNyiDoc, JitNyiReason, JitSnapshot, JitTrace, JitTraceTerminal,
    TraceJitEngine,
};
pub use vm::{CallOutcome, HostFunction, OpCode, Program, Value, Vm, VmError, VmResult, VmStatus};
pub use wire::{
    ValidationError, WireError, decode_program, encode_program, infer_local_count, validate_program,
};
