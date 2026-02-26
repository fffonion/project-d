use std::collections::HashSet;

use crate::debug::{ArgInfo, DebugFunction, DebugInfo, LineInfo, LocalInfo};
use crate::vm::{HostImport, OpCode, Program, Value};

const MAGIC: [u8; 4] = *b"VMBC";
const VERSION_V1: u16 = 1;
const VERSION_V2: u16 = 2;
const VERSION_V3: u16 = 3;
const VERSION_V4: u16 = 4;
const ENCODE_VERSION: u16 = VERSION_V4;
const FLAGS: u16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    UnexpectedEof,
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u16),
    UnsupportedFlags(u16),
    InvalidConstantTag(u8),
    InvalidBool(u8),
    InvalidDebugFlag(u8),
    InvalidUtf8,
    StringTooLong(usize),
    CodeTooLong(usize),
    LengthTooLarge(&'static str, usize),
    TrailingBytes,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::UnexpectedEof => write!(f, "unexpected end of input"),
            WireError::InvalidMagic(found) => write!(f, "invalid magic: {found:?}"),
            WireError::UnsupportedVersion(version) => {
                write!(f, "unsupported version: {version}")
            }
            WireError::UnsupportedFlags(flags) => write!(f, "unsupported flags: {flags}"),
            WireError::InvalidConstantTag(tag) => write!(f, "invalid constant tag: {tag}"),
            WireError::InvalidBool(value) => write!(f, "invalid bool value: {value}"),
            WireError::InvalidDebugFlag(value) => write!(f, "invalid debug flag: {value}"),
            WireError::InvalidUtf8 => write!(f, "invalid utf-8 string"),
            WireError::StringTooLong(len) => write!(f, "string too long: {len}"),
            WireError::CodeTooLong(len) => write!(f, "code too long: {len}"),
            WireError::LengthTooLarge(field, len) => {
                write!(f, "{field} length too large: {len}")
            }
            WireError::TrailingBytes => write!(f, "trailing bytes after program payload"),
        }
    }
}

impl std::error::Error for WireError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    TruncatedOperand {
        offset: usize,
        opcode: u8,
        expected_bytes: usize,
    },
    InvalidOpcode {
        offset: usize,
        opcode: u8,
    },
    InvalidConstant {
        offset: usize,
        index: u32,
    },
    InvalidCall {
        offset: usize,
        index: u16,
    },
    InvalidCallArity {
        offset: usize,
        index: u16,
        expected: u8,
        got: u8,
    },
    InvalidJumpTarget {
        offset: usize,
        target: u32,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::TruncatedOperand {
                offset,
                opcode,
                expected_bytes,
            } => write!(
                f,
                "truncated operand at offset {offset} for opcode {opcode:#04x}, expected {expected_bytes} bytes",
            ),
            ValidationError::InvalidOpcode { offset, opcode } => {
                write!(f, "invalid opcode {opcode:#04x} at offset {offset}")
            }
            ValidationError::InvalidConstant { offset, index } => write!(
                f,
                "invalid constant index {index} for ldc instruction at offset {offset}",
            ),
            ValidationError::InvalidCall { offset, index } => {
                write!(f, "invalid call index {index} at offset {offset}")
            }
            ValidationError::InvalidCallArity {
                offset,
                index,
                expected,
                got,
            } => write!(
                f,
                "invalid call arity {got} for import index {index} at offset {offset}, expected {expected}",
            ),
            ValidationError::InvalidJumpTarget { offset, target } => write!(
                f,
                "invalid jump target {target} referenced by instruction at offset {offset}",
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

pub fn encode_program(program: &Program) -> Result<Vec<u8>, WireError> {
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&ENCODE_VERSION.to_le_bytes());
    out.extend_from_slice(&FLAGS.to_le_bytes());
    write_u32_count("constants", program.constants.len(), &mut out)?;

    for constant in &program.constants {
        match constant {
            Value::Int(value) => {
                out.push(0);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Value::Float(value) => {
                out.push(3);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Value::Bool(value) => {
                out.push(1);
                out.push(u8::from(*value));
            }
            Value::String(value) => {
                out.push(2);
                write_u32_len("constant string", value.len(), &mut out)?;
                out.extend_from_slice(value.as_bytes());
            }
        }
    }

    write_u32_len("code", program.code.len(), &mut out)?;
    out.extend_from_slice(&program.code);

    if ENCODE_VERSION >= VERSION_V4 {
        write_u32_count("imports", program.imports.len(), &mut out)?;
        for import in &program.imports {
            write_string("import name", &import.name, &mut out)?;
            out.push(import.arity);
        }
    }

    if ENCODE_VERSION >= VERSION_V2 {
        write_debug_info(&mut out, program.debug.as_ref())?;
    }

    Ok(out)
}

pub fn decode_program(bytes: &[u8]) -> Result<Program, WireError> {
    let mut cursor = Cursor::new(bytes);

    let magic = cursor.read_exact_array::<4>()?;
    if magic != MAGIC {
        return Err(WireError::InvalidMagic(magic));
    }

    let version = cursor.read_u16()?;
    if version != VERSION_V1
        && version != VERSION_V2
        && version != VERSION_V3
        && version != VERSION_V4
    {
        return Err(WireError::UnsupportedVersion(version));
    }

    let flags = cursor.read_u16()?;
    if flags != FLAGS {
        return Err(WireError::UnsupportedFlags(flags));
    }

    let constant_count = cursor.read_u32()? as usize;
    let mut constants = Vec::with_capacity(constant_count);
    for _ in 0..constant_count {
        let tag = cursor.read_u8()?;
        let value = match tag {
            0 => Value::Int(cursor.read_i64()?),
            3 => Value::Float(cursor.read_f64()?),
            1 => {
                let raw = cursor.read_u8()?;
                match raw {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    other => return Err(WireError::InvalidBool(other)),
                }
            }
            2 => {
                let len = cursor.read_u32()? as usize;
                let text_bytes = cursor.read_exact(len)?;
                let text =
                    String::from_utf8(text_bytes.to_vec()).map_err(|_| WireError::InvalidUtf8)?;
                Value::String(text)
            }
            other => return Err(WireError::InvalidConstantTag(other)),
        };
        constants.push(value);
    }

    let code_len = cursor.read_u32()? as usize;
    let code = cursor.read_exact(code_len)?.to_vec();
    let imports = if version >= VERSION_V4 {
        let import_count = cursor.read_u32()? as usize;
        let mut imports = Vec::with_capacity(import_count);
        for _ in 0..import_count {
            imports.push(HostImport {
                name: cursor.read_string()?,
                arity: cursor.read_u8()?,
            });
        }
        imports
    } else {
        Vec::new()
    };
    let debug = if version >= VERSION_V2 {
        read_debug_info(&mut cursor, version)?
    } else {
        None
    };

    if !cursor.is_eof() {
        return Err(WireError::TrailingBytes);
    }

    Ok(Program::with_imports_and_debug(
        constants, code, imports, debug,
    ))
}

pub fn validate_program(program: &Program, host_fn_count: u16) -> Result<(), ValidationError> {
    analyze_program(program, Some(host_fn_count)).map(|_| ())
}

pub fn infer_local_count(program: &Program) -> Result<usize, ValidationError> {
    let analysis = analyze_program(program, None)?;
    Ok(match analysis.max_local_index {
        Some(index) => index as usize + 1,
        None => 0,
    })
}

struct ProgramAnalysis {
    max_local_index: Option<u8>,
}

fn analyze_program(
    program: &Program,
    host_fn_count: Option<u16>,
) -> Result<ProgramAnalysis, ValidationError> {
    let mut ip = 0usize;
    let mut instruction_starts = HashSet::new();
    let mut jump_targets: Vec<(usize, u32)> = Vec::new();
    let mut max_local_index: Option<u8> = None;
    let code = &program.code;

    while ip < code.len() {
        let start = ip;
        instruction_starts.insert(start);
        let opcode = code[ip];
        ip += 1;

        match opcode {
            x if x == OpCode::Nop as u8 || x == OpCode::Ret as u8 => {}
            x if x == OpCode::Ldc as u8 => {
                let index = read_u32(code, &mut ip).ok_or(ValidationError::TruncatedOperand {
                    offset: start,
                    opcode,
                    expected_bytes: 4,
                })?;
                if index as usize >= program.constants.len() {
                    return Err(ValidationError::InvalidConstant {
                        offset: start,
                        index,
                    });
                }
            }
            x if x == OpCode::Add as u8
                || x == OpCode::Sub as u8
                || x == OpCode::Mul as u8
                || x == OpCode::Div as u8
                || x == OpCode::Shl as u8
                || x == OpCode::Shr as u8
                || x == OpCode::Neg as u8
                || x == OpCode::Ceq as u8
                || x == OpCode::Clt as u8
                || x == OpCode::Cgt as u8
                || x == OpCode::Pop as u8
                || x == OpCode::Dup as u8 => {}
            x if x == OpCode::Br as u8 || x == OpCode::Brfalse as u8 => {
                let target = read_u32(code, &mut ip).ok_or(ValidationError::TruncatedOperand {
                    offset: start,
                    opcode,
                    expected_bytes: 4,
                })?;
                jump_targets.push((start, target));
            }
            x if x == OpCode::Ldloc as u8 || x == OpCode::Stloc as u8 => {
                let index = read_u8(code, &mut ip).ok_or(ValidationError::TruncatedOperand {
                    offset: start,
                    opcode,
                    expected_bytes: 1,
                })?;
                max_local_index = Some(max_local_index.map_or(index, |prev| prev.max(index)));
            }
            x if x == OpCode::Call as u8 => {
                let index = read_u16(code, &mut ip).ok_or(ValidationError::TruncatedOperand {
                    offset: start,
                    opcode,
                    expected_bytes: 3,
                })?;
                let argc = read_u8(code, &mut ip).ok_or(ValidationError::TruncatedOperand {
                    offset: start,
                    opcode,
                    expected_bytes: 3,
                })?;
                if program.imports.is_empty() {
                    if let Some(host_fn_count) = host_fn_count
                        && index >= host_fn_count
                    {
                        return Err(ValidationError::InvalidCall {
                            offset: start,
                            index,
                        });
                    }
                } else {
                    let Some(import) = program.imports.get(index as usize) else {
                        return Err(ValidationError::InvalidCall {
                            offset: start,
                            index,
                        });
                    };
                    if argc != import.arity {
                        return Err(ValidationError::InvalidCallArity {
                            offset: start,
                            index,
                            expected: import.arity,
                            got: argc,
                        });
                    }
                }
            }
            other => {
                return Err(ValidationError::InvalidOpcode {
                    offset: start,
                    opcode: other,
                });
            }
        }
    }

    for (offset, target) in &jump_targets {
        let target = *target as usize;
        if target >= code.len() || !instruction_starts.contains(&target) {
            return Err(ValidationError::InvalidJumpTarget {
                offset: *offset,
                target: target as u32,
            });
        }
    }

    Ok(ProgramAnalysis { max_local_index })
}

fn write_debug_info(out: &mut Vec<u8>, debug: Option<&DebugInfo>) -> Result<(), WireError> {
    match debug {
        None => {
            out.push(0);
            Ok(())
        }
        Some(debug) => {
            out.push(1);

            match &debug.source {
                None => out.push(0),
                Some(source) => {
                    out.push(1);
                    write_string("debug source", source, out)?;
                }
            }

            write_u32_count("debug lines", debug.lines.len(), out)?;
            for line in &debug.lines {
                out.extend_from_slice(&line.offset.to_le_bytes());
                out.extend_from_slice(&line.line.to_le_bytes());
            }

            write_u32_count("debug functions", debug.functions.len(), out)?;
            for function in &debug.functions {
                write_string("debug function name", &function.name, out)?;
                write_u32_count("debug function args", function.args.len(), out)?;
                for arg in &function.args {
                    write_string("debug arg name", &arg.name, out)?;
                    out.push(arg.position);
                }
            }

            write_u32_count("debug locals", debug.locals.len(), out)?;
            for local in &debug.locals {
                write_string("debug local name", &local.name, out)?;
                out.push(local.index);
            }

            Ok(())
        }
    }
}

fn read_debug_info(cursor: &mut Cursor<'_>, version: u16) -> Result<Option<DebugInfo>, WireError> {
    let flag = cursor.read_u8()?;
    match flag {
        0 => Ok(None),
        1 => {
            let source = match cursor.read_u8()? {
                0 => None,
                1 => Some(cursor.read_string()?),
                other => return Err(WireError::InvalidDebugFlag(other)),
            };

            let line_count = cursor.read_u32()? as usize;
            let mut lines = Vec::with_capacity(line_count);
            for _ in 0..line_count {
                lines.push(LineInfo {
                    offset: cursor.read_u32()?,
                    line: cursor.read_u32()?,
                });
            }

            let function_count = cursor.read_u32()? as usize;
            let mut functions = Vec::with_capacity(function_count);
            for _ in 0..function_count {
                let name = cursor.read_string()?;
                let arg_count = cursor.read_u32()? as usize;
                let mut args = Vec::with_capacity(arg_count);
                for _ in 0..arg_count {
                    args.push(ArgInfo {
                        name: cursor.read_string()?,
                        position: cursor.read_u8()?,
                    });
                }
                functions.push(DebugFunction { name, args });
            }

            let locals = if version >= VERSION_V3 {
                let local_count = cursor.read_u32()? as usize;
                let mut locals = Vec::with_capacity(local_count);
                for _ in 0..local_count {
                    locals.push(LocalInfo {
                        name: cursor.read_string()?,
                        index: cursor.read_u8()?,
                    });
                }
                locals
            } else {
                Vec::new()
            };

            Ok(Some(DebugInfo {
                source,
                lines,
                functions,
                locals,
            }))
        }
        other => Err(WireError::InvalidDebugFlag(other)),
    }
}

fn write_string(field: &'static str, value: &str, out: &mut Vec<u8>) -> Result<(), WireError> {
    write_u32_len(field, value.len(), out)?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_u32_len(field: &'static str, len: usize, out: &mut Vec<u8>) -> Result<(), WireError> {
    let len_u32 = u32::try_from(len).map_err(|_| WireError::LengthTooLarge(field, len))?;
    out.extend_from_slice(&len_u32.to_le_bytes());
    Ok(())
}

fn write_u32_count(field: &'static str, count: usize, out: &mut Vec<u8>) -> Result<(), WireError> {
    write_u32_len(field, count, out)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, WireError> {
        let value = self
            .bytes
            .get(self.offset)
            .ok_or(WireError::UnexpectedEof)?;
        self.offset += 1;
        Ok(*value)
    }

    fn read_u16(&mut self) -> Result<u16, WireError> {
        let bytes = self.read_exact_array::<2>()?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, WireError> {
        let bytes = self.read_exact_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64, WireError> {
        let bytes = self.read_exact_array::<8>()?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_f64(&mut self) -> Result<f64, WireError> {
        let bytes = self.read_exact_array::<8>()?;
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, WireError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| WireError::InvalidUtf8)
    }

    fn read_exact_array<const N: usize>(&mut self) -> Result<[u8; N], WireError> {
        let bytes = self.read_exact(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(WireError::UnexpectedEof)?;
        if end > self.bytes.len() {
            return Err(WireError::UnexpectedEof);
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn is_eof(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn read_u8(code: &[u8], ip: &mut usize) -> Option<u8> {
    let value = *code.get(*ip)?;
    *ip += 1;
    Some(value)
}

fn read_u16(code: &[u8], ip: &mut usize) -> Option<u16> {
    let bytes = code.get(*ip..(*ip + 2))?;
    *ip += 2;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u32(code: &[u8], ip: &mut usize) -> Option<u32> {
    let bytes = code.get(*ip..(*ip + 4))?;
    *ip += 4;
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}
