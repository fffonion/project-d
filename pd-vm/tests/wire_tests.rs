use vm::{
    ArgInfo, BytecodeBuilder, DebugFunction, DebugInfo, HostImport, LineInfo, LocalInfo, Program,
    ValidationError, Value, WireError, decode_program, encode_program, infer_local_count,
    validate_program,
};

#[test]
fn wire_roundtrip_preserves_constants_and_code() {
    let program = Program::with_imports_and_debug(
        vec![
            Value::Int(42),
            Value::Bool(true),
            Value::String("hello".to_string()),
        ],
        vec![0x00, 0x01, 0x02],
        vec![HostImport {
            name: "print".to_string(),
            arity: 1,
        }],
        Some(DebugInfo {
            source: Some("fn a(x);\na(1);".to_string()),
            lines: vec![
                LineInfo { offset: 0, line: 1 },
                LineInfo { offset: 1, line: 2 },
            ],
            functions: vec![DebugFunction {
                name: "a".to_string(),
                args: vec![ArgInfo {
                    name: "x".to_string(),
                    position: 0,
                }],
            }],
            locals: vec![LocalInfo {
                name: "v".to_string(),
                index: 0,
            }],
        }),
    );

    let encoded = encode_program(&program).expect("encode should succeed");
    let decoded = decode_program(&encoded).expect("decode should succeed");

    assert_eq!(decoded.constants, program.constants);
    assert_eq!(decoded.code, program.code);
    assert_eq!(decoded.imports, program.imports);
    assert_eq!(decoded.debug, program.debug);
}

#[test]
fn decode_rejects_invalid_magic_version_and_truncation() {
    let program = Program::new(vec![Value::Int(7)], vec![0x01]);
    let encoded = encode_program(&program).expect("encode should succeed");

    let mut bad_magic = encoded.clone();
    bad_magic[0..4].copy_from_slice(b"NOPE");
    assert!(matches!(
        decode_program(&bad_magic),
        Err(WireError::InvalidMagic(_))
    ));

    let mut bad_version = encoded.clone();
    bad_version[4..6].copy_from_slice(&99u16.to_le_bytes());
    assert!(matches!(
        decode_program(&bad_version),
        Err(WireError::UnsupportedVersion(99))
    ));

    let truncated = &encoded[..encoded.len() - 1];
    assert!(matches!(
        decode_program(truncated),
        Err(WireError::UnexpectedEof)
    ));
}

#[test]
fn validate_rejects_invalid_const_call_jump_and_opcode() {
    let bad_const = Program::new(vec![Value::Int(1)], vec![0x02, 0x01, 0x00, 0x00, 0x00]);
    assert!(matches!(
        validate_program(&bad_const, 4),
        Err(ValidationError::InvalidConstant { .. })
    ));

    let bad_call = Program::new(vec![], vec![0x11, 0x05, 0x00, 0x00]);
    assert!(matches!(
        validate_program(&bad_call, 4),
        Err(ValidationError::InvalidCall { index: 5, .. })
    ));

    let bad_jump = Program::new(vec![], vec![0x0B, 0xFF, 0x00, 0x00, 0x00]);
    assert!(matches!(
        validate_program(&bad_jump, 4),
        Err(ValidationError::InvalidJumpTarget { .. })
    ));

    let bad_opcode = Program::new(vec![], vec![0xFF]);
    assert!(matches!(
        validate_program(&bad_opcode, 4),
        Err(ValidationError::InvalidOpcode { opcode: 0xFF, .. })
    ));
}

#[test]
fn validate_accepts_known_good_program() {
    let mut bc = BytecodeBuilder::new();
    bc.ldc(0);
    bc.call(0, 1);
    bc.ret();

    let program = Program::with_imports_and_debug(
        vec![Value::String("x".to_string())],
        bc.finish(),
        vec![HostImport {
            name: "print".to_string(),
            arity: 1,
        }],
        None,
    );
    validate_program(&program, 4).expect("program should validate");
}

#[test]
fn validate_rejects_invalid_call_arity_for_import() {
    let mut bc = BytecodeBuilder::new();
    bc.call(0, 2);
    bc.ret();

    let program = Program::with_imports_and_debug(
        vec![],
        bc.finish(),
        vec![HostImport {
            name: "print".to_string(),
            arity: 1,
        }],
        None,
    );
    assert!(matches!(
        validate_program(&program, 4),
        Err(ValidationError::InvalidCallArity {
            index: 0,
            expected: 1,
            got: 2,
            ..
        })
    ));
}

#[test]
fn infer_local_count_finds_highest_local_index() {
    let mut bc = BytecodeBuilder::new();
    bc.ldloc(3);
    bc.stloc(7);
    bc.ret();

    let program = Program::new(vec![], bc.finish());
    let locals = infer_local_count(&program).expect("infer should succeed");
    assert_eq!(locals, 8);
}
