use vm::{
    Assembler, BytecodeBuilder, CallOutcome, Compiler, Expr, HostFunction, JitConfig, OpCode,
    Program, SourceFlavor, Stmt, Value, Vm, VmStatus, assemble, compile_source,
    compile_source_file, compile_source_with_flavor,
};

struct YieldOnce {
    yielded: bool,
}

impl HostFunction for YieldOnce {
    fn call(&mut self, _vm: &mut Vm, _args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        if !self.yielded {
            self.yielded = true;
            Ok(CallOutcome::Yield)
        } else {
            Ok(CallOutcome::Return(vec![Value::Int(42)]))
        }
    }
}

#[test]
fn arithmetic_works() {
    let constants = vec![Value::Int(2), Value::Int(3)];
    let mut bc = BytecodeBuilder::new();
    bc.ldc(0);
    bc.ldc(1);
    bc.add();
    bc.ret();

    let program = Program::new(constants, bc.finish());
    let mut vm = Vm::new(program);

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(5)]);
}

#[test]
fn shift_ops_and_msil_literals_work() {
    let source = r#"
        ldc 3
        ldc 2
        shl
        ldc 1
        shr
        ret
    "#;

    let program = assemble(source).expect("assemble should succeed");
    let mut vm = Vm::new(program);
    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(6)]);
}

#[test]
fn brfalse_skips_block() {
    let constants = vec![Value::Bool(false), Value::Int(1), Value::Int(2)];
    let mut bc = BytecodeBuilder::new();
    bc.ldc(0);
    bc.brfalse(16);
    bc.ldc(1);
    bc.ret();
    bc.ldc(2);
    bc.ret();

    let program = Program::new(constants, bc.finish());
    let mut vm = Vm::new(program);

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);
}

#[test]
fn call_can_yield_and_resume() {
    let mut bc = BytecodeBuilder::new();
    bc.call(0, 0);
    bc.ret();

    let program = Program::new(Vec::new(), bc.finish());
    let mut vm = Vm::new(program);
    vm.register_function(Box::new(YieldOnce { yielded: false }));

    let status = vm.run().expect("first run should yield");
    assert_eq!(status, VmStatus::Yielded);

    let status = vm.resume().expect("resume should halt");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);
}

#[test]
fn assembler_resolves_labels() {
    let mut asm = Assembler::new();
    asm.push_const(Value::Bool(false));
    asm.brfalse_label("target");
    asm.push_const(Value::Int(1));
    asm.ret();
    asm.label("target").expect("label should register");
    asm.push_const(Value::Int(2));
    asm.ret();

    let program = asm.finish_program().expect("assembler should finish");
    let mut vm = Vm::new(program);

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);
}

#[test]
fn compiler_emits_expression() {
    let expr = Expr::Mul(
        Box::new(Expr::Add(Box::new(Expr::Int(2)), Box::new(Expr::Int(3)))),
        Box::new(Expr::Int(4)),
    );
    let program = Compiler::new()
        .compile_program(&[Stmt::Expr { expr, line: 1 }])
        .expect("compiler should emit program");

    let mut vm = Vm::new(program);
    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(20)]);
}

#[test]
fn assemble_text_program() {
    let source = r#"
        ldc 2
        ldc 3
        add
        ret
    "#;

    let program = assemble(source).expect("assemble should succeed");
    let mut vm = Vm::new(program);
    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(5)]);
}

#[test]
fn assemble_text_with_labels() {
    let source = r#"
        ldc false
        brfalse target
        ldc 1
        ret
        .label target
        ldc 2
        ret
    "#;

    let program = assemble(source).expect("assemble should succeed");
    let mut vm = Vm::new(program);
    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);
}

#[test]
fn assemble_text_with_data_and_string() {
    let source = r#"
        .data
        string greeting "hello"
        .code
        ldc greeting
        ret
    "#;

    let program = assemble(source).expect("assemble should succeed");
    let mut vm = Vm::new(program);
    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::String("hello".to_string())]);
}

#[test]
fn assemble_rejects_legacy_opcode_literals() {
    let source = r#"
        const 1
        halt
    "#;
    let err = assemble(source).expect_err("legacy opcodes should be rejected");
    assert!(err.message.contains("unknown opcode"));
}

#[test]
fn compile_source_program() {
    let source = r#"
        let x = 2 + 3;
        let y = x * 4;
        if y > 10 {
            y;
        } else {
            0;
        }
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(20)]);
}

#[test]
fn assignment_updates_existing_local_without_new_slot() {
    let source = r#"
        let a = 1;
        a = 2;
        a;
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    assert_eq!(compiled.locals, 1);
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);
}

struct AddOne;
struct EchoString;
struct PrintBuiltin;
struct PrintNoReturn;

impl HostFunction for AddOne {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        let value = match args.first() {
            Some(Value::Int(value)) => *value,
            _ => 0,
        };
        Ok(CallOutcome::Return(vec![Value::Int(value + 1)]))
    }
}

impl HostFunction for EchoString {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        let value = match args.first() {
            Some(Value::String(value)) => value.clone(),
            _ => return Err(vm::VmError::TypeMismatch("string")),
        };
        Ok(CallOutcome::Return(vec![Value::String(value)]))
    }
}

impl HostFunction for PrintBuiltin {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        Ok(CallOutcome::Return(args.to_vec()))
    }
}

impl HostFunction for PrintNoReturn {
    fn call(&mut self, _vm: &mut Vm, _args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        Ok(CallOutcome::Return(vec![]))
    }
}

#[test]
fn compile_source_with_functions() {
    let source = include_str!("../examples/example.rss");

    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "add_one" => vm.register_function(Box::new(AddOne)),
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(6)]);
}

#[test]
fn compile_source_with_javascript_flavor() {
    let source = include_str!("../examples/example.js");

    let compiled = compile_source_with_flavor(source, SourceFlavor::JavaScript)
        .expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "add_one" => vm.register_function(Box::new(AddOne)),
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(6)]);
}

#[test]
fn javascript_assignment_updates_existing_local_without_new_slot() {
    let source = r#"
        let a = 1;
        a = 2;
        a;
    "#;
    let compiled = compile_source_with_flavor(source, SourceFlavor::JavaScript)
        .expect("compile should succeed");
    assert_eq!(compiled.locals, 1);

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);
}

#[test]
fn compile_source_with_lua_flavor() {
    let source = include_str!("../examples/example.lua");

    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Lua).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "add_one" => vm.register_function(Box::new(AddOne)),
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(6)]);
}

#[test]
fn lua_assignment_updates_existing_local_without_new_slot() {
    let source = r#"
        local a = 1
        a = 2
        a
    "#;
    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Lua).expect("compile should succeed");
    assert_eq!(compiled.locals, 1);

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);
}

#[test]
fn rust_like_print_macro_works_without_decl() {
    let source = r#"
        print!(40 + 2);
    "#;
    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);
}

#[test]
fn javascript_console_log_works_without_decl() {
    let source = r#"
        console.log(40 + 2);
    "#;
    let compiled = compile_source_with_flavor(source, SourceFlavor::JavaScript)
        .expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);
}

#[test]
fn lua_print_works_without_decl() {
    let source = r#"
        print(40 + 2)
    "#;
    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Lua).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);
}

#[test]
fn compile_source_with_closure() {
    let source = include_str!("../examples/closure.rss");
    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(12)]);
}

#[test]
fn closure_captures_outer_value_at_definition_time() {
    let source = r#"
        let base = 7;
        let add = |value| value + base;
        let base = 8;
        print!(add(5));
    "#;
    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(12)]);
}

#[test]
fn compile_source_with_javascript_closure_fixture() {
    let source = include_str!("../examples/closure.js");
    let compiled = compile_source_with_flavor(source, SourceFlavor::JavaScript)
        .expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(12)]);
}

#[test]
fn compile_source_with_lua_closure_fixture() {
    let source = include_str!("../examples/closure.lua");
    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Lua).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(12)]);
}

#[test]
fn lua_function_literal_without_return_is_rejected() {
    let source = r#"
        local base = 7
        local add = function(value) value + base end
        print(add(5))
    "#;
    let err = match compile_source_with_flavor(source, SourceFlavor::Lua) {
        Ok(_) => panic!("lua closure without return should fail"),
        Err(err) => err,
    };

    match err {
        vm::SourceError::Parse(err) => assert!(err.message.contains("return <expr>")),
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn break_and_continue_outside_loop_are_rejected() {
    let break_err = match compile_source("break;") {
        Ok(_) => panic!("break outside loop should fail"),
        Err(err) => err,
    };
    let continue_err = match compile_source("continue;") {
        Ok(_) => panic!("continue outside loop should fail"),
        Err(err) => err,
    };

    match break_err {
        vm::SourceError::Parse(err) => assert!(err.message.contains("inside loops")),
        other => panic!("unexpected error: {other}"),
    }
    match continue_err {
        vm::SourceError::Parse(err) => assert!(err.message.contains("inside loops")),
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn javascript_module_declarations_are_ignored() {
    let source = r#"
        import {
            add_one
        } from "pd-vm-host";
        const { ignored } = require("pd-vm-host");
        console.log(add_one(41));
    "#;
    let compiled = compile_source_with_flavor(source, SourceFlavor::JavaScript)
        .expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "add_one" => vm.register_function(Box::new(AddOne)),
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);
}

#[test]
fn lua_require_declaration_is_ignored() {
    let source = r#"
        local _host = require("pd-vm-host")
        print(add_one(41))
    "#;
    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Lua).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "add_one" => vm.register_function(Box::new(AddOne)),
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);
}

#[test]
fn compile_source_file_detects_extension() {
    let unique = format!(
        "vm_extension_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let base = std::env::temp_dir().join(unique);
    let path = base.with_extension("js");
    std::fs::write(&path, include_str!("../examples/example.js"))
        .expect("temp source should write");

    let compiled = compile_source_file(&path).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    for func in &compiled.functions {
        match func.name.as_str() {
            "add_one" => vm.register_function(Box::new(AddOne)),
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(6)]);

    let _ = std::fs::remove_file(path);
}

#[test]
fn compile_source_file_detects_lua_extension() {
    let unique = format!(
        "vm_extension_test_lua_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let base = std::env::temp_dir().join(unique);
    let path = base.with_extension("lua");
    std::fs::write(&path, include_str!("../examples/example.lua"))
        .expect("temp source should write");

    let compiled = compile_source_file(&path).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    for func in &compiled.functions {
        match func.name.as_str() {
            "add_one" => vm.register_function(Box::new(AddOne)),
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(6)]);

    let _ = std::fs::remove_file(path);
}

#[test]
fn compile_source_with_string_literals() {
    let source = r#"
        fn echo(x);
        echo("hello");
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "echo" => vm.register_function(Box::new(EchoString)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::String("hello".to_string())]);
}

#[test]
fn compile_source_emits_named_locals_in_debug_info() {
    let source = r#"
        let alpha = 1;
        let beta = alpha + 2;
        beta;
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    let debug = compiled
        .program
        .debug
        .as_ref()
        .expect("compiled program should have debug info");

    assert_eq!(debug.local_index("alpha"), Some(0));
    assert_eq!(debug.local_index("beta"), Some(1));
}

#[test]
fn trace_jit_compiles_hot_loop_and_is_dumpable() {
    let source = r#"
        let i = 0;
        let sum = 0;
        while i < 20 {
            sum = sum + i;
            i = i + 1;
        }
        sum;
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.set_jit_config(JitConfig {
        enabled: cfg!(target_arch = "x86_64"),
        hot_loop_threshold: 1,
        max_trace_len: 512,
    });

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(190)]);

    let dump = vm.dump_jit_info();
    let snapshot = vm.jit_snapshot();
    if cfg!(target_arch = "x86_64") {
        assert!(
            !snapshot.traces.is_empty(),
            "expected at least one compiled trace, dump:\n{dump}"
        );
        assert!(dump.contains("compiled traces:"));
        assert!(dump.contains("trace#"));
        assert!(dump.contains("native trace#"));
    } else {
        assert!(snapshot.traces.is_empty());
    }
}

#[test]
fn compiler_uses_shl_for_power_of_two_multiply_and_jit_accepts_it() {
    let source = r#"
        let i = 0;
        let sum = 0;
        while i < 8 {
            sum = sum + i * 8;
            i = i + 1;
        }
        sum;
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    assert!(
        compiled.program.code.contains(&(OpCode::Shl as u8)),
        "expected compiler to emit shl for power-of-two multiply"
    );

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.set_jit_config(JitConfig {
        enabled: cfg!(target_arch = "x86_64"),
        hot_loop_threshold: 1,
        max_trace_len: 512,
    });

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(224)]);

    if cfg!(target_arch = "x86_64") {
        let dump = vm.dump_jit_info();
        assert!(dump.contains(" shl"), "expected trace dump to include shl");
    }
}

#[test]
fn trace_jit_reports_nyi_for_host_calls() {
    let source = r#"
        fn print(x);
        let i = 0;
        while i < 4 {
            print(i);
            i = i + 1;
        }
        i;
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.set_jit_config(JitConfig {
        enabled: cfg!(target_arch = "x86_64"),
        hot_loop_threshold: 1,
        max_trace_len: 512,
    });
    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintNoReturn)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(4)]);

    let dump = vm.dump_jit_info();
    let snapshot = vm.jit_snapshot();
    if cfg!(target_arch = "x86_64") {
        assert!(
            snapshot
                .attempts
                .iter()
                .any(|attempt| matches!(attempt.result, Err(vm::JitNyiReason::HostCall))),
            "expected HostCall NYI, dump:\n{dump}"
        );
        assert!(dump.contains("opcode call is NYI in trace JIT"));
    }
}
