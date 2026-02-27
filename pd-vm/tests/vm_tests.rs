use vm::{
    Assembler, BytecodeBuilder, CallOutcome, Compiler, Expr, HostFunction, HostFunctionRegistry,
    JitConfig, OpCode, Program, SourceFlavor, Stmt, Value, Vm, VmStatus, assemble, compile_source,
    compile_source_file, compile_source_with_flavor,
};

fn native_jit_supported() -> bool {
    (cfg!(target_arch = "x86_64")
        && (cfg!(target_os = "windows") || (cfg!(unix) && !cfg!(target_os = "macos"))))
        || (cfg!(target_arch = "aarch64")
            && (cfg!(target_os = "linux") || cfg!(target_os = "macos")))
}

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
fn arithmetic_supports_float_and_mixed_numeric() {
    let constants = vec![Value::Float(1.5), Value::Int(2), Value::Float(8.0)];
    let mut bc = BytecodeBuilder::new();
    bc.ldc(0);
    bc.ldc(1);
    bc.add();
    bc.ldc(2);
    bc.clt();
    bc.ret();

    let program = Program::new(constants, bc.finish());
    let mut vm = Vm::new(program);

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Bool(true)]);
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

fn static_add_one(_vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, vm::VmError> {
    let value = match args.first() {
        Some(Value::Int(value)) => *value,
        _ => 0,
    };
    Ok(CallOutcome::Return(vec![Value::Int(value + 1)]))
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
fn compile_source_resolves_imports_by_name_not_registration_order() {
    let source = include_str!("../examples/example.rss");
    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    vm.bind_function("print", Box::new(PrintBuiltin));
    vm.bind_function("add_one", Box::new(AddOne));

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(6)]);
}

#[test]
fn run_fails_when_import_is_unbound() {
    let source = r#"
        fn add_one(x);
        add_one(41);
    "#;
    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.bind_function("print", Box::new(PrintBuiltin));

    let err = vm.run().expect_err("missing import should fail");
    assert!(matches!(err, vm::VmError::UnboundImport(name) if name == "add_one"));
}

#[test]
fn host_function_registry_caches_import_plan_across_vms() {
    let source = include_str!("../examples/example.rss");
    let compiled = compile_source(source).expect("compile should succeed");

    let mut registry = HostFunctionRegistry::new();
    registry.register("print", 1, || Box::new(PrintBuiltin));
    registry.register("add_one", 1, || Box::new(AddOne));

    let mut vm1 = Vm::with_locals(compiled.program.clone(), compiled.locals);
    registry
        .bind_vm_cached(&mut vm1)
        .expect("cached host binding should succeed");
    let status1 = vm1.run().expect("vm should run");
    assert_eq!(status1, VmStatus::Halted);
    assert_eq!(vm1.stack(), &[Value::Int(6)]);

    let mut vm2 = Vm::with_locals(compiled.program, compiled.locals);
    registry
        .bind_vm_cached(&mut vm2)
        .expect("cached host binding should succeed");
    let status2 = vm2.run().expect("vm should run");
    assert_eq!(status2, VmStatus::Halted);
    assert_eq!(vm2.stack(), &[Value::Int(6)]);
}

#[test]
fn compile_source_supports_static_function_pointer_binding() {
    let source = r#"
        fn add_one(x);
        add_one(41);
    "#;
    let compiled = compile_source(source).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.bind_static_function("add_one", static_add_one);

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);
}

#[test]
fn host_function_registry_caches_static_function_pointer_plan_across_vms() {
    let source = include_str!("../examples/example.rss");
    let compiled = compile_source(source).expect("compile should succeed");

    let mut registry = HostFunctionRegistry::new();
    registry.register_static("print", 1, |_vm, args| {
        Ok(CallOutcome::Return(args.to_vec()))
    });
    registry.register_static("add_one", 1, static_add_one);
    let plan = registry
        .prepare_plan(&compiled.program.imports)
        .expect("plan should build");

    let mut vm1 = Vm::with_locals(compiled.program.clone(), compiled.locals);
    registry
        .bind_vm_with_plan(&mut vm1, &plan)
        .expect("cached static host binding should succeed");
    let status1 = vm1.run().expect("vm should run");
    assert_eq!(status1, VmStatus::Halted);
    assert_eq!(vm1.stack(), &[Value::Int(6)]);

    let mut vm2 = Vm::with_locals(compiled.program, compiled.locals);
    registry
        .bind_vm_with_plan(&mut vm2, &plan)
        .expect("cached static host binding should succeed");
    let status2 = vm2.run().expect("vm should run");
    assert_eq!(status2, VmStatus::Halted);
    assert_eq!(vm2.stack(), &[Value::Int(6)]);
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
fn compile_source_with_scheme_flavor() {
    let source = include_str!("../examples/example.scm");

    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Scheme).expect("compile should succeed");
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
fn not_not_equal_and_else_if_are_supported_across_frontends() {
    let rustscript = r#"
        let a = 2;
        let out = 0;
        if !(a != 2) {
            out = 10;
        } else if a == 3 {
            out = 20;
        } else {
            out = 30;
        }
        out;
    "#;
    let javascript = r#"
        let a = 2;
        let out = 0;
        if (!(a != 2)) {
            out = 10;
        } else if (a == 3) {
            out = 20;
        } else {
            out = 30;
        }
        out;
    "#;
    let lua = r#"
        local a = 2
        local out = 0
        if not (a ~= 2) then
            out = 10
        elseif a == 3 then
            out = 20
        else
            out = 30
        end
        out
    "#;
    let scheme = r#"
        (define a 2)
        (define out 0)
        (if (not (/= a 2))
            (set! out 10)
            (if (= a 3)
                (set! out 20)
                (set! out 30)))
        out
    "#;

    let cases = [
        (SourceFlavor::RustScript, rustscript),
        (SourceFlavor::JavaScript, javascript),
        (SourceFlavor::Lua, lua),
        (SourceFlavor::Scheme, scheme),
    ];

    for (flavor, source) in cases {
        let compiled = compile_source_with_flavor(source, flavor).expect("compile should succeed");
        let mut vm = Vm::with_locals(compiled.program, compiled.locals);
        let status = vm.run().expect("vm should run");
        assert_eq!(status, VmStatus::Halted);
        assert_eq!(vm.stack(), &[Value::Int(10)]);
    }
}

#[test]
fn collections_are_created_and_accessed_in_all_frontends() {
    let rustscript = r#"
        let arr = [1, 2, 3];
        let second = arr[1];
        arr[1] = 9;
        let m = {"x": 1, "y": 2};
        m.z = 7;
        m["x"] = 4;
        let v1 = m.x;
        let v2 = m["z"];
        second + arr[1] + v1 + v2;
    "#;
    let javascript = r#"
        let arr = [1, 2, 3];
        let second = arr[1];
        arr[1] = 9;
        let m = { x: 1, y: 2 };
        m.z = 7;
        m["x"] = 4;
        let v1 = m.x;
        let v2 = m["z"];
        second + arr[1] + v1 + v2;
    "#;
    let lua = r#"
        local arr = {1, 2, 3}
        local second = arr[1]
        arr[1] = 9
        local m = { x = 1, y = 2 }
        m.z = 7
        m["x"] = 4
        local v1 = m.x
        local v2 = m["z"]
        second + arr[1] + v1 + v2
    "#;
    let scheme = r#"
        (define arr (vector 1 2 3))
        (define second (vector-ref arr 1))
        (vector-set! arr 1 9)
        (define m (hash (x 1) ("y" 2)))
        (hash-set! m z 7)
        (hash-set! m "x" 4)
        (define v1 (hash-ref m x))
        (define v2 (hash-ref m "z"))
        (+ second (vector-ref arr 1) v1 v2)
    "#;

    let cases = [
        (SourceFlavor::RustScript, rustscript),
        (SourceFlavor::JavaScript, javascript),
        (SourceFlavor::Lua, lua),
        (SourceFlavor::Scheme, scheme),
    ];

    for (flavor, source) in cases {
        let compiled = compile_source_with_flavor(source, flavor).expect("compile should succeed");
        assert!(
            compiled.functions.is_empty(),
            "collection intrinsics should be compiler-managed, not host imports"
        );
        let mut vm = Vm::with_locals(compiled.program, compiled.locals);
        let status = vm.run().expect("vm should run");
        assert_eq!(status, VmStatus::Halted);
        assert_eq!(vm.stack(), &[Value::Int(22)]);
    }
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
fn scheme_assignment_updates_existing_local_without_new_slot() {
    let source = r#"
        (define a 1)
        (set! a 2)
        a
    "#;
    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Scheme).expect("compile should succeed");
    assert_eq!(compiled.locals, 1);

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);
}

#[test]
fn scheme_do_loop_syntax_is_supported() {
    let source = r#"
        (do ((i 1 (+ i 1))
             (p 3 (* 3 p)))
            ((> i 4) p))
    "#;
    let compiled =
        compile_source_with_flavor(source, SourceFlavor::Scheme).expect("compile should succeed");

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(243)]);
}

#[test]
fn rss_print_macro_works_without_decl() {
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
fn compile_source_file_with_rustscript_complex_fixture() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/example_complex.rss");
    let compiled = compile_source_file(&path).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            "add_one" => vm.register_function(Box::new(AddOne)),
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
fn compile_source_file_with_javascript_complex_fixture() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/example_complex.js");
    let compiled = compile_source_file(&path).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            "add_one" => vm.register_function(Box::new(AddOne)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(12)]);
}

#[test]
fn compile_source_file_with_lua_complex_fixture() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/example_complex.lua");
    let compiled = compile_source_file(&path).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            "add_one" => vm.register_function(Box::new(AddOne)),
            _ => panic!("unexpected function {}", func.name),
        };
    }

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(12)]);
}

#[test]
fn compile_source_file_with_scheme_complex_fixture() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/example_complex.scm");
    let compiled = compile_source_file(&path).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);

    for func in &compiled.functions {
        match func.name.as_str() {
            "print" => vm.register_function(Box::new(PrintBuiltin)),
            "add_one" => vm.register_function(Box::new(AddOne)),
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
fn compile_source_file_detects_scheme_extension() {
    let unique = format!(
        "vm_extension_test_scheme_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let base = std::env::temp_dir().join(unique);
    let path = base.with_extension("scm");
    std::fs::write(&path, include_str!("../examples/example.scm"))
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
fn compile_source_file_rustscript_imports_merge_with_scoped_locals() {
    let unique = format!(
        "vm_rss_import_scope_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("module.rss");
    let main_path = root.join("main.rss");
    std::fs::write(
        &module_path,
        r#"
        pub fn add_one(x);
        let shared = 40;
    "#,
    )
    .expect("module source should write");
    std::fs::write(
        &main_path,
        r#"
        use module;
        let shared = add_one(1);
        shared;
    "#,
    )
    .expect("main source should write");

    let compiled = compile_source_file(&main_path).expect("compile should succeed");
    assert_eq!(
        compiled.locals, 2,
        "module and root locals should be isolated"
    );
    assert_eq!(
        compiled
            .functions
            .iter()
            .filter(|func| func.name == "add_one")
            .count(),
        1,
        "imported function should only be declared once",
    );

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.bind_function("add_one", Box::new(AddOne));
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(2)]);

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_rustscript_rejects_import_keyword() {
    let unique = format!(
        "vm_rss_use_keyword_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");
    let main_path = root.join("main.rss");
    std::fs::write(&main_path, "import \"./module.rss\";\n1;\n").expect("source should write");

    let err = match compile_source_file(&main_path) {
        Ok(_) => panic!("legacy import syntax should be rejected for RustScript"),
        Err(err) => err,
    };
    assert!(
        matches!(
            err,
            vm::SourcePathError::InvalidImportSyntax { ref message, .. }
            if message.contains("uses 'use', not 'import'")
        ),
        "expected use-keyword guidance, got {err:?}"
    );

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_supports_rss_modules_from_js_lua_and_scheme() {
    let unique = format!(
        "vm_cross_flavor_import_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");
    let module_path = root.join("module.rss");
    std::fs::write(&module_path, "pub fn add_one(x);\n").expect("module source should write");

    let js_path = root.join("main.js");
    std::fs::write(
        &js_path,
        r#"
        import { add_one } from "./module.rss";
        console.log(add_one(41));
    "#,
    )
    .expect("js source should write");
    let js_compiled = compile_source_file(&js_path).expect("js compile should succeed");
    let mut js_vm = Vm::with_locals(js_compiled.program, js_compiled.locals);
    for func in &js_compiled.functions {
        match func.name.as_str() {
            "add_one" => js_vm.register_function(Box::new(AddOne)),
            "print" => js_vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }
    let js_status = js_vm.run().expect("js vm should run");
    assert_eq!(js_status, VmStatus::Halted);
    assert_eq!(js_vm.stack(), &[Value::Int(42)]);

    let lua_path = root.join("main.lua");
    std::fs::write(
        &lua_path,
        r#"
        local _m = require("./module.rss")
        print(add_one(41))
    "#,
    )
    .expect("lua source should write");
    let lua_compiled = compile_source_file(&lua_path).expect("lua compile should succeed");
    let mut lua_vm = Vm::with_locals(lua_compiled.program, lua_compiled.locals);
    for func in &lua_compiled.functions {
        match func.name.as_str() {
            "add_one" => lua_vm.register_function(Box::new(AddOne)),
            "print" => lua_vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }
    let lua_status = lua_vm.run().expect("lua vm should run");
    assert_eq!(lua_status, VmStatus::Halted);
    assert_eq!(lua_vm.stack(), &[Value::Int(42)]);

    let scm_path = root.join("main.scm");
    std::fs::write(
        &scm_path,
        r#"
        (import "./module.rss")
        (print (add_one 41))
    "#,
    )
    .expect("scheme source should write");
    let scm_compiled = compile_source_file(&scm_path).expect("scheme compile should succeed");
    let mut scm_vm = Vm::with_locals(scm_compiled.program, scm_compiled.locals);
    for func in &scm_compiled.functions {
        match func.name.as_str() {
            "add_one" => scm_vm.register_function(Box::new(AddOne)),
            "print" => scm_vm.register_function(Box::new(PrintBuiltin)),
            _ => panic!("unexpected function {}", func.name),
        };
    }
    let scm_status = scm_vm.run().expect("scheme vm should run");
    assert_eq!(scm_status, VmStatus::Halted);
    assert_eq!(scm_vm.stack(), &[Value::Int(42)]);

    let _ = std::fs::remove_file(scm_path);
    let _ = std::fs::remove_file(lua_path);
    let _ = std::fs::remove_file(js_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_lua_supports_namespace_and_named_require_imports() {
    let unique = format!(
        "vm_lua_namespace_import_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("strings.rss");
    std::fs::write(
        &module_path,
        r#"
        fn eq(lhs, rhs) {
            lhs == rhs;
        }
        pub fn is_empty(value) {
            eq(value, "");
        }
        pub fn non_empty(value) {
            eq(is_empty(value), false);
        }
    "#,
    )
    .expect("module source should write");

    let main_path = root.join("main.lua");
    std::fs::write(
        &main_path,
        r#"
        local string = require("./strings.rss")
        local is_empty = require("./strings.rss").is_empty
        print(string.non_empty("rss"))
        print(is_empty(""))
    "#,
    )
    .expect("lua source should write");

    let compiled = compile_source_file(&main_path).expect("compile should succeed");
    assert_eq!(compiled.functions.len(), 1);
    assert_eq!(compiled.functions[0].name, "print");

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.bind_function("print", Box::new(PrintBuiltin));
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Bool(true), Value::Bool(true)]);

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_scheme_supports_library_import_sets() {
    let unique = format!(
        "vm_scheme_library_import_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("strings.rss");
    std::fs::write(
        &module_path,
        r#"
        fn eq(lhs, rhs) {
            lhs == rhs;
        }
        pub fn is_empty(value) {
            eq(value, "");
        }
        pub fn non_empty(value) {
            eq(is_empty(value), false);
        }
    "#,
    )
    .expect("module source should write");

    let main_path = root.join("main.scm");
    std::fs::write(
        &main_path,
        r#"
        (import (prefix "./strings.rss" string:))
        (import (only "./strings.rss" is_empty))
        (print (string:non_empty "rss"))
        (print (is_empty ""))
    "#,
    )
    .expect("scheme source should write");

    let compiled = compile_source_file(&main_path).expect("compile should succeed");
    assert_eq!(compiled.functions.len(), 1);
    assert_eq!(compiled.functions[0].name, "print");

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.bind_function("print", Box::new(PrintBuiltin));
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Bool(true), Value::Bool(true)]);

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_scheme_supports_module_language_require_sets() {
    let unique = format!(
        "vm_scheme_require_import_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("strings.rss");
    std::fs::write(
        &module_path,
        r#"
        fn eq(lhs, rhs) {
            lhs == rhs;
        }
        pub fn is_empty(value) {
            eq(value, "");
        }
        pub fn non_empty(value) {
            eq(is_empty(value), false);
        }
    "#,
    )
    .expect("module source should write");

    let main_path = root.join("main.scm");
    std::fs::write(
        &main_path,
        r#"
        (require (prefix-in string: "./strings.rss"))
        (require (only-in "./strings.rss" is_empty))
        (print (string:non_empty "rss"))
        (print (is_empty ""))
    "#,
    )
    .expect("scheme source should write");

    let compiled = compile_source_file(&main_path).expect("compile should succeed");
    assert_eq!(compiled.functions.len(), 1);
    assert_eq!(compiled.functions[0].name, "print");

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.bind_function("print", Box::new(PrintBuiltin));
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Bool(true), Value::Bool(true)]);

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_js_supports_namespace_and_named_alias_imports() {
    let unique = format!(
        "vm_js_namespace_import_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("strings.rss");
    std::fs::write(
        &module_path,
        r#"
        fn eq(lhs, rhs) {
            lhs == rhs;
        }
        pub fn is_empty(value) {
            eq(value, "");
        }
        pub fn non_empty(value) {
            eq(is_empty(value), false);
        }
    "#,
    )
    .expect("module source should write");

    let main_path = root.join("main.js");
    std::fs::write(
        &main_path,
        r#"
        import * as string from "./strings.rss";
        import { is_empty as is_empty } from "./strings.rss";

        console.log(string.non_empty("rss"));
        console.log(is_empty(""));
    "#,
    )
    .expect("js source should write");

    let compiled = compile_source_file(&main_path).expect("compile should succeed");
    assert_eq!(compiled.functions.len(), 1);
    assert_eq!(compiled.functions[0].name, "print");

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    vm.bind_function("print", Box::new(PrintBuiltin));
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Bool(true), Value::Bool(true)]);

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_rustscript_supports_namespace_and_named_imports() {
    let unique = format!(
        "vm_rustscript_namespace_import_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("strings.rss");
    std::fs::write(
        &module_path,
        r#"
        fn eq(lhs, rhs) {
            lhs == rhs;
        }
        pub fn is_empty(value) {
            eq(value, "");
        }
        pub fn non_empty(value) {
            eq(is_empty(value), false);
        }
    "#,
    )
    .expect("module source should write");

    let main_path = root.join("main.rss");
    std::fs::write(
        &main_path,
        r#"
        use strings as string;
        use strings::{is_empty as is_empty};

        string::non_empty("rss");
        is_empty("");
    "#,
    )
    .expect("main source should write");

    let compiled = compile_source_file(&main_path).expect("compile should succeed");
    assert!(
        compiled.functions.is_empty(),
        "module functions should be fully inlined for RustScript root"
    );

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Bool(true), Value::Bool(true)]);

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_rustscript_named_import_is_selective() {
    let unique = format!(
        "vm_rustscript_selective_import_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("module.rss");
    std::fs::write(
        &module_path,
        r#"
        pub fn add_one(x) {
            x + 1;
        }
        pub fn add_two(x) {
            x + 2;
        }
    "#,
    )
    .expect("module source should write");

    let main_path = root.join("main.rss");
    std::fs::write(
        &main_path,
        r#"
        use module::{add_one};
        add_two(40);
    "#,
    )
    .expect("main source should write");

    let err = match compile_source_file(&main_path) {
        Ok(_) => panic!("selective import should not expose unlisted exports"),
        Err(err) => err,
    };
    assert!(
        matches!(
            err,
            vm::SourcePathError::Source(vm::SourceError::Parse(vm::ParseError { ref message, .. }))
            if message.contains("unknown function 'add_two'")
        ),
        "expected unknown function error, got {err:?}"
    );

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_rustscript_module_exports_only_pub_functions() {
    let unique = format!(
        "vm_rustscript_pub_export_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let module_path = root.join("module.rss");
    std::fs::write(
        &module_path,
        r#"
        fn private_add(x) {
            x + 1;
        }
        pub fn public_add(x) {
            private_add(x);
        }
    "#,
    )
    .expect("module source should write");

    let ok_main_path = root.join("main_ok.rss");
    std::fs::write(
        &ok_main_path,
        r#"
        use module;
        public_add(41);
    "#,
    )
    .expect("ok main source should write");
    let compiled = compile_source_file(&ok_main_path).expect("compile should succeed");
    assert!(
        compiled.functions.is_empty(),
        "pure RustScript function module should not require host imports"
    );
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(42)]);

    let bad_main_path = root.join("main_bad.rss");
    std::fs::write(
        &bad_main_path,
        r#"
        use module;
        private_add(41);
    "#,
    )
    .expect("bad main source should write");
    let err = match compile_source_file(&bad_main_path) {
        Ok(_) => panic!("private import should fail"),
        Err(err) => err,
    };
    assert!(
        matches!(
            err,
            vm::SourcePathError::Source(vm::SourceError::Parse(vm::ParseError { ref message, .. }))
            if message.contains("unknown function 'private_add'")
        ),
        "expected unknown function error, got {err:?}"
    );

    let _ = std::fs::remove_file(bad_main_path);
    let _ = std::fs::remove_file(ok_main_path);
    let _ = std::fs::remove_file(module_path);
    let _ = std::fs::remove_dir(root);
}

#[test]
fn compile_source_file_rejects_import_cycles() {
    let unique = format!(
        "vm_import_cycle_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be valid")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).expect("temp module root should be created");

    let main_path = root.join("main.rss");
    let a_path = root.join("a.rss");
    let b_path = root.join("b.rss");
    std::fs::write(&main_path, "use a;\n1;\n").expect("main source should write");
    std::fs::write(&a_path, "use b;\n").expect("module a source should write");
    std::fs::write(&b_path, "use a;\n").expect("module b source should write");

    let err = match compile_source_file(&main_path) {
        Ok(_) => panic!("cycle should fail"),
        Err(err) => err,
    };
    assert!(matches!(err, vm::SourcePathError::ImportCycle(_)));

    let _ = std::fs::remove_file(main_path);
    let _ = std::fs::remove_file(a_path);
    let _ = std::fs::remove_file(b_path);
    let _ = std::fs::remove_dir(root);
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
fn rss_function_definition_is_inlined_without_host_imports() {
    let source = r#"
        fn eq(lhs, rhs) {
            lhs == rhs;
        }
        fn is_empty(value) {
            eq(value, "");
        }
        pub fn non_empty(value) {
            eq(is_empty(value), false);
        }
        non_empty("x");
    "#;

    let compiled = compile_source(source).expect("compile should succeed");
    assert!(
        compiled.functions.is_empty(),
        "rss-defined functions should not be emitted as host imports"
    );

    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Bool(true)]);
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
        enabled: native_jit_supported(),
        hot_loop_threshold: 1,
        max_trace_len: 512,
    });

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(190)]);

    let dump = vm.dump_jit_info();
    let snapshot = vm.jit_snapshot();
    if native_jit_supported() {
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
        enabled: native_jit_supported(),
        hot_loop_threshold: 1,
        max_trace_len: 512,
    });

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(224)]);

    if native_jit_supported() {
        let dump = vm.dump_jit_info();
        assert!(dump.contains(" shl"), "expected trace dump to include shl");
    }
}

#[test]
fn trace_jit_supports_host_calls_with_native_mixed_mode() {
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
        enabled: native_jit_supported(),
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
    if native_jit_supported() {
        assert!(
            snapshot
                .attempts
                .iter()
                .any(|attempt| attempt.result.is_ok()),
            "expected at least one successful trace compile, dump:\n{dump}"
        );
        assert!(
            snapshot.traces.iter().any(|trace| trace.has_call),
            "expected at least one call-containing trace, dump:\n{dump}"
        );
        assert!(
            dump.contains(" call"),
            "expected trace dump to include call"
        );
        assert!(
            vm.jit_native_trace_count() > 0,
            "expected call trace to compile to native code"
        );
        assert!(
            vm.jit_native_exec_count() > 0,
            "expected native call trace to execute at least once"
        );
    }
}
