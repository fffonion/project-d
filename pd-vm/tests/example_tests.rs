use std::path::Path;

use vm::{CallOutcome, FunctionDecl, HostFunction, Value, Vm, VmStatus, compile_source_file};

struct PrintFunction;
struct AddOneFunction;

impl HostFunction for PrintFunction {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        Ok(CallOutcome::Return(args.to_vec()))
    }
}

impl HostFunction for AddOneFunction {
    fn call(&mut self, _vm: &mut Vm, args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        let value = match args.first() {
            Some(Value::Int(value)) => *value,
            _ => return Err(vm::VmError::TypeMismatch("int")),
        };
        Ok(CallOutcome::Return(vec![Value::Int(value + 1)]))
    }
}

fn register_functions(vm: &mut Vm, functions: &[FunctionDecl]) {
    for decl in functions {
        match decl.name.as_str() {
            "print" => {
                vm.bind_function("print", Box::new(PrintFunction));
            }
            "add_one" => {
                vm.bind_function("add_one", Box::new(AddOneFunction));
            }
            other => panic!("unknown function '{other}'"),
        }
    }
}

fn run_compiled_file(path: &Path) -> Vec<Value> {
    let compiled = compile_source_file(path).expect("compile should succeed");
    let mut vm = Vm::with_locals(compiled.program, compiled.locals);
    register_functions(&mut vm, &compiled.functions);
    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    vm.stack().to_vec()
}

#[test]
fn examples_run() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");

    let stack = run_compiled_file(&root.join("example.rss"));
    assert_eq!(stack, vec![Value::Int(6)]);

    let stack = run_compiled_file(&root.join("example.js"));
    assert_eq!(stack, vec![Value::Int(6)]);

    let stack = run_compiled_file(&root.join("example.lua"));
    assert_eq!(stack, vec![Value::Int(6)]);

    let stack = run_compiled_file(&root.join("example.scm"));
    assert_eq!(stack, vec![Value::Int(6)]);

    // Feature examples for each frontend flavor.
    let stack = run_compiled_file(&root.join("example_complex.rss"));
    assert_eq!(stack, vec![Value::Int(12)]);

    let stack = run_compiled_file(&root.join("example_complex.js"));
    assert_eq!(stack, vec![Value::Int(12)]);

    let stack = run_compiled_file(&root.join("example_complex.lua"));
    assert_eq!(stack, vec![Value::Int(12)]);

    let stack = run_compiled_file(&root.join("example_complex.scm"));
    assert_eq!(stack, vec![Value::Int(12)]);

    // AES fixture should run in regular test mode as well (not only perf tests).
    let stack = run_compiled_file(&root.join("aes_128_cbc.rss"));
    assert_eq!(
        stack,
        vec![
            Value::Int(0),
            Value::Int(0x76),
            Value::Int(0x49),
            Value::Int(0xAB),
            Value::Int(0xAC),
            Value::Int(0x81),
            Value::Int(0x19),
            Value::Int(0xB2),
            Value::Int(0x46),
            Value::Int(0xCE),
            Value::Int(0xE9),
            Value::Int(0x8E),
            Value::Int(0x9B),
            Value::Int(0x12),
            Value::Int(0xE9),
            Value::Int(0x19),
            Value::Int(0x7D),
        ]
    );
}
