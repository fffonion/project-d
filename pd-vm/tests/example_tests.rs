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

    let stack = run_compiled_file(&root.join("closure.rss"));
    assert_eq!(stack, vec![Value::Int(12)]);

    let stack = run_compiled_file(&root.join("closure.js"));
    assert_eq!(stack, vec![Value::Int(12)]);

    let stack = run_compiled_file(&root.join("closure.lua"));
    assert_eq!(stack, vec![Value::Int(12)]);

    let stack = run_compiled_file(&root.join("assignment_example.rss"));
    assert_eq!(stack, vec![Value::Int(2)]);

    let stack = run_compiled_file(&root.join("assignment_example.js"));
    assert_eq!(stack, vec![Value::Int(2)]);

    let stack = run_compiled_file(&root.join("assignment_example.lua"));
    assert_eq!(stack, vec![Value::Int(2)]);
}
