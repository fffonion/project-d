use std::hint::black_box;
use std::time::Instant;

use vm::{
    CallOutcome, HostFunction, HostFunctionRegistry, JitConfig, OpCode, Program, Value, Vm,
    VmStatus, compile_source,
};

struct PerfNoopHost {
    _marker: u64,
}

impl HostFunction for PerfNoopHost {
    fn call(&mut self, _vm: &mut Vm, _args: &[Value]) -> Result<CallOutcome, vm::VmError> {
        Ok(CallOutcome::Return(Vec::new()))
    }
}

fn perf_noop_host_static(_vm: &mut Vm, _args: &[Value]) -> Result<CallOutcome, vm::VmError> {
    Ok(CallOutcome::Return(Vec::new()))
}

#[test]
#[ignore = "performance characterization test; run manually"]
fn perf_vm_creation_cleanup_speed_and_ram_usage() {
    let iterations = 25_000usize;
    let retained_count = 8_000usize;
    let program = Program::new(
        vec![Value::Int(1)],
        vec![OpCode::Ldc as u8, 0, 0, 0, 0, OpCode::Ret as u8],
    );

    let rss_before = current_rss_bytes();
    let started = Instant::now();
    for _ in 0..iterations {
        let vm = Vm::with_locals(program.clone(), 64);
        black_box(vm);
    }
    let elapsed = started.elapsed();
    let rss_after = current_rss_bytes();
    let per_vm_ram = rss_before
        .zip(rss_after)
        .map(|(before, after)| after.saturating_sub(before) / iterations as u64);

    println!(
        "vm creation/cleanup: iterations={iterations}, elapsed_ms={}, per_vm_ns={}",
        elapsed.as_millis(),
        elapsed.as_nanos() / iterations as u128
    );
    print_rss_delta("vm creation/cleanup", rss_before, rss_after);
    if let Some(per_vm) = per_vm_ram {
        println!(
            "vm creation/cleanup avg net rss growth per vm: {}B ({:.2} KiB)",
            per_vm,
            per_vm as f64 / 1024.0
        );
    } else {
        println!("vm creation/cleanup avg net rss growth per vm: unsupported on this platform");
    }

    let retained_rss_before = current_rss_bytes();
    let mut retained_vms = Vec::with_capacity(retained_count);
    for _ in 0..retained_count {
        retained_vms.push(Vm::with_locals(program.clone(), 64));
    }
    black_box(&retained_vms);
    let retained_rss_after = current_rss_bytes();
    print_rss_delta("vm retained batch", retained_rss_before, retained_rss_after);
    if let Some(per_vm) = retained_rss_before
        .zip(retained_rss_after)
        .map(|(before, after)| after.saturating_sub(before) / retained_count as u64)
    {
        println!(
            "vm retained avg ram per vm: {}B ({:.2} KiB) across {} retained vms",
            per_vm,
            per_vm as f64 / 1024.0,
            retained_count
        );
    } else {
        println!("vm retained avg ram per vm: unsupported on this platform");
    }

    drop(retained_vms);
    let retained_rss_after_drop = current_rss_bytes();
    print_rss_delta(
        "vm retained batch after drop",
        retained_rss_after,
        retained_rss_after_drop,
    );

    let host_import_count = 32usize;
    let host_iterations = 6_000usize;
    let host_source = build_host_import_stress_source(host_import_count);
    let host_compiled = compile_source(&host_source).expect("host import compile should succeed");
    let host_names: Vec<String> = host_compiled
        .functions
        .iter()
        .map(|func| func.name.clone())
        .collect();
    assert_eq!(host_names.len(), host_import_count);

    let plain_compiled = compile_source("let v = 1; v;").expect("plain compile should succeed");

    let plain_rss_before = current_rss_bytes();
    let plain_started = Instant::now();
    for _ in 0..host_iterations {
        let mut vm = Vm::with_locals(plain_compiled.program.clone(), plain_compiled.locals);
        let status = vm.run().expect("plain vm run should succeed");
        assert_eq!(status, VmStatus::Halted);
        black_box(vm.stack());
    }
    let plain_elapsed = plain_started.elapsed();
    let plain_rss_after = current_rss_bytes();
    print_rss_delta(
        "host overhead baseline (plain run)",
        plain_rss_before,
        plain_rss_after,
    );

    let host_rss_before = current_rss_bytes();
    let host_started = Instant::now();
    for _ in 0..host_iterations {
        let mut vm = Vm::with_locals(host_compiled.program.clone(), host_compiled.locals);
        for name in &host_names {
            vm.bind_function(name, Box::new(PerfNoopHost { _marker: 0 }));
        }
        let status = vm.run().expect("host import vm run should succeed");
        assert_eq!(status, VmStatus::Halted);
        black_box(vm.stack());
    }
    let host_elapsed = host_started.elapsed();
    let host_rss_after = current_rss_bytes();
    print_rss_delta(
        "host overhead (bind + import load)",
        host_rss_before,
        host_rss_after,
    );

    let plain_per_vm_ns = plain_elapsed.as_nanos() / host_iterations as u128;
    let host_per_vm_ns = host_elapsed.as_nanos() / host_iterations as u128;
    let overhead_per_vm_ns = host_per_vm_ns.saturating_sub(plain_per_vm_ns);
    let overhead_per_import_ns = overhead_per_vm_ns / host_import_count as u128;
    println!(
        "host register/load overhead: iterations={host_iterations}, imports_per_vm={host_import_count}, plain_per_vm_ns={plain_per_vm_ns}, host_per_vm_ns={host_per_vm_ns}, overhead_per_vm_ns={overhead_per_vm_ns}, overhead_per_import_ns={overhead_per_import_ns}",
    );

    let mut registry = HostFunctionRegistry::new();
    for name in &host_names {
        registry.register(name.clone(), 1, || Box::new(PerfNoopHost { _marker: 0 }));
    }
    let cached_plan = registry
        .prepare_plan(&host_compiled.program.imports)
        .expect("cached host plan should build");

    let cached_rss_before = current_rss_bytes();
    let cached_started = Instant::now();
    for _ in 0..host_iterations {
        let mut vm = Vm::with_locals(host_compiled.program.clone(), host_compiled.locals);
        registry
            .bind_vm_with_plan(&mut vm, &cached_plan)
            .expect("cached host binding should succeed");
        let status = vm.run().expect("cached host import vm run should succeed");
        assert_eq!(status, VmStatus::Halted);
        black_box(vm.stack());
    }
    let cached_elapsed = cached_started.elapsed();
    let cached_rss_after = current_rss_bytes();
    print_rss_delta(
        "host overhead (cached bind + cached import load)",
        cached_rss_before,
        cached_rss_after,
    );

    let cached_per_vm_ns = cached_elapsed.as_nanos() / host_iterations as u128;
    let cached_overhead_per_vm_ns = cached_per_vm_ns.saturating_sub(plain_per_vm_ns);
    let cached_overhead_per_import_ns = cached_overhead_per_vm_ns / host_import_count as u128;
    println!(
        "host cache overhead: iterations={host_iterations}, imports_per_vm={host_import_count}, plain_per_vm_ns={plain_per_vm_ns}, cached_per_vm_ns={cached_per_vm_ns}, overhead_per_vm_ns={cached_overhead_per_vm_ns}, overhead_per_import_ns={cached_overhead_per_import_ns}",
    );

    let mut static_registry = HostFunctionRegistry::new();
    for name in &host_names {
        static_registry.register_static(name.clone(), 1, perf_noop_host_static);
    }
    let static_cached_plan = static_registry
        .prepare_plan(&host_compiled.program.imports)
        .expect("static host plan should build");

    let static_cached_rss_before = current_rss_bytes();
    let static_cached_started = Instant::now();
    for _ in 0..host_iterations {
        let mut vm = Vm::with_locals(host_compiled.program.clone(), host_compiled.locals);
        static_registry
            .bind_vm_with_plan(&mut vm, &static_cached_plan)
            .expect("cached static host binding should succeed");
        let status = vm.run().expect("cached static host vm run should succeed");
        assert_eq!(status, VmStatus::Halted);
        black_box(vm.stack());
    }
    let static_cached_elapsed = static_cached_started.elapsed();
    let static_cached_rss_after = current_rss_bytes();
    print_rss_delta(
        "host overhead (cached static fn ptr)",
        static_cached_rss_before,
        static_cached_rss_after,
    );

    let static_cached_per_vm_ns = static_cached_elapsed.as_nanos() / host_iterations as u128;
    let static_cached_overhead_per_vm_ns = static_cached_per_vm_ns.saturating_sub(plain_per_vm_ns);
    let static_cached_overhead_per_import_ns =
        static_cached_overhead_per_vm_ns / host_import_count as u128;
    println!(
        "host static fn ptr overhead: iterations={host_iterations}, imports_per_vm={host_import_count}, plain_per_vm_ns={plain_per_vm_ns}, static_cached_per_vm_ns={static_cached_per_vm_ns}, overhead_per_vm_ns={static_cached_overhead_per_vm_ns}, overhead_per_import_ns={static_cached_overhead_per_import_ns}",
    );
}

#[test]
#[ignore = "performance characterization test; run manually"]
fn perf_compiler_speed_and_ram_usage() {
    let iterations = 200usize;
    let source = build_compiler_stress_source(1_000);

    let rss_before = current_rss_bytes();
    let started = Instant::now();
    for _ in 0..iterations {
        let compiled = compile_source(&source).expect("compile should succeed");
        black_box(compiled.locals);
    }
    let elapsed = started.elapsed();
    let per_compile_us = elapsed.as_micros() / iterations as u128;

    println!(
        "compiler perf: iterations={iterations}, elapsed_ms={}, per_compile_us={}",
        elapsed.as_millis(),
        per_compile_us
    );
    print_rss_delta("compiler", rss_before, current_rss_bytes());
}

#[test]
fn jit_emitted_machine_code_is_executed_on_x86_64() {
    let source = r#"
        let i = 0;
        let sum = 0;
        while i < 200 {
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
        max_trace_len: 1_024,
    });

    let status = vm.run().expect("vm should run");
    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(19_900)]);

    if cfg!(target_arch = "x86_64") {
        let native_trace_count = vm.jit_native_trace_count();
        let native_exec_count = vm.jit_native_exec_count();
        let dump = vm.dump_jit_info();

        assert!(
            native_trace_count > 0,
            "expected native traces > 0, dump:\n{dump}"
        );
        assert!(
            native_exec_count > 0,
            "expected native execution count > 0, dump:\n{dump}"
        );
        assert!(dump.contains("native trace#"), "missing native trace entry");
        assert!(dump.contains("code:"), "missing native machine code bytes");
    }
}

#[test]
#[ignore = "performance characterization test; run manually"]
fn perf_jit_native_reduces_tight_loop_latency() {
    if !cfg!(target_arch = "x86_64") {
        println!("skipping latency comparison on non-x86_64 target");
        return;
    }

    const INNER_LOOP_ITERS: i64 = 40_000;
    const OUTER_LOOPS: i64 = 8;
    const TRIALS: usize = 7;
    let source = format!(
        r#"
        let outer = 0;
        let i = 0;
        let sum = 0;
        while outer < {OUTER_LOOPS} {{
            i = 0;
            while i < {INNER_LOOP_ITERS} {{
                let a = i + 7;
                let b = a - 3;
                let c = b * 8;
                let d = c / 8;
                let e = d + i;
                let n = 0 - e;
                let p = 0 - n;
                sum = sum + p;
                i = i + 1;
            }}
            outer = outer + 1;
        }}
        sum;
    "#
    );

    let compiled = compile_source(&source).expect("compile should succeed");
    let expected_per_outer = INNER_LOOP_ITERS * INNER_LOOP_ITERS + 3 * INNER_LOOP_ITERS;
    let expected = OUTER_LOOPS * expected_per_outer;
    let expected_stack = vec![Value::Int(expected)];

    let warmup_interpreter =
        run_sum_loop_with_jit(&compiled.program, compiled.locals, false, expected);
    let warmup_jit = run_sum_loop_with_jit(&compiled.program, compiled.locals, true, expected);
    assert_eq!(warmup_interpreter.stack, expected_stack);
    assert_eq!(warmup_jit.stack, warmup_interpreter.stack);

    let mut interpreter_times = Vec::with_capacity(TRIALS);
    let mut jit_times = Vec::with_capacity(TRIALS);
    for trial in 0..TRIALS {
        let interpreter_run =
            run_sum_loop_with_jit(&compiled.program, compiled.locals, false, expected);
        let jit_run = run_sum_loop_with_jit(&compiled.program, compiled.locals, true, expected);

        assert_eq!(
            interpreter_run.stack, expected_stack,
            "interpreter result mismatch on trial {trial}",
        );
        assert_eq!(
            jit_run.stack, interpreter_run.stack,
            "native JIT result mismatch on trial {trial}",
        );

        interpreter_times.push(interpreter_run.elapsed);
        jit_times.push(jit_run.elapsed);
    }

    let interpreter_median = median_duration(&mut interpreter_times);
    let jit_median = median_duration(&mut jit_times);

    println!(
        "tight-loop latency median: interpreter={}ms jit={}ms speedup={:.2}x",
        interpreter_median.as_millis(),
        jit_median.as_millis(),
        interpreter_median.as_secs_f64() / jit_median.as_secs_f64(),
    );

    assert!(
        jit_median < interpreter_median,
        "expected JIT median latency to be lower (interpreter={:?}, jit={:?})",
        interpreter_median,
        jit_median
    );
}

fn run_sum_loop_with_jit(
    program: &Program,
    local_count: usize,
    enable_jit: bool,
    expected: i64,
) -> PerfRun {
    let mut vm = Vm::with_locals(program.clone(), local_count);
    vm.set_jit_config(JitConfig {
        enabled: enable_jit,
        hot_loop_threshold: 1,
        max_trace_len: 1_024,
    });

    let started = Instant::now();
    let status = vm.run().expect("vm should run");
    let elapsed = started.elapsed();

    assert_eq!(status, VmStatus::Halted);
    assert_eq!(vm.stack(), &[Value::Int(expected)]);

    if enable_jit {
        assert!(
            vm.jit_native_trace_count() > 0,
            "expected native trace count > 0"
        );
        assert!(
            vm.jit_native_exec_count() > 0,
            "expected native exec count > 0"
        );
    }

    PerfRun {
        elapsed,
        stack: vm.stack().to_vec(),
    }
}

struct PerfRun {
    elapsed: std::time::Duration,
    stack: Vec<Value>,
}

fn median_duration(samples: &mut [std::time::Duration]) -> std::time::Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn build_compiler_stress_source(line_count: usize) -> String {
    let mut source = String::from(
        r#"
        let i = 0;
        let sum = 0;
    "#,
    );
    for _ in 0..line_count {
        source.push_str("sum = sum + 1;\n");
    }
    source.push_str("sum;\n");
    source
}

fn build_host_import_stress_source(import_count: usize) -> String {
    let mut source = String::new();
    for index in 0..import_count {
        source.push_str(&format!("fn host_{index}(x);\n"));
    }
    source.push_str("let v = 1;\n");
    source.push_str("v;\n");
    source
}

fn print_rss_delta(label: &str, before: Option<u64>, rss_after: Option<u64>) {
    match (before, rss_after) {
        (Some(b), Some(a)) => {
            let delta = a as i128 - b as i128;
            println!("{label} rss: before={b}B after={a}B delta={delta}B");
        }
        _ => {
            println!("{label} rss: unsupported on this platform");
        }
    }
}

#[cfg(target_os = "windows")]
fn current_rss_bytes() -> Option<u64> {
    use std::mem::size_of;
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    let process = unsafe { GetCurrentProcess() };
    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    let ok = unsafe {
        K32GetProcessMemoryInfo(
            process,
            &mut counters,
            size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    };
    if ok == 0 {
        return None;
    }
    Some(counters.WorkingSetSize as u64)
}

#[cfg(unix)]
fn current_rss_bytes() -> Option<u64> {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage as *mut libc::rusage) };
    if rc != 0 {
        return None;
    }
    #[cfg(target_os = "macos")]
    {
        Some(usage.ru_maxrss as u64)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Some((usage.ru_maxrss as u64).saturating_mul(1024))
    }
}

#[cfg(not(any(unix, target_os = "windows")))]
fn current_rss_bytes() -> Option<u64> {
    None
}
