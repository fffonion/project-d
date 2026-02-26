use std::hint::black_box;
use std::time::Instant;

use vm::{JitConfig, OpCode, Program, Value, Vm, VmStatus, compile_source};

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

    const LOOP_ITERS: i64 = 150_000;
    const REPETITIONS: i64 = 12;
    const TRIALS: usize = 5;
    let source = format!(
        r#"
        let i = 0;
        let sum = 0;
        while i < {LOOP_ITERS} {{
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            sum = sum + i;
            i = i + 1;
        }}
        sum;
    "#
    );

    let compiled = compile_source(&source).expect("compile should succeed");
    let expected = REPETITIONS * LOOP_ITERS * (LOOP_ITERS - 1) / 2;

    let _ = run_sum_loop_with_jit(&compiled.program, compiled.locals, false, expected);
    let _ = run_sum_loop_with_jit(&compiled.program, compiled.locals, true, expected);

    let mut interpreter_times = Vec::with_capacity(TRIALS);
    let mut jit_times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        interpreter_times.push(run_sum_loop_with_jit(
            &compiled.program,
            compiled.locals,
            false,
            expected,
        ));
        jit_times.push(run_sum_loop_with_jit(
            &compiled.program,
            compiled.locals,
            true,
            expected,
        ));
    }

    let interpreter_median = median_duration(&mut interpreter_times);
    let jit_median = median_duration(&mut jit_times);

    println!(
        "tight-loop latency median: interpreter={}ms jit={}ms",
        interpreter_median.as_millis(),
        jit_median.as_millis()
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
) -> std::time::Duration {
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

    elapsed
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
