use super::super::{Program, Value, Vm, VmError, VmResult};
use super::{NativeBackend, STATUS_ERROR, STATUS_HALTED, STATUS_TRACE_EXIT};
use std::sync::OnceLock;

pub(super) struct X86_64Backend;

impl NativeBackend for X86_64Backend {
    type ExecutableMemory = BackendExecutableMemory;

    fn emit_trace_bytes(trace: &crate::jit::JitTrace) -> VmResult<Vec<u8>> {
        emit_native_trace_bytes(trace)
    }

    fn executable_memory_from_code(code: &[u8]) -> VmResult<Self::ExecutableMemory> {
        BackendExecutableMemory::from_code(code)
    }

    fn executable_memory_ptr(memory: &Self::ExecutableMemory) -> *mut u8 {
        memory.ptr
    }

    fn clear_bridge_error() {
        clear_bridge_error();
    }

    fn take_bridge_error() -> Option<VmError> {
        take_bridge_error()
    }
}

pub(super) struct BackendExecutableMemory {
    pub(super) ptr: *mut u8,
    len: usize,
}

impl BackendExecutableMemory {
    fn from_code(code: &[u8]) -> VmResult<Self> {
        let len = code.len();
        if len == 0 {
            return Err(VmError::JitNative(
                "cannot create executable region for empty code".to_string(),
            ));
        }
        let ptr = alloc_executable_region(len)?;
        unsafe {
            std::ptr::copy_nonoverlapping(code.as_ptr(), ptr, len);
        }
        Ok(Self { ptr, len })
    }
}

impl Drop for BackendExecutableMemory {
    fn drop(&mut self) {
        let _ = free_executable_region(self.ptr, self.len);
    }
}

#[derive(Clone, Copy)]
struct VecLayout {
    ptr_offset: i32,
    len_offset: i32,
    cap_offset: i32,
}

#[derive(Clone, Copy)]
struct ValueLayout {
    size: i32,
    tag_offset: i32,
    tag_size: u8,
    int_tag: u32,
    float_tag: u32,
    bool_tag: u32,
    string_tag: u32,
    int_payload_offset: i32,
    float_payload_offset: i32,
    bool_payload_offset: i32,
}

#[derive(Clone, Copy)]
struct NativeStackLayout {
    vm_stack_offset: i32,
    vm_locals_offset: i32,
    vm_program_offset: i32,
    vm_ip_offset: i32,
    program_constants_offset: i32,
    stack_vec: VecLayout,
    value: ValueLayout,
}

static NATIVE_STACK_LAYOUT: OnceLock<Result<NativeStackLayout, String>> = OnceLock::new();

fn emit_native_trace_bytes(trace: &crate::jit::JitTrace) -> VmResult<Vec<u8>> {
    let mut code = Vec::with_capacity(512);
    let mut jump_patches: Vec<usize> = Vec::new();
    let layout = detect_native_stack_layout()?;

    emit_native_prologue(&mut code);

    let steps = &trace.steps;
    let mut step_index = 0usize;
    while step_index < steps.len() {
        match &steps[step_index] {
            crate::jit::TraceStep::Nop => {}
            crate::jit::TraceStep::Ldc(index) => {
                emit_native_step_ldc_inline(&mut code, layout, *index)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Add => {
                emit_native_step_binary_numeric_inline(
                    &mut code,
                    layout,
                    NativeBinaryNumericOp::Add,
                )?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Sub => {
                emit_native_step_binary_numeric_inline(
                    &mut code,
                    layout,
                    NativeBinaryNumericOp::Sub,
                )?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Mul => {
                emit_native_step_binary_numeric_inline(
                    &mut code,
                    layout,
                    NativeBinaryNumericOp::Mul,
                )?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Div => {
                emit_native_step_binary_numeric_inline(
                    &mut code,
                    layout,
                    NativeBinaryNumericOp::Div,
                )?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Shl => {
                emit_native_step_shift_inline(&mut code, layout, true)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Shr => {
                emit_native_step_shift_inline(&mut code, layout, false)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Neg => {
                emit_native_step_neg_inline(&mut code, layout)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Ceq => {
                emit_native_step_ceq_inline(&mut code, layout)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Clt => {
                emit_native_step_binary_numeric_inline(
                    &mut code,
                    layout,
                    NativeBinaryNumericOp::Clt,
                )?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Cgt => {
                emit_native_step_binary_numeric_inline(
                    &mut code,
                    layout,
                    NativeBinaryNumericOp::Cgt,
                )?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Pop => {
                emit_native_step_pop_inline(&mut code, layout)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Dup => {
                emit_native_step_dup_inline(&mut code, layout)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Ldloc(index) => {
                emit_native_step_ldloc_inline(&mut code, layout, *index)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Stloc(index) => {
                emit_native_step_stloc_inline(&mut code, layout, *index)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::GuardFalse { exit_ip } => {
                let exit_ip = u32::try_from(*exit_ip).map_err(|_| {
                    VmError::JitNative(format!(
                        "guard exit ip {} exceeds u32 immediate range",
                        exit_ip
                    ))
                })?;
                emit_native_step_guard_false_inline(&mut code, layout, exit_ip)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::JumpToRoot => {
                let root_ip = u32::try_from(trace.root_ip).map_err(|_| {
                    VmError::JitNative(format!(
                        "trace root ip {} exceeds u32 immediate range",
                        trace.root_ip
                    ))
                })?;
                emit_native_step_jump_to_root_inline(&mut code, layout, root_ip)?;
                emit_native_status_check(&mut code, &mut jump_patches);
            }
            crate::jit::TraceStep::Ret => {
                emit_native_step_ret_inline(&mut code);
                emit_native_status_check(&mut code, &mut jump_patches);
            }
        }
        step_index += 1;
    }

    code.extend_from_slice(&[0x31, 0xC0]); // xor eax, eax

    let return_label = code.len();
    for disp_offset in jump_patches {
        let rel = (return_label as i64) - ((disp_offset + 4) as i64);
        let rel = i32::try_from(rel)
            .map_err(|_| VmError::JitNative("native patch displacement overflow".to_string()))?;
        code[disp_offset..disp_offset + 4].copy_from_slice(&rel.to_le_bytes());
    }

    emit_native_epilogue(&mut code);
    Ok(code)
}

fn emit_native_prologue(code: &mut Vec<u8>) {
    code.push(0x53); // push rbx
    #[cfg(target_os = "windows")]
    {
        code.extend_from_slice(&[0x48, 0x89, 0xCB]); // mov rbx, rcx
        code.extend_from_slice(&[0x48, 0x83, 0xEC, 0x20]); // sub rsp, 32
    }
    #[cfg(not(target_os = "windows"))]
    {
        code.extend_from_slice(&[0x48, 0x89, 0xFB]); // mov rbx, rdi
    }
}

fn emit_native_epilogue(code: &mut Vec<u8>) {
    #[cfg(target_os = "windows")]
    {
        code.extend_from_slice(&[0x48, 0x83, 0xC4, 0x20]); // add rsp, 32
    }
    code.push(0x5B); // pop rbx
    code.push(0xC3); // ret
}

#[derive(Clone, Copy)]
enum NativeBinaryNumericOp {
    Add,
    Sub,
    Mul,
    Div,
    Clt,
    Cgt,
}

fn detect_native_stack_layout() -> VmResult<NativeStackLayout> {
    let cached = NATIVE_STACK_LAYOUT
        .get_or_init(|| detect_native_stack_layout_uncached().map_err(layout_probe_error_message));
    match cached {
        Ok(layout) => Ok(*layout),
        Err(message) => Err(VmError::JitNative(message.clone())),
    }
}

fn detect_native_stack_layout_uncached() -> VmResult<NativeStackLayout> {
    let vm_stack_offset = usize_to_i32(std::mem::offset_of!(Vm, stack), "Vm::stack offset")?;
    let vm_locals_offset = usize_to_i32(std::mem::offset_of!(Vm, locals), "Vm::locals offset")?;
    let vm_program_offset = usize_to_i32(std::mem::offset_of!(Vm, program), "Vm::program offset")?;
    let vm_ip_offset = usize_to_i32(std::mem::offset_of!(Vm, ip), "Vm::ip offset")?;
    let program_constants_offset = usize_to_i32(
        std::mem::offset_of!(Program, constants),
        "Program::constants offset",
    )?;
    let stack_vec = detect_vec_layout()?;
    let value = detect_value_layout()?;
    Ok(NativeStackLayout {
        vm_stack_offset,
        vm_locals_offset,
        vm_program_offset,
        vm_ip_offset,
        program_constants_offset,
        stack_vec,
        value,
    })
}

fn layout_probe_error_message(error: VmError) -> String {
    match error {
        VmError::JitNative(message) => message,
        other => other.to_string(),
    }
}

fn emit_jcc_rel32(code: &mut Vec<u8>, opcodes: [u8; 2]) -> usize {
    code.extend_from_slice(&opcodes);
    let disp = code.len();
    code.extend_from_slice(&[0, 0, 0, 0]);
    disp
}

fn emit_jmp_rel32(code: &mut Vec<u8>) -> usize {
    code.push(0xE9);
    let disp = code.len();
    code.extend_from_slice(&[0, 0, 0, 0]);
    disp
}

fn patch_rel32(code: &mut [u8], disp_offset: usize, target: usize) -> VmResult<()> {
    let rel = (target as i64) - ((disp_offset + 4) as i64);
    let rel = i32::try_from(rel)
        .map_err(|_| VmError::JitNative("native patch displacement overflow".to_string()))?;
    code[disp_offset..disp_offset + 4].copy_from_slice(&rel.to_le_bytes());
    Ok(())
}

fn emit_status_continue(code: &mut Vec<u8>) {
    code.extend_from_slice(&[0x31, 0xC0]); // xor eax, eax
}

fn emit_status_error(code: &mut Vec<u8>) {
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&STATUS_ERROR.to_le_bytes());
}

fn emit_stack_binary_setup(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    min_len: u8,
) -> VmResult<()> {
    let stack_len_offset = checked_add_i32(
        layout.vm_stack_offset,
        layout.stack_vec.len_offset,
        "stack len offset overflow",
    )?;
    let stack_ptr_offset = checked_add_i32(
        layout.vm_stack_offset,
        layout.stack_vec.ptr_offset,
        "stack ptr offset overflow",
    )?;
    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32]
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x83, 0xF9, min_len]); // cmp rcx, min_len
    let short_stack = emit_jcc_rel32(code, [0x0F, 0x82]); // jb
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32]
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x41, 0xFF]); // lea rax, [rcx-1]
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x34, 0x02]); // lea rsi, [rdx+rax]
    code.extend_from_slice(&[0x48, 0x2D]); // sub rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax]
    let ready = emit_jmp_rel32(code);
    let short_stack_label = code.len();
    emit_status_error(code);
    let short_stack_done = emit_jmp_rel32(code);
    let end = code.len();
    patch_rel32(code, short_stack, short_stack_label)?;
    patch_rel32(code, ready, end)?;
    patch_rel32(code, short_stack_done, end)?;
    Ok(())
}

fn emit_stack_top_setup(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    min_len: u8,
) -> VmResult<()> {
    let stack_len_offset = checked_add_i32(
        layout.vm_stack_offset,
        layout.stack_vec.len_offset,
        "stack len offset overflow",
    )?;
    let stack_ptr_offset = checked_add_i32(
        layout.vm_stack_offset,
        layout.stack_vec.ptr_offset,
        "stack ptr offset overflow",
    )?;
    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32]
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x83, 0xF9, min_len]); // cmp rcx, min_len
    let short_stack = emit_jcc_rel32(code, [0x0F, 0x82]); // jb
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32]
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x41, 0xFF]); // lea rax, [rcx-1]
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax]
    let ready = emit_jmp_rel32(code);
    let short_stack_label = code.len();
    emit_status_error(code);
    let short_stack_done = emit_jmp_rel32(code);
    let end = code.len();
    patch_rel32(code, short_stack, short_stack_label)?;
    patch_rel32(code, ready, end)?;
    patch_rel32(code, short_stack_done, end)?;
    Ok(())
}

fn emit_adjust_stack_len_minus_one(code: &mut Vec<u8>, stack_len_offset: i32) {
    code.extend_from_slice(&[0x48, 0x8B, 0x83]); // mov rax, [rbx+disp32]
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0xFF, 0xC8]); // dec rax
    code.extend_from_slice(&[0x48, 0x89, 0x83]); // mov [rbx+disp32], rax
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
}

fn emit_load_tag_eax_from_rdi(code: &mut Vec<u8>, layout: ValueLayout) -> VmResult<()> {
    match layout.tag_size {
        1 => {
            code.extend_from_slice(&[0x0F, 0xB6, 0x87]); // movzx eax, byte [rdi+disp32]
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
        }
        2 => {
            code.extend_from_slice(&[0x0F, 0xB7, 0x87]); // movzx eax, word [rdi+disp32]
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
        }
        4 => {
            code.extend_from_slice(&[0x8B, 0x87]); // mov eax, dword [rdi+disp32]
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
        }
        other => {
            return Err(VmError::JitNative(format!(
                "unsupported native tag width {}",
                other
            )));
        }
    }
    Ok(())
}

fn emit_load_tag_edx_from_rsi(code: &mut Vec<u8>, layout: ValueLayout) -> VmResult<()> {
    match layout.tag_size {
        1 => {
            code.extend_from_slice(&[0x0F, 0xB6, 0x96]); // movzx edx, byte [rsi+disp32]
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
        }
        2 => {
            code.extend_from_slice(&[0x0F, 0xB7, 0x96]); // movzx edx, word [rsi+disp32]
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
        }
        4 => {
            code.extend_from_slice(&[0x8B, 0x96]); // mov edx, dword [rsi+disp32]
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
        }
        other => {
            return Err(VmError::JitNative(format!(
                "unsupported native tag width {}",
                other
            )));
        }
    }
    Ok(())
}

fn emit_store_tag_rdi(code: &mut Vec<u8>, layout: ValueLayout, tag: u32) -> VmResult<()> {
    match layout.tag_size {
        1 => {
            let tag = u8::try_from(tag).map_err(|_| {
                VmError::JitNative("native value tag out of byte range".to_string())
            })?;
            code.extend_from_slice(&[0xC6, 0x87]); // mov byte [rdi+disp32], imm8
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
            code.push(tag);
        }
        2 => {
            let tag = u16::try_from(tag).map_err(|_| {
                VmError::JitNative("native value tag out of word range".to_string())
            })?;
            code.extend_from_slice(&[0x66, 0xC7, 0x87]); // mov word [rdi+disp32], imm16
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
            code.extend_from_slice(&tag.to_le_bytes());
        }
        4 => {
            code.extend_from_slice(&[0xC7, 0x87]); // mov dword [rdi+disp32], imm32
            code.extend_from_slice(&layout.tag_offset.to_le_bytes());
            code.extend_from_slice(&tag.to_le_bytes());
        }
        other => {
            return Err(VmError::JitNative(format!(
                "unsupported native tag width {}",
                other
            )));
        }
    }
    Ok(())
}

fn emit_store_bool_from_al(code: &mut Vec<u8>, layout: ValueLayout) -> VmResult<()> {
    emit_store_tag_rdi(code, layout, layout.bool_tag)?;
    code.extend_from_slice(&[0x88, 0x87]); // mov [rdi+disp32], al
    code.extend_from_slice(&layout.bool_payload_offset.to_le_bytes());
    Ok(())
}

fn vec_ptr_disp(vec_base_offset: i32, vec_layout: VecLayout) -> VmResult<i32> {
    checked_add_i32(
        vec_base_offset,
        vec_layout.ptr_offset,
        "vec ptr offset overflow",
    )
}

fn vec_len_disp(vec_base_offset: i32, vec_layout: VecLayout) -> VmResult<i32> {
    checked_add_i32(
        vec_base_offset,
        vec_layout.len_offset,
        "vec len offset overflow",
    )
}

fn vec_cap_disp(vec_base_offset: i32, vec_layout: VecLayout) -> VmResult<i32> {
    checked_add_i32(
        vec_base_offset,
        vec_layout.cap_offset,
        "vec cap offset overflow",
    )
}

fn emit_copy_value_rsi_to_rdi(code: &mut Vec<u8>, value_layout: ValueLayout) -> VmResult<()> {
    let mut copied = 0i32;
    while copied + 8 <= value_layout.size {
        code.extend_from_slice(&[0x48, 0x8B, 0x86]); // mov rax, [rsi+disp32]
        code.extend_from_slice(&copied.to_le_bytes());
        code.extend_from_slice(&[0x48, 0x89, 0x87]); // mov [rdi+disp32], rax
        code.extend_from_slice(&copied.to_le_bytes());
        copied += 8;
    }
    let remaining = value_layout.size - copied;
    if remaining >= 4 {
        code.extend_from_slice(&[0x8B, 0x86]); // mov eax, [rsi+disp32]
        code.extend_from_slice(&copied.to_le_bytes());
        code.extend_from_slice(&[0x89, 0x87]); // mov [rdi+disp32], eax
        code.extend_from_slice(&copied.to_le_bytes());
        copied += 4;
    }
    if value_layout.size - copied >= 2 {
        code.extend_from_slice(&[0x0F, 0xB7, 0x86]); // movzx eax, word [rsi+disp32]
        code.extend_from_slice(&copied.to_le_bytes());
        code.extend_from_slice(&[0x66, 0x89, 0x87]); // mov [rdi+disp32], ax
        code.extend_from_slice(&copied.to_le_bytes());
        copied += 2;
    }
    if value_layout.size - copied >= 1 {
        code.extend_from_slice(&[0x0F, 0xB6, 0x86]); // movzx eax, byte [rsi+disp32]
        code.extend_from_slice(&copied.to_le_bytes());
        code.extend_from_slice(&[0x88, 0x87]); // mov [rdi+disp32], al
        code.extend_from_slice(&copied.to_le_bytes());
    }
    Ok(())
}

fn emit_native_step_binary_numeric_inline(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    op: NativeBinaryNumericOp,
) -> VmResult<()> {
    let stack_len_offset = checked_add_i32(
        layout.vm_stack_offset,
        layout.stack_vec.len_offset,
        "stack len offset overflow",
    )?;
    emit_stack_binary_setup(code, layout, 2)?;

    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let lhs_not_int = emit_jcc_rel32(code, [0x0F, 0x85]); // jne
    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let rhs_not_int = emit_jcc_rel32(code, [0x0F, 0x85]); // jne

    match op {
        NativeBinaryNumericOp::Add => {
            code.extend_from_slice(&[0x48, 0x8B, 0x86]); // mov rax, [rsi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x01, 0x87]); // add [rdi+disp32], rax
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            emit_store_tag_rdi(code, layout.value, layout.value.int_tag)?;
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
        NativeBinaryNumericOp::Sub => {
            code.extend_from_slice(&[0x48, 0x8B, 0x87]); // mov rax, [rdi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x2B, 0x86]); // sub rax, [rsi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x89, 0x87]); // mov [rdi+disp32], rax
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            emit_store_tag_rdi(code, layout.value, layout.value.int_tag)?;
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
        NativeBinaryNumericOp::Mul => {
            code.extend_from_slice(&[0x48, 0x8B, 0x87]); // mov rax, [rdi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x0F, 0xAF, 0x86]); // imul rax, [rsi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x89, 0x87]); // mov [rdi+disp32], rax
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            emit_store_tag_rdi(code, layout.value, layout.value.int_tag)?;
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
        NativeBinaryNumericOp::Div => {
            code.extend_from_slice(&[0x48, 0x83, 0xBE]); // cmp qword [rsi+disp32], imm8
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.push(0x00);
            let int_div_zero = emit_jcc_rel32(code, [0x0F, 0x84]); // je
            code.extend_from_slice(&[0x48, 0x8B, 0x87]); // mov rax, [rdi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x99]); // cqo
            code.extend_from_slice(&[0x48, 0xF7, 0xBE]); // idiv qword [rsi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x89, 0x87]); // mov [rdi+disp32], rax
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            emit_store_tag_rdi(code, layout.value, layout.value.int_tag)?;
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
            let int_ok = emit_jmp_rel32(code);
            let int_div_zero_label = code.len();
            emit_status_error(code);
            let int_div_zero_done = emit_jmp_rel32(code);
            let int_div_end = code.len();
            patch_rel32(code, int_div_zero, int_div_zero_label)?;
            patch_rel32(code, int_ok, int_div_end)?;
            patch_rel32(code, int_div_zero_done, int_div_end)?;
        }
        NativeBinaryNumericOp::Clt | NativeBinaryNumericOp::Cgt => {
            code.extend_from_slice(&[0x48, 0x8B, 0x87]); // mov rax, [rdi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            code.extend_from_slice(&[0x48, 0x3B, 0x86]); // cmp rax, [rsi+disp32]
            code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
            if matches!(op, NativeBinaryNumericOp::Clt) {
                code.extend_from_slice(&[0x0F, 0x9C, 0xC0]); // setl al
            } else {
                code.extend_from_slice(&[0x0F, 0x9F, 0xC0]); // setg al
            }
            emit_store_bool_from_al(code, layout.value)?;
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
    }
    let int_done = emit_jmp_rel32(code);

    let float_dispatch = code.len();
    patch_rel32(code, lhs_not_int, float_dispatch)?;
    patch_rel32(code, rhs_not_int, float_dispatch)?;

    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let lhs_is_int = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x3D]); // cmp eax, float_tag
    code.extend_from_slice(&layout.value.float_tag.to_le_bytes());
    let lhs_not_float = emit_jcc_rel32(code, [0x0F, 0x85]); // jne

    let lhs_float = code.len();
    code.extend_from_slice(&[0xF2, 0x0F, 0x10, 0x87]); // movsd xmm0, [rdi+disp32]
    code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
    let lhs_float_ready = emit_jmp_rel32(code);

    let lhs_int = code.len();
    code.extend_from_slice(&[0x48, 0x8B, 0x87]); // mov rax, [rdi+disp32]
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0xF2, 0x48, 0x0F, 0x2A, 0xC0]); // cvtsi2sd xmm0, rax

    let rhs_dispatch = code.len();
    patch_rel32(code, lhs_is_int, lhs_int)?;
    patch_rel32(code, lhs_not_float, rhs_dispatch)?;
    patch_rel32(code, lhs_float_ready, rhs_dispatch)?;
    let _ = lhs_float;

    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let rhs_is_int = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, float_tag
    code.extend_from_slice(&layout.value.float_tag.to_le_bytes());
    let rhs_not_float = emit_jcc_rel32(code, [0x0F, 0x85]); // jne
    code.extend_from_slice(&[0xF2, 0x0F, 0x10, 0x8E]); // movsd xmm1, [rsi+disp32]
    code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
    let rhs_ready = emit_jmp_rel32(code);

    let rhs_int = code.len();
    code.extend_from_slice(&[0x48, 0x8B, 0x86]); // mov rax, [rsi+disp32]
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0xF2, 0x48, 0x0F, 0x2A, 0xC8]); // cvtsi2sd xmm1, rax
    let rhs_int_ready = emit_jmp_rel32(code);

    let float_body = code.len();
    patch_rel32(code, rhs_is_int, rhs_int)?;
    patch_rel32(code, rhs_not_float, float_body)?;
    patch_rel32(code, rhs_ready, float_body)?;
    patch_rel32(code, rhs_int_ready, float_body)?;

    match op {
        NativeBinaryNumericOp::Add => {
            code.extend_from_slice(&[0xF2, 0x0F, 0x58, 0xC1]); // addsd xmm0, xmm1
            emit_store_tag_rdi(code, layout.value, layout.value.float_tag)?;
            code.extend_from_slice(&[0xF2, 0x0F, 0x11, 0x87]); // movsd [rdi+disp32], xmm0
            code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
        NativeBinaryNumericOp::Sub => {
            code.extend_from_slice(&[0xF2, 0x0F, 0x5C, 0xC1]); // subsd xmm0, xmm1
            emit_store_tag_rdi(code, layout.value, layout.value.float_tag)?;
            code.extend_from_slice(&[0xF2, 0x0F, 0x11, 0x87]); // movsd [rdi+disp32], xmm0
            code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
        NativeBinaryNumericOp::Mul => {
            code.extend_from_slice(&[0xF2, 0x0F, 0x59, 0xC1]); // mulsd xmm0, xmm1
            emit_store_tag_rdi(code, layout.value, layout.value.float_tag)?;
            code.extend_from_slice(&[0xF2, 0x0F, 0x11, 0x87]); // movsd [rdi+disp32], xmm0
            code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
        NativeBinaryNumericOp::Div => {
            code.extend_from_slice(&[0x66, 0x0F, 0x57, 0xD2]); // xorpd xmm2, xmm2
            code.extend_from_slice(&[0x66, 0x0F, 0x2E, 0xCA]); // ucomisd xmm1, xmm2
            let float_div_zero = emit_jcc_rel32(code, [0x0F, 0x84]); // je
            code.extend_from_slice(&[0xF2, 0x0F, 0x5E, 0xC1]); // divsd xmm0, xmm1
            emit_store_tag_rdi(code, layout.value, layout.value.float_tag)?;
            code.extend_from_slice(&[0xF2, 0x0F, 0x11, 0x87]); // movsd [rdi+disp32], xmm0
            code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
            let div_ok = emit_jmp_rel32(code);
            let div_zero_label = code.len();
            emit_status_error(code);
            let div_zero_done = emit_jmp_rel32(code);
            let div_end = code.len();
            patch_rel32(code, float_div_zero, div_zero_label)?;
            patch_rel32(code, div_ok, div_end)?;
            patch_rel32(code, div_zero_done, div_end)?;
        }
        NativeBinaryNumericOp::Clt | NativeBinaryNumericOp::Cgt => {
            code.extend_from_slice(&[0x66, 0x0F, 0x2E, 0xC1]); // ucomisd xmm0, xmm1
            code.extend_from_slice(&[0x0F, 0x9A, 0xC2]); // setp dl
            if matches!(op, NativeBinaryNumericOp::Clt) {
                code.extend_from_slice(&[0x0F, 0x92, 0xC0]); // setb al
            } else {
                code.extend_from_slice(&[0x0F, 0x97, 0xC0]); // seta al
            }
            code.extend_from_slice(&[0xF6, 0xD2]); // not dl
            code.extend_from_slice(&[0x20, 0xD0]); // and al, dl
            emit_store_bool_from_al(code, layout.value)?;
            emit_adjust_stack_len_minus_one(code, stack_len_offset);
            emit_status_continue(code);
        }
    }
    let float_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, lhs_not_float, error_label)?;
    patch_rel32(code, rhs_not_float, error_label)?;
    patch_rel32(code, int_done, done_label)?;
    patch_rel32(code, float_done, done_label)?;
    Ok(())
}

fn emit_native_step_neg_inline(code: &mut Vec<u8>, layout: NativeStackLayout) -> VmResult<()> {
    emit_stack_top_setup(code, layout, 1)?;
    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let not_int = emit_jcc_rel32(code, [0x0F, 0x85]); // jne

    code.extend_from_slice(&[0x48, 0xF7, 0x9F]); // neg qword [rdi+disp32]
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    emit_store_tag_rdi(code, layout.value, layout.value.int_tag)?;
    emit_status_continue(code);
    let int_done = emit_jmp_rel32(code);

    let float_check = code.len();
    patch_rel32(code, not_int, float_check)?;
    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, float_tag
    code.extend_from_slice(&layout.value.float_tag.to_le_bytes());
    let not_float = emit_jcc_rel32(code, [0x0F, 0x85]); // jne

    code.extend_from_slice(&[0xF2, 0x0F, 0x10, 0x87]); // movsd xmm0, [rdi+disp32]
    code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x66, 0x0F, 0x57, 0xC9]); // xorpd xmm1, xmm1
    code.extend_from_slice(&[0xF2, 0x0F, 0x5C, 0xC8]); // subsd xmm1, xmm0
    emit_store_tag_rdi(code, layout.value, layout.value.float_tag)?;
    code.extend_from_slice(&[0xF2, 0x0F, 0x11, 0x8F]); // movsd [rdi+disp32], xmm1
    code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
    emit_status_continue(code);
    let float_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, not_float, error_label)?;
    patch_rel32(code, int_done, done_label)?;
    patch_rel32(code, float_done, done_label)?;
    Ok(())
}

fn emit_native_step_shift_inline(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    is_shl: bool,
) -> VmResult<()> {
    let stack_len_offset = checked_add_i32(
        layout.vm_stack_offset,
        layout.stack_vec.len_offset,
        "stack len offset overflow",
    )?;
    emit_stack_binary_setup(code, layout, 2)?;
    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let lhs_not_int = emit_jcc_rel32(code, [0x0F, 0x85]); // jne
    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let rhs_not_int = emit_jcc_rel32(code, [0x0F, 0x85]); // jne

    code.extend_from_slice(&[0x4C, 0x8B, 0x86]); // mov r8, [rsi+disp32]
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x4D, 0x85, 0xC0]); // test r8, r8
    let neg_shift = emit_jcc_rel32(code, [0x0F, 0x88]); // js
    code.extend_from_slice(&[0x49, 0x83, 0xF8, 0x3F]); // cmp r8, 63
    let big_shift = emit_jcc_rel32(code, [0x0F, 0x87]); // ja

    code.extend_from_slice(&[0x48, 0x8B, 0x87]); // mov rax, [rdi+disp32]
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x44, 0x89, 0xC1]); // mov ecx, r8d
    if is_shl {
        code.extend_from_slice(&[0x48, 0xD3, 0xE0]); // shl rax, cl
    } else {
        code.extend_from_slice(&[0x48, 0xD3, 0xF8]); // sar rax, cl
    }
    code.extend_from_slice(&[0x48, 0x89, 0x87]); // mov [rdi+disp32], rax
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    emit_store_tag_rdi(code, layout.value, layout.value.int_tag)?;
    emit_adjust_stack_len_minus_one(code, stack_len_offset);
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, lhs_not_int, error_label)?;
    patch_rel32(code, rhs_not_int, error_label)?;
    patch_rel32(code, neg_shift, error_label)?;
    patch_rel32(code, big_shift, error_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_pop_inline(code: &mut Vec<u8>, layout: NativeStackLayout) -> VmResult<()> {
    let stack_len_offset = vec_len_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_ptr_offset = vec_ptr_disp(layout.vm_stack_offset, layout.stack_vec)?;

    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32]
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x83, 0xF9, 0x01]); // cmp rcx, 1
    let underflow = emit_jcc_rel32(code, [0x0F, 0x82]); // jb
    code.extend_from_slice(&[0x48, 0xFF, 0xC9]); // dec rcx
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32]
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x89, 0xC8]); // mov rax, rcx
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax]
    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let string_pop = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x48, 0x89, 0x8B]); // mov [rbx+disp32], rcx
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, underflow, error_label)?;
    patch_rel32(code, string_pop, error_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_dup_inline(code: &mut Vec<u8>, layout: NativeStackLayout) -> VmResult<()> {
    let stack_len_offset = vec_len_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_ptr_offset = vec_ptr_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_cap_offset = vec_cap_disp(layout.vm_stack_offset, layout.stack_vec)?;

    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32]
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x8B, 0x83]); // mov r8, [rbx+disp32]
    code.extend_from_slice(&stack_cap_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x83, 0xF9, 0x01]); // cmp rcx, 1
    let underflow = emit_jcc_rel32(code, [0x0F, 0x82]); // jb
    code.extend_from_slice(&[0x4C, 0x39, 0xC1]); // cmp rcx, r8
    let no_cap = emit_jcc_rel32(code, [0x0F, 0x83]); // jae
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32]
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x41, 0xFF]); // lea rax, [rcx-1]
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x34, 0x02]); // lea rsi, [rdx+rax]
    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let string_dup = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x48, 0x89, 0xC8]); // mov rax, rcx
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax]
    emit_copy_value_rsi_to_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x48, 0xFF, 0xC1]); // inc rcx
    code.extend_from_slice(&[0x48, 0x89, 0x8B]); // mov [rbx+disp32], rcx
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, underflow, error_label)?;
    patch_rel32(code, no_cap, error_label)?;
    patch_rel32(code, string_dup, error_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_ldc_inline(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    const_index: u32,
) -> VmResult<()> {
    let stack_len_offset = vec_len_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_ptr_offset = vec_ptr_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_cap_offset = vec_cap_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let constants_base = checked_add_i32(
        layout.vm_program_offset,
        layout.program_constants_offset,
        "vm constants base overflow",
    )?;
    let constants_len_offset = vec_len_disp(constants_base, layout.stack_vec)?;
    let constants_ptr_offset = vec_ptr_disp(constants_base, layout.stack_vec)?;

    code.extend_from_slice(&[0x4C, 0x8B, 0x83]); // mov r8, [rbx+disp32] ; constants len
    code.extend_from_slice(&constants_len_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&const_index.to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x39, 0xC0]); // cmp rax, r8
    let bad_index = emit_jcc_rel32(code, [0x0F, 0x83]); // jae
    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32] ; stack len
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x8B, 0x8B]); // mov r9, [rbx+disp32] ; stack cap
    code.extend_from_slice(&stack_cap_offset.to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x39, 0xC9]); // cmp rcx, r9
    let no_cap = emit_jcc_rel32(code, [0x0F, 0x83]); // jae
    code.extend_from_slice(&[0x4C, 0x8B, 0x8B]); // mov r9, [rbx+disp32] ; constants ptr
    code.extend_from_slice(&constants_ptr_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&const_index.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x49, 0x8D, 0x34, 0x01]); // lea rsi, [r9+rax]
    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let string_const = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32] ; stack ptr
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x89, 0xC8]); // mov rax, rcx
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax]
    emit_copy_value_rsi_to_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x48, 0xFF, 0xC1]); // inc rcx
    code.extend_from_slice(&[0x48, 0x89, 0x8B]); // mov [rbx+disp32], rcx
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, bad_index, error_label)?;
    patch_rel32(code, no_cap, error_label)?;
    patch_rel32(code, string_const, error_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_ldloc_inline(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    local_index: u8,
) -> VmResult<()> {
    let locals_len_offset = vec_len_disp(layout.vm_locals_offset, layout.stack_vec)?;
    let locals_ptr_offset = vec_ptr_disp(layout.vm_locals_offset, layout.stack_vec)?;
    let stack_len_offset = vec_len_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_ptr_offset = vec_ptr_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_cap_offset = vec_cap_disp(layout.vm_stack_offset, layout.stack_vec)?;

    code.extend_from_slice(&[0x4C, 0x8B, 0x83]); // mov r8, [rbx+disp32] ; locals len
    code.extend_from_slice(&locals_len_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&(local_index as u32).to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x39, 0xC0]); // cmp rax, r8
    let bad_index = emit_jcc_rel32(code, [0x0F, 0x83]); // jae
    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32] ; stack len
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x8B, 0x8B]); // mov r9, [rbx+disp32] ; stack cap
    code.extend_from_slice(&stack_cap_offset.to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x39, 0xC9]); // cmp rcx, r9
    let no_cap = emit_jcc_rel32(code, [0x0F, 0x83]); // jae
    code.extend_from_slice(&[0x4C, 0x8B, 0x8B]); // mov r9, [rbx+disp32] ; locals ptr
    code.extend_from_slice(&locals_ptr_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&(local_index as u32).to_le_bytes());
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x49, 0x8D, 0x34, 0x01]); // lea rsi, [r9+rax]
    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let string_local = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32] ; stack ptr
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x89, 0xC8]); // mov rax, rcx
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax]
    emit_copy_value_rsi_to_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x48, 0xFF, 0xC1]); // inc rcx
    code.extend_from_slice(&[0x48, 0x89, 0x8B]); // mov [rbx+disp32], rcx
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, bad_index, error_label)?;
    patch_rel32(code, no_cap, error_label)?;
    patch_rel32(code, string_local, error_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_stloc_inline(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    local_index: u8,
) -> VmResult<()> {
    let locals_len_offset = vec_len_disp(layout.vm_locals_offset, layout.stack_vec)?;
    let locals_ptr_offset = vec_ptr_disp(layout.vm_locals_offset, layout.stack_vec)?;
    let stack_len_offset = vec_len_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_ptr_offset = vec_ptr_disp(layout.vm_stack_offset, layout.stack_vec)?;

    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32] ; stack len
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x83, 0xF9, 0x01]); // cmp rcx, 1
    let underflow = emit_jcc_rel32(code, [0x0F, 0x82]); // jb
    code.extend_from_slice(&[0x4C, 0x8B, 0x83]); // mov r8, [rbx+disp32] ; locals len
    code.extend_from_slice(&locals_len_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&(local_index as u32).to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x39, 0xC0]); // cmp rax, r8
    let bad_index = emit_jcc_rel32(code, [0x0F, 0x83]); // jae
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32] ; stack ptr
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x41, 0xFF]); // lea rax, [rcx-1]
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x34, 0x02]); // lea rsi, [rdx+rax]
    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let src_string = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x4C, 0x8B, 0x8B]); // mov r9, [rbx+disp32] ; locals ptr
    code.extend_from_slice(&locals_ptr_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&(local_index as u32).to_le_bytes());
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x49, 0x8D, 0x3C, 0x01]); // lea rdi, [r9+rax]
    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let dst_string = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    emit_copy_value_rsi_to_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x48, 0xFF, 0xC9]); // dec rcx
    code.extend_from_slice(&[0x48, 0x89, 0x8B]); // mov [rbx+disp32], rcx
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, underflow, error_label)?;
    patch_rel32(code, bad_index, error_label)?;
    patch_rel32(code, src_string, error_label)?;
    patch_rel32(code, dst_string, error_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_ceq_inline(code: &mut Vec<u8>, layout: NativeStackLayout) -> VmResult<()> {
    let stack_len_offset = vec_len_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_ptr_offset = vec_ptr_disp(layout.vm_stack_offset, layout.stack_vec)?;

    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32]
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x83, 0xF9, 0x02]); // cmp rcx, 2
    let underflow = emit_jcc_rel32(code, [0x0F, 0x82]); // jb
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32]
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x41, 0xFF]); // lea rax, [rcx-1]
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x34, 0x02]); // lea rsi, [rdx+rax] rhs
    code.extend_from_slice(&[0x48, 0x2D]); // sub rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax] lhs

    emit_load_tag_eax_from_rdi(code, layout.value)?;
    emit_load_tag_edx_from_rsi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let lhs_string = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x81, 0xFA]); // cmp edx, string_tag
    code.extend_from_slice(&layout.value.string_tag.to_le_bytes());
    let rhs_string = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x39, 0xD0]); // cmp eax, edx
    let tags_not_equal = emit_jcc_rel32(code, [0x0F, 0x85]); // jne

    code.extend_from_slice(&[0x3D]); // cmp eax, int_tag
    code.extend_from_slice(&layout.value.int_tag.to_le_bytes());
    let cmp_int = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x3D]); // cmp eax, float_tag
    code.extend_from_slice(&layout.value.float_tag.to_le_bytes());
    let cmp_float = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    code.extend_from_slice(&[0x3D]); // cmp eax, bool_tag
    code.extend_from_slice(&layout.value.bool_tag.to_le_bytes());
    let cmp_bool = emit_jcc_rel32(code, [0x0F, 0x84]); // je
    let unknown_tag = emit_jmp_rel32(code);

    let cmp_int_label = code.len();
    code.extend_from_slice(&[0x4C, 0x8B, 0x87]); // mov r8, [rdi+disp32]
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x4C, 0x3B, 0x86]); // cmp r8, [rsi+disp32]
    code.extend_from_slice(&layout.value.int_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x0F, 0x94, 0xC0]); // sete al
    let result_ready = emit_jmp_rel32(code);

    let cmp_float_label = code.len();
    code.extend_from_slice(&[0xF2, 0x0F, 0x10, 0x87]); // movsd xmm0, [rdi+disp32]
    code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0xF2, 0x0F, 0x10, 0x8E]); // movsd xmm1, [rsi+disp32]
    code.extend_from_slice(&layout.value.float_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x66, 0x0F, 0x2E, 0xC1]); // ucomisd xmm0, xmm1
    code.extend_from_slice(&[0x0F, 0x94, 0xC0]); // sete al
    code.extend_from_slice(&[0x0F, 0x9B, 0xC2]); // setnp dl
    code.extend_from_slice(&[0x20, 0xD0]); // and al, dl
    let float_ready = emit_jmp_rel32(code);

    let cmp_bool_label = code.len();
    code.extend_from_slice(&[0x0F, 0xB6, 0x87]); // movzx eax, byte [rdi+disp32]
    code.extend_from_slice(&layout.value.bool_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x3A, 0x86]); // cmp al, [rsi+disp32]
    code.extend_from_slice(&layout.value.bool_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x0F, 0x94, 0xC0]); // sete al
    let bool_ready = emit_jmp_rel32(code);

    let not_equal_label = code.len();
    code.extend_from_slice(&[0x31, 0xC0]); // xor eax, eax
    let ne_ready = emit_jmp_rel32(code);

    let result_label = code.len();
    emit_store_bool_from_al(code, layout.value)?;
    code.extend_from_slice(&[0x48, 0xFF, 0xC9]); // dec rcx
    code.extend_from_slice(&[0x48, 0x89, 0x8B]); // mov [rbx+disp32], rcx
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();

    patch_rel32(code, cmp_int, cmp_int_label)?;
    patch_rel32(code, cmp_float, cmp_float_label)?;
    patch_rel32(code, cmp_bool, cmp_bool_label)?;
    patch_rel32(code, tags_not_equal, not_equal_label)?;
    patch_rel32(code, result_ready, result_label)?;
    patch_rel32(code, float_ready, result_label)?;
    patch_rel32(code, bool_ready, result_label)?;
    patch_rel32(code, ne_ready, result_label)?;
    patch_rel32(code, underflow, error_label)?;
    patch_rel32(code, lhs_string, error_label)?;
    patch_rel32(code, rhs_string, error_label)?;
    patch_rel32(code, unknown_tag, error_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_guard_false_inline(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    exit_ip: u32,
) -> VmResult<()> {
    let stack_len_offset = vec_len_disp(layout.vm_stack_offset, layout.stack_vec)?;
    let stack_ptr_offset = vec_ptr_disp(layout.vm_stack_offset, layout.stack_vec)?;

    code.extend_from_slice(&[0x48, 0x8B, 0x8B]); // mov rcx, [rbx+disp32]
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x83, 0xF9, 0x01]); // cmp rcx, 1
    let underflow = emit_jcc_rel32(code, [0x0F, 0x82]); // jb
    code.extend_from_slice(&[0x48, 0xFF, 0xC9]); // dec rcx
    code.extend_from_slice(&[0x48, 0x8B, 0x93]); // mov rdx, [rbx+disp32]
    code.extend_from_slice(&stack_ptr_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x89, 0xC8]); // mov rax, rcx
    code.extend_from_slice(&[0x48, 0x69, 0xC0]); // imul rax, rax, imm32
    code.extend_from_slice(&layout.value.size.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x8D, 0x3C, 0x02]); // lea rdi, [rdx+rax]
    emit_load_tag_eax_from_rdi(code, layout.value)?;
    code.extend_from_slice(&[0x3D]); // cmp eax, bool_tag
    code.extend_from_slice(&layout.value.bool_tag.to_le_bytes());
    let bad_type = emit_jcc_rel32(code, [0x0F, 0x85]); // jne
    code.extend_from_slice(&[0x0F, 0xB6, 0x87]); // movzx eax, byte [rdi+disp32]
    code.extend_from_slice(&layout.value.bool_payload_offset.to_le_bytes());
    code.extend_from_slice(&[0x48, 0x89, 0x8B]); // mov [rbx+disp32], rcx
    code.extend_from_slice(&stack_len_offset.to_le_bytes());
    code.extend_from_slice(&[0x84, 0xC0]); // test al, al
    let condition_true = emit_jcc_rel32(code, [0x0F, 0x85]); // jne
    code.extend_from_slice(&[0x48, 0xB8]); // mov rax, imm64
    code.extend_from_slice(&(exit_ip as u64).to_le_bytes());
    code.extend_from_slice(&[0x48, 0x89, 0x83]); // mov [rbx+disp32], rax
    code.extend_from_slice(&layout.vm_ip_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&STATUS_TRACE_EXIT.to_le_bytes());
    let false_done = emit_jmp_rel32(code);

    let true_label = code.len();
    emit_status_continue(code);
    let ok_done = emit_jmp_rel32(code);

    let error_label = code.len();
    emit_status_error(code);
    let done_label = code.len();
    patch_rel32(code, underflow, error_label)?;
    patch_rel32(code, bad_type, error_label)?;
    patch_rel32(code, condition_true, true_label)?;
    patch_rel32(code, false_done, done_label)?;
    patch_rel32(code, ok_done, done_label)?;
    Ok(())
}

fn emit_native_step_jump_to_root_inline(
    code: &mut Vec<u8>,
    layout: NativeStackLayout,
    root_ip: u32,
) -> VmResult<()> {
    code.extend_from_slice(&[0x48, 0xB8]); // mov rax, imm64
    code.extend_from_slice(&(root_ip as u64).to_le_bytes());
    code.extend_from_slice(&[0x48, 0x89, 0x83]); // mov [rbx+disp32], rax
    code.extend_from_slice(&layout.vm_ip_offset.to_le_bytes());
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&STATUS_TRACE_EXIT.to_le_bytes());
    Ok(())
}

fn emit_native_step_ret_inline(code: &mut Vec<u8>) {
    code.push(0xB8); // mov eax, imm32
    code.extend_from_slice(&STATUS_HALTED.to_le_bytes());
}

fn detect_vec_layout() -> VmResult<VecLayout> {
    let expected_size = std::mem::size_of::<[usize; 3]>();
    if std::mem::size_of::<Vec<Value>>() != expected_size {
        return Err(VmError::JitNative(format!(
            "unsupported Vec<Value> size {} for native emission",
            std::mem::size_of::<Vec<Value>>()
        )));
    }

    let mut sample = Vec::with_capacity(11);
    sample.push(Value::Int(1));
    sample.push(Value::Int(2));
    let ptr_value = sample.as_ptr() as usize;
    let len_value = sample.len();
    let cap_value = sample.capacity();

    let words = unsafe { &*((&sample as *const Vec<Value>) as *const [usize; 3]) };
    let ptr_index = find_unique_word_index(words, ptr_value, "Vec<Value> ptr field")?;
    let len_index = find_unique_word_index(words, len_value, "Vec<Value> len field")?;
    let cap_index = find_unique_word_index(words, cap_value, "Vec<Value> cap field")?;

    Ok(VecLayout {
        ptr_offset: usize_to_i32(
            ptr_index * std::mem::size_of::<usize>(),
            "Vec<Value>::ptr offset",
        )?,
        len_offset: usize_to_i32(
            len_index * std::mem::size_of::<usize>(),
            "Vec<Value>::len offset",
        )?,
        cap_offset: usize_to_i32(
            cap_index * std::mem::size_of::<usize>(),
            "Vec<Value>::cap offset",
        )?,
    })
}

fn find_unique_word_index(words: &[usize; 3], needle: usize, label: &str) -> VmResult<usize> {
    let mut match_index = None;
    for (index, value) in words.iter().enumerate() {
        if *value == needle {
            if match_index.is_some() {
                return Err(VmError::JitNative(format!(
                    "ambiguous {} while probing native layout",
                    label
                )));
            }
            match_index = Some(index);
        }
    }
    match_index.ok_or_else(|| {
        VmError::JitNative(format!(
            "failed to locate {} while probing native layout",
            label
        ))
    })
}

fn detect_value_layout() -> VmResult<ValueLayout> {
    let value_size = std::mem::size_of::<Value>();
    let int_a = 0x0102_0304_0506_0708_i64;
    let int_b = 0x1112_1314_1516_1718_i64;
    let float_a = 3.25_f64;
    let float_b = -11.5_f64;
    let int_a_bytes = encode_value_bytes(Value::Int(int_a));
    let int_b_bytes = encode_value_bytes(Value::Int(int_b));
    let float_a_bytes = encode_value_bytes(Value::Float(float_a));
    let float_b_bytes = encode_value_bytes(Value::Float(float_b));
    let bool_false_bytes = encode_value_bytes(Value::Bool(false));
    let bool_true_bytes = encode_value_bytes(Value::Bool(true));
    let string_a_bytes = encode_value_bytes(Value::String("a".to_string()));
    let string_b_bytes = encode_value_bytes(Value::String("b".to_string()));
    let (tag_offset, tag_size) = detect_tag_layout(
        &int_a_bytes,
        &int_b_bytes,
        &float_a_bytes,
        &float_b_bytes,
        &bool_false_bytes,
        &bool_true_bytes,
        &string_a_bytes,
        &string_b_bytes,
    )?;
    let int_tag = decode_tag(&int_a_bytes, tag_offset, tag_size);
    let float_tag = decode_tag(&float_a_bytes, tag_offset, tag_size);
    let bool_tag = decode_tag(&bool_false_bytes, tag_offset, tag_size);
    let string_tag = decode_tag(&string_a_bytes, tag_offset, tag_size);

    let payload_match_a = int_a.to_le_bytes();
    let payload_match_b = int_b.to_le_bytes();
    let mut int_payload_offset = None;
    for offset in 0..=value_size.saturating_sub(8) {
        if int_a_bytes[offset..offset + 8] == payload_match_a
            && int_b_bytes[offset..offset + 8] == payload_match_b
        {
            if int_payload_offset.is_some() {
                return Err(VmError::JitNative(
                    "ambiguous Value::Int payload offset for native emission".to_string(),
                ));
            }
            int_payload_offset = Some(offset);
        }
    }
    let int_payload_offset = int_payload_offset.ok_or_else(|| {
        VmError::JitNative(
            "unable to find Value::Int payload offset for native emission".to_string(),
        )
    })?;

    let float_match_a = float_a.to_le_bytes();
    let float_match_b = float_b.to_le_bytes();
    let mut float_payload_offset = None;
    for offset in 0..=value_size.saturating_sub(8) {
        if float_a_bytes[offset..offset + 8] == float_match_a
            && float_b_bytes[offset..offset + 8] == float_match_b
        {
            if float_payload_offset.is_some() {
                return Err(VmError::JitNative(
                    "ambiguous Value::Float payload offset for native emission".to_string(),
                ));
            }
            float_payload_offset = Some(offset);
        }
    }
    let float_payload_offset = float_payload_offset.ok_or_else(|| {
        VmError::JitNative(
            "unable to find Value::Float payload offset for native emission".to_string(),
        )
    })?;

    let mut bool_payload_offset = None;
    for offset in 0..value_size {
        if bool_false_bytes[offset] == bool_true_bytes[offset] {
            continue;
        }
        if offset >= tag_offset && offset < tag_offset + tag_size {
            continue;
        }
        bool_payload_offset = Some(offset);
        break;
    }
    let bool_payload_offset = bool_payload_offset.ok_or_else(|| {
        VmError::JitNative(
            "unable to find Value::Bool payload offset for native emission".to_string(),
        )
    })?;
    let false_byte = bool_false_bytes[bool_payload_offset];
    let true_byte = bool_true_bytes[bool_payload_offset];
    if false_byte != 0 || true_byte != 1 {
        return Err(VmError::JitNative(
            "unsupported Value::Bool byte encoding for native emission".to_string(),
        ));
    }

    Ok(ValueLayout {
        size: usize_to_i32(value_size, "Value size")?,
        tag_offset: usize_to_i32(tag_offset, "Value tag offset")?,
        tag_size: tag_size as u8,
        int_tag,
        float_tag,
        bool_tag,
        string_tag,
        int_payload_offset: usize_to_i32(int_payload_offset, "Value::Int payload offset")?,
        float_payload_offset: usize_to_i32(float_payload_offset, "Value::Float payload offset")?,
        bool_payload_offset: usize_to_i32(bool_payload_offset, "Value::Bool payload offset")?,
    })
}

fn detect_tag_layout(
    int_a: &[u8],
    int_b: &[u8],
    float_a: &[u8],
    float_b: &[u8],
    bool_false: &[u8],
    bool_true: &[u8],
    string_a: &[u8],
    string_b: &[u8],
) -> VmResult<(usize, usize)> {
    let size = int_a.len();
    for tag_size in [1usize, 2, 4] {
        if tag_size > size {
            continue;
        }
        for offset in 0..=size - tag_size {
            let int_slice = &int_a[offset..offset + tag_size];
            if int_slice != &int_b[offset..offset + tag_size] {
                continue;
            }
            let float_slice = &float_a[offset..offset + tag_size];
            if float_slice != &float_b[offset..offset + tag_size] {
                continue;
            }
            let bool_slice = &bool_false[offset..offset + tag_size];
            if bool_slice != &bool_true[offset..offset + tag_size] {
                continue;
            }
            let string_slice = &string_a[offset..offset + tag_size];
            if string_slice != &string_b[offset..offset + tag_size] {
                continue;
            }
            if int_slice == float_slice && int_slice == bool_slice && int_slice == string_slice {
                continue;
            }
            return Ok((offset, tag_size));
        }
    }
    Err(VmError::JitNative(
        "unable to find Value discriminant bytes for native emission".to_string(),
    ))
}

fn decode_tag(bytes: &[u8], offset: usize, size: usize) -> u32 {
    let mut out = 0u32;
    for index in 0..size {
        out |= (bytes[offset + index] as u32) << (index * 8);
    }
    out
}

fn encode_value_bytes(value: Value) -> Vec<u8> {
    let mut bytes = vec![0u8; std::mem::size_of::<Value>()];
    unsafe {
        let ptr = bytes.as_mut_ptr() as *mut Value;
        ptr.write(value);
        std::ptr::drop_in_place(ptr);
    }
    bytes
}

fn checked_add_i32(lhs: i32, rhs: i32, context: &str) -> VmResult<i32> {
    lhs.checked_add(rhs)
        .ok_or_else(|| VmError::JitNative(context.to_string()))
}

fn usize_to_i32(value: usize, context: &str) -> VmResult<i32> {
    i32::try_from(value)
        .map_err(|_| VmError::JitNative(format!("{} exceeds 32-bit displacement range", context)))
}

fn emit_native_status_check(code: &mut Vec<u8>, patches: &mut Vec<usize>) {
    code.extend_from_slice(&[0x85, 0xC0]); // test eax, eax
    code.extend_from_slice(&[0x0F, 0x85]); // jne rel32
    patches.push(code.len());
    code.extend_from_slice(&[0, 0, 0, 0]);
}

fn clear_bridge_error() {
    // Native trace steps are emitted as machine code and no longer bridge through Rust helpers.
}

fn take_bridge_error() -> Option<VmError> {
    None
}

#[cfg(target_os = "windows")]
fn alloc_executable_region(len: usize) -> VmResult<*mut u8> {
    use windows_sys::Win32::System::Memory::{
        MEM_COMMIT, MEM_RESERVE, PAGE_EXECUTE_READWRITE, VirtualAlloc,
    };

    let ptr = unsafe {
        VirtualAlloc(
            std::ptr::null_mut(),
            len,
            MEM_COMMIT | MEM_RESERVE,
            PAGE_EXECUTE_READWRITE,
        ) as *mut u8
    };
    if ptr.is_null() {
        return Err(VmError::JitNative(format!(
            "VirtualAlloc failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(ptr)
}

#[cfg(target_os = "windows")]
fn free_executable_region(ptr: *mut u8, _len: usize) -> VmResult<()> {
    use windows_sys::Win32::System::Memory::{MEM_RELEASE, VirtualFree};

    if ptr.is_null() {
        return Ok(());
    }
    let ok = unsafe { VirtualFree(ptr as *mut _, 0, MEM_RELEASE) };
    if ok == 0 {
        return Err(VmError::JitNative(format!(
            "VirtualFree failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn alloc_executable_region(len: usize) -> VmResult<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE | libc::PROT_EXEC,
            libc::MAP_ANON | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(VmError::JitNative(format!(
            "mmap failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(ptr as *mut u8)
}

#[cfg(target_os = "macos")]
fn alloc_executable_region(len: usize) -> VmResult<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE | libc::PROT_EXEC,
            libc::MAP_ANON | libc::MAP_PRIVATE | libc::MAP_JIT,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(VmError::JitNative(format!(
            "mmap(MAP_JIT) failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(ptr as *mut u8)
}

#[cfg(unix)]
fn free_executable_region(ptr: *mut u8, len: usize) -> VmResult<()> {
    if ptr.is_null() {
        return Ok(());
    }
    let rc = unsafe { libc::munmap(ptr as *mut _, len) };
    if rc != 0 {
        return Err(VmError::JitNative(format!(
            "munmap failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(not(any(unix, target_os = "windows")))]
fn alloc_executable_region(_len: usize) -> VmResult<*mut u8> {
    Err(VmError::JitNative(
        "executable memory allocation not implemented for this platform".to_string(),
    ))
}

#[cfg(not(any(unix, target_os = "windows")))]
fn free_executable_region(_ptr: *mut u8, _len: usize) -> VmResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::STATUS_CONTINUE;
    use super::*;
    use crate::jit::{JitTrace, JitTraceTerminal, TraceStep};
    use crate::vm::Program;

    type NativeEntry = unsafe extern "C" fn(*mut Vm) -> i32;

    fn build_single_step_trace(step: TraceStep) -> JitTrace {
        JitTrace {
            id: 0,
            root_ip: 0,
            start_line: None,
            steps: vec![step],
            terminal: JitTraceTerminal::LoopBack,
            executions: 0,
        }
    }

    fn execute_single_step(vm: &mut Vm, step: TraceStep) -> VmResult<i32> {
        let trace = build_single_step_trace(step);
        let code = emit_native_trace_bytes(&trace)?;
        let memory = BackendExecutableMemory::from_code(&code)?;
        let entry = unsafe { std::mem::transmute::<*mut u8, NativeEntry>(memory.ptr) };
        clear_bridge_error();
        let status = unsafe { entry(vm as *mut Vm) };
        drop(memory);
        Ok(status)
    }

    #[test]
    fn add_step_emits_no_helper_calls() {
        let trace = build_single_step_trace(TraceStep::Add);
        let code = emit_native_trace_bytes(&trace).expect("native add trace should compile");
        let call_count = code
            .windows(2)
            .filter(|window| *window == [0xFF, 0xD0])
            .count();
        assert_eq!(
            call_count, 0,
            "add should emit no call-outs, code bytes: {:02X?}",
            code
        );
    }

    #[test]
    fn arithmetic_steps_emit_without_helper_calls() {
        let steps = [
            TraceStep::Add,
            TraceStep::Sub,
            TraceStep::Mul,
            TraceStep::Div,
            TraceStep::Shl,
            TraceStep::Shr,
            TraceStep::Neg,
            TraceStep::Clt,
            TraceStep::Cgt,
        ];
        for step in steps {
            let trace = build_single_step_trace(step.clone());
            let code = emit_native_trace_bytes(&trace).expect("native trace should compile");
            let call_count = code
                .windows(2)
                .filter(|window| *window == [0xFF, 0xD0])
                .count();
            assert_eq!(
                call_count, 0,
                "step {:?} should emit no helper calls, code bytes: {:02X?}",
                step, code
            );
        }
    }

    #[test]
    fn non_arithmetic_steps_emit_without_helper_calls() {
        let steps = [
            TraceStep::Ldc(0),
            TraceStep::Ceq,
            TraceStep::Pop,
            TraceStep::Dup,
            TraceStep::Ldloc(0),
            TraceStep::Stloc(0),
            TraceStep::GuardFalse { exit_ip: 0 },
            TraceStep::JumpToRoot,
            TraceStep::Ret,
        ];
        for step in steps {
            let trace = build_single_step_trace(step.clone());
            let code = emit_native_trace_bytes(&trace).expect("native trace should compile");
            let call_count = code
                .windows(2)
                .filter(|window| *window == [0xFF, 0xD0])
                .count();
            assert_eq!(
                call_count, 0,
                "step {:?} should emit no helper calls, code bytes: {:02X?}",
                step, code
            );
        }
    }

    #[test]
    fn add_step_inline_success_updates_stack() {
        let mut vm = Vm::new(Program::new(Vec::new(), Vec::new()));
        vm.stack.push(Value::Int(2));
        vm.stack.push(Value::Int(3));

        let status = execute_single_step(&mut vm, TraceStep::Add).expect("native add should run");
        assert_eq!(status, STATUS_CONTINUE);
        assert_eq!(vm.stack, vec![Value::Int(5)]);
        assert!(
            take_bridge_error().is_none(),
            "success path should not set bridge error"
        );
    }

    #[test]
    fn add_step_inline_supports_float_and_mixed_numeric() {
        let mut vm = Vm::new(Program::new(Vec::new(), Vec::new()));
        vm.stack.push(Value::Float(1.5));
        vm.stack.push(Value::Int(2));

        let status = execute_single_step(&mut vm, TraceStep::Add).expect("native add should run");
        assert_eq!(status, STATUS_CONTINUE);
        match vm.stack.last() {
            Some(Value::Float(value)) => assert!((*value - 3.5).abs() < f64::EPSILON),
            other => panic!("expected float result, got {other:?}"),
        }
    }

    #[test]
    fn clt_step_inline_supports_float() {
        let mut vm = Vm::new(Program::new(Vec::new(), Vec::new()));
        vm.stack.push(Value::Float(1.5));
        vm.stack.push(Value::Float(2.0));

        let status = execute_single_step(&mut vm, TraceStep::Clt).expect("native clt should run");
        assert_eq!(status, STATUS_CONTINUE);
        assert_eq!(vm.stack, vec![Value::Bool(true)]);
    }
}
