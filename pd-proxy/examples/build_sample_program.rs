use std::{io, path::PathBuf};

use proxy::{ABI_VERSION, FUNCTIONS, HOST_FUNCTION_COUNT};
use reqwest::StatusCode;
use vm::{FunctionDecl, compile_source_file, encode_program, validate_program};

const SOURCE_PATH: &str = "examples/sample_proxy_program.rss";
const CONTROL_URL: &str = "http://127.0.0.1:8081/program";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_rel = std::env::args()
        .nth(1)
        .unwrap_or_else(|| SOURCE_PATH.to_string());
    let source_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(source_rel);
    let compiled = compile_source_file(&source_path)?;

    ensure_proxy_abi(&compiled.functions)?;
    validate_program(&compiled.program, HOST_FUNCTION_COUNT)?;

    let payload = encode_program(&compiled.program)?;
    let client = reqwest::Client::new();
    let response = client
        .put(CONTROL_URL)
        .header("content-type", "application/octet-stream")
        .body(payload)
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;
    if status != StatusCode::NO_CONTENT {
        return Err(
            io::Error::other(format!("upload failed: status={status}, body={body}",)).into(),
        );
    }

    println!(
        "compiled and uploaded source from {}",
        source_path.display()
    );
    println!("proxy abi version: {ABI_VERSION}");
    println!("control response: {status}");
    Ok(())
}

fn ensure_proxy_abi(functions: &[FunctionDecl]) -> Result<(), io::Error> {
    for function in FUNCTIONS {
        let index = function.index;
        let Some(actual) = functions.get(index as usize) else {
            return Err(io::Error::other(format!(
                "source missing required function declaration at index {index}: {}",
                function.name
            )));
        };
        if actual.name != function.name
            || actual.arity != function.arity
            || actual.index != function.index
        {
            return Err(io::Error::other(format!(
                "function ABI mismatch at index {index}: expected {}/{}, got {}/{}",
                function.name, function.arity, actual.name, actual.arity
            )));
        }
    }
    Ok(())
}
