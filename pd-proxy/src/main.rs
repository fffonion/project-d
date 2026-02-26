use std::{env, net::SocketAddr};

use proxy::{SharedState, build_control_app, build_data_app, init_logging};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging()?;

    let data_addr = parse_addr("DATA_ADDR", "0.0.0.0:8080")?;
    let control_addr = parse_addr("CONTROL_ADDR", "127.0.0.1:8081")?;
    let max_program_bytes = parse_max_program_bytes("MAX_PROGRAM_BYTES", 1024 * 1024)?;

    let state = SharedState::new(max_program_bytes);
    let data_app = build_data_app(state.clone());
    let control_app = build_control_app(state);

    let data_listener = tokio::net::TcpListener::bind(data_addr).await?;
    let control_listener = tokio::net::TcpListener::bind(control_addr).await?;

    info!(
        "data-plane listening on http://{}",
        data_listener.local_addr()?
    );
    info!(
        "control-plane listening on http://{}",
        control_listener.local_addr()?
    );

    let data_server = axum::serve(data_listener, data_app);
    let control_server = axum::serve(control_listener, control_app);

    tokio::select! {
        result = data_server => result?,
        result = control_server => result?,
    }

    Ok(())
}

fn parse_addr(key: &str, default: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let value = env::var(key).unwrap_or_else(|_| default.to_string());
    Ok(value.parse()?)
}

fn parse_max_program_bytes(key: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    match env::var(key) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}
