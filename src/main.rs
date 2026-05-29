//! streams — a persistent event engine (single binary).
//!
//! Phase 2 entrypoint: load config, init tracing, build the in-memory engine,
//! start the tokio + axum server, and shut down gracefully on a signal.

use std::sync::Arc;
use streams::clock::{SharedClock, SystemClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::http;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config = ServerConfig::from_env();

    if config.auth_enabled() {
        info!(keys = config.api_keys.len(), "bearer auth enabled");
    } else {
        warn!("STREAMS_API_KEYS not set: AUTH IS DISABLED (single-tenant dev mode)");
    }

    if let Some(dir) = &config.data_dir {
        info!(data_dir = %dir, "STREAMS_DATA_DIR set (phase 2 is in-memory; placeholder only)");
    }
    warn!("phase 2: all state is in-memory; a restart loses all data");

    let clock: SharedClock = Arc::new(SystemClock);
    let engine = Engine::new(config.clone(), clock);

    let app = http::build_router(engine.clone());

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    info!(addr = %config.bind_addr, "streams listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("shutdown complete");
    Ok(())
}

/// Initialize the tracing subscriber from `RUST_LOG` (default `info`).
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

/// Resolve when the process receives Ctrl-C or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received; draining");
}
