//! Server configuration and the hard-limit constants/defaults from
//! `docs/API.md` and `docs/ROADMAP.md`.

// ---------------------------------------------------------------------------
// Hard limits (API §2 batching limits, §3, §5, §7, DESIGN §1.2)
// ---------------------------------------------------------------------------

/// Max records per write request (`STREAMS_MAX_BATCH_RECORDS`).
pub const MAX_BATCH_RECORDS: usize = 10_000;
/// Max single record `data`+`meta` canonical bytes (`STREAMS_MAX_RECORD_BYTES`).
pub const MAX_RECORD_BYTES: usize = 1 << 20; // 1 MiB
/// Max total request body (`STREAMS_MAX_BODY_BYTES`).
pub const MAX_BODY_BYTES: usize = 64 << 20; // 64 MiB
/// Max `meta` per record (`STREAMS_MAX_META_BYTES`).
pub const MAX_META_BYTES: usize = 16 << 10; // 16 KiB
/// Max number of `meta` keys per record.
pub const MAX_META_KEYS: usize = 64;
/// Max `tag` length in bytes (`STREAMS_MAX_TAG_BYTES`).
pub const MAX_TAG_BYTES: usize = 256;
/// Max `node` length in bytes (`STREAMS_MAX_NODE_BYTES`).
pub const MAX_NODE_BYTES: usize = 128;
/// Max `idempotency_key` length in characters.
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 256;

/// Default diff batch limit.
pub const DEFAULT_LIMIT: u32 = 256;
/// Max diff batch limit (`STREAMS_MAX_LIMIT`) — clamped, not rejected.
pub const MAX_LIMIT: u32 = 1000;
/// Max `wait_ms` long-poll — clamped, not rejected.
pub const MAX_WAIT_MS: u32 = 30_000;

/// Default list page size.
pub const DEFAULT_PAGE_SIZE: usize = 100;
/// Max list page size.
pub const MAX_PAGE_SIZE: usize = 1000;

/// Max active delete filters per box (`STREAMS_MAX_DELETE_FILTERS`).
pub const MAX_DELETE_FILTERS: usize = 4096;

/// Max boxes per watch subscription (`STREAMS_MAX_WATCH_BOXES`).
pub const MAX_WATCH_BOXES: usize = 256;
/// Watch session TTL after no active GET (ms).
pub const SESSION_TTL_MS: u64 = 300_000;
/// Heartbeat clamp bounds (ms).
pub const MIN_HEARTBEAT_MS: u64 = 1_000;
pub const MAX_HEARTBEAT_MS: u64 = 60_000;
/// EventSource reconnect backoff advertised via `retry:` (ms).
pub const SSE_RETRY_MS: u64 = 2_000;

/// Max router forwarding hops when `allow_cycle` is set (`$ttl_hops`).
pub const MAX_ROUTER_HOPS: u8 = 8;

// ---------------------------------------------------------------------------
// Priority scheduler constants (DESIGN §3, ARCHITECTURE §7)
// ---------------------------------------------------------------------------

/// Priority clamp bounds.
pub const PRIORITY_MIN: i32 = -1000;
pub const PRIORITY_MAX: i32 = 1000;
/// Auto-recency peak bonus.
pub const AUTO_MAX: f64 = 500.0;
/// Auto-recency half-life (ms).
pub const HALF_LIFE_MS: f64 = 30_000.0;
/// After this much idle time, the auto term is forced to 0 (ms).
pub const AUTO_FLOOR_MS: u64 = 300_000;
/// Anti-starvation aging rate (priority per ms waited): +100 / s.
pub const AGE_RATE_PER_MS: f64 = 0.1;
/// Aging cap (ms): +1000 after 10 s.
pub const AGE_CAP_MS: u64 = 10_000;

// ---------------------------------------------------------------------------
// ServerConfig
// ---------------------------------------------------------------------------

/// Runtime server configuration, assembled at startup from environment.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address, e.g. `0.0.0.0:4000`.
    pub bind_addr: String,
    /// Accepted bearer API keys. Empty ⇒ auth disabled (dev mode).
    pub api_keys: Vec<String>,
    /// Whether health/ready/metrics probes require auth (`STREAMS_PROBE_AUTH`).
    pub probe_auth: bool,
    /// Max total request body before parse (`413`).
    pub max_body_bytes: usize,
    /// Data directory for the WAL/segments (`STREAMS_DATA_DIR`). Phase 2 keeps
    /// all state in memory, so this is an accepted placeholder, reported but
    /// unused until phase 4 wires the storage layer underneath.
    pub data_dir: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_addr: "0.0.0.0:4000".to_string(),
            api_keys: Vec::new(),
            probe_auth: false,
            max_body_bytes: MAX_BODY_BYTES,
            data_dir: None,
        }
    }
}

impl ServerConfig {
    /// Build the config from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut cfg = ServerConfig::default();

        if let Ok(host) = std::env::var("STREAMS_HOST") {
            // STREAMS_HOST may be a full host:port or just a host.
            if host.contains(':') {
                cfg.bind_addr = host;
            } else {
                let port = std::env::var("STREAMS_PORT").unwrap_or_else(|_| "4000".into());
                cfg.bind_addr = format!("{host}:{port}");
            }
        } else if let Ok(port) = std::env::var("STREAMS_PORT") {
            cfg.bind_addr = format!("0.0.0.0:{port}");
        }

        if let Ok(keys) = std::env::var("STREAMS_API_KEYS") {
            cfg.api_keys = keys
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
        }

        cfg.probe_auth = std::env::var("STREAMS_PROBE_AUTH")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        if let Ok(v) = std::env::var("STREAMS_MAX_BODY_BYTES") {
            if let Ok(n) = v.parse() {
                cfg.max_body_bytes = n;
            }
        }

        // Accepted placeholder; phase 2 is in-memory so the directory is only
        // recorded, never written to (phase 4 wires the WAL/segments here).
        if let Ok(dir) = std::env::var("STREAMS_DATA_DIR") {
            let dir = dir.trim();
            if !dir.is_empty() {
                cfg.data_dir = Some(dir.to_string());
            }
        }

        cfg
    }

    /// Whether bearer auth is enforced.
    pub fn auth_enabled(&self) -> bool {
        !self.api_keys.is_empty()
    }
}

/// Validate a box name against the documented charset
/// `^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$` (1–255 chars, starts alphanumeric).
pub fn is_valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return false;
    }
    let first = bytes[0];
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    bytes.iter().all(|&b| {
        b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b':' || b == b'-'
    })
}

/// Validate a router name. Routers use the box-name charset plus `>` so the
/// documented default-name convention `"<source>-><dest>"` (e.g. `jobs->audit`,
/// API §6.1) is a legal `:router` path segment.
pub fn is_valid_router_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return false;
    }
    let first = bytes[0];
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    bytes.iter().all(|&b| {
        b.is_ascii_alphanumeric()
            || b == b'.'
            || b == b'_'
            || b == b':'
            || b == b'-'
            || b == b'>'
    })
}
