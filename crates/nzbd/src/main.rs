//! nzbd daemon binary, phase 1: boots the download engine, serves the
//! native API and the compat shim, and offers a small control CLI
//! (`add`, `status`) that talks to a running daemon over the native API.

use clap::{Parser, Subcommand};
use nzbd_engine::{Engine, EngineConfig, Tuning};
use nzbd_types::CertLevel;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "nzbd",
    version,
    about = "Usenet downloader daemon (NZBGet reimplemented in Rust)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon.
    Run {
        /// Path to nzbd.toml (defaults are used if absent).
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// Override the listen address, e.g. 0.0.0.0:6789.
        #[arg(short, long)]
        bind: Option<String>,
    },
    /// Add an NZB file to a running daemon.
    Add {
        /// Path to the .nzb file.
        file: PathBuf,
        /// Daemon address.
        #[arg(long, default_value = "127.0.0.1:6789")]
        url: String,
        /// Job name (defaults to the file name).
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        category: Option<String>,
        #[arg(long, default_value_t = 0)]
        priority: i32,
    },
    /// Show queue status of a running daemon.
    Status {
        #[arg(long, default_value = "127.0.0.1:6789")]
        url: String,
    },
    /// Import an nzbget.conf into nzbd.toml (phase 3).
    ImportConfig { path: PathBuf },
}

fn main() -> anyhow_lite::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Run { config, bind } => run(config, bind),
        Command::Add {
            file,
            url,
            name,
            category,
            priority,
        } => client_add(file, url, name, category, priority),
        Command::Status { url } => client_status(url),
        Command::ImportConfig { path } => {
            let content = std::fs::read_to_string(&path)?;
            match nzbd_config::import_nzbget_conf(&content) {
                Ok(_) => Ok(()),
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

fn run(config: Option<PathBuf>, bind: Option<String>) -> anyhow_lite::Result<()> {
    let cfg = match &config {
        Some(path) => nzbd_config::Config::from_toml(&std::fs::read_to_string(path)?)
            .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?,
        None => nzbd_config::Config::default(),
    };
    let bind = bind.unwrap_or_else(|| cfg.api.bind.clone());

    let servers = cfg.server_defs();
    for s in &servers {
        if s.cert_verification == CertLevel::None {
            tracing::warn!(
                server = %s.name,
                "TLS certificate verification is DISABLED for this server"
            );
        }
    }
    if servers.is_empty() {
        tracing::warn!("no [[server]] configured — the queue will accept jobs but nothing can download");
    }

    let engine_cfg = EngineConfig {
        servers,
        state_dir: cfg.state_dir(),
        dest_dir: cfg.dest_dir(),
        tuning: Tuning {
            article_retries: cfg.queue.article_retries,
            retry_interval: Duration::from_secs(cfg.queue.retry_interval_secs),
            article_timeout: Duration::from_secs(cfg.queue.article_timeout_secs),
            propagation_delay: Duration::from_secs(cfg.queue.propagation_delay_mins as u64 * 60),
            ..Tuning::default()
        },
        speed_limit_bps: cfg.speed_limit_bps(),
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let engine = Engine::spawn(engine_cfg)
            .await
            .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;

        let compat_state = nzbd_compat::CompatState {
            config: Arc::new(nzbd_compat::CompatConfig {
                version: cfg.api.compat_version.clone(),
            }),
            engine: engine.clone(),
        };
        let app = nzbd_api::router(engine.clone()).merge(nzbd_compat::router(compat_state));

        let listener = tokio::net::TcpListener::bind(&bind).await?;
        tracing::info!(%bind, "nzbd listening (phase 1: core engine)");

        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
            })
            .await?;

        engine.shutdown().await;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// control-client commands (minimal HTTP/1.1 over loopback; the full native
// CLI arrives with the phase-3 API work)
// ---------------------------------------------------------------------------

fn client_add(
    file: PathBuf,
    url: String,
    name: Option<String>,
    category: Option<String>,
    priority: i32,
) -> anyhow_lite::Result<()> {
    let content = std::fs::read(&file)?;
    let name = name.unwrap_or_else(|| {
        file.file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "download".into())
    });
    let mut path = format!("/api/v1/jobs?name={}&priority={priority}", urlenc(&name));
    if let Some(c) = &category {
        path.push_str(&format!("&category={}", urlenc(c)));
    }
    let (status, body) = http_request(&url, "POST", &path, Some(content))?;
    if status == 201 {
        println!("{body}");
        Ok(())
    } else {
        eprintln!("add failed ({status}): {body}");
        std::process::exit(1);
    }
}

fn client_status(url: String) -> anyhow_lite::Result<()> {
    let (status, body) = http_request(&url, "GET", "/api/v1/status", None)?;
    if status == 200 {
        println!("{body}");
        Ok(())
    } else {
        eprintln!("status failed ({status}): {body}");
        std::process::exit(1);
    }
}

/// One-shot HTTP/1.1 request over TCP (loopback control traffic only).
fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
) -> anyhow_lite::Result<(u16, String)> {
    use std::io::{Read, Write};
    let addr = addr.trim_start_matches("http://").trim_end_matches('/');
    let mut sock = std::net::TcpStream::connect(addr)?;
    sock.set_read_timeout(Some(Duration::from_secs(30)))?;
    let body = body.unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes())?;
    sock.write_all(&body)?;
    let mut resp = Vec::new();
    sock.read_to_end(&mut resp)?;
    let text = String::from_utf8_lossy(&resp);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow_lite::Error::msg("malformed HTTP response"))?;
    let payload = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.trim().to_string())
        .unwrap_or_default();
    Ok((status, payload))
}

fn urlenc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Tiny stand-in for `anyhow` to keep deps lean.
mod anyhow_lite {
    pub type Result<T> = std::result::Result<T, Error>;

    #[derive(Debug)]
    pub struct Error(String);

    impl Error {
        pub fn msg(s: impl Into<String>) -> Self {
            Error(s.into())
        }
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl<E: std::error::Error> From<E> for Error {
        fn from(e: E) -> Self {
            Error(e.to_string())
        }
    }
}
