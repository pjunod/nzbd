//! nzbd daemon binary. Phase 0: boots, loads config, serves the native API
//! stub and the compat shim skeleton. The engine attaches in phase 1.

use clap::{Parser, Subcommand};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "nzbd", version, about = "Usenet downloader daemon (NZBGet reimplemented in Rust)")]
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
        config: Option<std::path::PathBuf>,
        /// Override the listen address, e.g. 0.0.0.0:6789.
        #[arg(short, long)]
        bind: Option<String>,
    },
    /// Import an nzbget.conf into nzbd.toml (phase 3).
    ImportConfig { path: std::path::PathBuf },
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

fn run(config: Option<std::path::PathBuf>, bind: Option<String>) -> anyhow_lite::Result<()> {
    let cfg = match &config {
        Some(path) => nzbd_config::Config::from_toml(&std::fs::read_to_string(path)?)
            .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?,
        None => nzbd_config::Config::default(),
    };
    let bind = bind.unwrap_or_else(|| cfg.api.bind.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let snapshot = nzbd_api::new_shared_snapshot();

        let compat_state = nzbd_compat::CompatState {
            config: Arc::new(nzbd_compat::CompatConfig {
                version: cfg.api.compat_version.clone(),
            }),
            snapshot: snapshot.clone(),
        };

        let app = nzbd_api::router(snapshot.clone()).merge(nzbd_compat::router(compat_state));

        let listener = tokio::net::TcpListener::bind(&bind).await?;
        tracing::info!(%bind, servers = cfg.servers.len(), "nzbd listening (phase 0 scaffold)");

        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
            })
            .await?;
        Ok(())
    })
}

/// Tiny stand-in for `anyhow` to keep phase-0 deps lean.
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
