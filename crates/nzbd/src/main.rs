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
    /// Import an nzbget.conf into nzbd.toml with a mapping report.
    ImportConfig {
        /// Path to the nzbget.conf to import.
        path: PathBuf,
        /// Where to write the converted config.
        #[arg(short, long, default_value = "nzbd.toml")]
        out: PathBuf,
    },
}

fn main() -> anyhow_lite::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
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
        Command::ImportConfig { path, out } => {
            let content = std::fs::read_to_string(&path)?;
            match nzbd_config::import_nzbget_conf(&content) {
                Ok((cfg, report)) => {
                    let toml_text = nzbd_config::to_toml(&cfg)
                        .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
                    std::fs::write(&out, toml_text)?;
                    println!("wrote {}", out.display());
                    println!(
                        "mapped {} options, skipped {} (recognized), {} unknown",
                        report.mapped.len(),
                        report.skipped.len(),
                        report.unknown.len()
                    );
                    for w in &report.warnings {
                        println!("warning: {w}");
                    }
                    if !report.unknown.is_empty() {
                        println!("review by hand: {}", report.unknown.join(", "));
                    }
                    Ok(())
                }
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

/// Map `[post]` config onto the PP manager's runtime config.
fn post_config(cfg: &nzbd_config::Config, slots: usize) -> nzbd_post::manager::PostConfig {
    nzbd_post::manager::PostConfig {
        par2_cmd: cfg.post.par2_cmd.clone(),
        unrar_cmd: cfg.post.unrar_cmd.clone(),
        sevenzip_cmd: cfg.post.sevenzip_cmd.clone(),
        scripts_dir: cfg
            .post
            .scripts_dir
            .as_ref()
            .map(|p| nzbd_config::expand_home(p)),
        unpack: cfg.post.unpack,
        cleanup: cfg.post.cleanup,
        health_action: nzbd_post::manager::HealthAction::parse(&cfg.post.health_action),
        slots,
        tool_timeout: Duration::from_secs(cfg.post.tool_timeout_secs.max(1)),
        script_timeout: Duration::from_secs(cfg.post.script_timeout_secs.max(1)),
        par_fetch_timeout: Duration::from_secs(cfg.post.par_fetch_timeout_secs.max(1)),
    }
}

/// NZBGet-style option projection for the compat shim's `config` method
/// (*arr clients read categories and paths from here).
fn compat_options(cfg: &nzbd_config::Config, bind: &str) -> Vec<(String, String)> {
    let port = bind.rsplit(':').next().unwrap_or("6789").to_string();
    let mut o = vec![
        ("Version".into(), cfg.api.compat_version.clone()),
        ("ControlPort".into(), port),
        ("ControlIP".into(), "0.0.0.0".into()),
        (
            "MainDir".into(),
            nzbd_config::expand_home(&cfg.paths.main_dir)
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "DestDir".into(),
            cfg.dest_dir().to_string_lossy().into_owned(),
        ),
        (
            "InterDir".into(),
            cfg.paths
                .inter_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        (
            "NzbDir".into(),
            cfg.paths
                .nzb_watch_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        (
            "ScriptDir".into(),
            cfg.post
                .scripts_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ),
        (
            "Unpack".into(),
            if cfg.post.unpack { "yes" } else { "no" }.into(),
        ),
        ("PostStrategy".into(), cfg.post.strategy.clone()),
    ];
    for (i, c) in cfg.categories.iter().enumerate() {
        let n = i + 1;
        o.push((format!("Category{n}.Name"), c.name.clone()));
        o.push((
            format!("Category{n}.DestDir"),
            c.dest_dir
                .as_ref()
                .map(|p| nzbd_config::expand_home(p).to_string_lossy().into_owned())
                .unwrap_or_default(),
        ));
        o.push((
            format!("Category{n}.Unpack"),
            if c.unpack.unwrap_or(cfg.post.unpack) {
                "yes"
            } else {
                "no"
            }
            .into(),
        ));
    }
    o
}

/// Open the history store: SQLite index in a node-local dir, authoritative
/// JSONL wherever `jsonl_dir` points (shared volume in cluster mode).
fn open_history(
    local_dir: &std::path::Path,
    jsonl_dir: &std::path::Path,
    node_tag: Option<&str>,
) -> anyhow_lite::Result<Arc<nzbd_state::history::HistoryDb>> {
    std::fs::create_dir_all(local_dir)?;
    std::fs::create_dir_all(jsonl_dir)?;
    nzbd_state::history::HistoryDb::open_tagged(
        &local_dir.join("history.sqlite"),
        Some(jsonl_dir),
        node_tag,
    )
    .map(Arc::new)
    .map_err(|e| anyhow_lite::Error::msg(format!("history db: {e}")))
}

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
        tracing::warn!(
            "no [[server]] configured — the queue will accept jobs but nothing can download"
        );
    }

    let tuning = Tuning {
        article_retries: cfg.queue.article_retries,
        retry_interval: Duration::from_secs(cfg.queue.retry_interval_secs),
        article_timeout: Duration::from_secs(cfg.queue.article_timeout_secs),
        propagation_delay: Duration::from_secs(cfg.queue.propagation_delay_mins as u64 * 60),
        min_free_disk_bytes: cfg.queue.min_free_disk_mb * 1024 * 1024,
        daily_quota_bytes: cfg.queue.daily_quota_mb * 1024 * 1024,
        monthly_quota_bytes: cfg.queue.monthly_quota_mb * 1024 * 1024,
        quota_start_day: cfg.queue.quota_start_day.clamp(1, 28),
        ..Tuning::default()
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    if cfg.cluster.enabled {
        return runtime.block_on(run_cluster(cfg, servers, tuning, bind));
    }

    let engine_cfg = EngineConfig::single_node(
        servers,
        cfg.state_dir(),
        cfg.dest_dir(),
        tuning,
        cfg.speed_limit_bps(),
    );

    runtime.block_on(async move {
        let engine = Engine::spawn(engine_cfg)
            .await
            .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;

        // Post-processing manager (par verify/repair → unpack → cleanup →
        // scripts), watching the engine's finish events.
        let pp_cancel = tokio_util::sync::CancellationToken::new();
        let pp_tracker = tokio_util::task::TaskTracker::new();
        let mut history = None;
        if cfg.post.enabled {
            let state_dir = cfg.state_dir();
            let db = open_history(&state_dir, &state_dir.join("history"), None)?;
            history = Some(db.clone());
            let slots = nzbd_post::manager::strategy_slots(&cfg.post.strategy);
            nzbd_post::manager::spawn_post_manager(
                engine.clone(),
                post_config(&cfg, slots),
                db,
                cfg.dest_dir(),
                None, // single node: always the authority
                pp_cancel.clone(),
                &pp_tracker,
            );
        }
        pp_tracker.close();

        let compat_state = nzbd_compat::CompatState {
            config: Arc::new(nzbd_compat::CompatConfig {
                version: cfg.api.compat_version.clone(),
            }),
            engine: engine.clone(),
            history: history.clone(),
            options: Arc::new(compat_options(&cfg, &bind)),
        };
        let app = nzbd_api::require_auth(
            nzbd_api::router_with(nzbd_api::ApiState {
                engine: engine.clone(),
                history,
            })
            .merge(nzbd_compat::router(compat_state)),
            nzbd_api::AuthConfig {
                username: cfg.api.username.clone(),
                password: cfg.api.password.clone(),
                token: cfg.api.token.clone(),
            },
        );

        let listener = tokio::net::TcpListener::bind(&bind).await?;
        tracing::info!(%bind, "nzbd listening (phase 2: engine + post-processing)");

        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
            })
            .await?;

        pp_cancel.cancel();
        pp_tracker.wait().await;
        engine.shutdown().await;
        Ok(())
    })
}

/// Cluster mode (docs/CLUSTERING.md): shared-volume state, elected leader,
/// distributed download work; this node serves the full API either way.
async fn run_cluster(
    cfg: nzbd_config::Config,
    servers: Vec<nzbd_types::ServerDef>,
    tuning: Tuning,
    bind: String,
) -> anyhow_lite::Result<()> {
    let c = &cfg.cluster;
    let secret = c
        .resolve_secret()
        .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;
    let shared_dir =
        nzbd_config::expand_home(c.shared_dir.as_ref().expect("validated: shared_dir set"));
    // Job data must be visible to every node: default dest to the shared
    // volume unless the operator pointed it there (or elsewhere) already.
    let dest_dir = cfg.dest_dir();
    if !dest_dir.starts_with(&shared_dir) {
        tracing::warn!(
            dest = %dest_dir.display(),
            shared = %shared_dir.display(),
            "dest_dir is outside the shared volume; remote post-processing (phase C2) will not see the files"
        );
    }

    let cluster_cfg = nzbd_cluster::ClusterConfig {
        node_name: c.node_name.clone(),
        shared_dir: shared_dir.clone(),
        advertise_url: c.advertise_url.clone(),
        secret,
        coordinator: c.coordinator,
        priority: c.priority,
        download: c.download,
        max_download_jobs: c.max_download_jobs,
        post_process: c.post_process,
        pp_slots: c.pp_slots.max(1),
        lease_interval: Duration::from_secs(c.lease_interval_secs.max(1)),
        takeover_after: Duration::from_secs(c.takeover_after_secs.max(2)),
        worker_ttl: Duration::from_secs(c.worker_ttl_secs.max(3)),
    };

    // Post-processing wiring (C2): PP runs wherever the leader's
    // anti-affinity scheduler assigns it — as a work lease on an idle node
    // when one exists. History: SQLite index stays node-local, the
    // authoritative JSONL lives on the shared volume.
    let pp = if cfg.post.enabled && cfg.cluster.post_process {
        let jsonl_dir = shared_dir.join(".nzbd-cluster/history");
        let history = open_history(&cfg.state_dir(), &jsonl_dir, Some(&c.node_name))?;
        Some(nzbd_cluster::PpSetup {
            post: post_config(&cfg, cfg.cluster.pp_slots.max(1) as usize),
            history,
        })
    } else {
        None
    };

    let runtime = nzbd_cluster::ClusterRuntime::start(
        cluster_cfg,
        servers,
        tuning,
        dest_dir,
        cfg.speed_limit_bps(),
        pp,
    )
    .await
    .map_err(|e| anyhow_lite::Error::msg(e.to_string()))?;

    let app = runtime.router_with_auth(
        &cfg.api.compat_version,
        compat_options(&cfg, &bind),
        nzbd_api::AuthConfig {
            username: cfg.api.username.clone(),
            password: cfg.api.password.clone(),
            token: cfg.api.token.clone(),
        },
    );
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(
        %bind,
        node = %cfg.cluster.node_name,
        "nzbd listening (cluster mode: C2 distributed post-processing)"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    runtime.shutdown().await;
    Ok(())
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
