//! Typed TOML configuration + (phase 3) `nzbget.conf` importer.

use nzbd_types::{CertLevel, ServerDef, ServerId, TlsMode};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("toml: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub paths: Paths,
    #[serde(default, rename = "server")]
    pub servers: Vec<ServerConfig>,
    #[serde(default, rename = "category")]
    pub categories: Vec<CategoryConfig>,
    #[serde(default, rename = "feed")]
    pub feeds: Vec<FeedConfig>,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub post: PostSection,
    #[serde(default)]
    pub cluster: ClusterConfig,
}

/// `[post]` — post-processing (ARCHITECTURE.md §9): par verify/repair,
/// unpack, cleanup, extension scripts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PostSection {
    pub enabled: bool,
    pub par2_cmd: String,
    pub unrar_cmd: String,
    pub sevenzip_cmd: String,
    /// Directory holding NZBGet-style extension scripts (legacy header or
    /// v2 `manifest.json`). None = no scripts.
    pub scripts_dir: Option<PathBuf>,
    pub unpack: bool,
    /// Delete archives/par2/sfv after a successful unpack.
    pub cleanup: bool,
    /// NZBGet `PostStrategy`: sequential | balanced | aggressive | rocket.
    pub strategy: String,
    /// What to do with health-gated failures: none | park | delete
    /// (NZBGet `HealthCheck`).
    pub health_action: String,
    pub tool_timeout_secs: u64,
    pub script_timeout_secs: u64,
    /// How long to wait for delayed par-block downloads during repair.
    pub par_fetch_timeout_secs: u64,
}

impl Default for PostSection {
    fn default() -> Self {
        PostSection {
            enabled: true,
            par2_cmd: "par2".into(),
            unrar_cmd: "unrar".into(),
            sevenzip_cmd: "7z".into(),
            scripts_dir: None,
            unpack: true,
            cleanup: true,
            strategy: "balanced".into(),
            health_action: "none".into(),
            tool_timeout_secs: 3600,
            script_timeout_secs: 3600,
            par_fetch_timeout_secs: 600,
        }
    }
}

/// `[cluster]` — multi-node work distribution (docs/CLUSTERING.md).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    pub enabled: bool,
    /// Unique, stable node name (journal fencing suffix, registry key).
    pub node_name: String,
    /// The shared work volume mount (Gluster).
    pub shared_dir: Option<PathBuf>,
    /// How peers reach this node's API, e.g. "http://10.0.0.11:6789".
    pub advertise_url: String,
    pub secret: Option<String>,
    pub secret_file: Option<PathBuf>,
    /// Eligible for leader election.
    pub coordinator: bool,
    /// Lower = preferred leader (staggers candidacy).
    pub priority: u32,
    pub download: bool,
    pub max_download_jobs: u32,
    /// PP executor role (effective from phase 2 / cluster C2).
    pub post_process: bool,
    pub pp_slots: u32,
    pub lease_interval_secs: u64,
    pub takeover_after_secs: u64,
    pub worker_ttl_secs: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            enabled: false,
            node_name: String::new(),
            shared_dir: None,
            advertise_url: String::new(),
            secret: None,
            secret_file: None,
            coordinator: true,
            priority: 10,
            download: true,
            max_download_jobs: 2,
            post_process: true,
            pp_slots: 1,
            lease_interval_secs: 5,
            takeover_after_secs: 20,
            worker_ttl_secs: 30,
        }
    }
}

impl ClusterConfig {
    /// Resolve the shared secret (inline beats file).
    pub fn resolve_secret(&self) -> Result<String, ConfigError> {
        if let Some(s) = &self.secret {
            return Ok(s.clone());
        }
        if let Some(f) = &self.secret_file {
            return std::fs::read_to_string(expand_home(f))
                .map(|s| s.trim().to_string())
                .map_err(|e| ConfigError::Invalid(format!("secret_file: {e}")));
        }
        Err(ConfigError::Invalid(
            "[cluster] requires secret or secret_file".into(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Paths {
    pub main_dir: PathBuf,
    pub dest_dir: PathBuf,
    pub inter_dir: Option<PathBuf>,
    pub nzb_watch_dir: Option<PathBuf>,
    pub queue_dir: Option<PathBuf>,
    pub temp_dir: Option<PathBuf>,
}

impl Default for Paths {
    fn default() -> Self {
        Paths {
            main_dir: PathBuf::from("~/downloads"),
            dest_dir: PathBuf::from("~/downloads/complete"),
            inter_dir: None,
            nzb_watch_dir: None,
            queue_dir: None,
            temp_dir: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub active: bool,
    pub tier: u8,
    pub group: u8,
    pub fill: bool,
    pub connections: u16,
    pub pipeline_depth: u8,
    pub retention_days: u32,
    pub cert_verification: CertVerification,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            name: String::new(),
            host: String::new(),
            port: 563,
            tls: true,
            username: None,
            password: None,
            active: true,
            tier: 0,
            group: 0,
            fill: false,
            connections: 8,
            pipeline_depth: 2,
            retention_days: 0,
            cert_verification: CertVerification::Strict,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CertVerification {
    None,
    Minimal,
    Strict,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct CategoryConfig {
    pub name: String,
    pub dest_dir: Option<PathBuf>,
    pub unpack: Option<bool>,
    pub extensions: Vec<String>,
}

/// `[[feed]]` — an RSS/Atom indexer feed with an NZBGet-style filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FeedConfig {
    pub name: String,
    pub url: String,
    pub interval_mins: u64,
    /// Filter script (Accept/Reject/Require lines); empty = accept all.
    pub filter: String,
    pub category: Option<String>,
    pub priority: i32,
    pub pause: bool,
}

impl Default for FeedConfig {
    fn default() -> Self {
        FeedConfig {
            name: String::new(),
            url: String::new(),
            interval_mins: 15,
            filter: String::new(),
            category: None,
            priority: 0,
            pause: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QueueConfig {
    pub article_retries: u8,
    pub retry_interval_secs: u64,
    pub article_timeout_secs: u64,
    pub article_cache_mb: u64,
    pub direct_write: bool,
    pub crc_check: bool,
    pub continue_partial: bool,
    pub propagation_delay_mins: u32,
    pub min_free_disk_mb: u64,
    pub speed_limit_kib: Option<u64>,
    /// Daily/monthly download quotas in MB (0 = unlimited); NZBGet
    /// `DailyQuota` / `MonthlyQuota` / `QuotaStartDay`.
    pub daily_quota_mb: u64,
    pub monthly_quota_mb: u64,
    pub quota_start_day: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        // NZBGet-compatible defaults (ARCHITECTURE.md §3.3)
        QueueConfig {
            article_retries: 3,
            retry_interval_secs: 10,
            article_timeout_secs: 60,
            article_cache_mb: 0,
            direct_write: true,
            crc_check: true,
            continue_partial: true,
            propagation_delay_mins: 0,
            min_free_disk_mb: 250,
            speed_limit_kib: None,
            daily_quota_mb: 0,
            monthly_quota_mb: 0,
            quota_start_day: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ApiConfig {
    pub bind: String,
    /// Report this version string on the compat shim's `version` method.
    pub compat_version: String,
    /// Opt-in legacy default credentials for migration (off by default).
    pub allow_legacy_default_credentials: bool,
    /// HTTP Basic auth (NZBGet `ControlUsername`/`ControlPassword`).
    /// Auth is enforced when a password is set; `/healthz` stays open.
    pub username: String,
    pub password: Option<String>,
    /// Bearer token accepted as an alternative to Basic auth.
    pub token: Option<String>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        ApiConfig {
            bind: "127.0.0.1:6789".into(),
            compat_version: "26.2".into(),
            allow_legacy_default_credentials: false,
            username: "nzbd".into(),
            password: None,
            token: None,
        }
    }
}

/// Expand a leading `~`/`~/` to `$HOME` (config-file ergonomics).
pub fn expand_home(p: &std::path::Path) -> PathBuf {
    let Some(s) = p.to_str() else {
        return p.to_path_buf();
    };
    if s == "~" || s.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            if s == "~" {
                return PathBuf::from(home);
            }
            return PathBuf::from(home).join(&s[2..]);
        }
    }
    p.to_path_buf()
}

impl Config {
    pub fn from_toml(s: &str) -> Result<Config, ConfigError> {
        let cfg: Config = toml::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Journal + queue snapshots directory (NZBGet `QueueDir` equivalent):
    /// `paths.queue_dir`, defaulting to `<main_dir>/queue`.
    pub fn state_dir(&self) -> PathBuf {
        match &self.paths.queue_dir {
            Some(d) => expand_home(d),
            None => expand_home(&self.paths.main_dir).join("queue"),
        }
    }

    pub fn dest_dir(&self) -> PathBuf {
        expand_home(&self.paths.dest_dir)
    }

    /// Configured speed limit in bytes/sec.
    pub fn speed_limit_bps(&self) -> Option<u64> {
        self.queue.speed_limit_kib.map(|k| k * 1024)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        for s in &self.servers {
            if s.host.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "server '{}' has no host",
                    s.name
                )));
            }
            if s.connections == 0 {
                return Err(ConfigError::Invalid(format!(
                    "server '{}' has zero connections",
                    s.name
                )));
            }
        }
        for f in &self.feeds {
            if f.name.trim().is_empty() || f.url.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "[[feed]] requires name and url".into(),
                ));
            }
        }
        if self.cluster.enabled {
            if self.cluster.node_name.trim().is_empty() {
                return Err(ConfigError::Invalid("[cluster] requires node_name".into()));
            }
            if self.cluster.shared_dir.is_none() {
                return Err(ConfigError::Invalid("[cluster] requires shared_dir".into()));
            }
            if self.cluster.advertise_url.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "[cluster] requires advertise_url".into(),
                ));
            }
            if self.cluster.secret.is_none() && self.cluster.secret_file.is_none() {
                return Err(ConfigError::Invalid(
                    "[cluster] requires secret or secret_file".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn server_defs(&self) -> Vec<ServerDef> {
        self.servers
            .iter()
            .enumerate()
            .map(|(i, s)| ServerDef {
                id: ServerId(i as u32 + 1),
                name: s.name.clone(),
                host: s.host.clone(),
                port: s.port,
                tls: if s.tls { TlsMode::Tls } else { TlsMode::None },
                username: s.username.clone(),
                password: s.password.clone(),
                active: s.active,
                tier: s.tier,
                group: s.group,
                fill: s.fill,
                max_connections: s.connections,
                pipeline_depth: s.pipeline_depth.max(1),
                retention_days: s.retention_days,
                cert_verification: match s.cert_verification {
                    CertVerification::None => CertLevel::None,
                    CertVerification::Minimal => CertLevel::Minimal,
                    CertVerification::Strict => CertLevel::Strict,
                },
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// nzbget.conf importer (ARCHITECTURE.md §11)
// ---------------------------------------------------------------------------

/// What the importer did with each nzbget.conf option.
#[derive(Debug, Default)]
pub struct ImportReport {
    /// Options mapped onto nzbd config (nzbget key → nzbd setting).
    pub mapped: Vec<(String, String)>,
    /// Recognized-but-intentionally-unmapped options (defaults differ or
    /// the feature is built-in) — safe to ignore.
    pub skipped: Vec<String>,
    /// Options nzbd does not know (yet) — review these by hand.
    pub unknown: Vec<String>,
    /// Anything suspicious (unparsable values, missing hosts, …).
    pub warnings: Vec<String>,
}

/// Map `nzbget.conf` (KEY=value lines with `${Var}` substitution plus
/// `ServerN.*`/`CategoryN.*` blocks) onto [`Config`] with a full report.
pub fn import_nzbget_conf(content: &str) -> Result<(Config, ImportReport), ConfigError> {
    // Pass 1: raw key/value pairs (last one wins, like NZBGet).
    let mut raw: Vec<(String, String)> = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            raw.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    // ${Var} substitution against earlier keys (NZBGet semantics).
    let lookup: std::collections::HashMap<String, String> = raw
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.clone()))
        .collect();
    let expand_once = |v: &str| -> String {
        let mut out = String::with_capacity(v.len());
        let mut rest = v;
        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            match rest[start + 2..].find('}') {
                Some(end) => {
                    let var = &rest[start + 2..start + 2 + end];
                    match lookup.get(&var.to_lowercase()) {
                        Some(val) => out.push_str(val),
                        None => {
                            out.push_str("${");
                            out.push_str(var);
                            out.push('}');
                        }
                    }
                    rest = &rest[start + 2 + end + 1..];
                }
                None => {
                    out.push_str(&rest[start..]);
                    rest = "";
                }
            }
        }
        out.push_str(rest);
        out
    };
    // Nested references (`Category1.DestDir=${DestDir}/movies` where
    // DestDir itself uses ${MainDir}) expand to a fixpoint, cycle-bounded.
    let expand = |v: &str| -> String {
        let mut cur = v.to_string();
        for _ in 0..10 {
            let next = expand_once(&cur);
            if next == cur {
                break;
            }
            cur = next;
        }
        cur
    };

    let mut cfg = Config::default();
    let mut report = ImportReport::default();
    let mut servers: std::collections::BTreeMap<u32, ServerConfig> = Default::default();
    let mut categories: std::collections::BTreeMap<u32, CategoryConfig> = Default::default();
    let mut feeds: std::collections::BTreeMap<u32, FeedConfig> = Default::default();
    let mut control_ip = "127.0.0.1".to_string();
    let mut control_port = "6789".to_string();

    let yes = |v: &str| v.eq_ignore_ascii_case("yes");
    for (key, rawv) in &raw {
        let v = expand(rawv);
        let lk = key.to_lowercase();

        // ServerN.* / CategoryN.* blocks
        if let Some(rest) = lk.strip_prefix("server") {
            if let Some((n, field)) = rest.split_once('.') {
                if let Ok(n) = n.parse::<u32>() {
                    let s = servers.entry(n).or_default();
                    let mapped = match field {
                        "name" => {
                            s.name = v.clone();
                            true
                        }
                        "host" => {
                            s.host = v.clone();
                            true
                        }
                        "port" => {
                            s.port = v.parse().unwrap_or(119);
                            true
                        }
                        "username" => {
                            s.username = (!v.is_empty()).then(|| v.clone());
                            true
                        }
                        "password" => {
                            s.password = (!v.is_empty()).then(|| v.clone());
                            true
                        }
                        "encryption" => {
                            s.tls = yes(&v);
                            true
                        }
                        "connections" => {
                            s.connections = v.parse().unwrap_or(4);
                            true
                        }
                        "level" => {
                            s.tier = v.parse().unwrap_or(0);
                            true
                        }
                        "group" => {
                            s.group = v.parse().unwrap_or(0);
                            true
                        }
                        "optional" => {
                            s.fill = yes(&v);
                            true
                        }
                        "retention" => {
                            s.retention_days = v.parse().unwrap_or(0);
                            true
                        }
                        "active" => {
                            s.active = yes(&v);
                            true
                        }
                        "certverification" => {
                            s.cert_verification = match v.to_lowercase().as_str() {
                                "none" => CertVerification::None,
                                "minimal" => CertVerification::Minimal,
                                _ => CertVerification::Strict,
                            };
                            true
                        }
                        "jointgroup" | "cipher" | "ipversion" | "notes" => {
                            report.skipped.push(key.clone());
                            false
                        }
                        _ => {
                            report.unknown.push(key.clone());
                            false
                        }
                    };
                    if mapped {
                        report
                            .mapped
                            .push((key.clone(), format!("server[{n}].{field}")));
                    }
                    continue;
                }
            }
        }
        if let Some(rest) = lk.strip_prefix("feed") {
            if let Some((n, field)) = rest.split_once('.') {
                if let Ok(n) = n.parse::<u32>() {
                    let f = feeds.entry(n).or_default();
                    let mapped = match field {
                        "name" => {
                            f.name = v.clone();
                            true
                        }
                        "url" => {
                            f.url = v.clone();
                            true
                        }
                        "interval" => {
                            f.interval_mins = v.parse().unwrap_or(15);
                            true
                        }
                        "filter" => {
                            f.filter = v.replace('%', "\n");
                            true
                        }
                        "category" => {
                            f.category = (!v.is_empty()).then(|| v.clone());
                            true
                        }
                        "priority" => {
                            f.priority = v.parse().unwrap_or(0);
                            true
                        }
                        "pausenzb" => {
                            f.pause = yes(&v);
                            true
                        }
                        "backlog" | "extensions" => {
                            report.skipped.push(key.clone());
                            false
                        }
                        _ => {
                            report.unknown.push(key.clone());
                            false
                        }
                    };
                    if mapped {
                        report
                            .mapped
                            .push((key.clone(), format!("feed[{n}].{field}")));
                    }
                    continue;
                }
            }
        }
        if let Some(rest) = lk.strip_prefix("category") {
            if let Some((n, field)) = rest.split_once('.') {
                if let Ok(n) = n.parse::<u32>() {
                    let c = categories.entry(n).or_default();
                    let mapped = match field {
                        "name" => {
                            c.name = v.clone();
                            true
                        }
                        "destdir" => {
                            c.dest_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                            true
                        }
                        "unpack" => {
                            c.unpack = Some(yes(&v));
                            true
                        }
                        "extensions" => {
                            c.extensions = v
                                .split(',')
                                .map(|e| e.trim().to_string())
                                .filter(|e| !e.is_empty())
                                .collect();
                            true
                        }
                        "aliases" => {
                            report.skipped.push(key.clone());
                            false
                        }
                        _ => {
                            report.unknown.push(key.clone());
                            false
                        }
                    };
                    if mapped {
                        report
                            .mapped
                            .push((key.clone(), format!("category[{n}].{field}")));
                    }
                    continue;
                }
            }
        }

        // Scalar options
        let mapped_to: Option<String> = match lk.as_str() {
            "maindir" => {
                cfg.paths.main_dir = PathBuf::from(&v);
                Some("paths.main_dir".into())
            }
            "destdir" => {
                cfg.paths.dest_dir = PathBuf::from(&v);
                Some("paths.dest_dir".into())
            }
            "interdir" => {
                cfg.paths.inter_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.inter_dir".into())
            }
            "nzbdir" => {
                cfg.paths.nzb_watch_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.nzb_watch_dir".into())
            }
            "queuedir" => {
                cfg.paths.queue_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.queue_dir".into())
            }
            "tempdir" => {
                cfg.paths.temp_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("paths.temp_dir".into())
            }
            "controlip" => {
                control_ip = if v == "0.0.0.0" || v.is_empty() {
                    "0.0.0.0".into()
                } else {
                    v.clone()
                };
                Some("api.bind (ip)".into())
            }
            "controlport" => {
                control_port = v.clone();
                Some("api.bind (port)".into())
            }
            "articleretries" | "retries" => {
                cfg.queue.article_retries = v.parse().unwrap_or(3);
                Some("queue.article_retries".into())
            }
            "articleinterval" | "retryinterval" => {
                cfg.queue.retry_interval_secs = v.parse().unwrap_or(10);
                Some("queue.retry_interval_secs".into())
            }
            "articletimeout" => {
                cfg.queue.article_timeout_secs = v.parse().unwrap_or(60);
                Some("queue.article_timeout_secs".into())
            }
            "articlecache" => {
                cfg.queue.article_cache_mb = v.parse().unwrap_or(0);
                Some("queue.article_cache_mb".into())
            }
            "directwrite" => {
                cfg.queue.direct_write = yes(&v);
                Some("queue.direct_write".into())
            }
            "crccheck" => {
                cfg.queue.crc_check = yes(&v);
                Some("queue.crc_check".into())
            }
            "continuepartial" => {
                cfg.queue.continue_partial = yes(&v);
                Some("queue.continue_partial".into())
            }
            "propagationdelay" => {
                cfg.queue.propagation_delay_mins = v.parse().unwrap_or(0);
                Some("queue.propagation_delay_mins".into())
            }
            "diskspace" => {
                cfg.queue.min_free_disk_mb = v.parse().unwrap_or(250);
                Some("queue.min_free_disk_mb".into())
            }
            "dailyquota" => {
                cfg.queue.daily_quota_mb = v.parse().unwrap_or(0);
                Some("queue.daily_quota_mb".into())
            }
            "monthlyquota" => {
                cfg.queue.monthly_quota_mb = v.parse().unwrap_or(0);
                Some("queue.monthly_quota_mb".into())
            }
            "quotastartday" => {
                cfg.queue.quota_start_day = v.parse().unwrap_or(1);
                Some("queue.quota_start_day".into())
            }
            "downloadrate" => {
                let kib: u64 = v.parse().unwrap_or(0);
                cfg.queue.speed_limit_kib = (kib > 0).then_some(kib);
                Some("queue.speed_limit_kib".into())
            }
            "unrarcmd" => {
                cfg.post.unrar_cmd = v.clone();
                Some("post.unrar_cmd".into())
            }
            "sevenzipcmd" => {
                cfg.post.sevenzip_cmd = v.clone();
                Some("post.sevenzip_cmd".into())
            }
            "scriptdir" => {
                cfg.post.scripts_dir = (!v.is_empty()).then(|| PathBuf::from(&v));
                Some("post.scripts_dir".into())
            }
            "unpack" => {
                cfg.post.unpack = yes(&v);
                Some("post.unpack".into())
            }
            "healthcheck" => {
                cfg.post.health_action = v.to_lowercase();
                Some("post.health_action".into())
            }
            "unpackcleanupdisk" => {
                cfg.post.cleanup = yes(&v);
                Some("post.cleanup".into())
            }
            "poststrategy" => {
                cfg.post.strategy = v.to_lowercase();
                Some("post.strategy".into())
            }
            // Recognized, intentionally unmapped (built-in, obsolete, or a
            // policy nzbd handles differently).
            "parcheck"
            | "parrepair"
            | "parscan"
            | "parbuffer"
            | "parthreads"
            | "parquick"
            | "parrename"
            | "rarrename"
            | "directunpack"
            | "scriptorder"
            | "extensions"
            | "shelloverride"
            | "eventinterval"
            | "umask"
            | "daemonusername"
            | "lockfile"
            | "logfile"
            | "writelog"
            | "rotatelog"
            | "errortarget"
            | "warningtarget"
            | "infotarget"
            | "detailtarget"
            | "debugtarget"
            | "nzblog"
            | "crashtrace"
            | "crashdump"
            | "timecorrection"
            | "outputmode"
            | "curses"
            | "updatecheck"
            | "appbin"
            | "appdir"
            | "version"
            | "configfile"
            | "webdir"
            | "confighome"
            | "securecontrol"
            | "secureport"
            | "securecert"
            | "securekey"
            | "certstore"
            | "certcheck"
            | "authorizedip"
            | "controlusername"
            | "controlpassword"
            | "restrictedusername"
            | "restrictedpassword"
            | "addusername"
            | "addpassword"
            | "formauth"
            | "urlconnections"
            | "urlforce"
            | "urlinterval"
            | "urltimeout"
            | "remotetimeout"
            | "downloadqueue"
            | "reloadqueue"
            | "flushqueue"
            | "dupecheck"
            | "tempdircleanup"
            | "keephistory"
            | "feedhistory"
            | "skipwrite"
            | "rawarticle"
            | "articlereadchunksize"
            | "nzbdirinterval"
            | "nzbdirfilesage"
            | "dupescope" => {
                report.skipped.push(key.clone());
                None
            }
            _ => {
                report.unknown.push(key.clone());
                None
            }
        };
        if let Some(target) = mapped_to {
            report.mapped.push((key.clone(), target));
        }
    }

    cfg.api.bind = format!("{control_ip}:{control_port}");
    cfg.servers = servers.into_values().collect();
    cfg.categories = categories
        .into_values()
        .filter(|c| !c.name.is_empty())
        .collect();
    cfg.feeds = feeds
        .into_values()
        .filter(|f| !f.name.is_empty() && !f.url.is_empty())
        .collect();

    for (i, s) in cfg.servers.iter().enumerate() {
        if s.host.is_empty() {
            report
                .warnings
                .push(format!("server #{} has no host — dropped", i + 1));
        }
    }
    cfg.servers.retain(|s| !s.host.is_empty());
    for s in &mut cfg.servers {
        if s.connections == 0 {
            report
                .warnings
                .push(format!("server '{}': Connections=0 raised to 1", s.name));
            s.connections = 1;
        }
    }

    cfg.validate()?;
    Ok((cfg, report))
}

/// Render a [`Config`] as nzbd.toml text.
pub fn to_toml(cfg: &Config) -> Result<String, ConfigError> {
    toml::to_string_pretty(cfg).map_err(|e| ConfigError::Invalid(format!("serialize: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[paths]
main_dir = "/data/usenet"
dest_dir = "/data/usenet/complete"

[[server]]
name = "primary"
host = "news.provider.example"
port = 563
tls = true
username = "u"
password = "p"
connections = 30
pipeline_depth = 4

[[server]]
name = "block"
host = "fill.provider.example"
tier = 1
fill = true
connections = 8

[queue]
article_cache_mb = 512

[api]
bind = "0.0.0.0:6789"
"#;

    #[test]
    fn parses_and_maps() {
        let cfg = Config::from_toml(SAMPLE).unwrap();
        assert_eq!(cfg.servers.len(), 2);
        assert_eq!(cfg.queue.article_cache_mb, 512);
        assert_eq!(cfg.queue.article_retries, 3); // default preserved
        let defs = cfg.server_defs();
        assert_eq!(defs[0].max_connections, 30);
        assert_eq!(defs[0].pipeline_depth, 4);
        assert_eq!(defs[1].tier, 1);
        assert!(defs[1].fill);
        assert_eq!(cfg.api.compat_version, "26.2");
    }

    #[test]
    fn rejects_bad_config() {
        assert!(Config::from_toml("[[server]]\nname = \"x\"").is_err()); // no host
        assert!(Config::from_toml("nonsense_key = 1").is_err()); // unknown field
    }

    #[test]
    fn defaults_match_nzbget() {
        let q = QueueConfig::default();
        assert!(q.direct_write);
        assert_eq!(q.article_retries, 3);
        assert_eq!(q.retry_interval_secs, 10);
        assert_eq!(q.article_timeout_secs, 60);
        assert_eq!(q.min_free_disk_mb, 250);
    }

    #[test]
    fn cluster_section_parses_and_validates() {
        let toml = r#"
[cluster]
enabled = true
node_name = "node-a"
shared_dir = "/mnt/work"
advertise_url = "http://10.0.0.11:6789"
secret = "hunter2"
priority = 3
download = true
max_download_jobs = 4
"#;
        let cfg = Config::from_toml(toml).unwrap();
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.node_name, "node-a");
        assert_eq!(cfg.cluster.priority, 3);
        assert_eq!(cfg.cluster.max_download_jobs, 4);
        assert_eq!(cfg.cluster.lease_interval_secs, 5); // default preserved
        assert_eq!(cfg.cluster.resolve_secret().unwrap(), "hunter2");

        // Missing requirements are rejected loudly.
        for broken in [
            "[cluster]\nenabled = true",
            "[cluster]\nenabled = true\nnode_name = \"a\"\nshared_dir = \"/x\"\nadvertise_url = \"http://a\"",
        ] {
            assert!(Config::from_toml(broken).is_err(), "{broken}");
        }
        // Disabled cluster needs nothing.
        assert!(Config::from_toml("[cluster]\nenabled = false").is_ok());
    }

    #[test]
    fn post_section_parses_with_defaults() {
        let cfg = Config::from_toml(
            "[post]\nstrategy = \"rocket\"\nscripts_dir = \"/opt/scripts\"\nunpack = false",
        )
        .unwrap();
        assert!(cfg.post.enabled);
        assert_eq!(cfg.post.strategy, "rocket");
        assert!(!cfg.post.unpack);
        assert!(cfg.post.cleanup);
        assert_eq!(cfg.post.par2_cmd, "par2");
        assert_eq!(cfg.post.scripts_dir, Some(PathBuf::from("/opt/scripts")));
        // Absent section = NZBGet-flavored defaults.
        let def = Config::from_toml("").unwrap();
        assert_eq!(def.post.strategy, "balanced");
        assert_eq!(def.post.tool_timeout_secs, 3600);
    }

    const NZBGET_CONF: &str = r#"
# Typical nzbget.conf excerpt
MainDir=/data/usenet
DestDir=${MainDir}/dst
InterDir=${MainDir}/inter
NzbDir=${MainDir}/nzb
QueueDir=${MainDir}/queue
TempDir=${MainDir}/tmp
ControlIP=0.0.0.0
ControlPort=6789
ControlUsername=nzbget
ControlPassword=tegbzn6789

Server1.Name=main
Server1.Level=0
Server1.Host=news.example.com
Server1.Port=563
Server1.Username=user1
Server1.Password=pass1
Server1.Encryption=yes
Server1.Connections=30
Server1.Retention=4500
Server1.Active=yes
Server1.CertVerification=strict

Server2.Name=fill
Server2.Level=1
Server2.Optional=yes
Server2.Host=fill.example.com
Server2.Port=119
Server2.Encryption=no
Server2.Connections=8
Server2.Active=yes

Category1.Name=movies
Category1.DestDir=${DestDir}/movies
Category2.Name=tv
Category2.Unpack=no

ArticleCache=700
DirectWrite=yes
CrcCheck=yes
ContinuePartial=yes
ArticleRetries=3
ArticleInterval=10
ArticleTimeout=60
DownloadRate=8000
DiskSpace=250
PropagationDelay=0

Unpack=yes
UnpackCleanupDisk=yes
UnrarCmd=unrar
SevenZipCmd=7z
ScriptDir=${MainDir}/scripts
PostStrategy=aggressive
ParCheck=auto
ParRepair=yes
KeepHistory=30
FutureOption=whatever
"#;

    #[test]
    fn nzbget_conf_import_maps_everything() {
        let (cfg, report) = import_nzbget_conf(NZBGET_CONF).unwrap();

        // ${Var} substitution + paths
        assert_eq!(cfg.paths.main_dir, PathBuf::from("/data/usenet"));
        assert_eq!(cfg.paths.dest_dir, PathBuf::from("/data/usenet/dst"));
        assert_eq!(
            cfg.paths.inter_dir,
            Some(PathBuf::from("/data/usenet/inter"))
        );
        assert_eq!(
            cfg.paths.queue_dir,
            Some(PathBuf::from("/data/usenet/queue"))
        );
        assert_eq!(cfg.api.bind, "0.0.0.0:6789");

        // Servers with NZBGet vocabulary translated (Level→tier,
        // Optional→fill, Encryption→tls)
        assert_eq!(cfg.servers.len(), 2);
        let s1 = &cfg.servers[0];
        assert_eq!(s1.name, "main");
        assert_eq!(s1.host, "news.example.com");
        assert_eq!(s1.port, 563);
        assert!(s1.tls);
        assert_eq!(s1.connections, 30);
        assert_eq!(s1.tier, 0);
        assert_eq!(s1.retention_days, 4500);
        assert_eq!(s1.username.as_deref(), Some("user1"));
        let s2 = &cfg.servers[1];
        assert_eq!(s2.tier, 1);
        assert!(s2.fill, "Optional=yes becomes a fill server");
        assert!(!s2.tls);

        // Categories
        assert_eq!(cfg.categories.len(), 2);
        assert_eq!(cfg.categories[0].name, "movies");
        assert_eq!(
            cfg.categories[0].dest_dir,
            Some(PathBuf::from("/data/usenet/dst/movies"))
        );
        assert_eq!(cfg.categories[1].unpack, Some(false));

        // Queue + post
        assert_eq!(cfg.queue.article_cache_mb, 700);
        assert_eq!(cfg.queue.speed_limit_kib, Some(8000));
        assert!(cfg.post.unpack);
        assert_eq!(cfg.post.strategy, "aggressive");
        assert_eq!(
            cfg.post.scripts_dir,
            Some(PathBuf::from("/data/usenet/scripts"))
        );

        // Report: mapped entries exist; auth options are recognized-skipped;
        // unknown future options surface for review.
        assert!(report.mapped.iter().any(|(k, _)| k == "MainDir"));
        assert!(report
            .mapped
            .iter()
            .any(|(k, t)| k == "Server1.Host" && t == "server[1].host"));
        assert!(report.skipped.iter().any(|k| k == "ControlPassword"));
        assert!(report.skipped.iter().any(|k| k == "ParCheck"));
        assert!(report.unknown.iter().any(|k| k == "FutureOption"));
        assert!(report.warnings.is_empty(), "{:?}", report.warnings);

        // The imported config round-trips through nzbd.toml.
        let toml_text = to_toml(&cfg).unwrap();
        let re = Config::from_toml(&toml_text).unwrap();
        assert_eq!(re.servers.len(), 2);
        assert_eq!(re.queue.article_cache_mb, 700);
    }

    #[test]
    fn nzbget_conf_import_drops_hostless_servers() {
        let (cfg, report) = import_nzbget_conf(
            "MainDir=/x
Server1.Name=ghost
Server1.Connections=4
             Server2.Host=ok.example
Server2.Connections=0
",
        )
        .unwrap();
        assert_eq!(cfg.servers.len(), 1);
        assert_eq!(cfg.servers[0].host, "ok.example");
        assert_eq!(cfg.servers[0].connections, 1, "zero raised to one");
        assert_eq!(report.warnings.len(), 2);
    }

    #[test]
    fn feed_sections_parse_and_import() {
        let cfg = Config::from_toml(
            "[[feed]]\nname = \"idx\"\nurl = \"https://idx.example/rss\"\n\
             interval_mins = 30\nfilter = \"Accept: *1080p*\"\ncategory = \"tv\"",
        )
        .unwrap();
        assert_eq!(cfg.feeds.len(), 1);
        assert_eq!(cfg.feeds[0].interval_mins, 30);
        assert_eq!(cfg.feeds[0].category.as_deref(), Some("tv"));
        // name+url required
        assert!(Config::from_toml("[[feed]]\nname = \"x\"").is_err());

        // nzbget.conf FeedN.* import (% is NZBGet's newline in filters).
        let (cfg, report) = import_nzbget_conf(
            "Feed1.Name=idx\nFeed1.URL=https://idx.example/rss\n\
             Feed1.Interval=45\nFeed1.Filter=Accept: *1080p* % Reject: *x265*\n\
             Feed1.Category=tv\nFeed1.PauseNzb=no\nFeed1.Backlog=yes\n",
        )
        .unwrap();
        assert_eq!(cfg.feeds.len(), 1);
        assert_eq!(cfg.feeds[0].interval_mins, 45);
        assert!(cfg.feeds[0].filter.contains('\n'), "% becomes newline");
        assert!(report.mapped.iter().any(|(k, _)| k == "Feed1.URL"));
        assert!(report.skipped.iter().any(|k| k == "Feed1.Backlog"));
    }

    #[test]
    fn path_helpers() {
        let cfg = Config::from_toml(SAMPLE).unwrap();
        assert_eq!(cfg.state_dir(), PathBuf::from("/data/usenet/queue"));
        assert_eq!(cfg.dest_dir(), PathBuf::from("/data/usenet/complete"));
        assert_eq!(cfg.speed_limit_bps(), None);

        let home = std::env::var("HOME").unwrap();
        let def = Config::default();
        assert_eq!(
            def.state_dir(),
            PathBuf::from(&home).join("downloads/queue")
        );
    }
}
