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
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
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
}

impl Default for ApiConfig {
    fn default() -> Self {
        ApiConfig {
            bind: "127.0.0.1:6789".into(),
            compat_version: "26.2".into(),
            allow_legacy_default_credentials: false,
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

/// Phase 3: map `nzbget.conf` (117 scalar options + `ServerN.*`/`CategoryN.*`/
/// `TaskN.*` blocks) onto [`Config`], with an import report.
pub fn import_nzbget_conf(_content: &str) -> Result<Config, ConfigError> {
    Err(ConfigError::Invalid(
        "nzbget.conf import lands in phase 3 (see ARCHITECTURE.md §11)".into(),
    ))
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
