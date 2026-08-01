//! Best-effort DNS-SD advertisement for first-party clients on the LAN.
//!
//! Discovery is intentionally connection metadata only. Credentials and
//! daemon state never leave the authenticated API.

use mdns_sd::{DaemonEvent, ServiceDaemon, ServiceInfo};
use nzbd_config::ApiConfig;
use std::net::{Ipv4Addr, SocketAddr};

const SERVICE_TYPE: &str = "_nzbd._tcp.local.";
const API_PATH: &str = "/api/v1";

/// Keeps the mDNS daemon alive for as long as the API listener is alive.
pub(crate) struct Advertiser {
    daemon: ServiceDaemon,
    fullname: String,
}

impl Advertiser {
    /// Advertise an API listener. Every error is logged and ignored: multicast
    /// availability must never decide whether nzbd itself can start.
    pub(crate) fn start(
        api: &ApiConfig,
        listener: SocketAddr,
        node_name: Option<&str>,
        tls: bool,
    ) -> Option<Self> {
        if !api.discovery {
            tracing::debug!("local API discovery is disabled");
            return None;
        }
        if listener.ip().is_loopback() {
            tracing::info!(%listener, "not advertising a loopback-only API listener");
            return None;
        }

        Self::start_service(listener, node_name, tls, auth_mode(api))
    }

    /// Advertise an API that is published on the host by another process.
    ///
    /// This is the Docker bridge companion path: the downloader stays on
    /// its application network, while this lightweight process joins the
    /// host network so multicast reaches the physical LAN.
    pub(crate) fn start_standalone(
        port: u16,
        node_name: Option<&str>,
        tls: bool,
        auth: &str,
    ) -> Option<Self> {
        let listener = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        Self::start_service(listener, node_name, tls, auth)
    }

    fn start_service(
        listener: SocketAddr,
        node_name: Option<&str>,
        tls: bool,
        auth: &str,
    ) -> Option<Self> {
        let host_label = host_label(node_name);
        let hostname = format!("{host_label}.local.");
        let instance = format!("nzbd on {}", instance_label(node_name, &host_label));
        let tls_value = if tls { "1" } else { "0" };
        let properties = [
            ("path", API_PATH),
            ("tls", tls_value),
            ("auth", auth),
            ("version", env!("CARGO_PKG_VERSION")),
        ];
        let auto_addresses = listener.ip().is_unspecified();
        let advertised_address = if auto_addresses {
            String::new()
        } else {
            listener.ip().to_string()
        };
        let mut service = match ServiceInfo::new(
            SERVICE_TYPE,
            &instance,
            &hostname,
            advertised_address,
            listener.port(),
            &properties[..],
        ) {
            Ok(service) => service,
            Err(error) => {
                tracing::warn!(%error, "could not construct local API advertisement");
                return None;
            }
        };
        if auto_addresses {
            service = service.enable_addr_auto();
        }
        let fullname = service.get_fullname().to_owned();
        let daemon = match ServiceDaemon::new() {
            Ok(daemon) => daemon,
            Err(error) => {
                tracing::warn!(%error, "could not start local API discovery");
                return None;
            }
        };

        if let Ok(events) = daemon.monitor() {
            let _ = std::thread::Builder::new()
                .name("nzbd-mdns-monitor".into())
                .spawn(move || {
                    while let Ok(event) = events.recv() {
                        match event {
                            DaemonEvent::Error(error) => {
                                tracing::warn!(%error, "local API discovery error");
                            }
                            DaemonEvent::NameChange(change) => {
                                tracing::info!(
                                    original = %change.original,
                                    new_name = %change.new_name,
                                    "resolved a local discovery name collision"
                                );
                            }
                            _ => {}
                        }
                    }
                });
        }

        if let Err(error) = daemon.register(service) {
            tracing::warn!(%error, "could not advertise the local API");
            let _ = daemon.shutdown();
            return None;
        }

        tracing::info!(
            service = %fullname,
            port = listener.port(),
            tls,
            auth,
            "advertising the local API"
        );
        Some(Self { daemon, fullname })
    }
}

impl Drop for Advertiser {
    fn drop(&mut self) {
        // Queue a goodbye before stopping the daemon. Both operations are
        // best-effort because Drop cannot block daemon shutdown/reload.
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

fn auth_mode(api: &ApiConfig) -> &'static str {
    match (api.password.is_some(), api.token.is_some()) {
        (false, false) => "none",
        (true, false) => "basic",
        (false, true) => "bearer",
        (true, true) => "basic,bearer",
    }
}

fn host_label(node_name: Option<&str>) -> String {
    let source = node_name
        .map(str::to_owned)
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_else(|| "nzbd".into());
    dns_label(&source)
}

fn instance_label<'a>(node_name: Option<&'a str>, fallback: &'a str) -> String {
    node_name
        .map(dns_label)
        .unwrap_or_else(|| fallback.to_owned())
}

fn dns_label(value: &str) -> String {
    let first_label = value.split('.').next().unwrap_or(value);
    let mut out = String::with_capacity(first_label.len().min(63));
    let mut last_was_dash = false;
    for character in first_label.chars() {
        if out.len() >= 63 {
            break;
        }
        let character = character.to_ascii_lowercase();
        if character.is_ascii_alphanumeric() {
            out.push(character);
            last_was_dash = false;
        } else if !out.is_empty() && !last_was_dash {
            out.push('-');
            last_was_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "nzbd".into()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_labels_are_safe_and_bounded() {
        assert_eq!(dns_label("Paul's MacBook.local"), "paul-s-macbook");
        assert_eq!(dns_label("---"), "nzbd");
        assert!(dns_label(&"a".repeat(80)).len() <= 63);
    }

    #[test]
    fn auth_metadata_never_contains_credentials() {
        let mut api = ApiConfig::default();
        assert_eq!(auth_mode(&api), "none");
        api.password = Some("secret".into());
        assert_eq!(auth_mode(&api), "basic");
        api.token = Some("more-secret".into());
        assert_eq!(auth_mode(&api), "basic,bearer");
        api.password = None;
        assert_eq!(auth_mode(&api), "bearer");
    }
}
