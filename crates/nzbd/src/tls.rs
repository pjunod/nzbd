//! Built-in HTTPS for the API/UI (`[api] tls = true`).
//!
//! With explicit `tls_cert`/`tls_key` paths, those PEM files are loaded.
//! With neither set, a self-signed certificate is generated ONCE into
//! `<state_dir>/tls/` and reused on every boot, so the fingerprint your
//! devices trusted stays stable. Browsers only grant service workers and
//! PWA install to origins they consider secure — a self-signed cert
//! qualifies once it's trusted on the device (clicking through the
//! warning is not enough on Chrome/Android).

use crate::anyhow_lite;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct TlsSetup {
    pub config: Arc<rustls::ServerConfig>,
    /// Human-readable note for the startup log (cert origin/fingerprint).
    pub note: String,
}

fn err(msg: String) -> anyhow_lite::Error {
    anyhow_lite::Error::msg(msg)
}

/// Build the server TLS config, generating a persistent self-signed cert
/// when none is configured. Returns None when `[api] tls` is off.
pub fn server_config(
    cfg: &nzbd_config::Config,
    state_dir: &Path,
) -> anyhow_lite::Result<Option<TlsSetup>> {
    if !cfg.api.tls {
        return Ok(None);
    }
    let (cert_path, key_path, generated) = match (&cfg.api.tls_cert, &cfg.api.tls_key) {
        (Some(c), Some(k)) => (
            nzbd_config::expand_home(c),
            nzbd_config::expand_home(k),
            false,
        ),
        (None, None) => {
            let dir = state_dir.join("tls");
            (dir.join("cert.pem"), dir.join("key.pem"), true)
        }
        _ => {
            return Err(err(
                "[api] tls_cert and tls_key must be set together (or neither, for self-signed)"
                    .into(),
            ))
        }
    };

    if generated && !cert_path.exists() {
        generate_self_signed(&cert_path, &key_path, &cfg.api.tls_sans)?;
    }

    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = {
        let pem = std::fs::read(&cert_path)
            .map_err(|e| err(format!("read {}: {e}", cert_path.display())))?;
        rustls_pemfile::certs(&mut pem.as_slice())
            .collect::<Result<_, _>>()
            .map_err(|e| err(format!("parse {}: {e}", cert_path.display())))?
    };
    if certs.is_empty() {
        return Err(err(format!(
            "{}: no certificates found",
            cert_path.display()
        )));
    }
    let key = {
        let pem = std::fs::read(&key_path)
            .map_err(|e| err(format!("read {}: {e}", key_path.display())))?;
        rustls_pemfile::private_key(&mut pem.as_slice())
            .map_err(|e| err(format!("parse {}: {e}", key_path.display())))?
            .ok_or_else(|| err(format!("{}: no private key found", key_path.display())))?
    };

    let fingerprint = sha256_hex(certs[0].as_ref());
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| err(format!("tls config: {e}")))?;
    let note = if generated {
        format!(
            "self-signed cert {} (sha256 {fingerprint}) — trust it on your devices for full PWA",
            cert_path.display()
        )
    } else {
        format!("cert {} (sha256 {fingerprint})", cert_path.display())
    };
    Ok(Some(TlsSetup {
        config: Arc::new(config),
        note,
    }))
}

fn generate_self_signed(
    cert_path: &PathBuf,
    key_path: &PathBuf,
    extra_sans: &[String],
) -> anyhow_lite::Result<()> {
    let mut sans: Vec<String> = vec!["localhost".into(), "nzbd".into()];
    if let Ok(host) = std::env::var("HOSTNAME") {
        if !host.is_empty() {
            sans.push(host);
        }
    }
    sans.extend(extra_sans.iter().cloned());
    sans.dedup();
    let cert = rcgen::generate_simple_self_signed(sans.clone())
        .map_err(|e| err(format!("generate self-signed cert: {e}")))?;
    if let Some(parent) = cert_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| err(format!("create {}: {e}", parent.display())))?;
    }
    std::fs::write(cert_path, cert.cert.pem())
        .map_err(|e| err(format!("write {}: {e}", cert_path.display())))?;
    write_private(key_path, cert.key_pair.serialize_pem().as_bytes())?;
    tracing::info!(
        cert = %cert_path.display(),
        sans = %sans.join(", "),
        "generated self-signed TLS certificate (persisted; reused on next boot)"
    );
    Ok(())
}

/// 0600 on unix; plain write elsewhere.
fn write_private(path: &PathBuf, bytes: &[u8]) -> anyhow_lite::Result<()> {
    std::fs::write(path, bytes).map_err(|e| err(format!("write {}: {e}", path.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

fn sha256_hex(der: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    Sha256::digest(der)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}
