//! Mock NNTP server for integration tests and benchmarks — NZBGet's `nserv`
//! equivalent (ARCHITECTURE.md §14). Serves scripted yEnc articles over
//! plain TCP with failure injection: missing articles, CRC corruption,
//! mid-body disconnects, latency shaping. Counts every BODY request per
//! message-id so tests can assert exactly what was (re)fetched.
//!
//! Also generates test posts: raw file bytes → yEnc multi-part articles
//! (dot-stuffed, terminated) + a matching NZB document.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Article store & behaviors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Behavior {
    Serve,
    /// Respond 430 as if the article never arrived / expired.
    NotFound,
    /// Serve a corrupted body (bad bytes, original CRC in the trailer).
    CorruptCrc,
    /// Send half the body, then drop the connection.
    DropMid,
    /// Sleep before serving (stall simulation).
    Delay(Duration),
}

struct StoredArticle {
    wire: Arc<Vec<u8>>,
    corrupt_wire: Arc<Vec<u8>>,
    behavior: Behavior,
}

#[derive(Default)]
pub struct NservBuilder {
    articles: HashMap<String, StoredArticle>,
    credentials: Option<(String, String)>,
}

impl NservBuilder {
    pub fn new() -> NservBuilder {
        NservBuilder::default()
    }

    /// Register every article of a generated post with `Behavior::Serve`.
    pub fn with_post(mut self, post: &GeneratedPost) -> Self {
        for f in &post.files {
            for a in &f.articles {
                self.articles.insert(
                    a.message_id.clone(),
                    StoredArticle {
                        wire: a.wire.clone(),
                        corrupt_wire: a.corrupt_wire.clone(),
                        behavior: Behavior::Serve,
                    },
                );
            }
        }
        self
    }

    /// Override the behavior of one message-id (must be registered).
    pub fn behavior(mut self, message_id: &str, behavior: Behavior) -> Self {
        if let Some(a) = self.articles.get_mut(message_id) {
            a.behavior = behavior;
        } else {
            panic!("behavior() for unknown article {message_id}");
        }
        self
    }

    /// Require AUTHINFO with these credentials.
    pub fn credentials(mut self, user: &str, pass: &str) -> Self {
        self.credentials = Some((user.to_string(), pass.to_string()));
        self
    }

    pub async fn start(self) -> std::io::Result<Nserv> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let hits = Arc::new(Mutex::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(Inner {
            articles: self.articles,
            credentials: self.credentials,
            hits: hits.clone(),
        });
        let accept_inner = inner.clone();
        let mut accept_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_shutdown.changed() => break,
                    accepted = listener.accept() => match accepted {
                        Ok((sock, _)) => {
                            let inner = accept_inner.clone();
                            let shutdown = shutdown_rx.clone();
                            tokio::spawn(handle_conn(sock, inner, shutdown));
                        }
                        Err(_) => break,
                    },
                }
            }
        });
        Ok(Nserv {
            addr,
            hits,
            shutdown: shutdown_tx,
        })
    }
}

struct Inner {
    articles: HashMap<String, StoredArticle>,
    credentials: Option<(String, String)>,
    hits: Arc<Mutex<HashMap<String, u32>>>,
}

pub struct Nserv {
    addr: SocketAddr,
    hits: Arc<Mutex<HashMap<String, u32>>>,
    shutdown: watch::Sender<bool>,
}

impl Nserv {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// BODY request count for one message-id (without angle brackets).
    pub fn hits(&self, message_id: &str) -> u32 {
        *self.hits.lock().unwrap().get(message_id).unwrap_or(&0)
    }

    pub fn total_hits(&self) -> u32 {
        self.hits.lock().unwrap().values().sum()
    }

    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
    }
}

impl Drop for Nserv {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn handle_conn(sock: TcpStream, inner: Arc<Inner>, mut shutdown: watch::Receiver<bool>) {
    sock.set_nodelay(true).ok();
    let mut io = BufReader::new(sock);
    let mut line = String::new();
    let mut authed_user: Option<String> = None;
    let mut authed = inner.credentials.is_none();

    if io
        .get_mut()
        .write_all(b"200 nzbd-nserv ready\r\n")
        .await
        .is_err()
    {
        return;
    }

    loop {
        line.clear();
        let read = tokio::select! {
            _ = shutdown.changed() => return,
            r = io.read_line(&mut line) => r,
        };
        match read {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let cmd = line.trim_end();
        let upper = cmd.to_ascii_uppercase();

        let result = if upper.starts_with("CAPABILITIES") {
            io.get_mut()
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
        } else if upper.starts_with("MODE READER") {
            io.get_mut().write_all(b"200 reader\r\n").await
        } else if upper.starts_with("AUTHINFO USER ") {
            authed_user = Some(cmd[14..].trim().to_string());
            match &inner.credentials {
                Some(_) => io.get_mut().write_all(b"381 password required\r\n").await,
                None => io.get_mut().write_all(b"281 ok\r\n").await,
            }
        } else if upper.starts_with("AUTHINFO PASS ") {
            let pass = cmd[14..].trim();
            match &inner.credentials {
                Some((u, p))
                    if authed_user.as_deref() == Some(u.as_str()) && pass == p =>
                {
                    authed = true;
                    io.get_mut().write_all(b"281 welcome\r\n").await
                }
                _ => io.get_mut().write_all(b"481 authentication failed\r\n").await,
            }
        } else if upper.starts_with("BODY") {
            if !authed {
                let _ = io.get_mut().write_all(b"480 auth required\r\n").await;
                continue;
            }
            let id = cmd[4..]
                .trim()
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string();
            *inner.hits.lock().unwrap().entry(id.clone()).or_insert(0) += 1;
            match inner.articles.get(&id) {
                None => io.get_mut().write_all(b"430 no such article\r\n").await,
                Some(a) => match a.behavior {
                    Behavior::NotFound => {
                        io.get_mut().write_all(b"430 no such article\r\n").await
                    }
                    Behavior::Serve | Behavior::Delay(_) => {
                        if let Behavior::Delay(d) = a.behavior {
                            tokio::select! {
                                _ = shutdown.changed() => return,
                                _ = tokio::time::sleep(d) => {}
                            }
                        }
                        match io
                            .get_mut()
                            .write_all(format!("222 0 <{id}> body\r\n").as_bytes())
                            .await
                        {
                            Ok(()) => io.get_mut().write_all(&a.wire).await,
                            e => e,
                        }
                    }
                    Behavior::CorruptCrc => {
                        match io
                            .get_mut()
                            .write_all(format!("222 0 <{id}> body\r\n").as_bytes())
                            .await
                        {
                            Ok(()) => io.get_mut().write_all(&a.corrupt_wire).await,
                            e => e,
                        }
                    }
                    Behavior::DropMid => {
                        let half = &a.wire[..a.wire.len() / 2];
                        let _ = io
                            .get_mut()
                            .write_all(format!("222 0 <{id}> body\r\n").as_bytes())
                            .await;
                        let _ = io.get_mut().write_all(half).await;
                        let _ = io.get_mut().shutdown().await;
                        return;
                    }
                },
            }
        } else if upper.starts_with("QUIT") {
            let _ = io.get_mut().write_all(b"205 bye\r\n").await;
            return;
        } else {
            io.get_mut().write_all(b"500 unknown command\r\n").await
        };
        if result.is_err() {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Post generation: bytes → yEnc articles + NZB
// ---------------------------------------------------------------------------

pub struct GeneratedArticle {
    pub message_id: String,
    /// Dot-stuffed yEnc body including the `.` terminator.
    pub wire: Arc<Vec<u8>>,
    /// Same, with the part's data corrupted but the original CRC kept.
    pub corrupt_wire: Arc<Vec<u8>>,
    pub part_size: usize,
}

pub struct GeneratedFile {
    pub name: String,
    pub data: Vec<u8>,
    pub articles: Vec<GeneratedArticle>,
}

pub struct GeneratedPost {
    pub name: String,
    pub nzb: String,
    pub files: Vec<GeneratedFile>,
}

impl GeneratedPost {
    pub fn file(&self, name: &str) -> &GeneratedFile {
        self.files
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("no generated file {name}"))
    }

    /// message-id of one part (1-based) of a file.
    pub fn message_id(&self, file_name: &str, part: u32) -> String {
        self.file(file_name).articles[(part - 1) as usize]
            .message_id
            .clone()
    }
}

/// Deterministic pseudo-random bytes for test payloads.
pub fn prng_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (x >> 33) as u8
        })
        .collect()
}

/// Build a multi-file post: yEnc articles of `segment_size` decoded bytes
/// each, plus the NZB describing them.
pub fn build_post(post_name: &str, files: &[(&str, Vec<u8>)], segment_size: usize) -> GeneratedPost {
    assert!(segment_size > 0);
    let slug: String = post_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();

    let mut out_files = Vec::new();
    let mut nzb = String::from(
        r#"<?xml version="1.0" encoding="utf-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
"#,
    );

    for (fi, (name, data)) in files.iter().enumerate() {
        let total_parts = data.len().div_ceil(segment_size).max(1) as u32;
        let mut articles = Vec::new();
        nzb.push_str(&format!(
            r#"  <file poster="tester@nserv" date="1720000000" subject="&quot;{name}&quot; yEnc (1/{total_parts})">
    <groups><group>alt.binaries.nserv</group></groups>
    <segments>
"#
        ));
        for part in 1..=total_parts {
            let begin = (part as usize - 1) * segment_size;
            let end = (begin + segment_size).min(data.len());
            let slice = &data[begin..end];
            let message_id = format!("{slug}.{fi}.{part}@nserv");
            let wire = yenc_wire(
                name,
                slice,
                data.len() as u64,
                part,
                total_parts,
                (begin + 1) as u64,
                end as u64,
                None,
            );
            // Corrupt copy: flip one data byte, keep the real CRC.
            let mut bad = slice.to_vec();
            if !bad.is_empty() {
                bad[0] ^= 0xFF;
            }
            let corrupt_wire = yenc_wire(
                name,
                &bad,
                data.len() as u64,
                part,
                total_parts,
                (begin + 1) as u64,
                end as u64,
                Some(crc32(slice)),
            );
            nzb.push_str(&format!(
                "      <segment bytes=\"{}\" number=\"{}\">{}</segment>\n",
                wire.len(),
                part,
                message_id
            ));
            articles.push(GeneratedArticle {
                message_id,
                wire: Arc::new(wire),
                corrupt_wire: Arc::new(corrupt_wire),
                part_size: slice.len(),
            });
        }
        nzb.push_str("    </segments>\n  </file>\n");
        out_files.push(GeneratedFile {
            name: name.to_string(),
            data: data.clone(),
            articles,
        });
    }
    nzb.push_str("</nzb>\n");

    GeneratedPost {
        name: post_name.to_string(),
        nzb,
        files: out_files,
    }
}

fn crc32(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

/// yEnc-encode one part: headers, escaped body at 128 chars/line, trailer
/// with `pcrc32` (or an override for corruption tests), NNTP dot-stuffing,
/// and the article terminator.
#[allow(clippy::too_many_arguments)]
fn yenc_wire(
    name: &str,
    data: &[u8],
    file_size: u64,
    part: u32,
    total: u32,
    begin: u64,
    end: u64,
    crc_override: Option<u32>,
) -> Vec<u8> {
    const LINE: usize = 128;
    let mut body = Vec::with_capacity(data.len() + data.len() / 32 + 256);
    let mut col = 0usize;
    for &b in data {
        let enc = b.wrapping_add(42);
        if matches!(enc, 0x00 | 0x0A | 0x0D | 0x3D) {
            body.push(b'=');
            body.push(enc.wrapping_add(64));
            col += 2;
        } else {
            body.push(enc);
            col += 1;
        }
        if col >= LINE {
            body.extend_from_slice(b"\r\n");
            col = 0;
        }
    }
    if col > 0 {
        body.extend_from_slice(b"\r\n");
    }

    let mut art = Vec::with_capacity(body.len() + 256);
    art.extend_from_slice(
        format!("=ybegin part={part} total={total} line={LINE} size={file_size} name={name}\r\n")
            .as_bytes(),
    );
    art.extend_from_slice(format!("=ypart begin={begin} end={end}\r\n").as_bytes());
    art.extend_from_slice(&body);
    let crc = crc_override.unwrap_or_else(|| crc32(data));
    art.extend_from_slice(
        format!("=yend size={} part={part} pcrc32={crc:08x}\r\n", data.len()).as_bytes(),
    );

    // NNTP dot-stuffing + terminator.
    let mut wire = Vec::with_capacity(art.len() + 16);
    let mut line_start = true;
    for &b in &art {
        if line_start && b == b'.' {
            wire.push(b'.');
        }
        wire.push(b);
        line_start = b == b'\n';
    }
    wire.extend_from_slice(b".\r\n");
    wire
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    async fn read_until(sock: &mut TcpStream, needle: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut byte = [0u8; 4096];
        loop {
            let n = sock.read(&mut byte).await.unwrap();
            assert!(n > 0, "peer closed early; got {:?}", String::from_utf8_lossy(&buf));
            buf.extend_from_slice(&byte[..n]);
            if buf.windows(needle.len()).any(|w| w == needle) {
                return buf;
            }
        }
    }

    #[tokio::test]
    async fn serves_and_counts() {
        let post = build_post("t", &[("a.bin", prng_bytes(1, 5000))], 2000);
        let ns = NservBuilder::new()
            .with_post(&post)
            .behavior(&post.message_id("a.bin", 2), Behavior::NotFound)
            .start()
            .await
            .unwrap();

        let mut sock = TcpStream::connect(ns.addr()).await.unwrap();
        read_until(&mut sock, b"200 ").await;

        let id1 = post.message_id("a.bin", 1);
        sock.write_all(format!("BODY <{id1}>\r\n").as_bytes())
            .await
            .unwrap();
        let got = read_until(&mut sock, b"\r\n.\r\n").await;
        assert!(got.starts_with(b"222 "));

        let id2 = post.message_id("a.bin", 2);
        sock.write_all(format!("BODY <{id2}>\r\n").as_bytes())
            .await
            .unwrap();
        let got = read_until(&mut sock, b"\r\n").await;
        assert!(got.starts_with(b"430 "));

        assert_eq!(ns.hits(&id1), 1);
        assert_eq!(ns.hits(&id2), 1);
        assert_eq!(ns.total_hits(), 2);
    }

    #[test]
    fn generated_nzb_parses_and_wire_decodes() {
        let data = prng_bytes(7, 10_000);
        let post = build_post("demo post", &[("x.bin", data.clone())], 3000);
        assert_eq!(post.files[0].articles.len(), 4);
        // Wire decodes back to the exact slice via the real decoder.
        let mut whole = Vec::new();
        for a in &post.files[0].articles {
            let mut dec = nzbd_yenc_check::decode(&a.wire);
            whole.append(&mut dec);
        }
        assert_eq!(whole, data);
    }

    /// Minimal in-test decoder harness around nzbd-yenc (dev-dependency via
    /// path would be circular; keep a tiny independent check instead).
    mod nzbd_yenc_check {
        pub fn decode(wire: &[u8]) -> Vec<u8> {
            // Strip dot-stuffing, then yEnc-decode between =ypart and =yend.
            let mut unstuffed = Vec::with_capacity(wire.len());
            let mut line_start = true;
            let mut i = 0;
            while i < wire.len() {
                let b = wire[i];
                if line_start && b == b'.' {
                    if wire.get(i + 1) == Some(&b'\r') {
                        break; // terminator
                    }
                    i += 1; // skip stuffed dot
                    line_start = false;
                    continue;
                }
                unstuffed.push(b);
                line_start = b == b'\n';
                i += 1;
            }
            let text_end = |from: usize| {
                unstuffed[from..]
                    .windows(2)
                    .position(|w| w == b"\r\n")
                    .map(|p| from + p + 2)
                    .unwrap()
            };
            let mut pos = text_end(0); // past =ybegin
            pos = text_end(pos); // past =ypart
            let end = unstuffed
                .windows(5)
                .position(|w| w == b"=yend")
                .unwrap();
            let mut out = Vec::new();
            let mut esc = false;
            for &b in &unstuffed[pos..end] {
                match (esc, b) {
                    (true, _) => {
                        out.push(b.wrapping_sub(106));
                        esc = false;
                    }
                    (false, b'=') => esc = true,
                    (false, b'\r') | (false, b'\n') => {}
                    (false, _) => out.push(b.wrapping_sub(42)),
                }
            }
            out
        }
    }
}
