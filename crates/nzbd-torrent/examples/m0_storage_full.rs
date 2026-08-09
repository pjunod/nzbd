use anyhow::Result;
use librqbit::storage::filesystem::FilesystemStorageFactory;
use librqbit::storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage};
use librqbit::{ManagedTorrentShared, TorrentMetadata};
use nzbd_torrent::{TorrentAddConfig, TorrentPhase, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};
use std::error::Error;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const FAULT_FILE_NAME: &str = "storage-full.bin";
const CONTROL_FILE_NAME: &str = "storage-control.bin";
const PIECE_LENGTH: usize = 16 * 1024;
const FAULT_PAYLOAD_BYTES: usize = 256 * 1024;
const CONTROL_PAYLOAD_BYTES: usize = 64 * 1024;
const FAULT_DEADLINE: Duration = Duration::from_secs(20);
const CONTROL_DEADLINE: Duration = Duration::from_secs(20);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const INJECTED_ERROR: &str = "injected M0 storage-full fault";

#[derive(Clone)]
struct StorageFullFactory {
    write_attempts: Arc<AtomicUsize>,
    successful_bytes: Arc<AtomicUsize>,
    successful_writes: usize,
}

impl StorageFactory for StorageFullFactory {
    type Storage = StorageFullStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> Result<Self::Storage> {
        Ok(StorageFullStorage {
            inner: Box::new(FilesystemStorageFactory::default().create(shared, metadata)?),
            write_attempts: self.write_attempts.clone(),
            successful_bytes: self.successful_bytes.clone(),
            successful_writes: self.successful_writes,
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

struct StorageFullStorage {
    inner: Box<dyn TorrentStorage>,
    write_attempts: Arc<AtomicUsize>,
    successful_bytes: Arc<AtomicUsize>,
    successful_writes: usize,
}

impl TorrentStorage for StorageFullStorage {
    fn init(&mut self, shared: &ManagedTorrentShared, metadata: &TorrentMetadata) -> Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buffer: &mut [u8]) -> Result<()> {
        self.inner.pread_exact(file_id, offset, buffer)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buffer: &[u8]) -> Result<()> {
        let attempt = self.write_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt >= self.successful_writes {
            return Err(io::Error::new(io::ErrorKind::StorageFull, INJECTED_ERROR).into());
        }
        self.inner.pwrite_all(file_id, offset, buffer)?;
        self.successful_bytes
            .fetch_add(buffer.len(), Ordering::SeqCst);
        Ok(())
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> Result<()> {
        self.inner.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &Path) -> Result<()> {
        self.inner.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> Result<()> {
        self.inner.ensure_file_length(file_id, length)
    }

    fn take(&self) -> Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(Self {
            inner: self.inner.take()?,
            write_attempts: self.write_attempts.clone(),
            successful_bytes: self.successful_bytes.clone(),
            successful_writes: self.successful_writes,
        }))
    }
}

fn payload(size: usize, salt: usize) -> Vec<u8> {
    (0..size)
        .map(|index| ((index * 31 + index / 7 + salt) % 251) as u8)
        .collect()
}

fn bencode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(bytes.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(bytes);
}

fn metainfo(payload: &[u8], filename: &str) -> Vec<u8> {
    let mut pieces = Vec::new();
    for piece in payload.chunks(PIECE_LENGTH) {
        pieces.extend_from_slice(&Sha1::digest(piece));
    }

    let mut info = Vec::new();
    info.push(b'd');
    bencode_bytes(&mut info, b"length");
    info.extend_from_slice(format!("i{}e", payload.len()).as_bytes());
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, filename.as_bytes());
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(format!("i{PIECE_LENGTH}e").as_bytes());
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &pieces);
    info.push(b'e');

    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.push(b'e');
    torrent
}

fn free_port_range() -> Range<u16> {
    loop {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("bind a temporary loopback listener");
        let port = listener
            .local_addr()
            .expect("read the temporary listener address")
            .port();
        drop(listener);
        if port < u16::MAX {
            return port..port + 1;
        }
    }
}

async fn stop_with_deadline(session: TorrentSession, name: &str) -> Result<(), Box<dyn Error>> {
    tokio::time::timeout(SHUTDOWN_DEADLINE, session.stop())
        .await
        .map_err(|_| io::Error::other(format!("{name} shutdown exceeded 10 seconds")))?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let fault_payload = payload(FAULT_PAYLOAD_BYTES, 7);
    let control_payload = payload(CONTROL_PAYLOAD_BYTES, 19);
    let fault_metainfo = metainfo(&fault_payload, FAULT_FILE_NAME);
    let control_metainfo = metainfo(&control_payload, CONTROL_FILE_NAME);

    let seed_root = tempfile::tempdir()?;
    std::fs::write(seed_root.path().join(FAULT_FILE_NAME), &fault_payload)?;
    std::fs::write(seed_root.path().join(CONTROL_FILE_NAME), &control_payload)?;
    let listen_ports = free_port_range();
    let seeder_port = listen_ports.start;
    let seeder = TorrentSession::start(
        seed_root.path().to_path_buf(),
        TorrentSessionConfig {
            listen_port_range: Some(listen_ports),
            ..Default::default()
        },
    )
    .await?;
    let fault_seed = seeder
        .add_metainfo(
            fault_metainfo.clone(),
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    let control_seed = seeder
        .add_metainfo(
            control_metainfo.clone(),
            TorrentAddConfig {
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    tokio::time::timeout(FAULT_DEADLINE, fault_seed.wait_until_completed())
        .await
        .map_err(|_| io::Error::other("fault seeder initialization exceeded 20 seconds"))??;
    tokio::time::timeout(FAULT_DEADLINE, control_seed.wait_until_completed())
        .await
        .map_err(|_| io::Error::other("control seeder initialization exceeded 20 seconds"))??;

    let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, seeder_port));
    let download_root = tempfile::tempdir()?;
    let downloader =
        TorrentSession::start(download_root.path().to_path_buf(), Default::default()).await?;
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let successful_bytes = Arc::new(AtomicUsize::new(0));
    let fault = downloader
        .add_metainfo_with_storage_for_m0(
            fault_metainfo,
            TorrentAddConfig {
                overwrite: true,
                initial_peers: vec![peer],
                ..Default::default()
            },
            StorageFullFactory {
                write_attempts: write_attempts.clone(),
                successful_bytes: successful_bytes.clone(),
                successful_writes: 1,
            }
            .boxed(),
        )
        .await?;

    let fault_started = Instant::now();
    tokio::time::timeout(FAULT_DEADLINE, async {
        loop {
            if fault.stats().phase == TorrentPhase::Error {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::other("injected storage-full fault was not visible within 20 seconds")
    })?;
    let fault_elapsed = fault_started.elapsed();
    let stats_started = Instant::now();
    let fault_stats = fault.stats();
    let stats_elapsed = stats_started.elapsed();
    if fault_stats.finished
        || fault_stats.progress_bytes >= fault_stats.total_bytes
        || fault_stats
            .error
            .as_deref()
            .is_none_or(|error| !error.contains(INJECTED_ERROR))
        || write_attempts.load(Ordering::SeqCst) < 2
        || successful_bytes.load(Ordering::SeqCst) != PIECE_LENGTH
    {
        return Err(io::Error::other(format!(
            "storage-full state did not match the contract: stats={fault_stats:?}, write_attempts={}, successful_bytes={}",
            write_attempts.load(Ordering::SeqCst),
            successful_bytes.load(Ordering::SeqCst)
        ))
        .into());
    }
    if stats_elapsed > Duration::from_secs(1) {
        return Err(io::Error::other("faulted torrent stats took more than one second").into());
    }
    if std::fs::read(download_root.path().join(FAULT_FILE_NAME))? == fault_payload {
        return Err(io::Error::other("faulted payload was incorrectly complete").into());
    }

    let control_started = Instant::now();
    let control = downloader
        .add_metainfo(
            control_metainfo,
            TorrentAddConfig {
                overwrite: true,
                initial_peers: vec![peer],
                ..Default::default()
            },
        )
        .await?;
    tokio::time::timeout(CONTROL_DEADLINE, control.wait_until_completed())
        .await
        .map_err(|_| io::Error::other("same-session control download exceeded 20 seconds"))??;
    let control_elapsed = control_started.elapsed();
    if std::fs::read(download_root.path().join(CONTROL_FILE_NAME))? != control_payload {
        return Err(io::Error::other("same-session control payload did not match").into());
    }

    stop_with_deadline(downloader, "downloader").await?;
    stop_with_deadline(seeder, "seeder").await?;
    println!(
        "bittorrent_storage_full fault_kind=StorageFull successful_writes=1 successful_bytes={} write_attempts={} fault_phase={:?} fault_progress_bytes={} fault_total_bytes={} fault_ms={} stats_ms={} control_bytes={} control_ms={}",
        successful_bytes.load(Ordering::SeqCst),
        write_attempts.load(Ordering::SeqCst),
        fault_stats.phase,
        fault_stats.progress_bytes,
        fault_stats.total_bytes,
        fault_elapsed.as_millis(),
        stats_elapsed.as_millis(),
        control_payload.len(),
        control_elapsed.as_millis()
    );
    Ok(())
}
