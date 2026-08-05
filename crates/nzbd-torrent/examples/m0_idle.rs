use nzbd_torrent::{TorrentSession, TorrentSessionConfig};

#[tokio::main]
async fn main() {
    let output = tempfile::tempdir().expect("create M0 output directory");
    let session =
        TorrentSession::start(output.path().to_path_buf(), TorrentSessionConfig::default())
            .await
            .expect("start isolated torrent session");
    session.stop().await;
}
