use nzbd_torrent::{
    install_process_crypto_provider, CryptoProviderInstall, TorrentSession, TorrentSessionConfig,
};

#[tokio::test]
async fn explicit_aws_lc_provider_allows_librqbit_rustls_client() {
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_none(),
        "this integration-test process must begin before rustls auto-selection"
    );
    assert_eq!(
        install_process_crypto_provider().unwrap(),
        CryptoProviderInstall::InstalledAwsLc
    );
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());

    let dir = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(dir.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    assert_eq!(session.tcp_listen_port(), None);
    session.stop().await;
}
