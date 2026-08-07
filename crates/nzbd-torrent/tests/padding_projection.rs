use nzbd_torrent::{TorrentAddConfig, TorrentSession, TorrentSessionConfig};
use sha1::{Digest, Sha1};

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn file(out: &mut Vec<u8>, path: &str, len: usize, padding: bool) {
    out.push(b'd');
    if padding {
        bencode_bytes(out, b"attr");
        bencode_bytes(out, b"p");
    }
    bencode_bytes(out, b"length");
    out.extend_from_slice(format!("i{len}e").as_bytes());
    bencode_bytes(out, b"path");
    out.push(b'l');
    bencode_bytes(out, path.as_bytes());
    out.push(b'e');
    out.push(b'e');
}

fn metainfo_with_padding() -> Vec<u8> {
    let payload = [b'v', 0, b'r'];
    let mut info = vec![b'd'];
    bencode_bytes(&mut info, b"files");
    info.push(b'l');
    file(&mut info, "video.mkv", 1, false);
    file(&mut info, ".pad-1", 1, true);
    file(&mut info, "readme.txt", 1, false);
    info.push(b'e');
    bencode_bytes(&mut info, b"name");
    bencode_bytes(&mut info, b"release");
    bencode_bytes(&mut info, b"piece length");
    info.extend_from_slice(b"i4e");
    bencode_bytes(&mut info, b"pieces");
    bencode_bytes(&mut info, &Sha1::digest(payload));
    info.push(b'e');

    let mut metainfo = b"d4:info".to_vec();
    metainfo.extend_from_slice(&info);
    metainfo.push(b'e');
    metainfo
}

#[tokio::test]
async fn bep47_padding_is_not_projected_as_importable_content() {
    let root = tempfile::tempdir().unwrap();
    let session = TorrentSession::start(root.path().to_path_buf(), TorrentSessionConfig::default())
        .await
        .unwrap();
    let handle = session
        .add_metainfo(
            metainfo_with_padding(),
            TorrentAddConfig {
                paused: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let files = handle.stats().content_files;
    assert_eq!(files.len(), 2);
    assert!(files
        .iter()
        .any(|file| file.relative_path.ends_with("video.mkv")));
    assert!(files
        .iter()
        .any(|file| file.relative_path.ends_with("readme.txt")));
    assert!(files
        .iter()
        .all(|file| !file.relative_path.to_string_lossy().contains(".pad")));

    session.delete(&handle, false).await.unwrap();
    session.stop().await;
}
