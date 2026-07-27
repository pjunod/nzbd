//! Per-file disk writer tasks (ARCHITECTURE.md §8.4).
//!
//! One task owns each output file; decoded segments arrive over a bounded
//! channel (backpressure) from whichever connection task decoded them.
//! DirectWrite semantics: the file is preallocated sparse to its full yEnc
//! size on first write, each part is written at its yEnc offset
//! (`begin − 1`), gaps stay zero-filled, and completion is an atomic rename
//! from `<name>.part` to `<name>`. Resume reopens the `.part` file without
//! truncation.

use crate::owner::EngineMsg;
use nzbd_types::{FileId, JobId, ServerId};
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

#[derive(Debug)]
pub enum WriteCmd {
    Segment {
        seg_number: u32,
        offset: u64,
        data: Vec<u8>,
        crc: u32,
        /// Total output-file size from the yEnc header (0 = unknown).
        file_size: u64,
        server: ServerId,
    },
    /// All segments accounted for: extend/trim to `file_size`, fsync,
    /// rename into place. `combined_crc` is the whole-file CRC when every
    /// segment succeeded contiguously.
    Finalize {
        file_size: u64,
        combined_crc: Option<u32>,
    },
}

#[derive(Clone)]
pub struct WriterHandle {
    pub tx: mpsc::Sender<WriteCmd>,
}

/// Bounded queue per file: decoded segments are large, keep few in flight.
const WRITER_QUEUE: usize = 8;

pub fn spawn_writer(
    tracker: &TaskTracker,
    job: JobId,
    file: FileId,
    dir: PathBuf,
    final_name: String,
    engine_tx: mpsc::Sender<EngineMsg>,
) -> WriterHandle {
    let (tx, rx) = mpsc::channel(WRITER_QUEUE);
    tracker.spawn(writer_task(job, file, dir, final_name, rx, engine_tx));
    WriterHandle { tx }
}

async fn writer_task(
    job: JobId,
    file_id: FileId,
    dir: PathBuf,
    final_name: String,
    mut rx: mpsc::Receiver<WriteCmd>,
    engine_tx: mpsc::Sender<EngineMsg>,
) {
    let part_path = dir.join(format!("{final_name}.part"));
    let final_path = dir.join(&final_name);
    let mut out: Option<File> = None;
    let mut preallocated = false;

    while let Some(cmd) = rx.recv().await {
        match cmd {
            WriteCmd::Segment {
                seg_number,
                offset,
                data,
                crc,
                file_size,
                server,
            } => {
                let result = write_segment(
                    &dir,
                    &part_path,
                    &mut out,
                    &mut preallocated,
                    offset,
                    &data,
                    file_size,
                )
                .await;
                let msg = match result {
                    Ok(()) => EngineMsg::SegmentWritten {
                        job,
                        file: file_id,
                        seg_number,
                        offset,
                        len: data.len() as u32,
                        crc,
                        file_size,
                        server,
                    },
                    Err(e) => EngineMsg::WriterError {
                        job,
                        file: file_id,
                        error: format!("write {}: {e}", part_path.display()),
                    },
                };
                if engine_tx.send(msg).await.is_err() {
                    break; // engine gone
                }
            }
            WriteCmd::Finalize {
                file_size,
                combined_crc,
            } => {
                let result = finalize(&part_path, &final_path, &mut out, file_size).await;
                let msg = match result {
                    Ok(()) => EngineMsg::WriterFinalized {
                        job,
                        file: file_id,
                        ok: true,
                        final_path: Some(final_path.clone()),
                        combined_crc,
                    },
                    Err(e) => {
                        tracing::warn!(job = job.0, file = file_id.0, error = %e, "finalize failed");
                        EngineMsg::WriterFinalized {
                            job,
                            file: file_id,
                            ok: false,
                            final_path: None,
                            combined_crc,
                        }
                    }
                };
                let _ = engine_tx.send(msg).await;
                return;
            }
        }
    }
    // Channel closed without Finalize: job deleted or engine stopping.
    // Leave the `.part` file for resume / directory cleanup.
}

async fn write_segment(
    dir: &PathBuf,
    part_path: &PathBuf,
    out: &mut Option<File>,
    preallocated: &mut bool,
    offset: u64,
    data: &[u8],
    file_size: u64,
) -> std::io::Result<()> {
    if out.is_none() {
        tokio::fs::create_dir_all(dir).await?;
        // No truncate: resume must keep already-written parts.
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(part_path)
            .await?;
        *out = Some(f);
    }
    let f = out.as_mut().unwrap();
    if !*preallocated && file_size > 0 {
        let current = f.metadata().await?.len();
        if current < file_size {
            // Sparse preallocation (POSIX truncate-up), NZBGet DirectWrite.
            f.set_len(file_size).await?;
        }
        *preallocated = true;
    }
    f.seek(SeekFrom::Start(offset)).await?;
    f.write_all(data).await?;
    Ok(())
}

async fn finalize(
    part_path: &PathBuf,
    final_path: &PathBuf,
    out: &mut Option<File>,
    file_size: u64,
) -> std::io::Result<()> {
    if out.is_none() {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(part_path)
            .await
        {
            Ok(f) => *out = Some(f),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if tokio::fs::try_exists(final_path).await.unwrap_or(false) {
                    return Ok(()); // already finalized (recovery re-run)
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        }
    }
    let f = out.as_mut().unwrap();
    if file_size > 0 {
        // Zero-fill trailing gap / trim over-preallocation.
        f.set_len(file_size).await?;
    }
    // Data durability at the completion boundary; per-segment writes stay
    // relaxed (page cache), matching the configured-default policy.
    //
    // This is also the only point at which a deferred writeback failure can
    // reach us. Per-segment writes land in the page cache and return Ok; on a
    // network filesystem the ENOSPC/EDQUOT/EIO that killed them surfaces here
    // or nowhere at all.
    f.sync_data().await?;
    drop(out.take()); // close before rename

    // Then check the obvious thing, because the obvious thing turned out not
    // to be true: is the file the length it is supposed to be?
    //
    // Two 40-60 GB remuxes finished as SUCCESS with 500 MiB on disk — stopped
    // dead on a 500 MiB boundary by a limit outside this process, and reported
    // complete because nothing ever compared what was written against what was
    // promised. Every other check in the engine is about the article set:
    // whether the bytes arrived off the wire. None of them look at the file. A
    // downloader may report that it failed; it may not report success for a
    // file it did not write.
    if file_size > 0 {
        let on_disk = tokio::fs::metadata(part_path).await?.len();
        if on_disk != file_size {
            return Err(std::io::Error::other(format!(
                "{} is {on_disk} bytes on disk but should be {file_size}: \
                 {} bytes never reached the filesystem",
                part_path.display(),
                file_size.saturating_sub(on_disk),
            )));
        }
    }

    tokio::fs::rename(part_path, final_path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

    /// Writes segments out of order with a gap, finalizes, and checks the
    /// assembled bytes (gap zero-filled, exact final size, `.part` renamed).
    #[tokio::test]
    async fn assembles_out_of_order_with_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();
        let (etx, mut erx) = channel(64);
        let h = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            tmp.path().to_path_buf(),
            "out.bin".into(),
            etx,
        );

        let file_size = 10u64;
        // segment 2 first: bytes 5..8, then segment 1: bytes 0..3. Gap at 3..5 and 8..10.
        h.tx.send(WriteCmd::Segment {
            seg_number: 2,
            offset: 5,
            data: vec![0xBB; 3],
            crc: 0,
            file_size,
            server: ServerId(1),
        })
        .await
        .unwrap();
        h.tx.send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: vec![0xAA; 3],
            crc: 0,
            file_size,
            server: ServerId(1),
        })
        .await
        .unwrap();
        h.tx.send(WriteCmd::Finalize {
            file_size,
            combined_crc: None,
        })
        .await
        .unwrap();

        let mut written = 0;
        let mut finalized = false;
        while let Some(msg) = erx.recv().await {
            match msg {
                EngineMsg::SegmentWritten { .. } => written += 1,
                EngineMsg::WriterFinalized { ok, .. } => {
                    assert!(ok);
                    finalized = true;
                    break;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(written, 2);
        assert!(finalized);

        let bytes = std::fs::read(tmp.path().join("out.bin")).unwrap();
        let mut expected = vec![0u8; 10];
        expected[0..3].fill(0xAA);
        expected[5..8].fill(0xBB);
        assert_eq!(bytes, expected);
        assert!(!tmp.path().join("out.bin.part").exists());
    }

    /// A finalize that cannot complete must report failure, not success.
    ///
    /// Regression test for the worst bug this engine has had: two 40-60 GB
    /// remuxes reported SUCCESS with 500 MiB on disk. `sync_data` is the only
    /// point at which a deferred writeback error can reach us — per-segment
    /// writes land in the page cache and return Ok, so on a network
    /// filesystem the ENOSPC/EDQUOT/EIO that killed them surfaces here or
    /// nowhere. Losing it means calling a download that never reached the
    /// disk finished.
    ///
    /// The failure is simulated the only way it can be from a unit test:
    /// the `.part` file is removed while the destination directory is gone
    /// too, so reopening it cannot succeed and the rename has nothing to
    /// move. What matters is the plumbing — an error here becomes
    /// `ok: false`, and the owner turns `ok: false` into a failed job rather
    /// than a healthy one.
    #[tokio::test]
    async fn a_finalize_that_fails_reports_not_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();
        let (etx, mut erx) = channel(64);
        let dir = tmp.path().join("gone");
        std::fs::create_dir_all(&dir).unwrap();
        let h = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            dir.clone(),
            "x.bin".into(),
            etx,
        );

        h.tx.send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: vec![0xAA; 16],
            crc: 0,
            file_size: 16,
            server: ServerId(1),
        })
        .await
        .unwrap();
        match erx.recv().await {
            Some(EngineMsg::SegmentWritten { .. }) => {}
            other => panic!("unexpected {other:?}"),
        }

        // The destination disappears out from under the writer, which is what
        // a filesystem going away looks like from in here.
        std::fs::remove_dir_all(&dir).unwrap();

        h.tx.send(WriteCmd::Finalize {
            file_size: 16,
            combined_crc: None,
        })
        .await
        .unwrap();
        match erx.recv().await {
            Some(EngineMsg::WriterFinalized { ok, .. }) => assert!(
                !ok,
                "a finalize that could not place the file must report ok:false — \
                 reporting success here is what called 500 MiB of a 48 GiB remux \
                 a completed download"
            ),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn resume_reopen_preserves_existing_data() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();

        // First writer: one segment.
        let (etx, mut erx) = channel(64);
        let h = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            tmp.path().to_path_buf(),
            "r.bin".into(),
            etx,
        );
        h.tx.send(WriteCmd::Segment {
            seg_number: 1,
            offset: 0,
            data: vec![1, 2, 3, 4],
            crc: 0,
            file_size: 8,
            server: ServerId(1),
        })
        .await
        .unwrap();
        erx.recv().await.unwrap();
        drop(h); // "crash": writer exits on channel close, no finalize

        // Second writer (recovery): remaining segment + finalize.
        let (etx2, mut erx2) = channel(64);
        let h2 = spawn_writer(
            &tracker,
            JobId(1),
            FileId(1),
            tmp.path().to_path_buf(),
            "r.bin".into(),
            etx2,
        );
        h2.tx
            .send(WriteCmd::Segment {
                seg_number: 2,
                offset: 4,
                data: vec![5, 6, 7, 8],
                crc: 0,
                file_size: 8,
                server: ServerId(1),
            })
            .await
            .unwrap();
        h2.tx
            .send(WriteCmd::Finalize {
                file_size: 8,
                combined_crc: None,
            })
            .await
            .unwrap();
        loop {
            match erx2.recv().await.unwrap() {
                EngineMsg::WriterFinalized { ok, .. } => {
                    assert!(ok);
                    break;
                }
                EngineMsg::SegmentWritten { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(
            std::fs::read(tmp.path().join("r.bin")).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
    }
}
