use crate::error::Error;
use crate::metadata::Metadata;
use crate::wal::WalFileSet;
use std::sync::{Arc, RwLock};
use std::thread;
use tokio::sync::oneshot;
use tracing::{debug, error};

#[allow(clippy::type_complexity)]
pub enum Action {
    Logs {
        from: u64,
        until: u64,
        ack: flume::Sender<LogReadResponse>,
    },
    LogState(oneshot::Sender<Result<LogState, Error>>),
    Vote(oneshot::Sender<Result<Option<Vec<u8>>, Error>>),
    Shutdown,
}

#[derive(Debug)]
pub enum LogReadResponse {
    Record(Vec<u8>),
    Done(Result<(), Error>),
}

#[derive(Debug)]
pub struct LogState {
    pub last_purged_log_id: Option<Vec<u8>>,
    pub last_log: Option<Vec<u8>>,
}

/// Memorizes the last read log to speed up future lookups and have a saved starting position.
/// Logs are always read sequential, apart from during app start, when once Logs will be read
/// backwards to find the latest membership config.
/// This saves us from maintaining a complete index in memory, which is not necessary at all.
/// Each reader usually reads each log max once, followed by the next one guaranteed in sequential
/// order. This means (apart from the very first start), this memoized position will always be used.
#[derive(Debug)]
pub struct LogReadMemo {
    pub wal_incarnation: u64,
    pub last_wal_no: u64,
    pub last_log_id: u64,
    pub data_end: u32,
}

pub fn spawn(
    meta: Arc<RwLock<Metadata>>,
    wal_locked: Arc<RwLock<WalFileSet>>,
) -> Result<flume::Sender<Action>, Error> {
    let (tx, rx) = flume::bounded::<Action>(1);
    thread::spawn(move || run(meta, wal_locked, rx));
    Ok(tx)
}

/// There are a lot of `unwrap()`s in this task. The reason is simply, if most of these fail, it can
/// only be because of a non-recoverable error anyway and the application should crash, so that
/// the next health check can restart it.
///
/// Everything related to locking and memory mapping is being `unwrap()`ped. If anything fails in
/// this regard, it's either a physical storage or OS issue and this code an do nothing about it.
fn run(
    meta: Arc<RwLock<Metadata>>,
    wal_locked: Arc<RwLock<WalFileSet>>,
    rx: flume::Receiver<Action>,
) {
    // we keep the local set for faster access inside the loop and lazily update if necessary
    let mut wal = wal_locked.read().unwrap().clone_no_map();
    // openraft will read chunks of 64 logs for bigger tasks
    let mut buf = Vec::with_capacity(64);
    let mut memo: Option<LogReadMemo> = None;

    while let Ok(action) = rx.recv() {
        match action {
            Action::Logs { from, until, ack } => {
                debug!("WAL Reader - Action::Logs - read from {from} until {until}");
                let result = {
                    let wal_upd = wal_locked.read().unwrap();
                    wal.refresh_from_no_mmap(&wal_upd);
                    // Keep the shared layout guard through mmap creation and
                    // the complete read. Remove/truncate takes the exclusive
                    // guard before changing any path or record layout.
                    read_requested_logs(&mut wal, from, until, &mut memo, &mut buf, &ack)
                };
                complete_log_read(ack, result);
            }
            Action::LogState(ack) => {
                debug!("WAL Reader - Action::LogState");
                let result = {
                    let wal_upd = wal_locked.read().unwrap();
                    wal.refresh_from_no_mmap(&wal_upd);
                    read_log_state(&meta, &mut wal, &mut memo, &mut buf)
                };
                if let Err(err) = &result {
                    error!("Error reading WAL log state: {err:?}");
                }
                let _ = ack.send(result);
            }
            Action::Vote(ack) => {
                debug!("WAL Reader - Action::Vote");
                let vote = meta.read().unwrap().vote.clone();
                let _ = ack.send(Ok(vote));
            }
            Action::Shutdown => {
                debug!("Raft logs store reader is being shut down");
                break;
            }
        }
    }

    debug!("Logs Reader exiting");
}

fn complete_log_read(ack: flume::Sender<LogReadResponse>, result: Result<bool, Error>) {
    match result {
        Ok(true) => {
            let _ = ack.send(LogReadResponse::Done(Ok(())));
        }
        Ok(false) => {
            debug!("WAL log response receiver closed before completion");
        }
        Err(err) => {
            error!("Error reading logs: {err:?}");
            let _ = ack.send(LogReadResponse::Done(Err(err)));
        }
    }
}

fn read_requested_logs(
    wal: &mut WalFileSet,
    from: u64,
    until: u64,
    memo: &mut Option<LogReadMemo>,
    buf: &mut Vec<(u64, Vec<u8>)>,
    ack: &flume::Sender<LogReadResponse>,
) -> Result<bool, Error> {
    if until < from {
        return Err(Error::Generic(
            "requested WAL range ends before it starts".into(),
        ));
    }

    let mut from_next = from;
    let mut read_any = false;

    for log in wal.files.iter_mut() {
        if log.id_until < from_next {
            debug!(
                "log.id_until < from_next -> {} < {}",
                log.id_until, from_next
            );
            continue;
        }

        if read_any && log.id_from > from_next {
            return Err(Error::Integrity(
                format!(
                    "retained WAL set has a gap before log {} (next file starts at {})",
                    from_next, log.id_from
                )
                .into(),
            ));
        }

        let file_from = from_next.max(log.id_from);
        if file_from > until {
            break;
        }
        let file_until = until.min(log.id_until);

        log.mmap()?;
        buf.clear();
        log.read_logs(file_from, file_until, memo, buf)?;
        read_any = true;
        for (_id, data) in buf.drain(..) {
            if ack.send(LogReadResponse::Record(data)).is_err() {
                return Ok(false);
            }
        }

        if file_until == until {
            break;
        }

        // The next file owns the remainder. Drop completed-file mappings so a
        // snapshot catch-up does not pin every historical WAL in memory.
        log.mmap_drop();
        from_next = file_until
            .checked_add(1)
            .ok_or_else(|| Error::Integrity("requested WAL range continuation overflow".into()))?;
    }

    Ok(true)
}

fn read_log_state(
    meta: &Arc<RwLock<Metadata>>,
    wal: &mut WalFileSet,
    memo: &mut Option<LogReadMemo>,
    buf: &mut Vec<(u64, Vec<u8>)>,
) -> Result<LogState, Error> {
    let latest_log_id = {
        let Some(file) = wal.files.back() else {
            return Err(Error::Integrity("WAL file set is empty".into()));
        };
        if file.data_start.is_some() {
            Some(file.id_until)
        } else if wal.files.len() > 1 {
            // We may be between rolling the WAL and appending to the new file.
            Some(
                wal.files
                    .get(wal.files.len() - 2)
                    .ok_or_else(|| Error::Integrity("WAL rollover previous file is absent".into()))?
                    .id_until,
            )
        } else {
            None
        }
    };

    let last_log = if let Some(latest_log_id) = latest_log_id {
        buf.clear();
        let active_has_data = wal
            .files
            .back()
            .is_some_and(|file| file.data_start.is_some());
        let file = if active_has_data {
            wal.files
                .back_mut()
                .ok_or_else(|| Error::Integrity("WAL file set is empty".into()))?
        } else {
            let previous = wal
                .files
                .len()
                .checked_sub(2)
                .ok_or_else(|| Error::Integrity("WAL rollover has no previous file".into()))?;
            wal.files
                .get_mut(previous)
                .ok_or_else(|| Error::Integrity("WAL rollover previous file is absent".into()))?
        };
        file.mmap()?;
        file.read_logs(latest_log_id, latest_log_id, memo, buf)?;
        if !active_has_data {
            file.mmap_drop();
        }
        let (_, data) = buf
            .pop()
            .ok_or_else(|| Error::Integrity("latest WAL log was not readable".into()))?;
        Some(data)
    } else {
        None
    };

    let state = LogState {
        last_purged_log_id: meta.read().unwrap().last_purged_log_id.clone(),
        last_log,
    };
    debug!(
        "WAL Reader - Action::LogState -> latest_log_id: {:?}\n{:?}",
        latest_log_id, state
    );
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{Duration, Instant};

    const WAL_SIZE: u32 = 2 * 1024 * 1024;

    #[test]
    fn failed_claimed_range_does_not_poison_the_next_reader_action() -> Result<(), Error> {
        let base_path = "test_data/reader_error_recovery".to_owned();
        let _ = fs::remove_dir_all(&base_path);
        fs::create_dir_all(&base_path)?;

        let mut writer = WalFileSet::read(base_path.clone(), WAL_SIZE)?;
        writer.active().mmap_mut()?;
        let mut header = Vec::with_capacity(32);
        writer.active().append_log(1, b"one", &mut header)?;

        let wal_locked = Arc::new(RwLock::new(writer.clone_no_map()));
        wal_locked.write().unwrap().active().id_until = 2;
        let meta = Arc::new(RwLock::new(Metadata {
            last_purged_log_id: None,
            vote: None,
        }));
        let (tx, rx) = flume::bounded(1);
        let thread_wal = wal_locked.clone();
        let handle = thread::spawn(move || run(meta, thread_wal, rx));

        let (ack, result) = flume::bounded(1);
        tx.send(Action::Logs {
            from: 1,
            until: 2,
            ack,
        })
        .unwrap();
        let error = match result.recv().unwrap() {
            LogReadResponse::Done(Err(error)) => error,
            response => panic!("claimed range must return one terminal error, got {response:?}"),
        };
        assert!(matches!(error, Error::Integrity(_)));

        wal_locked.write().unwrap().refresh_from_no_mmap(&writer);
        let (ack, result) = flume::bounded(1);
        tx.send(Action::Logs {
            from: 1,
            until: 1,
            ack,
        })
        .unwrap();
        assert!(matches!(
            result.recv().unwrap(),
            LogReadResponse::Record(data) if data == b"one"
        ));
        assert!(matches!(
            result.recv().unwrap(),
            LogReadResponse::Done(Ok(()))
        ));

        tx.send(Action::Shutdown).unwrap();
        handle.join().unwrap();
        drop(wal_locked);
        drop(writer);
        fs::remove_dir_all(base_path)?;
        Ok(())
    }

    #[test]
    fn range_before_the_retained_floor_may_be_absent() -> Result<(), Error> {
        let base_path = "test_data/reader_absent_range".to_owned();
        let _ = fs::remove_dir_all(&base_path);
        fs::create_dir_all(&base_path)?;

        let mut wal = WalFileSet::read(base_path.clone(), WAL_SIZE)?;
        wal.active().mmap_mut()?;
        let mut header = Vec::with_capacity(32);
        wal.active().append_log(10, b"ten", &mut header)?;

        let mut memo = None;
        let mut buf = Vec::with_capacity(1);
        let (ack, records) = flume::bounded(1);
        assert!(read_requested_logs(
            &mut wal, 1, 9, &mut memo, &mut buf, &ack
        )?);
        assert!(records.try_recv().is_err());

        drop(wal);
        fs::remove_dir_all(base_path)?;
        Ok(())
    }

    #[test]
    fn range_spanning_the_retained_floor_streams_only_retained_records() -> Result<(), Error> {
        let base_path = "test_data/reader_spanning_retained_floor".to_owned();
        let _ = fs::remove_dir_all(&base_path);
        fs::create_dir_all(&base_path)?;

        let mut wal = WalFileSet::read(base_path.clone(), WAL_SIZE)?;
        wal.active().mmap_mut()?;
        let mut header = Vec::with_capacity(32);
        wal.active().append_log(10, b"ten", &mut header)?;

        let mut memo = None;
        let mut buf = Vec::with_capacity(1);
        let (ack, responses) = flume::bounded(2);
        let result = read_requested_logs(&mut wal, 1, 10, &mut memo, &mut buf, &ack);
        complete_log_read(ack, result);
        assert!(matches!(
            responses.recv().unwrap(),
            LogReadResponse::Record(data) if data == b"ten"
        ));
        assert!(matches!(
            responses.recv().unwrap(),
            LogReadResponse::Done(Ok(()))
        ));

        drop(wal);
        fs::remove_dir_all(base_path)?;
        Ok(())
    }

    #[test]
    fn internal_retained_gap_is_a_terminal_error_after_any_prior_records() -> Result<(), Error> {
        let base_path = "test_data/reader_internal_gap".to_owned();
        let _ = fs::remove_dir_all(&base_path);
        fs::create_dir_all(&base_path)?;

        let mut wal = WalFileSet::read(base_path.clone(), WAL_SIZE)?;
        wal.active().mmap_mut()?;
        let mut header = Vec::with_capacity(32);
        wal.active().append_log(1, b"one", &mut header)?;
        header.clear();
        wal.roll_over(WAL_SIZE, &mut header)?;
        header.clear();
        wal.active().append_log(3, b"three", &mut header)?;

        let mut memo = None;
        let mut buf = Vec::with_capacity(2);
        let (ack, responses) = flume::bounded(2);
        let result = read_requested_logs(&mut wal, 1, 3, &mut memo, &mut buf, &ack);
        complete_log_read(ack, result);
        assert!(matches!(
            responses.recv().unwrap(),
            LogReadResponse::Record(data) if data == b"one"
        ));
        assert!(matches!(
            responses.recv().unwrap(),
            LogReadResponse::Done(Err(Error::Integrity(_)))
        ));

        drop(wal);
        fs::remove_dir_all(base_path)?;
        Ok(())
    }

    #[test]
    fn log_responses_apply_capacity_one_backpressure_before_the_terminal_result()
    -> Result<(), Error> {
        let base_path = "test_data/reader_bounded_stream".to_owned();
        let _ = fs::remove_dir_all(&base_path);
        fs::create_dir_all(&base_path)?;

        let mut wal = WalFileSet::read(base_path.clone(), WAL_SIZE)?;
        wal.active().mmap_mut()?;
        let mut header = Vec::with_capacity(32);
        wal.active().append_log(1, b"one", &mut header)?;
        header.clear();
        wal.active().append_log(2, b"two", &mut header)?;

        let (ack, responses) = flume::bounded(1);
        let (finished, completion) = flume::bounded(1);
        let handle = thread::spawn(move || {
            let mut memo = None;
            let mut buf = Vec::with_capacity(2);
            let result = read_requested_logs(&mut wal, 1, 2, &mut memo, &mut buf, &ack);
            complete_log_read(ack, result);
            finished.send(()).unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        while responses.is_empty() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert_eq!(
            responses.len(),
            1,
            "the first record must reach the bounded channel"
        );
        assert!(
            completion.try_recv().is_err(),
            "the producer must block before it can queue the second record and terminal result"
        );

        assert!(matches!(
            responses.recv().unwrap(),
            LogReadResponse::Record(data) if data == b"one"
        ));
        assert!(matches!(
            responses.recv().unwrap(),
            LogReadResponse::Record(data) if data == b"two"
        ));
        assert!(matches!(
            responses.recv().unwrap(),
            LogReadResponse::Done(Ok(()))
        ));
        completion.recv().unwrap();
        handle.join().unwrap();

        fs::remove_dir_all(base_path)?;
        Ok(())
    }
}
