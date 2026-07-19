//! Connection tasks (ARCHITECTURE.md §8.3): one tokio task per NNTP
//! connection, pull model. Each task asks the owner for up to
//! `pipeline_depth` leases, sends the BODY commands in one write, then
//! streams each response through the incremental yEnc decoder straight into
//! the file's writer channel. Idle connections are closed after the hold
//! time (task stays parked on the work-epoch watch at near-zero cost);
//! reconnect happens lazily when work arrives.

use crate::failover::AttemptOutcome;
use crate::owner::{EngineMsg, Lease};
use crate::rate::{RateLimiter, SpeedMeter};
use crate::writer::WriteCmd;
use nzbd_nntp::transport::{NntpConnection, TlsClientConfig, TransportError};
use nzbd_nntp::Command;
use nzbd_types::ServerDef;
use nzbd_yenc::{Status, YencDecoder};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

pub(crate) struct ConnCtx {
    pub server: ServerDef,
    /// This task's index within the server's pool — parked while the
    /// cluster connection budget (CLUSTERING.md §6.3) is below it.
    pub conn_index: u16,
    pub tls: Option<TlsClientConfig>,
    pub engine_tx: mpsc::Sender<EngineMsg>,
    pub epoch: watch::Receiver<u64>,
    pub budgets: watch::Receiver<std::collections::HashMap<nzbd_types::ServerId, u16>>,
    pub limiter: Arc<RateLimiter>,
    pub meter: Arc<SpeedMeter>,
    pub cancel: CancellationToken,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub idle_hold: Duration,
}

/// AIMD pipeline-depth controller (phase 5 "per-provider adaptive
/// pipelining"): climb by one after a sustained run of clean articles,
/// halve on any connection-level failure. The configured `pipeline_depth`
/// is the ceiling; the floor is 1. Providers that can't sustain deep
/// pipelines (drops, timeouts) settle low; healthy ones ride the ceiling.
#[derive(Debug)]
pub(crate) struct AdaptiveDepth {
    cur: usize,
    max: usize,
    ok_run: usize,
}

impl AdaptiveDepth {
    pub(crate) fn new(max: usize) -> AdaptiveDepth {
        let max = max.max(1);
        AdaptiveDepth {
            // Start midway: deep enough to be fast out of the gate, shallow
            // enough that a weak provider halves to sane quickly.
            cur: max.div_ceil(2),
            max,
            ok_run: 0,
        }
    }

    pub(crate) fn get(&self) -> usize {
        self.cur
    }

    /// `n` articles completed cleanly on the connection.
    pub(crate) fn on_success(&mut self, n: usize) {
        self.ok_run += n;
        // Additive increase after ~4 clean batches at the current depth.
        if self.cur < self.max && self.ok_run >= self.cur * 4 {
            self.cur += 1;
            self.ok_run = 0;
        }
    }

    /// Connection-level failure (death, timeout): multiplicative decrease.
    pub(crate) fn on_error(&mut self) {
        self.cur = (self.cur / 2).max(1);
        self.ok_run = 0;
    }
}

pub(crate) async fn connection_task(mut ctx: ConnCtx) {
    let mut conn: Option<NntpConnection> = None;
    let mut adaptive = AdaptiveDepth::new(ctx.server.pipeline_depth.max(1) as usize);

    loop {
        if ctx.cancel.is_cancelled() {
            break;
        }
        // Cluster connection budget: park (and drop the socket) while this
        // task's index is beyond the server's current allowance.
        let allowance = ctx
            .budgets
            .borrow_and_update()
            .get(&ctx.server.id)
            .copied()
            .unwrap_or(u16::MAX);
        if ctx.conn_index >= allowance {
            if let Some(c) = conn.take() {
                c.quit().await;
            }
            tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                r = ctx.budgets.changed() => { if r.is_err() { break } }
            }
            continue;
        }
        // Mark the epoch seen *before* asking, so a bump that races the
        // request still wakes us.
        ctx.epoch.borrow_and_update();

        let (reply_tx, reply_rx) = oneshot::channel();
        if ctx
            .engine_tx
            .send(EngineMsg::WorkRequest {
                server: ctx.server.id,
                max: adaptive.get(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            break; // engine gone
        }
        let leases = match reply_rx.await {
            Ok(l) => l,
            Err(_) => break,
        };

        if leases.is_empty() {
            let idle = tokio::time::sleep(ctx.idle_hold);
            tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                changed = ctx.epoch.changed() => {
                    if changed.is_err() { break }
                }
                _ = idle, if conn.is_some() => {
                    if let Some(c) = conn.take() {
                        c.quit().await; // retire the idle connection
                    }
                    // Then park until something changes.
                    tokio::select! {
                        _ = ctx.cancel.cancelled() => break,
                        changed = ctx.epoch.changed() => if changed.is_err() { break },
                    }
                }
            }
            continue;
        }

        if conn.is_none() {
            // Stagger cold reconnects across the pool: after a resume, all
            // tasks wake at once, and a thundering herd of TCP+TLS+AUTH can
            // trip provider connection limits.
            if ctx.conn_index > 0 {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(75 * ctx.conn_index as u64)) => {}
                }
            }
            match connect_and_auth(&ctx).await {
                Ok(c) => conn = Some(c),
                Err(e) => {
                    tracing::debug!(server = %ctx.server.name, error = %e, "connect failed");
                    let _ = ctx
                        .engine_tx
                        .send(EngineMsg::ConnectFailed {
                            server: ctx.server.id,
                        })
                        .await;
                    fail_leases(&ctx, &leases, AttemptOutcome::ConnectionFailed).await;
                    // The owner has blocked the server; wait out our share.
                    tokio::select! {
                        _ = ctx.cancel.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    }
                    continue;
                }
            }
        }

        let c = conn.as_mut().unwrap();
        if run_leases(c, &leases, &ctx).await.is_err() {
            conn = None; // connection is unusable; leases already reported
            let before = adaptive.get();
            adaptive.on_error();
            if adaptive.get() != before {
                tracing::debug!(
                    server = %ctx.server.name,
                    depth = adaptive.get(),
                    "pipeline depth halved after connection failure"
                );
            }
        } else {
            let before = adaptive.get();
            adaptive.on_success(leases.len());
            if adaptive.get() != before {
                tracing::debug!(
                    server = %ctx.server.name,
                    depth = adaptive.get(),
                    "pipeline depth raised"
                );
            }
        }
    }

    if let Some(c) = conn {
        c.quit().await;
    }
}

async fn connect_and_auth(ctx: &ConnCtx) -> Result<NntpConnection, TransportError> {
    let (mut conn, _greeting) = NntpConnection::connect(
        &ctx.server,
        ctx.tls.clone(),
        ctx.connect_timeout,
        ctx.read_timeout,
    )
    .await?;
    if let (Some(user), Some(pass)) = (&ctx.server.username, &ctx.server.password) {
        conn.authenticate(user, pass).await?;
    }
    tracing::debug!(server = %ctx.server.name, "connected");
    Ok(conn)
}

async fn fail_leases(ctx: &ConnCtx, leases: &[Lease], outcome: AttemptOutcome) {
    for lease in leases {
        let _ = ctx
            .engine_tx
            .send(EngineMsg::SegmentFailed {
                job: lease.r.job,
                file: lease.r.file,
                seg_number: lease.r.seg_number,
                server: ctx.server.id,
                outcome,
            })
            .await;
    }
}

/// Send BODY for every lease (one write), then read the responses in order.
/// `Err(())` means the connection died — every unprocessed lease has been
/// reported back as `ConnectionFailed`.
async fn run_leases(conn: &mut NntpConnection, leases: &[Lease], ctx: &ConnCtx) -> Result<(), ()> {
    let cmds: Vec<Command<'_>> = leases
        .iter()
        .map(|l| Command::Body(l.message_id.as_str()))
        .collect();
    if let Err(e) = conn.send_pipelined(&cmds).await {
        tracing::debug!(server = %ctx.server.name, error = %e, "pipelined send failed");
        fail_leases(ctx, leases, AttemptOutcome::ConnectionFailed).await;
        return Err(());
    }

    for (i, lease) in leases.iter().enumerate() {
        match handle_one(conn, lease, ctx).await {
            Ok(()) => {}
            Err(e) => {
                tracing::debug!(server = %ctx.server.name, error = %e, "connection lost mid-lease");
                fail_leases(ctx, &leases[i..], AttemptOutcome::ConnectionFailed).await;
                return Err(());
            }
        }
    }
    Ok(())
}

/// Process a single BODY response. `Err` = connection-level failure (the
/// caller reports this and the remaining leases); article-level outcomes
/// are reported inside.
async fn handle_one(
    conn: &mut NntpConnection,
    lease: &Lease,
    ctx: &ConnCtx,
) -> Result<(), TransportError> {
    let resp = conn.read_response().await?;

    if resp.code == nzbd_nntp::codes::BODY_FOLLOWS {
        return stream_body(conn, lease, ctx).await;
    }

    let outcome = if resp.is_article_missing() {
        AttemptOutcome::ArticleMissing
    } else if resp.code == nzbd_nntp::codes::AUTH_REQUIRED {
        // Session lost its auth. Return a connection-level error: the caller
        // reports this lease (and the rest) as ConnectionFailed and the
        // segment retries on a fresh, re-authenticated connection.
        return Err(TransportError::AuthRejected(resp.code, resp.text));
    } else {
        tracing::debug!(code = resp.code, text = %resp.text, "unexpected BODY response");
        AttemptOutcome::Other
    };
    fail_leases(ctx, std::slice::from_ref(lease), outcome).await;
    Ok(())
}

async fn stream_body(
    conn: &mut NntpConnection,
    lease: &Lease,
    ctx: &ConnCtx,
) -> Result<(), TransportError> {
    let mut decoder = YencDecoder::new();
    let mut out: Vec<u8> = Vec::with_capacity(768 * 1024);

    loop {
        let chunk_len = {
            let chunk = conn.body_chunk().await?;
            if chunk.is_empty() {
                return Err(TransportError::Closed);
            }
            match decoder.push(chunk, &mut out) {
                Ok((Status::NeedMore, consumed)) => {
                    debug_assert_eq!(consumed, chunk.len());
                    consumed
                }
                Ok((Status::Finished, consumed)) => {
                    conn.consume(consumed);
                    ctx.meter.add(consumed as u64);
                    ctx.limiter.debit(consumed).await;
                    break;
                }
                Err(e) => {
                    // Broken article: drain to the terminator to stay in
                    // protocol sync, then report an article-level failure.
                    let len = chunk.len();
                    conn.consume(len);
                    ctx.meter.add(len as u64);
                    tracing::debug!(msgid = %lease.message_id, error = %e, "yEnc decode failed");
                    conn.drain_body().await?;
                    fail_leases(ctx, std::slice::from_ref(lease), AttemptOutcome::Other).await;
                    return Ok(());
                }
            }
        };
        conn.consume(chunk_len);
        ctx.meter.add(chunk_len as u64);
        ctx.limiter.debit(chunk_len).await;
    }

    let Some(result) = decoder.take_result() else {
        fail_leases(ctx, std::slice::from_ref(lease), AttemptOutcome::Other).await;
        return Ok(());
    };

    if result.crc_ok == Some(false) {
        tracing::debug!(msgid = %lease.message_id, "CRC mismatch");
        fail_leases(ctx, std::slice::from_ref(lease), AttemptOutcome::CrcError).await;
        return Ok(());
    }
    if !result.len_ok {
        tracing::debug!(
            msgid = %lease.message_id,
            got = result.decoded_len,
            "article length mismatch"
        );
        fail_leases(ctx, std::slice::from_ref(lease), AttemptOutcome::Other).await;
        return Ok(());
    }

    // Hand the decoded part to the file's writer. A closed writer means the
    // job was deleted mid-flight — nothing to report.
    let _ = lease
        .writer
        .send(WriteCmd::Segment {
            seg_number: lease.r.seg_number,
            offset: result.offset,
            data: out,
            crc: result.crc32,
            file_size: result.header.size,
            server: ctx.server.id,
        })
        .await;
    Ok(())
}

#[cfg(test)]
mod adaptive_tests {
    use super::AdaptiveDepth;

    #[test]
    fn climbs_on_sustained_success_and_halves_on_error() {
        let mut a = AdaptiveDepth::new(8);
        assert_eq!(a.get(), 4, "starts midway");
        // Sustained clean batches climb one step at a time to the ceiling.
        for _ in 0..200 {
            let d = a.get();
            a.on_success(d);
        }
        assert_eq!(a.get(), 8, "reaches the configured ceiling");
        a.on_error();
        assert_eq!(a.get(), 4);
        a.on_error();
        a.on_error();
        assert_eq!(a.get(), 1, "floor is one");
        // Recovery climbs again.
        for _ in 0..200 {
            let d = a.get();
            a.on_success(d);
        }
        assert_eq!(a.get(), 8);
    }

    #[test]
    fn depth_one_config_disables_adaptivity() {
        let mut a = AdaptiveDepth::new(1);
        assert_eq!(a.get(), 1);
        a.on_success(100);
        assert_eq!(a.get(), 1);
        a.on_error();
        assert_eq!(a.get(), 1);
    }
}
