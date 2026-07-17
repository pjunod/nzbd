//! The per-article server failover ladder.
//!
//! Behavioral contract carried from NZBGet (ArticleDownloader.cpp L56–76 and
//! L200–283, ServerPool.cpp), reproduced here as a pure state machine:
//!
//! - Servers have a **tier** (normalized `Level`: 0 = main, 1 = first
//!   backup, …), an optional **group** (same tier+group ⇒ interchangeable —
//!   an article-level failure on one skips the whole group), and a **fill**
//!   flag (`Optional`) — a blocked fill server never stalls progress.
//! - Connect/transfer errors ⇒ retry the *same* server indefinitely (server
//!   temporarily blocked, `RetryInterval` = 10 s default), retries NOT spent.
//! - "No such article" (43x) and CRC errors ⇒ that server (and its group)
//!   is failed *for this article*; move to the next server.
//! - Other failures ⇒ spend one retry (`ArticleRetries` = 3 default); when
//!   exhausted, fail the server for this article.
//! - Per-server retention: articles older than the server's retention window
//!   are failed on that server immediately.
//! - All servers at the current tier failed/exhausted ⇒ escalate to the next
//!   tier. Past the last tier ⇒ the article has failed.

use nzbd_types::{ServerDef, ServerId};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    Success,
    /// TCP connect / TLS handshake failure, or the connection died mid-read.
    ConnectionFailed,
    /// NNTP 430/420/423-class: the server does not have the article.
    ArticleMissing,
    CrcError,
    /// Article older than this server's retention window.
    RetentionExceeded,
    /// Incomplete body, malformed yEnc, timeouts at protocol level, …
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    /// Lease the segment to this server.
    Server(ServerId),
    /// Usable servers exist at the current tier but all are temporarily
    /// blocked (and at least one is non-fill): try again shortly.
    WaitForBlocked,
    /// Every tier exhausted: the article is unrecoverable.
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Done,
    /// Retry the same server after its block expires. `block_server` asks the
    /// pool to block it (connection-level failures).
    RetrySame { block_server: bool },
    /// This server is failed for this article; ask [`Ladder::select`] again.
    NextServer,
    Failed,
}

/// Per-segment failover bookkeeping.
#[derive(Debug, Clone)]
pub struct SegmentAttempt {
    pub tier: u8,
    pub failed: BTreeSet<ServerId>,
    pub retries_left: u8,
}

impl SegmentAttempt {
    pub fn new(retries: u8) -> Self {
        SegmentAttempt {
            tier: 0,
            failed: BTreeSet::new(),
            retries_left: retries,
        }
    }
}

pub struct Ladder<'a> {
    servers: &'a [ServerDef],
    max_tier: u8,
}

impl<'a> Ladder<'a> {
    pub fn new(servers: &'a [ServerDef]) -> Self {
        let max_tier = servers
            .iter()
            .filter(|s| s.active)
            .map(|s| s.tier)
            .max()
            .unwrap_or(0);
        Ladder { servers, max_tier }
    }

    fn is_group_failed(&self, att: &SegmentAttempt, s: &ServerDef) -> bool {
        if att.failed.contains(&s.id) {
            return true;
        }
        s.group != 0
            && self.servers.iter().any(|other| {
                other.tier == s.tier && other.group == s.group && att.failed.contains(&other.id)
            })
    }

    /// Pick a server for the segment. `is_blocked` reflects pool-level
    /// temporary blocks (10 s after connection failures).
    pub fn select(
        &self,
        att: &mut SegmentAttempt,
        is_blocked: &dyn Fn(ServerId) -> bool,
    ) -> Selection {
        loop {
            let candidates: Vec<&ServerDef> = self
                .servers
                .iter()
                .filter(|s| s.active && s.tier == att.tier && !self.is_group_failed(att, s))
                .collect();

            if candidates.is_empty() {
                if att.tier >= self.max_tier {
                    return Selection::Exhausted;
                }
                att.tier += 1;
                continue;
            }

            if let Some(s) = candidates.iter().find(|s| !is_blocked(s.id)) {
                return Selection::Server(s.id);
            }

            // All candidates blocked. Fill servers must never stall the
            // queue: if every blocked candidate is a fill server, escalate.
            if candidates.iter().all(|s| s.fill) {
                if att.tier >= self.max_tier {
                    return Selection::Exhausted;
                }
                att.tier += 1;
                continue;
            }
            return Selection::WaitForBlocked;
        }
    }

    /// Apply an attempt outcome. On `NextServer`, call `select` again (it
    /// escalates tiers automatically once the current tier is exhausted).
    pub fn on_outcome(
        &self,
        att: &mut SegmentAttempt,
        server: ServerId,
        outcome: AttemptOutcome,
    ) -> Verdict {
        match outcome {
            AttemptOutcome::Success => Verdict::Done,
            AttemptOutcome::ConnectionFailed => Verdict::RetrySame { block_server: true },
            AttemptOutcome::ArticleMissing
            | AttemptOutcome::CrcError
            | AttemptOutcome::RetentionExceeded => {
                att.failed.insert(server);
                Verdict::NextServer
            }
            AttemptOutcome::Other => {
                if att.retries_left > 1 {
                    att.retries_left -= 1;
                    Verdict::RetrySame { block_server: false }
                } else {
                    att.failed.insert(server);
                    Verdict::NextServer
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_types::{CertLevel, TlsMode};

    fn server(id: u32, tier: u8, group: u8, fill: bool) -> ServerDef {
        ServerDef {
            id: ServerId(id),
            name: format!("s{id}"),
            host: "news.example".into(),
            port: 563,
            tls: TlsMode::Tls,
            username: None,
            password: None,
            active: true,
            tier,
            group,
            fill,
            max_connections: 8,
            pipeline_depth: 2,
            retention_days: 0,
            cert_verification: CertLevel::Strict,
        }
    }

    const NOT_BLOCKED: fn(ServerId) -> bool = |_| false;

    #[test]
    fn article_missing_escalates_through_tiers() {
        let servers = vec![server(1, 0, 0, false), server(2, 1, 0, false)];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);

        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Server(ServerId(1)));
        assert_eq!(
            ladder.on_outcome(&mut att, ServerId(1), AttemptOutcome::ArticleMissing),
            Verdict::NextServer
        );
        // tier 0 exhausted -> tier 1
        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Server(ServerId(2)));
        assert_eq!(att.tier, 1);
        assert_eq!(
            ladder.on_outcome(&mut att, ServerId(2), AttemptOutcome::ArticleMissing),
            Verdict::NextServer
        );
        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Exhausted);
    }

    #[test]
    fn group_peers_fail_together() {
        // Two servers in tier-0 group 1 (two accounts on one provider),
        // plus an independent tier-0 server.
        let servers = vec![
            server(1, 0, 1, false),
            server(2, 0, 1, false),
            server(3, 0, 0, false),
        ];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);

        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Server(ServerId(1)));
        ladder.on_outcome(&mut att, ServerId(1), AttemptOutcome::ArticleMissing);
        // server 2 shares the group -> skipped; goes straight to 3
        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Server(ServerId(3)));
    }

    #[test]
    fn connection_failures_do_not_spend_retries() {
        let servers = vec![server(1, 0, 0, false)];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);

        for _ in 0..10 {
            assert_eq!(
                ladder.on_outcome(&mut att, ServerId(1), AttemptOutcome::ConnectionFailed),
                Verdict::RetrySame { block_server: true }
            );
        }
        assert_eq!(att.retries_left, 3);
        assert!(att.failed.is_empty());
    }

    #[test]
    fn other_failures_exhaust_retries_then_fail_server() {
        let servers = vec![server(1, 0, 0, false), server(2, 1, 0, false)];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);

        assert_eq!(
            ladder.on_outcome(&mut att, ServerId(1), AttemptOutcome::Other),
            Verdict::RetrySame { block_server: false }
        );
        assert_eq!(
            ladder.on_outcome(&mut att, ServerId(1), AttemptOutcome::Other),
            Verdict::RetrySame { block_server: false }
        );
        // third strike: server failed for this article
        assert_eq!(
            ladder.on_outcome(&mut att, ServerId(1), AttemptOutcome::Other),
            Verdict::NextServer
        );
        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Server(ServerId(2)));
    }

    #[test]
    fn blocked_fill_server_never_stalls() {
        let servers = vec![server(1, 0, 0, true), server(2, 1, 0, false)];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);
        let blocked = |id: ServerId| id == ServerId(1);

        // fill server blocked -> fall through to next tier instead of waiting
        assert_eq!(ladder.select(&mut att, &blocked), Selection::Server(ServerId(2)));
    }

    #[test]
    fn blocked_regular_server_means_wait() {
        let servers = vec![server(1, 0, 0, false), server(2, 1, 0, false)];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);
        let blocked = |id: ServerId| id == ServerId(1);

        assert_eq!(ladder.select(&mut att, &blocked), Selection::WaitForBlocked);
        assert_eq!(att.tier, 0, "must not escalate past a temporarily-blocked main server");
    }

    #[test]
    fn crc_error_fails_server_immediately() {
        let servers = vec![server(1, 0, 0, false), server(2, 0, 0, false)];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);

        assert_eq!(
            ladder.on_outcome(&mut att, ServerId(1), AttemptOutcome::CrcError),
            Verdict::NextServer
        );
        assert_eq!(att.retries_left, 3, "CRC errors don't spend retries");
        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Server(ServerId(2)));
    }

    #[test]
    fn inactive_servers_are_invisible() {
        let mut s1 = server(1, 0, 0, false);
        s1.active = false;
        let servers = vec![s1, server(2, 1, 0, false)];
        let ladder = Ladder::new(&servers);
        let mut att = SegmentAttempt::new(3);
        assert_eq!(ladder.select(&mut att, &NOT_BLOCKED), Selection::Server(ServerId(2)));
    }
}
