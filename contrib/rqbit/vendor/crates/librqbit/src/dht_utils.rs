use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
};

use anyhow::Context;
use buffers::ByteBufOwned;
use futures::{stream::FuturesUnordered, Stream, StreamExt};
use librqbit_core::torrent_metainfo::TorrentMetaV1Info;
use tracing::{debug, error_span, Instrument};

use crate::{
    peer_connection::PeerConnectionOptions, peer_info_reader, spawn_utils::BlockingSpawner,
    stream_connect::StreamConnector,
};
use librqbit_core::hash_id::Id20;

const MAX_CONCURRENT_METADATA_PEERS: usize = 128;
const MAX_PENDING_METADATA_PEERS: usize = 256;
const MAX_METADATA_PEER_CANDIDATES: usize = 4096;

#[derive(Debug, PartialEq, Eq)]
enum CandidateAdmission {
    Admitted,
    Duplicate,
    Untracked,
}

fn admit_metadata_peer(seen: &mut HashSet<SocketAddr>, addr: SocketAddr) -> CandidateAdmission {
    if seen.contains(&addr) {
        return CandidateAdmission::Duplicate;
    }
    if seen.len() >= MAX_METADATA_PEER_CANDIDATES {
        return CandidateAdmission::Untracked;
    }
    seen.insert(addr);
    CandidateAdmission::Admitted
}

struct MetadataPeerQueues<F> {
    active: FuturesUnordered<F>,
    pending: VecDeque<SocketAddr>,
}

impl<F> MetadataPeerQueues<F> {
    fn new() -> Self {
        Self {
            active: FuturesUnordered::new(),
            pending: VecDeque::new(),
        }
    }

    fn can_accept(&self) -> bool {
        self.active.len() < MAX_CONCURRENT_METADATA_PEERS
            || self.pending.len() < MAX_PENDING_METADATA_PEERS
    }

    fn enqueue<M>(&mut self, addr: SocketAddr, make_future: &M) -> bool
    where
        M: Fn(SocketAddr) -> F,
    {
        if self.active.len() < MAX_CONCURRENT_METADATA_PEERS {
            self.active.push(make_future(addr));
            true
        } else if self.pending.len() < MAX_PENDING_METADATA_PEERS {
            self.pending.push_back(addr);
            true
        } else {
            false
        }
    }

    fn promote_pending<M>(&mut self, make_future: &M)
    where
        M: Fn(SocketAddr) -> F,
    {
        while self.active.len() < MAX_CONCURRENT_METADATA_PEERS {
            let Some(addr) = self.pending.pop_front() else {
                break;
            };
            self.active.push(make_future(addr));
        }
    }

    fn is_empty(&self) -> bool {
        self.active.is_empty() && self.pending.is_empty()
    }

    async fn next(&mut self) -> Option<F::Output>
    where
        F: std::future::Future,
    {
        self.active.next().await
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReadMetainfoResult<Rx> {
    Found {
        info: TorrentMetaV1Info<ByteBufOwned>,
        info_bytes: ByteBufOwned,
        rx: Rx,
        seen: HashSet<SocketAddr>,
    },
    ChannelClosed {
        #[allow(dead_code)]
        seen: HashSet<SocketAddr>,
    },
}

pub async fn read_metainfo_from_peer_receiver<A: Stream<Item = SocketAddr> + Unpin>(
    peer_id: Id20,
    info_hash: Id20,
    initial_addrs: Vec<SocketAddr>,
    addrs_stream: A,
    peer_connection_options: Option<PeerConnectionOptions>,
    connector: Arc<StreamConnector>,
) -> ReadMetainfoResult<A> {
    let mut seen = HashSet::<SocketAddr>::new();
    let mut addrs = addrs_stream;
    let mut initial_addrs = VecDeque::from(initial_addrs);
    let mut initial_addrs_completed = false;
    let mut candidate_limit_logged = false;

    let read_info_guarded = |addr| {
        let connector = connector.clone();
        async move {
            let ret = peer_info_reader::read_metainfo_from_peer(
                addr,
                peer_id,
                info_hash,
                peer_connection_options,
                BlockingSpawner::new(true),
                connector,
            )
            .instrument(error_span!("read_metainfo_from_peer", ?addr))
            .await
            .with_context(|| format!("error reading metainfo from {addr}"));
            ret
        }
    };

    let mut queues = MetadataPeerQueues::new();

    let mut addrs_completed = false;

    loop {
        queues.promote_pending(&read_info_guarded);

        while !initial_addrs_completed && queues.can_accept() {
            let Some(addr) = initial_addrs.pop_front() else {
                initial_addrs_completed = true;
                break;
            };
            match admit_metadata_peer(&mut seen, addr) {
                CandidateAdmission::Admitted => {
                    debug_assert!(queues.enqueue(addr, &read_info_guarded));
                }
                CandidateAdmission::Duplicate => {}
                CandidateAdmission::Untracked => {
                    if !candidate_limit_logged {
                        debug!(
                            limit = MAX_METADATA_PEER_CANDIDATES,
                            "metadata peer deduplication limit reached; continuing without tracking new candidates"
                        );
                        candidate_limit_logged = true;
                    }
                    debug_assert!(queues.enqueue(addr, &read_info_guarded));
                }
            }
        }

        if initial_addrs_completed && addrs_completed && queues.is_empty() {
            return ReadMetainfoResult::ChannelClosed { seen };
        }

        tokio::select! {
            done = queues.next(), if !queues.active.is_empty() => {
                match done {
                    Some(Ok((info, info_bytes))) => return ReadMetainfoResult::Found { info, info_bytes, seen, rx: addrs },
                    Some(Err(e)) => {
                        debug!("{:#}", e);
                    },
                    None => unreachable!()
                }
            }

            next_addr = addrs.next(), if initial_addrs_completed
                && !addrs_completed
                && queues.can_accept() => {
                match next_addr {
                    Some(addr) => {
                        match admit_metadata_peer(&mut seen, addr) {
                            CandidateAdmission::Admitted => {
                                debug_assert!(queues.enqueue(addr, &read_info_guarded));
                            }
                            CandidateAdmission::Duplicate => {}
                            CandidateAdmission::Untracked => {
                                if !candidate_limit_logged {
                                    debug!(
                                        limit = MAX_METADATA_PEER_CANDIDATES,
                                        "metadata peer deduplication limit reached; continuing without tracking new candidates"
                                    );
                                    candidate_limit_logged = true;
                                }
                                debug_assert!(queues.enqueue(addr, &read_info_guarded));
                            }
                        }
                        continue;
                    },
                    None => {
                        addrs_completed = true;
                    },
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use dht::{DhtBuilder, Id20};
    use librqbit_core::peer_id::generate_peer_id;

    use super::*;
    use std::{
        str::FromStr,
        sync::{Arc, Once},
    };

    static LOG_INIT: Once = Once::new();

    #[test]
    fn production_metadata_queues_close_at_exact_boundaries() {
        let mut queues = MetadataPeerQueues::new();
        let make_future = |_| std::future::pending::<()>();
        for port in 1..=(MAX_CONCURRENT_METADATA_PEERS + MAX_PENDING_METADATA_PEERS) {
            let addr = SocketAddr::from(([127, 0, 0, 1], port as u16));
            assert!(queues.enqueue(addr, &make_future));
        }
        assert_eq!(queues.active.len(), MAX_CONCURRENT_METADATA_PEERS);
        assert_eq!(queues.pending.len(), MAX_PENDING_METADATA_PEERS);
        assert!(!queues.can_accept());
        assert!(!queues.enqueue(SocketAddr::from(([127, 0, 0, 2], 1)), &make_future));

        let mut seen = HashSet::new();
        for port in 1..=MAX_METADATA_PEER_CANDIDATES {
            let addr = SocketAddr::from(([127, 0, 0, 1], port as u16));
            assert_eq!(
                admit_metadata_peer(&mut seen, addr),
                CandidateAdmission::Admitted
            );
        }
        let duplicate = SocketAddr::from(([127, 0, 0, 1], 1));
        assert_eq!(
            admit_metadata_peer(&mut seen, duplicate),
            CandidateAdmission::Duplicate
        );
        let overflow = SocketAddr::from(([127, 0, 0, 2], 1));
        assert_eq!(
            admit_metadata_peer(&mut seen, overflow),
            CandidateAdmission::Untracked
        );
        assert_eq!(seen.len(), MAX_METADATA_PEER_CANDIDATES);
    }

    fn init_logging() {
        #[allow(unused_must_use)]
        LOG_INIT.call_once(|| {
            // pretty_env_logger::try_init();
        })
    }

    #[tokio::test]
    #[ignore]
    async fn read_metainfo_from_dht() {
        init_logging();

        let info_hash = Id20::from_str("cab507494d02ebb1178b38f2e9d7be299c86b862").unwrap();
        let dht = DhtBuilder::new().await.unwrap();

        let peer_rx = dht.get_peers(info_hash, None);
        let peer_id = generate_peer_id(b"-xx1234-");
        match read_metainfo_from_peer_receiver(
            peer_id,
            info_hash,
            Vec::new(),
            peer_rx,
            None,
            Arc::new(Default::default()),
        )
        .await
        {
            ReadMetainfoResult::Found { info, .. } => dbg!(info),
            ReadMetainfoResult::ChannelClosed { .. } => todo!("should not have happened"),
        };
    }
}
