//! How Runner identifies itself to trackers and peers.
//!
//! Private trackers check that a client's identifiers agree with each other
//! and with a client they recognize. Runner therefore reports one name and
//! one version everywhere it is identified:
//!
//! - **Peer ID** (every tracker announce and peer handshake): a BEP 20
//!   Azureus-style prefix `-RN<major><minor><patch>0-` followed by twelve
//!   random bytes, generated once per engine session. `RN` is not a
//!   registered client code in the peer-ID lists trackers use.
//! - **HTTP `User-Agent`** (HTTP(S) tracker announces and `.torrent` source
//!   fetches): `Runner/<version>`.
//! - **BEP 10 extended-handshake `v`**: `Runner <version>`.
//!
//! The version is the workspace package version, so all three always agree.

use librqbit::dht::Id20;

/// The client name reported to trackers and peers.
pub const CLIENT_NAME: &str = "Runner";

/// The version reported alongside [`CLIENT_NAME`].
pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// HTTP `User-Agent` for tracker announces and `.torrent` fetches.
pub const CLIENT_USER_AGENT: &str = concat!("Runner/", env!("CARGO_PKG_VERSION"));

/// BEP 10 extended-handshake `v` string.
pub const CLIENT_HANDSHAKE_VERSION: &str = concat!("Runner ", env!("CARGO_PKG_VERSION"));

/// BEP 20 two-character client code.
pub const PEER_ID_CLIENT_CODE: [u8; 2] = *b"RN";

/// Azureus-style version digits: 0-9, then A-Z for 10-35, then a-z for
/// 36-61. Larger components saturate at `z` rather than failing.
fn version_digit(component: u64) -> u8 {
    const DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    DIGITS[component.min(61) as usize]
}

fn version_component(value: &str) -> u64 {
    value.parse().unwrap_or(0)
}

/// The eight-byte peer-ID prefix for a `major.minor.patch` version.
pub fn peer_id_prefix_for(major: u64, minor: u64, patch: u64) -> [u8; 8] {
    [
        b'-',
        PEER_ID_CLIENT_CODE[0],
        PEER_ID_CLIENT_CODE[1],
        version_digit(major),
        version_digit(minor),
        version_digit(patch),
        b'0',
        b'-',
    ]
}

/// This build's eight-byte peer-ID prefix, e.g. `-RN0200-` for 0.2.0.
pub fn peer_id_prefix() -> [u8; 8] {
    peer_id_prefix_for(
        version_component(env!("CARGO_PKG_VERSION_MAJOR")),
        version_component(env!("CARGO_PKG_VERSION_MINOR")),
        version_component(env!("CARGO_PKG_VERSION_PATCH")),
    )
}

/// A fresh peer ID: [`peer_id_prefix`] followed by twelve random bytes.
pub fn generate_peer_id() -> Id20 {
    librqbit::generate_peer_id(&peer_id_prefix())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_follows_bep20_azureus_style() {
        assert_eq!(&peer_id_prefix_for(0, 2, 0), b"-RN0200-");
        assert_eq!(&peer_id_prefix_for(1, 10, 35), b"-RN1AZ0-");
        assert_eq!(&peer_id_prefix_for(36, 61, 999), b"-RNazz0-");
    }

    #[test]
    fn peer_ids_share_the_prefix_and_randomize_the_rest() {
        let first = generate_peer_id();
        let second = generate_peer_id();
        assert_eq!(first.0[..8], peer_id_prefix());
        assert_eq!(second.0[..8], peer_id_prefix());
        assert_ne!(first.0[8..], second.0[8..]);
    }

    #[test]
    fn engine_decodes_this_build_client_and_version() {
        let Some(librqbit::PeerId::AzureusStyle(decoded)) =
            librqbit::try_decode_peer_id(generate_peer_id())
        else {
            panic!("peer ID must decode as Azureus-style");
        };
        assert!(matches!(
            decoded.kind,
            librqbit::AzureusStyleKind::Other(code) if code == PEER_ID_CLIENT_CODE
        ));
        let expected = [
            env!("CARGO_PKG_VERSION_MAJOR"),
            env!("CARGO_PKG_VERSION_MINOR"),
            env!("CARGO_PKG_VERSION_PATCH"),
        ]
        .map(|component| component.parse::<u8>().unwrap());
        // rqbit 8.1.1 decodes only 0-9 correctly (its letter arm subtracts
        // from '0'); larger components are covered by the encoder test above.
        if expected.iter().all(|component| *component <= 9) {
            assert_eq!(decoded.version, [expected[0], expected[1], expected[2], 0]);
        }
    }

    #[test]
    fn every_identifier_names_the_same_client_and_version() {
        assert_eq!(CLIENT_USER_AGENT, format!("{CLIENT_NAME}/{CLIENT_VERSION}"));
        assert_eq!(
            CLIENT_HANDSHAKE_VERSION,
            format!("{CLIENT_NAME} {CLIENT_VERSION}")
        );
    }
}
