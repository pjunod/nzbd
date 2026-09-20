use crate::helpers::deserialize;
use crate::network::challenge_response::{Challenge, ChallengeResponse, ResponseFinal};
use crate::network::frame_io::write_socket_frame_flushed;
use crate::network::serialize_network;
use crate::{Error, LEADER_STREAM_CONNECT_TIMEOUT, NodeId};
use fastwebsockets::{Frame, OpCode, Payload, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::debug;

pub struct HandshakeSecret;

impl HandshakeSecret {
    pub async fn client(
        ws: &mut WebSocket<TokioIo<Upgraded>>,
        secret: &[u8],
        node_id: NodeId,
    ) -> Result<(), Error> {
        debug!("Executing HandshakeSecret::client");
        let frame = ws.read_frame().await?;
        let challenge_response = match frame.opcode {
            OpCode::Binary => {
                let bytes = frame.payload.as_ref();
                let challenge: Challenge = deserialize(bytes)?;
                ChallengeResponse::new(node_id, &challenge, secret)?
            }
            _ => {
                return Err(Error::BadRequest("Invalid Challenge from Server".into()));
            }
        };

        let frame = Frame::binary(Payload::from(serialize_network(&challenge_response)));
        write_socket_frame_flushed(ws, frame).await?;

        let frame = ws.read_frame().await?;
        match frame.opcode {
            OpCode::Binary => {
                let bytes = frame.payload.as_ref();
                let response: ResponseFinal = deserialize(bytes)?;
                response.verify(&challenge_response, secret)?;
            }
            _ => {
                return Err(Error::BadRequest(
                    "Invalid ResponseFinal from Server".into(),
                ));
            }
        };

        debug!("HandshakeSecret::client finished");
        Ok(())
    }

    pub(crate) async fn server(
        ws: &mut WebSocket<TokioIo<Upgraded>>,
        secret: &[u8],
    ) -> Result<NodeId, Error> {
        Self::server_with_timeout(ws, secret, LEADER_STREAM_CONNECT_TIMEOUT).await
    }

    async fn server_with_timeout<S>(
        ws: &mut WebSocket<S>,
        secret: &[u8],
        timeout: Duration,
    ) -> Result<NodeId, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        tokio::time::timeout(timeout, Self::server_exchange(ws, secret))
            .await
            .map_err(|_| {
                Error::Connect(format!(
                    "WebSocket server handshake exceeded {} ms",
                    timeout.as_millis()
                ))
            })?
    }

    async fn server_exchange<S>(ws: &mut WebSocket<S>, secret: &[u8]) -> Result<NodeId, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        debug!("Executing HandshakeSecret::server");
        let challenge = Challenge::new()?;

        let frame = Frame::binary(Payload::from(serialize_network(&challenge)));
        write_socket_frame_flushed(ws, frame).await?;

        // we are not using a fragment collector and don't check for a full frame either
        // it should never be an issue though because the handshake packets are tiny
        let frame = ws.read_frame().await?;
        let (node_id, response) = match frame.opcode {
            OpCode::Binary => {
                let bytes = frame.payload.as_ref();
                let challenge_response: ChallengeResponse = deserialize(bytes)?;
                let resp = challenge_response.verify(&challenge, secret)?;
                (challenge_response.node_id, resp)
            }
            _ => {
                return Err(Error::BadRequest(
                    "Invalid ChallengeResponse from Client".into(),
                ));
            }
        };

        let frame = Frame::binary(Payload::from(serialize_network(&response)));
        write_socket_frame_flushed(ws, frame).await?;

        debug!("HandshakeSecret::server finished");
        Ok(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastwebsockets::Role;

    #[tokio::test(start_paused = true)]
    async fn silent_peer_cannot_hold_the_server_handshake_open() {
        let (server_io, peer_io) = tokio::io::duplex(4 * 1024);
        let mut server = WebSocket::after_handshake(server_io, Role::Server);
        let _peer = WebSocket::after_handshake(peer_io, Role::Client);
        let budget = Duration::from_millis(50);
        let started = tokio::time::Instant::now();

        let error = HandshakeSecret::server_with_timeout(&mut server, b"secret", budget)
            .await
            .expect_err("a silent peer must exhaust the handshake budget");

        assert!(matches!(error, Error::Connect(ref message) if message.contains("50 ms")));
        assert_eq!(started.elapsed(), budget);
    }
}
