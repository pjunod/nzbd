use crate::helpers::{deserialize, serialize};
use crate::network::api::{
    ApiStreamRequest, ApiStreamRequestPayload, ApiStreamResponse, ApiStreamResponsePayload,
    WsWriteMsg,
};
use crate::network::frame_io::{
    CLOSE_WRITE_TIMEOUT, write_close_frame_flushed, write_frame_flushed,
    write_socket_close_frame_flushed,
};
use crate::network::handshake::HandshakeSecret;
use crate::server::proxy::handlers::AppStateExt;
use crate::store::state_machine::sqlite::state_machine::Query;
use crate::{Client, Error};
use fastwebsockets::{FragmentCollectorRead, Frame, OpCode, Payload, upgrade};
use tokio::sync::{oneshot, watch};
use tokio::task;
use tracing::{debug, error};

pub async fn handle_socket(
    state: AppStateExt,
    socket: upgrade::UpgradeFut,
) -> Result<(), fastwebsockets::WebSocketError> {
    let mut ws = socket.await?;
    ws.set_auto_close(true);

    if let Err(err) = HandshakeSecret::server(&mut ws, state.secret_api.as_bytes()).await {
        error!("Error during WebSocket handshake: {}", err);
        write_socket_close_frame_flushed(&mut ws, Frame::close(1000, b"Invalid Handshake")).await?;
        return Ok(());
    };

    let (tx_write, rx_write) = flume::bounded::<WsWriteMsg>(1);
    // let (tx_write, rx_write) = flume::unbounded::<WsWriteMsg>();
    // let (tx_read, rx_read) = flume::unbounded();

    // TODO splitting needs `unstable-split` feature right now but is about to be stabilized soon
    let (rx, mut write) = ws.split(tokio::io::split);
    // IMPORTANT: the reader is NOT CANCEL SAFE in v0.8!
    let mut read = FragmentCollectorRead::new(rx);

    let (tx_connection_closed, mut rx_connection_closed) = watch::channel(false);
    let (tx_writer_finished, mut rx_writer_finished) = oneshot::channel();
    let writer_connection_closed = tx_connection_closed.clone();
    let handle_write = task::spawn(async move {
        let outcome = loop {
            let req = match rx_write.recv_async().await {
                Ok(req) => req,
                Err(_) => break Ok(()),
            };
            match req {
                WsWriteMsg::Payload(resp) => {
                    let bytes = serialize(&resp).unwrap();
                    if let Err(err) = write_proxy_response_frame(&mut write, &bytes).await {
                        error!("Error during WebSocket write: {}", err);
                        // // if we have a WebSocket error, save all open requests into the client_buffer
                        // let payload = bincode::serialize(&resp).unwrap();
                        // buf_tx
                        //     .send_async(payload)
                        //     .await
                        //     .expect("client_buffer to always be working");

                        break Err(err.to_string());
                    }
                }
                WsWriteMsg::Break => {
                    // we ignore any errors here since it may be possible that the reader
                    // has closed already - we just try a graceful connection close
                    let _ = write_close_frame_flushed(
                        &mut write,
                        Frame::close(1000, b"Invalid Request"),
                    )
                    .await;
                    debug!("server stream break message");
                    break Ok(());
                }
            }
        };

        writer_connection_closed.send_replace(true);
        let _ = tx_writer_finished.send(outcome);
        debug!("server stream exiting");
    });

    let (tx_read, rx_read) = flume::bounded(1);
    let (tx_reader_finished, mut rx_reader_finished) = oneshot::channel();
    let reader_connection_closed = tx_connection_closed.clone();
    let handle_read = task::spawn(async move {
        let outcome = loop {
            let frame = match read
                .read_frame(&mut |frame| async move {
                    debug!(
                        "Received obligated send in stream client: OpCode: {:?}: {:?}",
                        frame.opcode.clone(),
                        frame.payload
                    );
                    Ok::<(), Error>(())
                })
                .await
            {
                Ok(frame) => frame,
                Err(err) => break Err(err.to_string()),
            };
            let req = match frame.opcode {
                OpCode::Close => break Ok(()),
                OpCode::Binary => match deserialize::<ApiStreamRequest>(&frame.payload) {
                    Ok(req) => req,
                    Err(err) => break Err(format!("invalid proxy stream request: {err}")),
                },
                _ => break Err("non-binary proxy stream payload".to_owned()),
            };
            tokio::select! {
                _ = rx_connection_closed.changed() => break Ok(()),
                result = tx_read.send_async(req) => {
                    if result.is_err() {
                        break Ok(());
                    }
                }
            }
        };
        reader_connection_closed.send_replace(true);
        let _ = tx_reader_finished.send(outcome);
    });

    let mut writer_failed = false;
    loop {
        let req = tokio::select! {
            biased;
            writer = &mut rx_writer_finished => {
                match writer {
                    Ok(Ok(())) => error!("proxy WebSocket writer exited while connected"),
                    Ok(Err(err)) => error!("proxy WebSocket writer failed: {err}"),
                    Err(_) => error!("proxy WebSocket writer panicked or was cancelled"),
                }
                writer_failed = true;
                break;
            }
            reader = &mut rx_reader_finished => {
                match reader {
                    Ok(Ok(())) => debug!("proxy WebSocket reader exited"),
                    Ok(Err(err)) => error!("proxy WebSocket reader failed: {err}"),
                    Err(_) => error!("proxy WebSocket reader panicked or was cancelled"),
                }
                break;
            }
            req = rx_read.recv_async() => match req {
                Ok(req) => req,
                Err(_) => break,
            },
        };

        let state = state.clone();
        let tx_write = tx_write.clone();
        task::spawn(async move {
            let client = &state.client;
            // exchange orig req id for our own to avoid conflicts
            let request_id = req.request_id;

            let res = match req.payload {
                ApiStreamRequestPayload::Execute(sql) => {
                    let res = client.execute(sql.sql, sql.params).await;
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Execute(res),
                    }
                }

                ApiStreamRequestPayload::ExecuteReturning(query) => {
                    match client.execute_returning_req(query.clone()).await {
                        Ok(res) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::ExecuteReturning(Ok(res)),
                        },
                        Err(err) => {
                            if client
                                .was_leader_update_error(&err, &client.inner.leader_db)
                                .await
                            {
                                let res = client.execute_returning_req(query).await;
                                ApiStreamResponse {
                                    request_id,
                                    result: ApiStreamResponsePayload::ExecuteReturning(res),
                                }
                            } else {
                                ApiStreamResponse {
                                    request_id,
                                    result: ApiStreamResponsePayload::ExecuteReturning(Err(err)),
                                }
                            }
                        }
                    }
                }

                ApiStreamRequestPayload::Transaction(queries) => {
                    let res = match client.txn_execute(queries.clone()).await {
                        Ok(res) => Ok(res),
                        Err(err) => {
                            if client
                                .was_leader_update_error(&err, &client.inner.leader_db)
                                .await
                            {
                                client.txn_execute(queries).await
                            } else {
                                Err(err)
                            }
                        }
                    };
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Transaction(res),
                    }
                }

                ApiStreamRequestPayload::QueryConsistent(q) => {
                    query(client, request_id, q, true).await
                }

                ApiStreamRequestPayload::Batch(sql) => {
                    let res = client.batch(sql).await;
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Batch(res),
                    }
                }

                ApiStreamRequestPayload::Migrate(migrations) => {
                    let res = match client.migrate_execute(migrations.clone()).await {
                        Ok(res) => Ok(res),
                        Err(err) => {
                            if client
                                .was_leader_update_error(&err, &client.inner.leader_db)
                                .await
                            {
                                client.migrate_execute(migrations).await
                            } else {
                                Err(err)
                            }
                        }
                    };
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Migrate(res),
                    }
                }

                ApiStreamRequestPayload::Backup(_node_id) => {
                    let res = client.backup().await;
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Backup(res),
                    }
                }

                ApiStreamRequestPayload::Query(q) => query(client, request_id, q, false).await,

                ApiStreamRequestPayload::KV(cache_req) => {
                    let res = client.cache_req_retry(cache_req, false).await;
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::KV(res),
                    }
                }

                ApiStreamRequestPayload::KVGet(cache_req) => {
                    let res = client.cache_req_retry(cache_req, true).await;
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::KV(res),
                    }
                }

                ApiStreamRequestPayload::LockAwait(cache_req) => {
                    match client.lock_req_retry(cache_req, true).await {
                        Ok(res) => ApiStreamResponse {
                            request_id,
                            result: ApiStreamResponsePayload::Lock(res),
                        },
                        Err(_) => {
                            todo!(
                                "how should be handle await errors? wrap state inside inner result just for the proxy or retry endlessly?"
                            )
                        } // Err(err) => ApiStreamResponse {
                          //     request_id,
                          //     result: ApiStreamResponsePayload::Lock(Err(err)),
                          // },
                    }
                }

                ApiStreamRequestPayload::Notify(cache_req) => {
                    let res = client.notify_req(cache_req).await;
                    ApiStreamResponse {
                        request_id,
                        result: ApiStreamResponsePayload::Notify(res),
                    }
                }
            };

            if let Err(err) = tx_write.send_async(WsWriteMsg::Payload(res)).await {
                error!("Error sending payload to tx_write: {}", err);
            }
        });
    }

    tx_connection_closed.send_replace(true);
    let mut handle_write = handle_write;
    let writer_finished = if writer_failed {
        false
    } else {
        tokio::time::timeout(CLOSE_WRITE_TIMEOUT, async {
            tx_write.send_async(WsWriteMsg::Break).await.ok()?;
            Some((&mut handle_write).await)
        })
        .await
        .ok()
        .flatten()
        .is_some()
    };
    drop(tx_write);
    if !writer_finished {
        handle_write.abort();
        let _ = handle_write.await;
    }
    handle_read.abort();
    let _ = handle_read.await;

    Ok(())
}

async fn write_proxy_response_frame<S>(
    write: &mut fastwebsockets::WebSocketWrite<S>,
    bytes: &[u8],
) -> Result<(), fastwebsockets::WebSocketError>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    write_frame_flushed(write, Frame::binary(Payload::Borrowed(bytes))).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastwebsockets::Role;

    #[tokio::test]
    async fn production_proxy_response_writer_flushes_serialized_response_through_tls() {
        let response = ApiStreamResponse {
            request_id: 23,
            result: ApiStreamResponsePayload::Query(Ok(Vec::new())),
        };
        let bytes = serialize(&response).expect("serialize proxy response");
        crate::network::frame_io::tests::exercise_gated_tls_writer(
            Role::Server,
            bytes,
            true,
            |mut write, bytes| async move { write_proxy_response_frame(&mut write, &bytes).await },
        )
        .await;
    }
}

#[inline]
async fn query(
    client: &Client,
    request_id: usize,
    query: Query,
    consistent: bool,
) -> ApiStreamResponse {
    let res = match client.query_remote_req(query.clone(), consistent).await {
        Ok(res) => Ok(res),
        Err(err) => {
            if client
                .was_leader_update_error(&err, &client.inner.leader_db)
                .await
            {
                client.query_remote_req(query, consistent).await
            } else {
                Err(err)
            }
        }
    };

    let result = if consistent {
        ApiStreamResponsePayload::QueryConsistent(res)
    } else {
        ApiStreamResponsePayload::Query(res)
    };
    ApiStreamResponse { request_id, result }
}
