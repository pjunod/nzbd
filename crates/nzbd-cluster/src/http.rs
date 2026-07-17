//! Minimal intra-cluster HTTP client (worker → leader JSON calls and the
//! any-node → leader reverse proxy) on hyper-util's pooled legacy client.

use crate::proto::SECRET_HEADER;
use axum::body::Body;
use http_body_util::BodyExt;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Clone)]
pub struct ClusterClient {
    inner: HyperClient<HttpConnector, Body>,
    secret: String,
}

impl ClusterClient {
    pub fn new(secret: String) -> ClusterClient {
        ClusterClient {
            inner: HyperClient::builder(TokioExecutor::new()).build_http(),
            secret,
        }
    }

    pub async fn post_json<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        base_url: &str,
        path: &str,
        req: &Req,
    ) -> Result<Resp, String> {
        let uri: hyper::Uri = format!("{}{}", base_url.trim_end_matches('/'), path)
            .parse()
            .map_err(|e| format!("bad url: {e}"))?;
        let body = serde_json::to_vec(req).map_err(|e| e.to_string())?;
        let request = hyper::Request::post(uri)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(SECRET_HEADER, &self.secret)
            .body(Body::from(body))
            .map_err(|e| e.to_string())?;
        let resp = self
            .inner
            .request(request)
            .await
            .map_err(|e| format!("request: {e}"))?;
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| format!("body: {e}"))?
            .to_bytes();
        if !status.is_success() {
            return Err(format!(
                "{status}: {}",
                String::from_utf8_lossy(&bytes[..bytes.len().min(200)])
            ));
        }
        serde_json::from_slice(&bytes).map_err(|e| format!("decode: {e}"))
    }

    /// Forward an arbitrary request to the leader (streaming both ways).
    pub async fn forward(
        &self,
        base_url: &str,
        mut req: hyper::Request<Body>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, String> {
        let path_q = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".into());
        let uri: hyper::Uri = format!("{}{}", base_url.trim_end_matches('/'), path_q)
            .parse()
            .map_err(|e| format!("bad url: {e}"))?;
        *req.uri_mut() = uri;
        self.inner
            .request(req)
            .await
            .map_err(|e| format!("proxy: {e}"))
    }
}

/// Constant-time-ish secret comparison (length + fold, no early exit).
pub fn secret_matches(provided: Option<&str>, expected: &str) -> bool {
    let Some(p) = provided else { return false };
    if p.len() != expected.len() {
        return false;
    }
    p.bytes()
        .zip(expected.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_compare() {
        assert!(secret_matches(Some("abc"), "abc"));
        assert!(!secret_matches(Some("abd"), "abc"));
        assert!(!secret_matches(Some("ab"), "abc"));
        assert!(!secret_matches(None, "abc"));
    }
}
