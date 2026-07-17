//! Any-node API: non-leaders transparently reverse-proxy client traffic
//! (`/api/v1/*`, `/jsonrpc`) to the current leader (CLUSTERING.md §8).
//! Election gaps surface as 503 — *arr clients retry.

use crate::election::LeaderView;
use crate::http::ClusterClient;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use tokio::sync::watch;

pub const FORWARDED_HEADER: &str = "x-nzbd-forwarded";

#[derive(Clone)]
pub struct ProxyState {
    pub node: String,
    pub view: watch::Receiver<LeaderView>,
    pub client: ClusterClient,
}

pub async fn proxy_to_leader(
    State(px): State<ProxyState>,
    mut req: Request,
    next: Next,
) -> Response {
    let v = px.view.borrow().clone();
    if v.is_me {
        return next.run(req).await;
    }
    if req.headers().contains_key(FORWARDED_HEADER) {
        // A forwarded request landing on a non-leader means the cluster
        // disagrees about leadership mid-election. Don't bounce it around.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "leadership is changing; retry"})),
        )
            .into_response();
    }
    let Some(leader_url) = v.leader_url().map(|s| s.to_string()) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "no leader elected yet; retry"})),
        )
            .into_response();
    };

    req.headers_mut().insert(
        FORWARDED_HEADER,
        px.node.parse().unwrap_or_else(|_| "node".parse().unwrap()),
    );
    match px.client.forward(&leader_url, req).await {
        Ok(resp) => resp.map(Body::new).into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("leader unreachable: {e}")})),
        )
            .into_response(),
    }
}
