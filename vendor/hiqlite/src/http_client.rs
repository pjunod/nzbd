use std::time::Duration;

// not really dead code
// It will be used in any (real) scenario. This is only to get rid of a warning during some
// `clippy` checks.
#[allow(dead_code)]
pub fn build_http_client(tls_no_verify: bool) -> reqwest::Client {
    #[allow(unused_mut)]
    let mut builder = reqwest::Client::builder()
        .http2_prior_knowledge()
        // API requests carry a custom shared-secret header. Reqwest does not
        // classify that header as sensitive across origins, so management
        // clients must never follow a configured peer's redirect.
        .redirect(reqwest::redirect::Policy::none())
        .tls_danger_accept_invalid_certs(tls_no_verify)
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(30));

    #[cfg(feature = "webpki-roots")]
    {
        builder = builder.tls_certs_merge(
            webpki_root_certs::TLS_SERVER_ROOT_CERTS
                .iter()
                .map(|c| reqwest::Certificate::from_der(c).unwrap()),
        );
    }

    #[cfg(test)]
    {
        // Unit tests use only loopback HTTP or explicit no-verification TLS.
        // Keep them deterministic on headless macOS runners where the native
        // keychain can be unavailable even though no test requests HTTPS.
        builder = builder.tls_certs_only(std::iter::empty::<reqwest::Certificate>());
    }

    builder.build().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::HEADER_NAME_SECRET;
    use axum::Router;
    use axum::http::header::LOCATION;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::get;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn management_client_does_not_forward_api_secret_across_redirects() {
        let redirect_seen = Arc::new(AtomicBool::new(false));
        let secret_seen = Arc::new(AtomicBool::new(false));
        let sink_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink_listener.local_addr().unwrap();
        let sink_redirect_seen = Arc::clone(&redirect_seen);
        let sink_secret_seen = Arc::clone(&secret_seen);
        let sink = Router::new().route(
            "/",
            get(move |headers: HeaderMap| {
                let redirect_seen = Arc::clone(&sink_redirect_seen);
                let secret_seen = Arc::clone(&sink_secret_seen);
                async move {
                    redirect_seen.store(true, Ordering::SeqCst);
                    secret_seen.store(headers.contains_key(HEADER_NAME_SECRET), Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        );
        let sink_task = tokio::spawn(async move {
            axum::serve(sink_listener, sink).await.unwrap();
        });

        let redirect_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_addr = redirect_listener.local_addr().unwrap();
        let target = format!("http://{sink_addr}/");
        let redirect = Router::new().route(
            "/",
            get(move || {
                let target = target.clone();
                async move { (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, target)]) }
            }),
        );
        let redirect_task = tokio::spawn(async move {
            axum::serve(redirect_listener, redirect).await.unwrap();
        });

        let response = build_http_client(false)
            .get(format!("http://{redirect_addr}/"))
            .header(HEADER_NAME_SECRET, "redirect-secret-sentinel")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        tokio::task::yield_now().await;
        assert!(!redirect_seen.load(Ordering::SeqCst));
        assert!(!secret_seen.load(Ordering::SeqCst));

        redirect_task.abort();
        sink_task.abort();
    }
}
