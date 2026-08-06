//! Hyper client helper for Prometheus remote write.

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, Request, StatusCode, Uri};
use http_body_util::{BodyExt as _, Full};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

pub(crate) type HttpClient = Client<HttpConnector, Full<Bytes>>;

/// Build a plain (no TLS) HTTP/1.1 client.
pub(crate) fn client() -> HttpClient {
    Client::builder(TokioExecutor::new()).build_http()
}

/// POST `body` to `uri` with the given headers; returns the response status.
pub(crate) async fn post(
    client: &HttpClient,
    uri: &Uri,
    headers: &[(HeaderName, HeaderValue)],
    body: Vec<u8>,
) -> Result<StatusCode, String> {
    let mut builder = Request::builder().method(Method::POST).uri(uri.clone());
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    let request = builder
        .body(Full::new(Bytes::from(body)))
        .map_err(|err| format!("build request: {err}"))?;
    let response = client
        .request(request)
        .await
        .map_err(|err| format!("request to {uri} failed: {err}"))?;
    let status = response.status();
    // Drain the body so the connection can be reused.
    let _ = response.into_body().collect().await;
    Ok(status)
}
