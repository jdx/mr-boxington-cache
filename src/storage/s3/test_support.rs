//! Small stateful S3 fixture for the download redirect path.
//!
//! Only HeadObject and GetObject are implemented. The real locked SDK performs
//! request signing and response parsing against this loopback endpoint.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use aws_sdk_s3::{
    Client,
    config::{Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use tokio::net::TcpListener;

use super::S3Store;

const BUCKET: &str = "cache";

#[derive(Default)]
struct MockState {
    objects: HashMap<String, Vec<u8>>,
    head_count: usize,
    get_count: usize,
    head_failure: Option<u16>,
    last_get_headers: Vec<(String, String)>,
}

#[derive(Clone)]
pub(crate) struct MockS3 {
    url: String,
    state: Arc<Mutex<MockState>>,
}

impl MockS3 {
    pub(crate) async fn start() -> Self {
        let state = Arc::new(Mutex::new(MockState::default()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(any(handle))
            .with_state(Arc::clone(&state));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{address}"),
            state,
        }
    }

    pub(crate) fn store(&self, redirect: bool) -> S3Store {
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .endpoint_url(&self.url)
            .force_path_style(true)
            .retry_config(
                RetryConfig::standard()
                    .with_max_attempts(3)
                    .with_initial_backoff(std::time::Duration::from_millis(1)),
            )
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(std::time::Duration::from_secs(5))
                    .build(),
            )
            .credentials_provider(Credentials::new(
                "test-access-key",
                "test-secret-key",
                None,
                None,
                "test",
            ));
        S3Store::from_client(Client::from_conf(config.build()), BUCKET, "v1", redirect)
    }

    pub(crate) fn insert(&self, key: &str, bytes: &[u8]) {
        self.state
            .lock()
            .unwrap()
            .objects
            .insert(key.to_owned(), bytes.to_vec());
    }

    pub(crate) fn head_count(&self) -> usize {
        self.state.lock().unwrap().head_count
    }

    pub(crate) fn get_count(&self) -> usize {
        self.state.lock().unwrap().get_count
    }

    pub(crate) fn fail_head(&self, status: u16) {
        self.state.lock().unwrap().head_failure = Some(status);
    }

    pub(crate) fn last_get_headers(&self) -> Vec<(String, String)> {
        self.state.lock().unwrap().last_get_headers.clone()
    }
}

async fn handle(State(state): State<Arc<Mutex<MockState>>>, request: Request) -> Response {
    let method = request.method().clone();
    let key = request
        .uri()
        .path()
        .trim_start_matches('/')
        .split_once('/')
        .map(|(_, key)| key.to_owned())
        .unwrap_or_default();
    let mut state = state.lock().unwrap();
    if method == axum::http::Method::HEAD {
        state.head_count += 1;
        if let Some(status) = state.head_failure {
            return StatusCode::from_u16(status).unwrap().into_response();
        }
        let Some(object) = state.objects.get(&key) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, object.len())
            .body(Body::empty())
            .unwrap();
    }
    if method == axum::http::Method::GET {
        state.get_count += 1;
        state.last_get_headers = request
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let Some(object) = state.objects.get(&key) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, object.len())
            .body(Body::from(object.clone()))
            .unwrap();
    }
    StatusCode::METHOD_NOT_ALLOWED.into_response()
}

pub(crate) fn object_key(digest: &mbx_cache_protocol::Digest) -> String {
    format!("v1/blobs/{}", digest.key())
}
