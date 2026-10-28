//! A small, stateful S3 protocol fixture.
//!
//! This is a test-only service, not an S3 emulator. It exists so the real
//! locked AWS SDK performs serialization, HTTP exchange, retries, response
//! parsing against a local endpoint without cloud credentials.
//! It cannot prove every AWS or third-party S3 behavior.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State as AxumState},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use base64::Engine as _;
use quick_xml::{Reader, events::Event};
use sha2::{Digest as _, Sha256};
use tokio::{net::TcpListener, sync::Notify};

use super::S3Store;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
};

const BUCKET: &str = "cache";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Create,
    UploadPart,
    Complete,
    Abort,
    PutObject,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Operation {
    Create,
    UploadPart {
        part_number: i32,
        len: usize,
        checksum: String,
    },
    Complete {
        if_none_match: bool,
    },
    Abort,
    PutObject {
        len: usize,
        if_none_match: bool,
    },
}

#[derive(Clone, Debug)]
enum CompleteFailure {
    Status(u16, &'static str),
    Embedded200,
    Malformed,
}

#[derive(Clone, Debug, Default)]
struct Gate {
    started: Arc<Notify>,
    release: Arc<Notify>,
    used: Arc<AtomicBool>,
}

impl Gate {
    async fn pass(&self) {
        if !self.used.swap(true, Ordering::SeqCst) {
            self.started.notify_one();
            self.release.notified().await;
        }
    }
}

#[derive(Default)]
struct MockState {
    objects: HashMap<String, Vec<u8>>,
    uploads: HashMap<String, ActiveUpload>,
    next_upload: u64,
    operations: Vec<Operation>,
    part_arrivals: usize,
    part_failures: HashMap<i32, (usize, u16)>,
    complete_failure: Option<CompleteFailure>,
    abort_failures: VecDeque<u16>,
    missing_part_checksum: bool,
    gate: Option<(Op, Gate)>,
}

struct ActiveUpload {
    key: String,
    parts: BTreeMap<i32, StoredPart>,
}

struct StoredPart {
    bytes: Vec<u8>,
    etag: String,
    checksum: String,
}

#[derive(Clone)]
pub(crate) struct MockS3 {
    url: String,
    state: Arc<Mutex<MockState>>,
    _server: Arc<ServerGuard>,
}

struct ServerGuard(tokio::task::JoinHandle<()>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl MockS3 {
    pub(crate) async fn start() -> Self {
        let state = Arc::new(Mutex::new(MockState::default()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(any(handle))
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{address}"),
            state,
            _server: Arc::new(ServerGuard(server)),
        }
    }

    pub(crate) fn store(&self) -> S3Store {
        self.store_with_budget(super::UPLOAD_BUFFER_BYTES)
    }

    pub(crate) fn store_with_budget(&self, upload_buffer_bytes: usize) -> S3Store {
        S3Store::from_client(self.client(), BUCKET, "v1", upload_buffer_bytes)
    }

    fn client(&self) -> Client {
        let builder = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .endpoint_url(self.url.clone())
            .force_path_style(true)
            .retry_config(
                RetryConfig::standard()
                    .with_max_attempts(3)
                    .with_initial_backoff(Duration::from_millis(1)),
            )
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(5))
                    .build(),
            )
            .credentials_provider(Credentials::new(
                "test-access-key",
                "test-secret-key",
                None,
                None,
                "test",
            ));
        Client::from_conf(builder.build())
    }

    pub(crate) fn object(&self, key: &str) -> Option<Vec<u8>> {
        self.state.lock().unwrap().objects.get(key).cloned()
    }

    pub(crate) fn upload_ids(&self) -> Vec<String> {
        self.state.lock().unwrap().uploads.keys().cloned().collect()
    }

    pub(crate) fn operations(&self) -> Vec<Operation> {
        self.state.lock().unwrap().operations.clone()
    }

    pub(crate) fn operation_count(&self, predicate: impl Fn(&Operation) -> bool) -> usize {
        self.operations().iter().filter(|op| predicate(op)).count()
    }

    pub(crate) fn put_count(&self) -> usize {
        self.operation_count(|op| matches!(op, Operation::PutObject { .. }))
    }

    pub(crate) fn create_count(&self) -> usize {
        self.operation_count(|op| matches!(op, Operation::Create))
    }

    pub(crate) fn abort_count(&self) -> usize {
        self.operation_count(|op| matches!(op, Operation::Abort))
    }

    pub(crate) fn part_arrivals(&self) -> usize {
        self.state.lock().unwrap().part_arrivals
    }

    pub(crate) fn part_operations(&self) -> Vec<Operation> {
        self.operations()
            .into_iter()
            .filter(|op| matches!(op, Operation::UploadPart { .. }))
            .collect()
    }

    pub(crate) fn block_next(&self, op: Op) {
        self.state.lock().unwrap().gate = Some((op, Gate::default()));
    }

    pub(crate) async fn wait_until_blocked(&self, op: Op) {
        loop {
            let gate = self
                .state
                .lock()
                .unwrap()
                .gate
                .as_ref()
                .filter(|(blocked, _)| *blocked == op)
                .map(|(_, gate)| gate.clone());
            if let Some(gate) = gate {
                gate.started.notified().await;
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    pub(crate) fn release(&self) {
        if let Some((_, gate)) = self.state.lock().unwrap().gate.clone() {
            gate.release.notify_one();
        }
    }

    pub(crate) fn fail_part(&self, part_number: i32, times: usize, status: u16) {
        self.state
            .lock()
            .unwrap()
            .part_failures
            .insert(part_number, (times, status));
    }

    pub(crate) fn fail_complete(&self, status: u16, code: &'static str) {
        self.state.lock().unwrap().complete_failure = Some(CompleteFailure::Status(status, code));
    }

    pub(crate) fn fail_complete_embedded_200(&self) {
        self.state.lock().unwrap().complete_failure = Some(CompleteFailure::Embedded200);
    }

    pub(crate) fn fail_complete_malformed(&self) {
        self.state.lock().unwrap().complete_failure = Some(CompleteFailure::Malformed);
    }

    pub(crate) fn fail_abort(&self, times: usize, status: u16) {
        let mut state = self.state.lock().unwrap();
        for _ in 0..times {
            state.abort_failures.push_back(status);
        }
    }

    pub(crate) fn omit_part_checksum(&self) {
        self.state.lock().unwrap().missing_part_checksum = true;
    }
}

async fn handle(AxumState(mock): AxumState<Arc<Mutex<MockState>>>, request: Request) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let path = uri.path().trim_start_matches('/');
    let (_, key) = path.split_once('/').unwrap_or((path, ""));
    let key = key.to_owned();
    let query = uri.query().unwrap_or_default();

    let op = if method == axum::http::Method::POST && has_query(query, "uploads") {
        Op::Create
    } else if method == axum::http::Method::PUT && query_value(query, "uploadId").is_some() {
        Op::UploadPart
    } else if method == axum::http::Method::POST && query_value(query, "uploadId").is_some() {
        Op::Complete
    } else if method == axum::http::Method::DELETE && query_value(query, "uploadId").is_some() {
        Op::Abort
    } else if method == axum::http::Method::PUT {
        Op::PutObject
    } else {
        return error_response(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed");
    };

    if op == Op::UploadPart {
        mock.lock().unwrap().part_arrivals += 1;
    }
    let gate = {
        let state = mock.lock().unwrap();
        state
            .gate
            .as_ref()
            .filter(|(blocked, _)| *blocked == op)
            .map(|(_, gate)| gate.clone())
    };
    if let Some(gate) = gate {
        gate.pass().await;
    }

    match op {
        Op::Create => create(&mock, &key, &headers),
        Op::UploadPart => {
            let body = to_bytes(request.into_body(), usize::MAX).await.unwrap();
            upload_part(&mock, &key, &headers, query, &body)
        }
        Op::Complete => {
            let body = to_bytes(request.into_body(), usize::MAX).await.unwrap();
            complete(&mock, &key, &headers, query, &body)
        }
        Op::Abort => abort(&mock, query),
        Op::PutObject => {
            let body = to_bytes(request.into_body(), usize::MAX).await.unwrap();
            put_object(&mock, &key, &headers, &body)
        }
    }
}

fn create(mock: &Arc<Mutex<MockState>>, key: &str, headers: &HeaderMap) -> Response {
    let mut state = mock.lock().unwrap();
    state.next_upload += 1;
    let upload_id = format!("upload-{}", state.next_upload);
    let checksum_algorithm =
        header_value(headers, "x-amz-checksum-algorithm").unwrap_or_else(|| "SHA256".into());
    let checksum_type =
        header_value(headers, "x-amz-checksum-type").unwrap_or_else(|| "COMPOSITE".into());
    state.uploads.insert(
        upload_id.clone(),
        ActiveUpload {
            key: key.to_owned(),
            parts: BTreeMap::new(),
        },
    );
    state.operations.push(Operation::Create);
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Bucket>{BUCKET}</Bucket><Key>{key}</Key><UploadId>{upload_id}</UploadId>\
         </InitiateMultipartUploadResult>"
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .header("x-amz-checksum-algorithm", checksum_algorithm)
        .header("x-amz-checksum-type", checksum_type)
        .body(Body::from(body))
        .unwrap()
}

fn upload_part(
    mock: &Arc<Mutex<MockState>>,
    key: &str,
    headers: &HeaderMap,
    query: &str,
    body: &[u8],
) -> Response {
    let part_number = query_value(query, "partNumber")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    let upload_id = query_value(query, "uploadId").unwrap_or_default();
    let submitted_checksum = header_value(headers, "x-amz-checksum-sha256").unwrap_or_default();
    let checksum = base64::engine::general_purpose::STANDARD.encode(Sha256::digest(body));
    let content_length_matches = header_value(headers, "content-length")
        .and_then(|value| value.parse::<usize>().ok())
        == Some(body.len());

    let mut state = mock.lock().unwrap();
    state.operations.push(Operation::UploadPart {
        part_number,
        len: body.len(),
        checksum: checksum.clone(),
    });
    if let Some((remaining, status)) = state.part_failures.get_mut(&part_number)
        && *remaining > 0
    {
        *remaining -= 1;
        return error_response(
            StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            "InternalError",
        );
    }
    if !(1..=10_000).contains(&part_number) || body.is_empty() || !content_length_matches {
        return error_response(StatusCode::BAD_REQUEST, "InvalidPart");
    }
    if submitted_checksum != checksum {
        return error_response(StatusCode::BAD_REQUEST, "BadDigest");
    }
    let Some(upload) = state.uploads.get_mut(&upload_id) else {
        return error_response(StatusCode::NOT_FOUND, "NoSuchUpload");
    };
    if upload.key != key {
        return error_response(StatusCode::NOT_FOUND, "NoSuchUpload");
    }
    let etag = format!("part-{part_number}-etag");
    upload.parts.insert(
        part_number,
        StoredPart {
            bytes: body.to_vec(),
            etag: etag.clone(),
            checksum: checksum.clone(),
        },
    );
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{etag}\""))
        .header("x-amz-checksum-type", "COMPOSITE");
    if !state.missing_part_checksum {
        builder = builder.header("x-amz-checksum-sha256", checksum);
    }
    builder.body(Body::empty()).unwrap()
}

fn complete(
    mock: &Arc<Mutex<MockState>>,
    key: &str,
    headers: &HeaderMap,
    query: &str,
    body: &[u8],
) -> Response {
    let upload_id = query_value(query, "uploadId").unwrap_or_default();
    let if_none_match = header_value(headers, "if-none-match").as_deref() == Some("*");
    let mut state = mock.lock().unwrap();
    state.operations.push(Operation::Complete { if_none_match });

    if let Some(failure) = state.complete_failure.clone() {
        return match failure {
            CompleteFailure::Status(status, code) => {
                error_response(StatusCode::from_u16(status).unwrap(), code)
            }
            CompleteFailure::Embedded200 => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(
                    "<?xml version=\"1.0\"?><Error><Code>InternalError</Code>\
                     <Message>completion failed</Message><RequestId>test</RequestId></Error>",
                ))
                .unwrap(),
            CompleteFailure::Malformed => Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from("<CompleteMultipartUploadResult><Key>"))
                .unwrap(),
        };
    }

    let requested = match parse_completion(body) {
        Ok(parts) => parts,
        Err(()) => return error_response(StatusCode::BAD_REQUEST, "MalformedXML"),
    };
    if !if_none_match {
        return error_response(StatusCode::BAD_REQUEST, "InvalidRequest");
    }
    let Some(upload) = state.uploads.get(&upload_id) else {
        return error_response(StatusCode::NOT_FOUND, "NoSuchUpload");
    };
    if upload.key != key || requested.len() != upload.parts.len() {
        return error_response(StatusCode::BAD_REQUEST, "InvalidPart");
    }
    let mut bytes = Vec::new();
    for (index, (part_number, etag, checksum)) in requested.iter().enumerate() {
        let Some(part) = upload.parts.get(part_number) else {
            return error_response(StatusCode::BAD_REQUEST, "InvalidPart");
        };
        if part.etag != *etag || checksum.as_deref() != Some(part.checksum.as_str()) {
            return error_response(StatusCode::BAD_REQUEST, "InvalidPart");
        }
        if index + 1 < requested.len() && part.bytes.len() < 5 * 1024 * 1024 {
            return error_response(StatusCode::BAD_REQUEST, "EntityTooSmall");
        }
        bytes.extend_from_slice(&part.bytes);
    }
    if state.objects.contains_key(key) {
        return error_response(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
    }
    let etag = format!("\"complete-{upload_id}\"");
    state.objects.insert(key.to_owned(), bytes);
    state.uploads.remove(&upload_id);
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Location>http://example.invalid/{BUCKET}/{key}</Location><Bucket>{BUCKET}</Bucket>\
         <Key>{key}</Key><ETag>{etag}</ETag></CompleteMultipartUploadResult>"
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .unwrap()
}

fn abort(mock: &Arc<Mutex<MockState>>, query: &str) -> Response {
    let upload_id = query_value(query, "uploadId").unwrap_or_default();
    let mut state = mock.lock().unwrap();
    state.operations.push(Operation::Abort);
    if let Some(status) = state.abort_failures.pop_front() {
        return error_response(
            StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
            "ServiceUnavailable",
        );
    }
    if state.uploads.remove(&upload_id).is_none() {
        return error_response(StatusCode::NOT_FOUND, "NoSuchUpload");
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

fn put_object(
    mock: &Arc<Mutex<MockState>>,
    key: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    let mut state = mock.lock().unwrap();
    let if_none_match = header_value(headers, "if-none-match").as_deref() == Some("*");
    if !if_none_match {
        return error_response(StatusCode::BAD_REQUEST, "InvalidRequest");
    }
    state.operations.push(Operation::PutObject {
        len: body.len(),
        if_none_match,
    });
    if state.objects.contains_key(key) {
        return error_response(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
    }
    state.objects.insert(key.to_owned(), body.to_vec());
    Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, "\"put-etag\"")
        .body(Body::empty())
        .unwrap()
}

fn parse_completion(body: &[u8]) -> Result<Vec<(i32, String, Option<String>)>, ()> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut parts = Vec::new();
    let mut in_part = false;
    let mut text_tag: Option<Vec<u8>> = None;
    let mut part_number = None;
    let mut etag = None;
    let mut checksum = None;
    let mut buffer = Vec::new();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(event)) => match event.name().as_ref() {
                b"Part" => {
                    in_part = true;
                    part_number = None;
                    etag = None;
                    checksum = None;
                }
                name @ (b"PartNumber" | b"ETag" | b"ChecksumSHA256") if in_part => {
                    text_tag = Some(name.to_vec());
                }
                _ => {}
            },
            Ok(Event::Text(event)) if in_part => {
                if let Some(tag) = text_tag.as_ref() {
                    let decoded = event.decode().unwrap_or_default();
                    let value = quick_xml::escape::unescape(&decoded)
                        .unwrap_or_default()
                        .into_owned();
                    match tag.as_slice() {
                        b"PartNumber" => part_number = value.parse::<i32>().ok(),
                        b"ETag" => etag = Some(value),
                        b"ChecksumSHA256" => checksum = Some(value),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(event)) if event.name().as_ref() == b"Part" => {
                let Some(part_number) = part_number else {
                    return Err(());
                };
                let Some(etag) = etag.take() else {
                    return Err(());
                };
                parts.push((part_number, etag, checksum.take()));
                in_part = false;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => return Err(()),
        }
        buffer.clear();
    }
    Ok(parts)
}

fn error_response(status: StatusCode, code: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{code}</Message><RequestId>test</RequestId></Error>"
    );
    (
        status,
        [(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/xml"),
        )],
        body,
    )
        .into_response()
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn has_query(query: &str, wanted: &str) -> bool {
    query
        .split('&')
        .any(|pair| pair == wanted || pair.starts_with(&format!("{wanted}=")))
}

fn query_value(query: &str, wanted: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        (name == wanted).then(|| value.to_owned())
    })
}

pub(crate) fn test_digest(bytes: &[u8]) -> mbx_cache_protocol::Digest {
    mbx_cache_protocol::Digest::blake3(bytes)
}

pub(crate) fn object_key(digest: &mbx_cache_protocol::Digest) -> String {
    format!("v1/blobs/{}", digest.key())
}
