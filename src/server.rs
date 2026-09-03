use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{StreamExt, TryStreamExt, stream};
use mbx_cache_protocol::{
    ACTION_RESULT_BATCH_MEDIA_TYPE, ActionKindCapability,
    BLOB_PACK_BLOBS_HEADER as PACK_BLOBS_HEADER, BLOB_PACK_BYTES_HEADER as PACK_BYTES_HEADER,
    BLOB_PACK_MAGIC, BLOB_PACK_MEDIA_TYPE, BLOB_PACK_RECEIPT_MEDIA_TYPE, Capabilities,
    CapabilityFeatures, CapabilityLimits, CapabilityProtocol, MAX_BATCH_ITEMS, PROTOCOL_VERSION,
    TASK_ACTION_MANIFEST_MEDIA_TYPE, canonical_json,
};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use std::{
    collections::{BTreeMap, HashSet},
    io,
    sync::Arc,
    time::Instant,
};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tower_http::{
    compression::{CompressionLayer, CompressionLevel},
    decompression::RequestDecompressionLayer,
    limit::RequestBodyLimitLayer,
    trace::TraceLayer,
};

use crate::{
    auth::{Access, Authorizer},
    metadata::{CommitOutcome, ManifestCommitOutcome, MetadataStore},
    metrics::Metrics,
    model::{
        ActionResult, Algorithm, Digest, Directory, RustcAction, RustcMetadata, TaskAction,
        TaskActionManifest, TaskMetadata,
    },
    pack::{PackError, PackEvent, PackReader},
    storage::{BlobStore, PutOutcome},
};

const BLOB_PACK_HEADER_BYTES: usize = mbx_cache_protocol::BLOB_PACK_HEADER_BYTES as usize;
const MAX_PACK_STORAGE_READS: usize = 16;
const MAX_ACTION_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub blobs: Arc<dyn BlobStore>,
    pub metadata: Arc<dyn MetadataStore>,
    pub auth: Authorizer,
    pub max_blob_bytes: u64,
    metrics: Arc<Metrics>,
}

impl AppState {
    pub fn new(
        blobs: Arc<dyn BlobStore>,
        metadata: Arc<dyn MetadataStore>,
        auth: Authorizer,
        max_blob_bytes: u64,
    ) -> Self {
        Self {
            blobs,
            metadata,
            auth,
            max_blob_bytes,
            metrics: Arc::new(Metrics::new()),
        }
    }
}

pub fn router(state: AppState) -> Router {
    let limit = usize::try_from(state.max_blob_bytes).unwrap_or(usize::MAX);
    // Blob uploads are the only compressed requests, so decompression is
    // scoped to them rather than applied to every route. The JSON handlers
    // buffer and parse their whole body before they authenticate anyone, and
    // there is no reason to let an unauthenticated caller spend a few
    // kilobytes to fill that buffer. Axum's own 2 MB default bounds them --
    // `json_routes_keep_axums_default_body_limit` pins it -- and the largest
    // legitimate request, MAX_BATCH_ITEMS digests, is about 1.1 MB. Action
    // manifests are the exception: a large build can legitimately carry more
    // than 2 MB of predictions, so that route has its own explicit bound.
    let blobs = Router::new()
        .route(
            "/v1/blobs/{algorithm}/{hash}/{size}",
            get(get_blob).put(put_blob),
        )
        // Innermost: what the handler reads, after decoding.
        .layer(RequestBodyLimitLayer::new(limit))
        .layer(RequestDecompressionLayer::new())
        // Outermost: what crosses the wire. Decoding leaves this unbounded on
        // its own -- a skippable frame is arbitrarily large and decodes to
        // nothing, so a limit on decoded bytes never sees it.
        .layer(RequestBodyLimitLayer::new(limit));
    // `max_pack_bytes` is a payload budget. Leave room for the magic prefix
    // and one fixed-size frame header per possible blob so a pack carrying the
    // advertised maximum payload is not rejected by the transport layer.
    let pack_limit = limit
        .saturating_add(BLOB_PACK_MAGIC.len())
        .saturating_add(BLOB_PACK_HEADER_BYTES.saturating_mul(MAX_BATCH_ITEMS));
    let pack_uploads = Router::new()
        .route("/v1/blobs:pack-upload", post(put_blob_pack))
        .layer(RequestBodyLimitLayer::new(pack_limit))
        .layer(RequestDecompressionLayer::new())
        .layer(RequestBodyLimitLayer::new(pack_limit));
    let action_manifests = Router::new()
        .route(
            "/v1/action-manifests/{algorithm}/{hash}/{size}",
            get(get_action_manifest).put(put_action_manifest),
        )
        .layer(DefaultBodyLimit::max(MAX_ACTION_MANIFEST_BYTES));
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/capabilities", get(capabilities))
        .merge(blobs)
        .merge(pack_uploads)
        .merge(action_manifests)
        .route("/v1/blobs:missing", post(missing_blobs))
        .route("/v1/blobs:pack", post(pack_blobs))
        .route(
            "/v1/action-results/{algorithm}/{hash}/{size}",
            get(get_action_result).put(put_action_result),
        )
        .route("/v1/action-results:batch", post(batch_action_results))
        .route("/metrics", get(metrics))
        // Fastest: rustc artifacts still compress well below level 1's cost,
        // and a cache server's CPU is better spent serving than squeezing.
        .layer(CompressionLayer::new().quality(CompressionLevel::Fastest))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Note a read so a future garbage collector can evict by real use.
///
/// Losing an access record only makes eviction slightly less well informed, so
/// this never turns a served blob into a failed request.
async fn record_blob_access(state: &AppState, namespace: &str, digests: &[Digest]) {
    if let Err(error) = state.metadata.touch_blobs(namespace, digests).await {
        tracing::debug!(%error, "could not record blob access");
    }
}

async fn status() -> impl IntoResponse {
    Json(serde_json::json!({"status":"ok","protocol":PROTOCOL_VERSION}))
}

async fn capabilities(State(state): State<AppState>) -> impl IntoResponse {
    // Built through the protocol crate's constructors rather than as struct
    // literals: the records are non-exhaustive so that a later minor can
    // describe a feature this service does not implement, and only what is
    // assigned here is claimed.
    let mut capabilities = Capabilities::new(CapabilityProtocol::new(PROTOCOL_VERSION, 0));
    capabilities.digest_algorithms = vec!["blake3".into(), "sha256".into()];
    capabilities.compressors = vec!["identity".into(), "zstd".into()];
    capabilities.action_kinds = BTreeMap::from([
        ("build-script".into(), ActionKindCapability::new(2, 1)),
        ("cc".into(), ActionKindCapability::new(1, 1)),
        ("rustc".into(), ActionKindCapability::new(1, 1)),
        ("task".into(), ActionKindCapability::new(1, 1)),
    ]);
    let mut features = CapabilityFeatures::default();
    features.action_manifests = true;
    features.batch = true;
    features.action_batch = true;
    features.blob_packs = true;
    features.blob_pack_uploads = true;
    capabilities.features = features;
    let mut limits = CapabilityLimits::default();
    limits.max_batch_items = MAX_BATCH_ITEMS as u64;
    limits.max_inline_blob_bytes = 1_048_576;
    limits.max_blob_bytes = state.max_blob_bytes;
    limits.max_pack_bytes = state.max_blob_bytes;
    capabilities.limits = limits;
    Json(capabilities)
}

async fn get_blob(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(parts): Path<(String, String, u64)>,
) -> Result<Response, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Read).await?;
    let digest = parse_digest(parts)?;
    if !state
        .metadata
        .blob_visible(&namespace, &digest)
        .await
        .map_err(ApiError::internal)?
    {
        return Err(ApiError::not_found("blob not found"));
    }
    record_blob_access(&state, &namespace, std::slice::from_ref(&digest)).await;
    let blob = state
        .blobs
        .get(&digest)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("blob not found"))?;
    state.metrics.inc_blob_hit();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, blob.size)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ETAG, format!("\"{}\"", digest.hash))
        .header("digest", format!("{}={}", digest.algorithm, digest.hash))
        .body(Body::from_stream(blob.stream))
        .map_err(ApiError::internal)
}

async fn put_blob(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(parts): Path<(String, String, u64)>,
    body: Body,
) -> Result<StatusCode, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Write).await?;
    require_immutable_precondition(&headers)?;
    let digest = parse_digest(parts)?;
    if digest.size > state.max_blob_bytes {
        return Err(ApiError::too_large("blob exceeds configured limit"));
    }
    let temp = NamedTempFile::new().map_err(ApiError::internal)?;
    let path = temp.path().to_owned();
    let mut file = tokio::fs::File::create(&path)
        .await
        .map_err(ApiError::internal)?;
    let mut stream = body.into_data_stream();
    let mut size = 0_u64;
    let mut blake3 = blake3::Hasher::new();
    let mut sha256 = sha2::Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ApiError::bad_request(error.to_string()))?;
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| ApiError::too_large("blob is too large"))?;
        if size > digest.size || size > state.max_blob_bytes {
            return Err(ApiError::too_large(
                "blob exceeds declared or configured size",
            ));
        }
        match digest.algorithm_kind().map_err(ApiError::bad_request)? {
            Algorithm::Blake3 => {
                blake3.update(&chunk);
            }
            Algorithm::Sha256 => {
                sha256.update(&chunk);
            }
        }
        file.write_all(&chunk).await.map_err(ApiError::internal)?;
    }
    file.flush().await.map_err(ApiError::internal)?;
    let actual_hash = match digest.algorithm_kind().map_err(ApiError::bad_request)? {
        Algorithm::Blake3 => blake3.finalize().to_hex().to_string(),
        Algorithm::Sha256 => hex::encode(sha256.finalize()),
    };
    if size != digest.size || actual_hash != digest.hash {
        return Err(ApiError::bad_request(
            "content does not match the requested digest",
        ));
    }
    let outcome = state
        .blobs
        .put(&digest, &path)
        .await
        .map_err(ApiError::internal)?;
    state
        .metadata
        .register_blob(&namespace, &digest, outcome)
        .await
        .map_err(ApiError::internal)?;
    state.metrics.inc_blob_upload();
    Ok(match outcome {
        PutOutcome::Created => StatusCode::CREATED,
        PutOutcome::AlreadyExists => StatusCode::NO_CONTENT,
    })
}

#[derive(Serialize)]
struct BlobPackReceipt {
    created: u64,
    existing: u64,
}

/// Accept several blobs in one framed request.
///
/// Rustc output is many small objects, so a client that publishes them one
/// request at a time spends most of its time in round trips. Each frame is
/// verified against the digest it declares before it is stored, exactly as a
/// single upload is: a pack is a transport for uploads, not a weaker kind of
/// one. A frame that fails ends the request, which may leave the blobs before
/// it stored -- they are content-addressed and immutable, so storing one that a
/// client then re-sends individually costs nothing but the transfer.
async fn put_blob_pack(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Write).await?;
    let declared_blobs = required_u64_header(&headers, PACK_BLOBS_HEADER)?;
    let declared_bytes = required_u64_header(&headers, PACK_BYTES_HEADER)?;
    if declared_blobs > MAX_BATCH_ITEMS as u64 {
        return Err(ApiError::bad_request(
            "blob pack declares more blobs than the supported batch size",
        ));
    }
    if declared_bytes > state.max_blob_bytes {
        return Err(ApiError::too_large(
            "blob pack exceeds the configured size limit",
        ));
    }
    let mut reader = PackReader::new(state.max_blob_bytes, declared_blobs, declared_bytes);
    let mut stream = body.into_data_stream();
    let mut frame: Option<PackFrame> = None;
    let mut created = 0;
    let mut existing = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ApiError::bad_request(error.to_string()))?;
        for event in reader.push(&chunk).map_err(pack_error)? {
            match event {
                PackEvent::Started(digest) => {
                    frame = Some(PackFrame::new(digest).await?);
                }
                PackEvent::Payload(payload) => {
                    frame
                        .as_mut()
                        .ok_or_else(|| ApiError::internal("payload outside a blob pack frame"))?
                        .write(&payload)
                        .await?;
                }
                PackEvent::Complete => {
                    let finished = frame.take().ok_or_else(|| {
                        ApiError::internal("blob pack frame ended before it began")
                    })?;
                    match finished.store(&state, &namespace).await? {
                        PutOutcome::Created => created += 1,
                        PutOutcome::AlreadyExists => existing += 1,
                    }
                }
            }
        }
    }
    reader.finish().map_err(pack_error)?;
    if reader.blobs() != declared_blobs || reader.payload_bytes() != declared_bytes {
        return Err(ApiError::bad_request(
            "blob pack does not carry what its headers declared",
        ));
    }
    state.metrics.inc_blob_pack_upload(reader.blobs());
    Ok((
        [(header::CONTENT_TYPE, BLOB_PACK_RECEIPT_MEDIA_TYPE)],
        Json(BlobPackReceipt { created, existing }),
    )
        .into_response())
}

/// One frame of an uploaded pack, spooled and hashed as it arrives.
struct PackFrame {
    digest: Digest,
    temp: NamedTempFile,
    file: tokio::fs::File,
    size: u64,
    blake3: blake3::Hasher,
    sha256: sha2::Sha256,
}

impl PackFrame {
    /// Start spooling and hashing the payload declared by one frame header.
    async fn new(digest: Digest) -> Result<Self, ApiError> {
        let temp = NamedTempFile::new().map_err(ApiError::internal)?;
        let file = tokio::fs::File::create(temp.path())
            .await
            .map_err(ApiError::internal)?;
        Ok(Self {
            digest,
            temp,
            file,
            size: 0,
            blake3: blake3::Hasher::new(),
            sha256: sha2::Sha256::new(),
        })
    }

    /// Extend the spool and the digest calculation with one payload chunk.
    async fn write(&mut self, payload: &[u8]) -> Result<(), ApiError> {
        self.size += payload.len() as u64;
        match self
            .digest
            .algorithm_kind()
            .map_err(ApiError::bad_request)?
        {
            Algorithm::Blake3 => {
                self.blake3.update(payload);
            }
            Algorithm::Sha256 => {
                self.sha256.update(payload);
            }
        }
        self.file
            .write_all(payload)
            .await
            .map_err(ApiError::internal)
    }

    /// Verify and publish a completed frame as an ordinary blob upload.
    async fn store(mut self, state: &AppState, namespace: &str) -> Result<PutOutcome, ApiError> {
        self.file.flush().await.map_err(ApiError::internal)?;
        let actual = match self
            .digest
            .algorithm_kind()
            .map_err(ApiError::bad_request)?
        {
            Algorithm::Blake3 => self.blake3.finalize().to_hex().to_string(),
            Algorithm::Sha256 => hex::encode(self.sha256.finalize()),
        };
        if self.size != self.digest.size || actual != self.digest.hash {
            return Err(ApiError::bad_request(
                "packed blob does not match its declared digest",
            ));
        }
        let outcome = state
            .blobs
            .put(&self.digest, self.temp.path())
            .await
            .map_err(ApiError::internal)?;
        state
            .metadata
            .register_blob(namespace, &self.digest, outcome)
            .await
            .map_err(ApiError::internal)?;
        state.metrics.inc_blob_upload();
        Ok(outcome)
    }
}

/// A malformed pack is the client's error; an oversized one says which limit.
fn pack_error(error: PackError) -> ApiError {
    match error {
        PackError::BlobTooLarge(..) | PackError::TooManyBytes(_) | PackError::TooManyBlobs(_) => {
            ApiError::too_large(error.to_string())
        }
        other => ApiError::bad_request(other.to_string()),
    }
}

/// Parse a required unsigned request header.
fn required_u64_header(headers: &HeaderMap, name: &str) -> Result<u64, ApiError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| ApiError::bad_request(format!("{name} must be an unsigned integer")))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MissingRequest {
    digests: Vec<Digest>,
}

#[derive(Serialize)]
struct MissingResponse {
    missing: Vec<Digest>,
}

async fn missing_blobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<MissingRequest>,
) -> Result<Json<MissingResponse>, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Read).await?;
    if request.digests.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request(
            "at most 10000 digests may be checked",
        ));
    }
    for digest in &request.digests {
        validate_digest(digest)?;
    }
    let visible = state
        .metadata
        .visible_blobs(&namespace, &request.digests)
        .await
        .map_err(ApiError::internal)?
        .into_iter()
        .collect::<HashSet<_>>();
    let missing = request
        .digests
        .into_iter()
        .filter(|digest| !visible.contains(digest))
        .collect();
    Ok(Json(MissingResponse { missing }))
}

async fn pack_blobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<MissingRequest>,
) -> Result<Response, ApiError> {
    let request_started = Instant::now();
    let namespace = state.auth.authorize(&headers, Access::Read).await?;
    if request.digests.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request("at most 10000 digests may be packed"));
    }
    let mut total_bytes = 0_u64;
    let mut seen = HashSet::new();
    let mut requested = Vec::with_capacity(request.digests.len());
    for digest in request.digests {
        validate_digest(&digest)?;
        if seen.insert(digest.clone()) {
            total_bytes = total_bytes
                .checked_add(digest.size)
                .ok_or_else(|| ApiError::too_large("blob pack is too large"))?;
            if total_bytes > state.max_blob_bytes {
                return Err(ApiError::too_large(
                    "blob pack exceeds the configured size limit",
                ));
            }
            requested.push(digest);
        }
    }
    let metadata_started = Instant::now();
    let visible = match state.metadata.visible_blobs(&namespace, &requested).await {
        Ok(visible) => {
            state
                .metrics
                .observe_pack_metadata_query("success", metadata_started.elapsed());
            visible
        }
        Err(error) => {
            state
                .metrics
                .observe_pack_metadata_query("error", metadata_started.elapsed());
            return Err(ApiError::internal(error));
        }
    };
    record_blob_access(&state, &namespace, &visible).await;
    let missing_blobs = requested.len().saturating_sub(visible.len()) as u64;
    let mut pack_guard = state.metrics.start_pack(
        request_started,
        requested.len() as u64,
        total_bytes,
        missing_blobs,
    );
    let mut pack_bytes = BLOB_PACK_MAGIC.len() as u64;
    let mut pack_payload_bytes = 0_u64;
    let store = state.blobs.clone();
    let metrics = state.metrics.clone();
    let reads = stream::iter(visible.into_iter().map(|digest| {
        let store = store.clone();
        async move {
            let size = match store.size(&digest).await {
                Ok(Some(size)) => size,
                Ok(None) => {
                    return Err(ApiError::internal(io::Error::new(
                        io::ErrorKind::NotFound,
                        "visible blob is missing from storage",
                    )));
                }
                Err(error) => {
                    return Err(ApiError::internal(error));
                }
            };
            if size != digest.size {
                return Err(ApiError::internal(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stored blob size does not match its digest",
                )));
            }
            Ok::<_, ApiError>((pack_blob_header(&digest)?, digest))
        }
    }));
    let mut reads = reads.buffered(MAX_PACK_STORAGE_READS);
    let mut entries = Vec::new();
    while let Some(entry) = reads.next().await {
        let (header, digest) = match entry {
            Ok(entry) => entry,
            Err(error) => {
                pack_guard.error();
                return Err(error);
            }
        };
        pack_payload_bytes = pack_payload_bytes
            .checked_add(digest.size)
            .ok_or_else(|| ApiError::too_large("blob pack is too large"))?;
        pack_bytes = pack_bytes
            .checked_add(BLOB_PACK_HEADER_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(digest.size))
            .ok_or_else(|| ApiError::too_large("blob pack is too large"))?;
        entries.push((header, digest));
    }
    drop(reads);
    let pack_blobs = entries.len();
    let stream_metrics = metrics.clone();
    let response_stream = async_stream::try_stream! {
        pack_guard.record_first_byte();
        yield Bytes::from_static(BLOB_PACK_MAGIC);
        let reads = stream::iter(entries.into_iter().map(|(header, digest)| {
            let store = store.clone();
            let stream_metrics = stream_metrics.clone();
            async move {
                let started = Instant::now();
                let blob = match store.get(&digest).await {
                    Ok(Some(blob)) => {
                        stream_metrics.observe_pack_storage_get("hit", started.elapsed());
                        blob
                    }
                    Ok(None) => {
                        stream_metrics.observe_pack_storage_get("missing", started.elapsed());
                        return Err(io::Error::new(io::ErrorKind::NotFound, "visible blob is missing from storage"));
                    }
                    Err(error) => {
                        stream_metrics.observe_pack_storage_get("error", started.elapsed());
                        return Err(io::Error::other(error));
                    }
                };
                if blob.size != digest.size {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "stored blob size does not match its digest"));
                }
                Ok::<_, io::Error>((header, blob))
            }
        }));
        let mut reads = reads.buffered(MAX_PACK_STORAGE_READS);
        while let Some(entry) = reads.next().await {
            let (header, mut blob) = entry.inspect_err(|_| {
                pack_guard.error();
            })?;
            yield header;
            let mut streamed = 0_u64;
            while let Some(chunk) = blob.stream.next().await {
                let chunk = chunk.inspect_err(|_| {
                    pack_guard.error();
                })?;
                streamed = streamed.checked_add(chunk.len() as u64).ok_or_else(|| {
                    pack_guard.error();
                    io::Error::new(io::ErrorKind::InvalidData, "stored blob stream is too large")
                })?;
                if streamed > blob.size {
                    pack_guard.error();
                    Err(io::Error::new(io::ErrorKind::InvalidData, "stored blob stream is larger than advertised"))?;
                }
                pack_guard.add_served_bytes(chunk.len() as u64);
                yield chunk;
            }
            if streamed != blob.size {
                pack_guard.error();
                Err(io::Error::new(io::ErrorKind::InvalidData, "stored blob stream is shorter than advertised"))?;
            }
            pack_guard.blob_served();
            stream_metrics.inc_blob_hit();
        }
        pack_guard.complete();
    }
    .map_err(|error: io::Error| error);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, BLOB_PACK_MEDIA_TYPE)
        .header(header::CONTENT_LENGTH, pack_bytes)
        .header(PACK_BLOBS_HEADER, pack_blobs.to_string())
        .header(PACK_BYTES_HEADER, pack_payload_bytes.to_string())
        .body(Body::from_stream(response_stream))
        .map_err(ApiError::internal)
}

fn pack_blob_header(digest: &Digest) -> Result<Bytes, ApiError> {
    let hash = hex::decode(&digest.hash).map_err(ApiError::bad_request)?;
    if hash.len() != 32 {
        return Err(ApiError::bad_request("invalid digest hash length"));
    }
    let mut header = Vec::with_capacity(BLOB_PACK_HEADER_BYTES);
    header.push(
        match digest.algorithm_kind().map_err(ApiError::bad_request)? {
            Algorithm::Blake3 => 1,
            Algorithm::Sha256 => 2,
        },
    );
    header.extend_from_slice(&hash);
    header.extend_from_slice(&digest.size.to_be_bytes());
    Ok(Bytes::from(header))
}

async fn get_action_result(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(parts): Path<(String, String, u64)>,
) -> Result<Json<ActionResult>, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Read).await?;
    let action = parse_action_digest(parts)?;
    match state
        .metadata
        .get(&namespace, &action)
        .await
        .map_err(ApiError::internal)?
    {
        Some(result) => {
            state.metrics.inc_action_hit();
            Ok(Json(result))
        }
        None => {
            state.metrics.inc_action_miss();
            Err(ApiError::not_found("action result not found"))
        }
    }
}

#[derive(Serialize)]
struct ActionResultBatchResponse {
    results: Vec<ActionResult>,
}

/// Answer for as many actions as one request asks about.
///
/// A build knows every action it wants before it asks for any of them, and
/// asking one at a time costs a round trip each. The response carries only what
/// this namespace holds; each record names its own action, so a client binds
/// them back to its request without relying on order.
async fn batch_action_results(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<MissingRequest>,
) -> Result<Response, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Read).await?;
    if request.digests.len() > MAX_BATCH_ITEMS {
        return Err(ApiError::bad_request("batch exceeds the supported size"));
    }
    let mut requested = Vec::with_capacity(request.digests.len());
    let mut seen = HashSet::new();
    for digest in request.digests {
        let digest = validate_action_digest(digest)?;
        if seen.insert(digest.clone()) {
            requested.push(digest);
        }
    }
    let results = state
        .metadata
        .get_batch(&namespace, &requested)
        .await
        .map_err(ApiError::internal)?;
    state.metrics.inc_action_batch(
        results.len() as u64,
        (requested.len() - results.len()) as u64,
    );
    Ok((
        [(header::CONTENT_TYPE, ACTION_RESULT_BATCH_MEDIA_TYPE)],
        Json(ActionResultBatchResponse { results }),
    )
        .into_response())
}

async fn put_action_result(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(parts): Path<(String, String, u64)>,
    Json(result): Json<ActionResult>,
) -> Result<StatusCode, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Write).await?;
    require_immutable_precondition(&headers)?;
    let action = parse_action_digest(parts)?;
    if result.version != 1 || result.action != action {
        return Err(ApiError::bad_request(
            "action result does not match request",
        ));
    }
    let action_kind = validate_action_descriptor(&state, &namespace, &action).await?;
    validate_action_result_shape(&result, action_kind)?;
    if let Some(metadata) = &result.metadata {
        validate_client_metadata(&state, &namespace, metadata, action_kind).await?;
    }
    if let Some(root) = &result.output_root {
        validate_tree(&state, &namespace, root).await?;
    }
    let outcome = state
        .metadata
        .commit(&namespace, &action, &result)
        .await
        .map_err(ApiError::internal)?;
    state.metrics.inc_action_commit();
    match outcome {
        CommitOutcome::Created => Ok(StatusCode::CREATED),
        CommitOutcome::AlreadyExists => Ok(StatusCode::NO_CONTENT),
        CommitOutcome::Conflict => Err(ApiError::conflict(
            "an immutable action result already exists",
        )),
    }
}

async fn get_action_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(parts): Path<(String, String, u64)>,
) -> Result<Response, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Read).await?;
    let key = parse_action_digest(parts)?;
    let record = state
        .metadata
        .get_manifest(&namespace, &key)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("action manifest not found"))?;
    // Canonical, not serde's field order: the digest a client checks a manifest
    // against is taken over canonical bytes, and `ActionPrediction` does not
    // declare its fields in the order the canonicalization scheme sorts them.
    let body = canonical_json(&record.manifest).map_err(ApiError::internal)?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, TASK_ACTION_MANIFEST_MEDIA_TYPE)
        .header(header::ETAG, quoted_etag(&record.etag))
        .body(Body::from(body))
        .map_err(ApiError::internal)
}

async fn put_action_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(parts): Path<(String, String, u64)>,
    Json(manifest): Json<TaskActionManifest>,
) -> Result<Response, ApiError> {
    let namespace = state.auth.authorize(&headers, Access::Write).await?;
    let key = parse_action_digest(parts)?;
    if !manifest.validate() || manifest.selector_digest() != key {
        return Err(ApiError::bad_request(
            "action manifest does not match request",
        ));
    }
    let expected_etag = manifest_precondition(&headers)?;
    // Over the same canonical bytes `get_action_manifest` serves, so an entity
    // tag identifies what a client will actually read back.
    let bytes = canonical_json(&manifest).map_err(ApiError::internal)?;
    let etag = blake3::hash(&bytes).to_hex().to_string();
    let outcome = state
        .metadata
        .commit_manifest(&namespace, &key, expected_etag.as_deref(), &etag, &manifest)
        .await
        .map_err(ApiError::internal)?;
    let status = match outcome {
        ManifestCommitOutcome::Created => StatusCode::CREATED,
        ManifestCommitOutcome::Updated => StatusCode::NO_CONTENT,
        ManifestCommitOutcome::PreconditionFailed => {
            return Err(ApiError::precondition(
                "action manifest changed; read and merge it before retrying",
            ));
        }
    };
    Response::builder()
        .status(status)
        .header(header::ETAG, quoted_etag(&etag))
        .body(Body::empty())
        .map_err(ApiError::internal)
}

async fn require_blob(
    state: &AppState,
    namespace: &str,
    digest: &Digest,
    label: &str,
) -> Result<(), ApiError> {
    validate_digest(digest)?;
    if state
        .metadata
        .blob_visible(namespace, digest)
        .await
        .map_err(ApiError::internal)?
    {
        Ok(())
    } else {
        Err(ApiError::unprocessable(format!("{label} is missing")))
    }
}

async fn validate_tree(state: &AppState, namespace: &str, root: &Digest) -> Result<(), ApiError> {
    enum Visit {
        Enter(Digest),
        Exit(Digest),
    }

    let mut pending = vec![Visit::Enter(root.clone())];
    let mut visiting = HashSet::new();
    let mut seen = HashSet::new();
    while let Some(visit) = pending.pop() {
        let digest = match visit {
            Visit::Exit(digest) => {
                visiting.remove(&digest);
                seen.insert(digest);
                continue;
            }
            Visit::Enter(digest) => digest,
        };
        if seen.contains(&digest) {
            continue;
        }
        if !visiting.insert(digest.clone()) {
            return Err(ApiError::unprocessable("directory graph contains a cycle"));
        }
        if seen.len() + visiting.len() > 100_000 {
            return Err(ApiError::unprocessable("directory graph is too large"));
        }
        if digest.size > 16 * 1024 * 1024 {
            return Err(ApiError::unprocessable("directory object is too large"));
        }
        if !state
            .metadata
            .blob_visible(namespace, &digest)
            .await
            .map_err(ApiError::internal)?
        {
            return Err(ApiError::unprocessable("directory object is missing"));
        }
        let mut blob = state
            .blobs
            .get(&digest)
            .await
            .map_err(ApiError::internal)?
            .ok_or_else(|| ApiError::unprocessable("directory object is missing"))?;
        let mut bytes = Vec::with_capacity(digest.size as usize);
        while let Some(chunk) = blob.stream.next().await {
            bytes.extend_from_slice(&chunk.map_err(ApiError::internal)?);
        }
        let directory: Directory = serde_json::from_slice(&bytes)
            .map_err(|_| ApiError::unprocessable("directory object is invalid"))?;
        if serde_json::to_vec(&directory).map_err(ApiError::internal)? != bytes {
            return Err(ApiError::unprocessable(
                "directory object is not canonical JSON",
            ));
        }
        if directory.version != 1 {
            return Err(ApiError::unprocessable(
                "unsupported directory object version",
            ));
        }
        validate_directory_entries(&directory)?;
        for file in directory.files {
            require_blob(state, namespace, &file.digest, "file blob").await?;
        }
        pending.push(Visit::Exit(digest));
        pending.extend(
            directory
                .directories
                .into_iter()
                .map(|node| Visit::Enter(node.digest)),
        );
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActionKind {
    BuildScript,
    Cc,
    Rustc,
    Task,
}

fn validate_action_result_shape(
    result: &ActionResult,
    action_kind: ActionKind,
) -> Result<(), ApiError> {
    if action_kind != ActionKind::Task
        && (result.metadata.is_none() || result.output_root.is_none())
    {
        return Err(ApiError::unprocessable(
            "compiler action results require metadata and an output root",
        ));
    }
    Ok(())
}

async fn validate_action_descriptor(
    state: &AppState,
    namespace: &str,
    digest: &Digest,
) -> Result<ActionKind, ApiError> {
    let value = read_canonical_object(state, namespace, digest, "action descriptor").await?;
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::unprocessable("action descriptor must be a JSON object"))?;
    let kind = object
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ApiError::unprocessable("action descriptor kind is required"))?
        .to_owned();
    let version = object
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ApiError::unprocessable("action descriptor version is required"))?;
    match kind.as_str() {
        "build-script" => validate_build_script_action(value, version),
        "cc" => validate_cc_action(value, version),
        "task" => validate_task_action(value, version),
        "rustc" => validate_rustc_action(value, version),
        _ => Err(ApiError::unprocessable(format!(
            "unsupported action kind {kind:?}"
        ))),
    }
}

fn validate_build_script_action(
    value: serde_json::Value,
    version: u64,
) -> Result<ActionKind, ApiError> {
    if version != 2 {
        return Err(ApiError::unprocessable(format!(
            "unsupported build-script action schema {version}"
        )));
    }
    let action =
        serde_json::from_value::<crate::model::BuildScriptAction>(value).map_err(|error| {
            ApiError::unprocessable(format!("invalid build-script action: {error}"))
        })?;
    if !action.validate() {
        return Err(ApiError::unprocessable(
            "invalid build-script action values",
        ));
    }
    Ok(ActionKind::BuildScript)
}

fn validate_cc_action(value: serde_json::Value, version: u64) -> Result<ActionKind, ApiError> {
    if version != 1 {
        return Err(ApiError::unprocessable(format!(
            "unsupported cc action schema {version}"
        )));
    }
    let action = serde_json::from_value::<crate::model::CcAction>(value)
        .map_err(|error| ApiError::unprocessable(format!("invalid cc action: {error}")))?;
    if !action.validate() {
        return Err(ApiError::unprocessable("invalid cc action values"));
    }
    Ok(ActionKind::Cc)
}

fn validate_task_action(value: serde_json::Value, version: u64) -> Result<ActionKind, ApiError> {
    if version != 1 {
        return Err(ApiError::unprocessable(format!(
            "unsupported task action schema {version}"
        )));
    }
    let object = value
        .as_object()
        .expect("action descriptors are checked to be objects");
    for field in [
        "version",
        "kind",
        "task",
        "phase",
        "run",
        "args",
        "shell",
        "outputs",
        "root",
        "source_hash",
        "environment",
        "vars",
        "tools",
        "os",
        "arch",
    ] {
        if !object.contains_key(field) {
            return Err(ApiError::unprocessable(format!(
                "task action field {field:?} is required"
            )));
        }
    }
    let action = serde_json::from_value::<TaskAction>(value)
        .map_err(|error| ApiError::unprocessable(format!("invalid task action: {error}")))?;
    if !action.validate() {
        return Err(ApiError::unprocessable("invalid task action values"));
    }
    Ok(ActionKind::Task)
}

fn validate_rustc_action(value: serde_json::Value, version: u64) -> Result<ActionKind, ApiError> {
    if version != 1 {
        return Err(ApiError::unprocessable(format!(
            "unsupported rustc action schema {version}"
        )));
    }
    let action = serde_json::from_value::<RustcAction>(value)
        .map_err(|error| ApiError::unprocessable(format!("invalid rustc action: {error}")))?;
    if !action.validate() {
        return Err(ApiError::unprocessable("invalid rustc action values"));
    }
    Ok(ActionKind::Rustc)
}

async fn validate_client_metadata(
    state: &AppState,
    namespace: &str,
    digest: &Digest,
    action_kind: ActionKind,
) -> Result<(), ApiError> {
    let value = read_canonical_object(state, namespace, digest, "client metadata").await?;
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::unprocessable("client metadata must be a JSON object"))?;
    let kind = object
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ApiError::unprocessable("client metadata kind is required"))?;
    let version = object
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ApiError::unprocessable("client metadata version is required"))?;
    let expected_kind = match action_kind {
        ActionKind::BuildScript => "build-script",
        ActionKind::Cc => "cc",
        ActionKind::Rustc => "rustc",
        ActionKind::Task => "task",
    };
    if kind != expected_kind {
        return Err(ApiError::unprocessable(
            "client metadata kind does not match action kind",
        ));
    }
    match action_kind {
        ActionKind::Task => validate_task_metadata(value, version),
        ActionKind::BuildScript => {
            validate_build_script_metadata(state, namespace, value, version).await
        }
        ActionKind::Cc => validate_cc_metadata(state, namespace, value, version).await,
        ActionKind::Rustc => validate_rustc_metadata(state, namespace, value, version).await,
    }
}

async fn validate_build_script_metadata(
    state: &AppState,
    namespace: &str,
    value: serde_json::Value,
    version: u64,
) -> Result<(), ApiError> {
    if version != 1 {
        return Err(ApiError::unprocessable(format!(
            "unsupported build-script metadata schema {version}"
        )));
    }
    let metadata: crate::model::BuildScriptMetadata =
        serde_json::from_value(value).map_err(|error| {
            ApiError::unprocessable(format!("invalid build-script metadata: {error}"))
        })?;
    if !metadata.validate() {
        return Err(ApiError::unprocessable(
            "invalid build-script metadata values",
        ));
    }
    require_output_blobs(
        state,
        namespace,
        &metadata.stdout,
        &metadata.stderr,
        "build-script",
    )
    .await
}

async fn validate_cc_metadata(
    state: &AppState,
    namespace: &str,
    value: serde_json::Value,
    version: u64,
) -> Result<(), ApiError> {
    if version != 1 {
        return Err(ApiError::unprocessable(format!(
            "unsupported cc metadata schema {version}"
        )));
    }
    let metadata: crate::model::CcMetadata = serde_json::from_value(value)
        .map_err(|error| ApiError::unprocessable(format!("invalid cc metadata: {error}")))?;
    if !metadata.validate() {
        return Err(ApiError::unprocessable("invalid cc metadata values"));
    }
    require_output_blobs(state, namespace, &metadata.stdout, &metadata.stderr, "cc").await
}

fn validate_task_metadata(value: serde_json::Value, version: u64) -> Result<(), ApiError> {
    if version != 1 {
        return Err(ApiError::unprocessable(format!(
            "unsupported task metadata schema {version}"
        )));
    }
    let metadata: TaskMetadata = serde_json::from_value(value)
        .map_err(|error| ApiError::unprocessable(format!("invalid task metadata: {error}")))?;
    if !metadata.validate() {
        return Err(ApiError::unprocessable("invalid task metadata values"));
    }
    for root in metadata.roots {
        validate_task_root(&root)?;
    }
    Ok(())
}

async fn validate_rustc_metadata(
    state: &AppState,
    namespace: &str,
    value: serde_json::Value,
    version: u64,
) -> Result<(), ApiError> {
    if version != 1 {
        return Err(ApiError::unprocessable(format!(
            "unsupported rustc metadata schema {version}"
        )));
    }
    let metadata: RustcMetadata = serde_json::from_value(value)
        .map_err(|error| ApiError::unprocessable(format!("invalid rustc metadata: {error}")))?;
    if !metadata.validate() {
        return Err(ApiError::unprocessable("invalid rustc metadata values"));
    }
    require_output_blobs(
        state,
        namespace,
        &metadata.stdout,
        &metadata.stderr,
        "rustc",
    )
    .await
}

async fn require_output_blobs(
    state: &AppState,
    namespace: &str,
    stdout: &Digest,
    stderr: &Digest,
    kind: &str,
) -> Result<(), ApiError> {
    require_blob(state, namespace, stdout, &format!("{kind} stdout blob")).await?;
    require_blob(state, namespace, stderr, &format!("{kind} stderr blob")).await
}

fn validate_task_root(root: &str) -> Result<(), ApiError> {
    if root.is_empty()
        || root.starts_with('/')
        || root.contains(['\\', '\0'])
        || root
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(ApiError::unprocessable(
            "task metadata root must be a safe relative path",
        ));
    }
    Ok(())
}

async fn read_canonical_object(
    state: &AppState,
    namespace: &str,
    digest: &Digest,
    label: &str,
) -> Result<serde_json::Value, ApiError> {
    validate_digest(digest)?;
    if digest.size > 16 * 1024 * 1024 {
        return Err(ApiError::unprocessable(format!("{label} is too large")));
    }
    if !state
        .metadata
        .blob_visible(namespace, digest)
        .await
        .map_err(ApiError::internal)?
    {
        return Err(ApiError::unprocessable(format!("{label} blob is missing")));
    }
    let mut blob = state
        .blobs
        .get(digest)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::unprocessable(format!("{label} blob is missing")))?;
    let mut bytes = Vec::with_capacity(digest.size as usize);
    while let Some(chunk) = blob.stream.next().await {
        bytes.extend_from_slice(&chunk.map_err(ApiError::internal)?);
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::unprocessable(format!("{label} is invalid JSON")))?;
    if !value.is_object() || serde_json::to_vec(&value).map_err(ApiError::internal)? != bytes {
        return Err(ApiError::unprocessable(format!(
            "{label} is not canonical JSON"
        )));
    }
    Ok(value)
}

fn validate_directory_entries(directory: &Directory) -> Result<(), ApiError> {
    let mut names = HashSet::new();
    for node in &directory.directories {
        validate_entry(&mut names, &node.name, node.mode)?;
    }
    for node in &directory.files {
        validate_entry(&mut names, &node.name, node.mode)?;
        let _ = node.executable;
    }
    for node in &directory.symlinks {
        validate_entry(&mut names, &node.name, node.mode)?;
        if node.target.is_empty() || node.target.contains('\0') {
            return Err(ApiError::unprocessable("invalid symlink target"));
        }
    }
    Ok(())
}

fn validate_entry(names: &mut HashSet<String>, name: &str, mode: u32) -> Result<(), ApiError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains(['/', '\\', '\0'])
        || mode > 0o7777
    {
        return Err(ApiError::unprocessable("invalid directory entry"));
    }
    if !names.insert(name.to_owned()) {
        return Err(ApiError::unprocessable("duplicate directory entry"));
    }
    Ok(())
}

async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let body = state.metrics.encode().map_err(ApiError::internal)?;
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(body))
        .map_err(ApiError::internal)
}

fn parse_digest((algorithm, hash, size): (String, String, u64)) -> Result<Digest, ApiError> {
    let digest = Digest {
        algorithm,
        hash,
        size,
    };
    validate_digest(&digest)?;
    Ok(digest)
}

fn parse_action_digest(parts: (String, String, u64)) -> Result<Digest, ApiError> {
    let digest = parse_digest(parts)?;
    if digest.algorithm != "blake3" {
        return Err(ApiError::bad_request("action result keys must use blake3"));
    }
    Ok(digest)
}

/// Check a digest a request body named, rather than one from a path.
fn validate_action_digest(digest: Digest) -> Result<Digest, ApiError> {
    validate_digest(&digest)?;
    if digest.algorithm != "blake3" {
        return Err(ApiError::bad_request("action result keys must use blake3"));
    }
    Ok(digest)
}

fn validate_digest(digest: &Digest) -> Result<(), ApiError> {
    if digest.validate().is_ok() {
        Ok(())
    } else {
        Err(ApiError::bad_request("invalid digest"))
    }
}

fn require_immutable_precondition(headers: &HeaderMap) -> Result<(), ApiError> {
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        == Some("*")
    {
        Ok(())
    } else {
        Err(ApiError::precondition("If-None-Match: * is required"))
    }
}

fn manifest_precondition(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        == Some("*")
    {
        return Ok(None);
    }
    let value = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            ApiError::precondition_required("If-None-Match: * or If-Match is required")
        })?;
    let etag = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .filter(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| ApiError::bad_request("If-Match must contain one strong BLAKE3 ETag"))?;
    Ok(Some(etag.to_owned()))
}

fn quoted_etag(etag: &str) -> String {
    format!("\"{etag}\"")
}

pub struct ApiError {
    status: StatusCode,
    message: String,
    advertise_protocol: bool,
}

impl ApiError {
    fn new(status: StatusCode, message: impl ToString) -> Self {
        Self {
            status,
            message: message.to_string(),
            advertise_protocol: false,
        }
    }
    pub fn bad_request(message: impl ToString) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
    pub fn unauthorized(message: impl ToString) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, message)
    }
    pub fn forbidden(message: impl ToString) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }
    pub fn not_found(message: impl ToString) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
    pub fn conflict(message: impl ToString) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }
    pub fn too_large(message: impl ToString) -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, message)
    }
    pub fn unprocessable(message: impl ToString) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, message)
    }
    pub fn precondition(message: impl ToString) -> Self {
        Self::new(StatusCode::PRECONDITION_FAILED, message)
    }
    pub fn precondition_required(message: impl ToString) -> Self {
        Self::new(StatusCode::PRECONDITION_REQUIRED, message)
    }
    pub fn upgrade_required() -> Self {
        Self {
            status: StatusCode::UPGRADE_REQUIRED,
            message: "mbx cache protocol version 1 is required".into(),
            advertise_protocol: true,
        }
    }
    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "request failed");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response =
            (self.status, Json(serde_json::json!({"error":self.message}))).into_response();
        if self.advertise_protocol {
            response
                .headers_mut()
                .insert("mbx-cache-protocol", header::HeaderValue::from_static("1"));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{metadata::MemoryMetadata, storage::FilesystemStore};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn test_app() -> (Router, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let blobs = Arc::new(FilesystemStore::new(directory.path()).await.unwrap());
        let metadata = Arc::new(MemoryMetadata::default());
        let auth = Authorizer::new(None, None, true, None).await.unwrap();
        (
            router(AppState::new(blobs, metadata, auth, 1024 * 1024)),
            directory,
        )
    }

    fn request(method: &str, uri: String, body: Body) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("mbx-cache-protocol", "1")
            .header("mbx-cache-namespace", "test/project")
            .header(header::IF_NONE_MATCH, "*")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap()
    }

    fn digest(bytes: &[u8]) -> Digest {
        Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: blake3::hash(bytes).to_hex().to_string(),
            size: bytes.len() as u64,
        }
    }

    fn blob_path(root: &std::path::Path, digest: &Digest) -> std::path::PathBuf {
        root.join("blobs")
            .join(&digest.algorithm)
            .join(&digest.hash[..2])
            .join(&digest.hash)
            .join(digest.size.to_string())
    }

    fn decode_blob_pack(bytes: &[u8]) -> Vec<(Digest, Vec<u8>)> {
        assert!(bytes.starts_with(BLOB_PACK_MAGIC));
        let mut offset = BLOB_PACK_MAGIC.len();
        let mut entries = Vec::new();
        while offset < bytes.len() {
            assert!(bytes.len() - offset >= BLOB_PACK_HEADER_BYTES);
            let algorithm = match bytes[offset] {
                1 => Algorithm::Blake3,
                2 => Algorithm::Sha256,
                value => panic!("unexpected blob pack algorithm {value}"),
            };
            let hash = hex::encode(&bytes[offset + 1..offset + 33]);
            let size = u64::from_be_bytes(
                bytes[offset + 33..offset + BLOB_PACK_HEADER_BYTES]
                    .try_into()
                    .unwrap(),
            );
            offset += BLOB_PACK_HEADER_BYTES;
            let end = offset + usize::try_from(size).unwrap();
            assert!(end <= bytes.len());
            entries.push((
                Digest {
                    algorithm: algorithm.into(),
                    hash,
                    size,
                },
                bytes[offset..end].to_vec(),
            ));
            offset = end;
        }
        entries
    }

    fn canonical(value: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&value).unwrap()
    }

    fn task_action(version: u8) -> Vec<u8> {
        canonical(serde_json::json!({
            "arch":"x86_64",
            "args":[],
            "command_inputs":[],
            "dependency_keys":[],
            "environment":{},
            "kind":"task",
            "os":"linux",
            "outputs":["target/debug/widget"],
            "phase":"normal",
            "root":".",
            "run":["cargo build"],
            "shell":null,
            "source_hash":"blake3:source",
            "task":"build",
            "tools":["core:rust@1.92.0"],
            "vars":{},
            "version":version
        }))
    }

    fn task_metadata(kind: &str, version: u8) -> Vec<u8> {
        canonical(serde_json::json!({
            "execution_duration_ns":1,
            "kind":kind,
            "output":[{"line":"built widget","stream":"stdout"}],
            "restored_bytes":42,
            "roots":["target/debug/widget"],
            "task_identity":"build:.",
            "version":version
        }))
    }

    fn rustc_action(input: &Digest, version: u8) -> Vec<u8> {
        canonical(serde_json::json!({
            "adapter_version":1,
            "arguments":[
                "--crate-name=widget",
                "--crate-type=lib",
                "--emit=metadata,link",
                "--out-dir=${target}/debug/deps"
            ],
            "compiler":{
                "host":"x86_64-unknown-linux-gnu",
                "rustc_version":"1.97.1 (test)",
                "toolchain":"core:rust@1.97.1"
            },
            "environment":{"CARGO_PKG_VERSION":"1.0.0"},
            "inputs":[{"digest":input,"path":"${workspace}/src/lib.rs"}],
            "kind":"rustc",
            "version":version
        }))
    }

    fn cc_action(input: &Digest, version: u8) -> Vec<u8> {
        canonical(serde_json::json!({
            "adapter_version":1,
            "arguments":["-c", "${workspace}/src/widget.c", "-o", "${target}/widget.o"],
            "assembly_input_model":null,
            "compiler":{
                "assembler":"cc",
                "family":"gnu",
                "target":"x86_64-unknown-linux-gnu",
                "version_text":"cc 15.2.0"
            },
            "environment":{},
            "inputs":[
                {"digest":input,"path":"${workspace}/src/widget.c"},
                {"digest":input,"path":"/usr/include/stdio.h"},
                {"digest":input,"path":"@include-manifest:${workspace}/src"}
            ],
            "kind":"cc",
            "version":version
        }))
    }

    fn build_script_action(binary_action: &Digest, version: u8) -> Vec<u8> {
        canonical(serde_json::json!({
            "binary_action":binary_action,
            "cargo_environment":{"TARGET":"x86_64-unknown-linux-gnu"},
            "environment":{},
            "inputs":{"${workspace}/build.rs":{"kind":"missing"}},
            "kind":"build-script",
            "out_dir":null,
            "version":version
        }))
    }

    fn compiler_metadata(kind: &str, stdout: &Digest, stderr: &Digest, version: u8) -> Vec<u8> {
        canonical(serde_json::json!({
            "kind":kind,
            "stderr":stderr,
            "stdout":stdout,
            "version":version
        }))
    }

    fn output_directory(artifact: &Digest) -> Vec<u8> {
        canonical(serde_json::json!({
            "directories":[],
            "files":[{
                "digest":artifact,
                "executable":false,
                "mode":420,
                "name":"libwidget.rlib"
            }],
            "symlinks":[],
            "version":1
        }))
    }

    fn action_manifest(task: &str, actions: &[&[u8]]) -> TaskActionManifest {
        TaskActionManifest {
            predictions: actions
                .iter()
                .enumerate()
                .map(|(index, action)| crate::model::TaskActionPrediction {
                    action: digest(action),
                    adapter: "rustc".into(),
                    invocation: digest(format!("invocation-{index}").as_bytes()),
                    payload: "{}".into(),
                })
                .collect(),
            task: task.into(),
            version: 1,
        }
    }

    async fn upload_blob(app: &Router, bytes: &[u8]) -> Digest {
        let digest = digest(bytes);
        let uri = format!(
            "/v1/blobs/{}/{}/{}",
            digest.algorithm, digest.hash, digest.size
        );
        assert_eq!(
            app.clone()
                .oneshot(request("PUT", uri, Body::from(bytes.to_vec())))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        digest
    }

    async fn scrape_metrics(app: &Router) -> (HeaderMap, String) {
        let response = app
            .clone()
            .oneshot(request("GET", "/metrics".into(), Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers().clone();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (headers, String::from_utf8(body.to_vec()).unwrap())
    }

    /// A zstd frame that decodes to almost nothing but is large on the wire.
    ///
    /// Skippable frames carry arbitrary bytes a decoder discards, so they are
    /// the cheapest way to ask a server to read a lot and produce nothing.
    fn skippable_frame(payload_bytes: usize) -> Vec<u8> {
        let mut frame = Vec::with_capacity(payload_bytes + 8);
        frame.extend_from_slice(&0x184D_2A50_u32.to_le_bytes());
        frame.extend_from_slice(&(payload_bytes as u32).to_le_bytes());
        frame.extend(std::iter::repeat_n(0_u8, payload_bytes));
        frame
    }

    async fn app_with_blob_limit(max_blob_bytes: u64) -> (Router, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let blobs = Arc::new(FilesystemStore::new(directory.path()).await.unwrap());
        let metadata = Arc::new(MemoryMetadata::default());
        let auth = Authorizer::new(None, None, true, None).await.unwrap();
        (
            router(AppState::new(blobs, metadata, auth, max_blob_bytes)),
            directory,
        )
    }

    #[tokio::test]
    async fn json_routes_do_not_decompress_their_bodies() {
        // Compression is scoped to blob uploads. These handlers buffer and
        // parse the whole body before they authenticate anyone, so an
        // unauthenticated caller must not be able to spend a few kilobytes to
        // fill that buffer.
        let (app, _directory) = app_with_blob_limit(1024 * 1024 * 1024).await;
        let payload = serde_json::json!({"digests": []}).to_string();

        // Control: uncompressed, the same body is understood.
        let response = app
            .clone()
            .oneshot(request(
                "POST",
                "/v1/blobs:missing".into(),
                Body::from(payload.clone()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Compressed, it is not: the route never decodes, so the extractor
        // sees zstd bytes and rejects them rather than expanding anything.
        let compressed = zstd::encode_all(payload.as_bytes(), 0).unwrap();
        let mut encoded = request("POST", "/v1/blobs:missing".into(), Body::from(compressed));
        encoded
            .headers_mut()
            .insert(header::CONTENT_ENCODING, "zstd".parse().unwrap());
        let response = app.oneshot(encoded).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn json_routes_keep_axums_default_body_limit() {
        // A generous blob allowance must not become a generous JSON allowance.
        // Axum's 2 MB default is what bounds these; pinned here so raising the
        // blob limit, or a change in that default, cannot loosen them quietly.
        let (app, _directory) = app_with_blob_limit(1024 * 1024 * 1024).await;
        let oversized = vec![b'a'; 4 * 1024 * 1024];

        let response = app
            .oneshot(request(
                "POST",
                "/v1/blobs:missing".into(),
                Body::from(oversized),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn blob_uploads_bound_what_crosses_the_wire() {
        // Decoding leaves the encoded stream unbounded on its own: a skippable
        // frame is arbitrarily large and decodes to nothing, so a limit on
        // decoded bytes never sees it and the connection reads until EOF.
        let digest = digest(b"unused");
        let uri = format!(
            "/v1/blobs/{}/{}/{}",
            digest.algorithm, digest.hash, digest.size
        );

        // Declared length: refused outright, without reading the body.
        let (app, _directory) = app_with_blob_limit(64 * 1024).await;
        let frame = skippable_frame(256 * 1024);
        let length = frame.len();
        let mut declared = request("PUT", uri.clone(), Body::from(frame.clone()));
        declared
            .headers_mut()
            .insert(header::CONTENT_ENCODING, "zstd".parse().unwrap());
        declared
            .headers_mut()
            .insert(header::CONTENT_LENGTH, length.to_string().parse().unwrap());
        let response = app.oneshot(declared).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        // Chunked, which is how compressed uploads are actually sent, since
        // the encoded length is unknown up front. The limit still cuts the
        // stream; the decoder just reports the truncation it runs into before
        // the limit's own error surfaces, so this is a 400 rather than a 413.
        // Either way the server stops reading at the limit, which is the part
        // that matters.
        let (app, _directory) = app_with_blob_limit(64 * 1024).await;
        let mut chunked = request("PUT", uri, Body::from(frame));
        chunked
            .headers_mut()
            .insert(header::CONTENT_ENCODING, "zstd".parse().unwrap());
        let response = app.oneshot(chunked).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn round_trips_zstd_compressed_transfers() {
        let (app, _directory) = test_app().await;
        // Long enough to clear the compression layer's minimum-size predicate,
        // repetitive enough that compression visibly shrinks it.
        let bytes = b"cached output ".repeat(64);
        let digest = digest(&bytes);
        let uri = format!(
            "/v1/blobs/{}/{}/{}",
            digest.algorithm, digest.hash, digest.size
        );

        // Upload compressed: the digest describes the *decompressed* content,
        // which is what the handler must see after the decompression layer.
        let compressed = zstd::encode_all(bytes.as_slice(), 0).unwrap();
        assert!(compressed.len() < bytes.len());
        let mut upload = request("PUT", uri.clone(), Body::from(compressed));
        upload
            .headers_mut()
            .insert(header::CONTENT_ENCODING, "zstd".parse().unwrap());
        let response = app.clone().oneshot(upload).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Download compressed: the body on the wire is zstd, and decoding it
        // returns the exact stored bytes.
        let mut download = request("GET", uri.clone(), Body::empty());
        download
            .headers_mut()
            .insert(header::ACCEPT_ENCODING, "zstd".parse().unwrap());
        let response = app.clone().oneshot(download).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_ENCODING)
                .and_then(|value| value.to_str().ok()),
            Some("zstd")
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(body.len() < bytes.len());
        assert_eq!(zstd::decode_all(&body[..]).unwrap(), bytes);

        // A client that never asks for compression still gets identity bytes,
        // which is what keeps already-released clients working unchanged.
        let response = app
            .oneshot(request("GET", uri, Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn streams_and_validates_blobs() {
        let (app, _directory) = test_app().await;
        let bytes = b"cached output";
        let digest = digest(bytes);
        let uri = format!(
            "/v1/blobs/{}/{}/{}",
            digest.algorithm, digest.hash, digest.size
        );
        let response = app
            .clone()
            .oneshot(request("PUT", uri.clone(), Body::from(bytes.as_slice())))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(request("GET", uri, Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn streams_visible_blobs_in_a_deduplicated_pack() {
        let (app, _directory) = test_app().await;
        let first_bytes = b"first cached output";
        let second_bytes = b"second cached output";
        let first = upload_blob(&app, first_bytes).await;
        let second = upload_blob(&app, second_bytes).await;
        let missing = digest(b"missing cached output");
        let body = serde_json::to_vec(&serde_json::json!({
            "digests":[second, missing, first, second]
        }))
        .unwrap();
        let response = app
            .oneshot(request("POST", "/v1/blobs:pack".into(), Body::from(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            BLOB_PACK_MEDIA_TYPE
        );
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            &header::HeaderValue::from_str(
                &(BLOB_PACK_MAGIC.len()
                    + (BLOB_PACK_HEADER_BYTES * 2)
                    + first_bytes.len()
                    + second_bytes.len())
                .to_string()
            )
            .unwrap()
        );
        assert_eq!(
            response.headers().get(PACK_BLOBS_HEADER).unwrap(),
            &header::HeaderValue::from_static("2")
        );
        assert_eq!(
            response.headers().get(PACK_BYTES_HEADER).unwrap(),
            &header::HeaderValue::from_str(&(first_bytes.len() + second_bytes.len()).to_string())
                .unwrap()
        );
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            decode_blob_pack(&bytes),
            vec![
                (second, second_bytes.to_vec()),
                (first, first_bytes.to_vec())
            ]
        );
    }

    #[tokio::test]
    async fn rejects_blob_pack_when_storage_object_is_short() {
        let (app, directory) = test_app().await;
        let stored = upload_blob(&app, b"cached output").await;
        tokio::fs::write(blob_path(directory.path(), &stored), b"short")
            .await
            .unwrap();
        let body = serde_json::to_vec(&serde_json::json!({"digests":[stored]})).unwrap();

        let response = app
            .oneshot(request("POST", "/v1/blobs:pack".into(), Body::from(body)))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn rejects_blob_pack_when_storage_object_is_oversized() {
        let (app, directory) = test_app().await;
        let stored = upload_blob(&app, b"cached output").await;
        tokio::fs::write(
            blob_path(directory.path(), &stored),
            b"cached output with extra bytes",
        )
        .await
        .unwrap();
        let body = serde_json::to_vec(&serde_json::json!({"digests":[stored]})).unwrap();

        let response = app
            .oneshot(request("POST", "/v1/blobs:pack".into(), Body::from(body)))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn exports_completed_blob_pack_metrics() {
        let (app, _directory) = test_app().await;
        let stored = upload_blob(&app, b"cached output").await;
        let missing = digest(b"missing output");
        let requested_bytes = stored.size + missing.size;
        let body = serde_json::to_vec(&serde_json::json!({
            "digests":[stored.clone(), missing]
        }))
        .unwrap();
        let response = app
            .clone()
            .oneshot(request("POST", "/v1/blobs:pack".into(), Body::from(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();

        let (headers, metrics) = scrape_metrics(&app).await;
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap(),
            "application/openmetrics-text; version=1.0.0; charset=utf-8"
        );
        assert!(metrics.contains("mbx_cache_build_info{"));
        assert!(metrics.contains("mbx_cache_blob_hits_total 1"));
        assert!(metrics.contains("mbx_cache_blob_uploads_total 1"));
        assert!(metrics.contains("mbx_cache_pack_requests_total{outcome=\"completed\"} 1"));
        assert!(metrics.contains("mbx_cache_pack_in_flight 0"));
        assert!(metrics.contains("mbx_cache_pack_blobs_total{kind=\"requested\"} 2"));
        assert!(metrics.contains("mbx_cache_pack_blobs_total{kind=\"served\"} 1"));
        assert!(metrics.contains("mbx_cache_pack_blobs_total{kind=\"missing\"} 1"));
        assert!(metrics.contains(&format!(
            "mbx_cache_pack_bytes_total{{kind=\"requested\"}} {requested_bytes}"
        )));
        assert!(metrics.contains(&format!(
            "mbx_cache_pack_bytes_total{{kind=\"served\"}} {}",
            stored.size
        )));
        assert!(metrics.contains("mbx_cache_pack_duration_seconds_count{outcome=\"completed\"} 1"));
        assert!(metrics.contains("mbx_cache_pack_time_to_first_byte_seconds_count 1"));
        assert!(metrics.contains(
            "mbx_cache_pack_metadata_query_duration_seconds_count{outcome=\"success\"} 1"
        ));
        assert!(metrics.contains("mbx_cache_pack_storage_gets_total{outcome=\"hit\"} 1"));
        assert!(
            metrics
                .contains("mbx_cache_pack_storage_get_duration_seconds_count{outcome=\"hit\"} 1")
        );
    }

    #[tokio::test]
    async fn records_cancelled_blob_pack_streams() {
        let (app, _directory) = test_app().await;
        let stored = upload_blob(&app, b"cached output").await;
        let body = serde_json::to_vec(&serde_json::json!({"digests":[stored]})).unwrap();
        let response = app
            .clone()
            .oneshot(request("POST", "/v1/blobs:pack".into(), Body::from(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);

        let (_, metrics) = scrape_metrics(&app).await;
        assert!(metrics.contains("mbx_cache_pack_requests_total{outcome=\"cancelled\"} 1"));
        assert!(metrics.contains("mbx_cache_pack_duration_seconds_count{outcome=\"cancelled\"} 1"));
        assert!(metrics.contains("mbx_cache_pack_in_flight 0"));
    }

    #[tokio::test]
    async fn blob_packs_do_not_disclose_other_namespaces() {
        let (app, _directory) = test_app().await;
        let stored = upload_blob(&app, b"private cached output").await;
        let body = serde_json::to_vec(&serde_json::json!({"digests":[stored]})).unwrap();
        let mut request = request("POST", "/v1/blobs:pack".into(), Body::from(body));
        request.headers_mut().insert(
            "mbx-cache-namespace",
            header::HeaderValue::from_static("other/project"),
        );
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            &header::HeaderValue::from_str(&BLOB_PACK_MAGIC.len().to_string()).unwrap()
        );
        assert_eq!(
            response.headers().get(PACK_BLOBS_HEADER).unwrap(),
            &header::HeaderValue::from_static("0")
        );
        assert_eq!(
            response.headers().get(PACK_BYTES_HEADER).unwrap(),
            &header::HeaderValue::from_static("0")
        );
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), BLOB_PACK_MAGIC);
    }

    #[tokio::test]
    async fn rejects_blob_packs_over_the_configured_limit() {
        let (app, _directory) = test_app().await;
        let oversized = Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: "0".repeat(64),
            size: 1024 * 1024 + 1,
        };
        let body = serde_json::to_vec(&serde_json::json!({"digests":[oversized]})).unwrap();
        let response = app
            .oneshot(request("POST", "/v1/blobs:pack".into(), Body::from(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn publishes_action_result_only_after_references_exist() {
        let (app, _directory) = test_app().await;
        let action = upload_blob(&app, &task_action(1)).await;
        let metadata = upload_blob(&app, &task_metadata("task", 1)).await;

        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let result = ActionResult {
            action,
            metadata: Some(metadata),
            output_root: None,
            version: 1,
        };
        let body = serde_json::to_vec(&result).unwrap();
        assert_eq!(
            app.clone()
                .oneshot(request("PUT", result_uri.clone(), Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        let response = app
            .oneshot(request("GET", result_uri, Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["version"], 1);
        assert!(body.get("result").is_none());
        assert!(body.get("signatures").is_none());
    }

    #[tokio::test]
    async fn advertises_action_schemas() {
        let (app, _directory) = test_app().await;
        let response = app
            .oneshot(request("GET", "/v1/capabilities".into(), Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["action_kinds"]["task"]["action_schema"], 1);
        assert_eq!(body["action_kinds"]["task"]["metadata_schema"], 1);
        assert_eq!(body["action_kinds"]["build-script"]["action_schema"], 2);
        assert_eq!(body["action_kinds"]["build-script"]["metadata_schema"], 1);
        assert_eq!(body["action_kinds"]["cc"]["action_schema"], 1);
        assert_eq!(body["action_kinds"]["cc"]["metadata_schema"], 1);
        assert_eq!(body["action_kinds"]["rustc"]["action_schema"], 1);
        assert_eq!(body["action_kinds"]["rustc"]["metadata_schema"], 1);
        assert_eq!(body["features"]["blob_packs"], true);
        assert_eq!(body["features"]["blob_pack_uploads"], true);
        assert_eq!(body["features"]["action_batch"], true);
        assert_eq!(body["limits"]["max_pack_bytes"], 1024 * 1024);
        // Not implemented here, and so not claimed.
        assert_eq!(body["features"]["resumable_uploads"], false);
        assert_eq!(body["features"]["delegated_transfers"], false);
        assert!(body["features"].get("signed_results").is_none());
    }

    /// Publish one committed action result, returning its action digest.
    async fn publish_rustc_action(app: &Router, version: u8) -> Digest {
        let action = upload_blob(app, &task_action(version)).await;
        let metadata = upload_blob(app, &task_metadata("task", version)).await;
        let uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let result = ActionResult {
            action: action.clone(),
            metadata: Some(metadata),
            output_root: None,
            version: 1,
        };
        assert_eq!(
            app.clone()
                .oneshot(request(
                    "PUT",
                    uri,
                    Body::from(serde_json::to_vec(&result).unwrap())
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        action
    }

    async fn publish_compiler_result(app: &Router, action: &[u8], kind: &str) -> StatusCode {
        let action = upload_blob(app, action).await;
        let stdout = upload_blob(app, b"").await;
        let stderr = upload_blob(app, b"diagnostic\n").await;
        let metadata = upload_blob(app, &compiler_metadata(kind, &stdout, &stderr, 1)).await;
        let artifact = upload_blob(app, b"compiled artifact").await;
        let output_root = upload_blob(app, &output_directory(&artifact)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: Some(metadata),
            output_root: Some(output_root),
            version: 1,
        })
        .unwrap();
        app.clone()
            .oneshot(request("PUT", result_uri, Body::from(body)))
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn publishes_cc_results() {
        let (app, _directory) = test_app().await;
        let input = digest(b"int widget(void) { return 42; }\n");
        assert_eq!(
            publish_compiler_result(&app, &cc_action(&input, 1), "cc").await,
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn publishes_build_script_results() {
        let (app, _directory) = test_app().await;
        let binary_action = digest(b"build script binary action");
        assert_eq!(
            publish_compiler_result(
                &app,
                &build_script_action(&binary_action, 2),
                "build-script",
            )
            .await,
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn accepts_action_manifests_larger_than_axums_default_limit() {
        let (app, _directory) = test_app().await;
        let manifest = TaskActionManifest {
            predictions: (0..10_000)
                .map(|index| crate::model::TaskActionPrediction {
                    action: digest(format!("action-{index}").as_bytes()),
                    adapter: "rustc".into(),
                    invocation: digest(format!("invocation-{index}").as_bytes()),
                    payload: "{}".into(),
                })
                .collect(),
            task: "b".repeat(64),
            version: 1,
        };
        let key = manifest.selector_digest();
        let body = serde_json::to_vec(&manifest).unwrap();
        assert!(body.len() > 2 * 1024 * 1024);
        assert!(body.len() < MAX_ACTION_MANIFEST_BYTES);
        let uri = format!(
            "/v1/action-manifests/{}/{}/{}",
            key.algorithm, key.hash, key.size
        );

        let response = app
            .oneshot(request("PUT", uri, Body::from(body)))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    /// A manifest is read back byte-for-byte as the canonical JSON its digest
    /// is taken over.
    ///
    /// `ActionPrediction` does not declare its fields in the order the
    /// canonicalization scheme sorts them, so serving serde's output made every
    /// manifest carrying a prediction unreadable to a client -- which is the
    /// entry point for warming a fresh checkout.
    #[tokio::test]
    async fn serves_action_manifests_as_canonical_json() {
        let (app, _directory) = test_app().await;
        let task = "b".repeat(64);
        let manifest = action_manifest(&task, &[b"first action", b"second action"]);
        let key = manifest.selector_digest();
        let uri = format!(
            "/v1/action-manifests/{}/{}/{}",
            key.algorithm, key.hash, key.size
        );
        let expected = canonical_json(&manifest).unwrap();
        assert_ne!(
            expected,
            serde_json::to_vec(&manifest).unwrap(),
            "this manifest must actually distinguish the two encodings"
        );
        let put = app
            .clone()
            .oneshot(request(
                "PUT",
                uri.clone(),
                Body::from(serde_json::to_vec(&manifest).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::CREATED);
        let put_etag = put
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);

        let response = app
            .oneshot(request("GET", uri, Body::empty()))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(TASK_ACTION_MANIFEST_MEDIA_TYPE)
        );
        // The tag a client echoes into `If-Match` names the bytes it just read.
        assert_eq!(
            response
                .headers()
                .get(header::ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            put_etag
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), expected);
    }

    /// The framing a client writes, mirroring `decode_blob_pack`.
    fn encode_blob_pack(entries: &[(&Digest, &[u8])]) -> Vec<u8> {
        let mut pack = BLOB_PACK_MAGIC.to_vec();
        for (digest, payload) in entries {
            pack.push(match digest.algorithm_kind().unwrap() {
                Algorithm::Blake3 => 1,
                Algorithm::Sha256 => 2,
            });
            pack.extend(hex::decode(&digest.hash).unwrap());
            pack.extend(digest.size.to_be_bytes());
            pack.extend_from_slice(payload);
        }
        pack
    }

    fn pack_request(pack: Vec<u8>, blobs: u64, bytes: u64) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .method("POST")
            .uri("/v1/blobs:pack-upload")
            .header("mbx-cache-protocol", "1")
            .header("mbx-cache-namespace", "test/project")
            .header(header::CONTENT_TYPE, BLOB_PACK_MEDIA_TYPE)
            .header(PACK_BLOBS_HEADER, blobs)
            .header(PACK_BYTES_HEADER, bytes)
            .body(Body::from(pack))
            .unwrap()
    }

    #[tokio::test]
    async fn stores_every_blob_in_an_uploaded_pack() {
        let (app, _directory) = test_app().await;
        let first_bytes = b"first packed upload";
        let second_bytes = b"second packed upload";
        let first = digest(first_bytes);
        let second = digest(second_bytes);
        let pack = encode_blob_pack(&[
            (&first, first_bytes.as_slice()),
            (&second, second_bytes.as_slice()),
        ]);

        let response = app
            .clone()
            .oneshot(pack_request(pack, 2, first.size + second.size))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["created"], 2);
        assert_eq!(body["existing"], 0);
        // Every blob is readable afterwards, exactly as individual uploads.
        for (digest, expected) in [
            (&first, first_bytes.as_slice()),
            (&second, second_bytes.as_slice()),
        ] {
            let uri = format!(
                "/v1/blobs/{}/{}/{}",
                digest.algorithm, digest.hash, digest.size
            );
            let response = app
                .clone()
                .oneshot(request("GET", uri, Body::empty()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let served = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(served.as_ref(), expected);
        }
    }

    #[tokio::test]
    async fn counts_blobs_an_uploaded_pack_already_held() {
        let (app, _directory) = test_app().await;
        let bytes = b"already held";
        let digest = upload_blob(&app, bytes).await;
        let pack = encode_blob_pack(&[(&digest, bytes.as_slice())]);

        let response = app
            .oneshot(pack_request(pack, 1, digest.size))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["created"], 0);
        assert_eq!(body["existing"], 1);
    }

    #[tokio::test]
    async fn refuses_a_packed_blob_that_does_not_match_its_digest() {
        let (app, _directory) = test_app().await;
        let bytes = b"claimed contents";
        let digest = digest(bytes);
        // The frame declares one digest and carries different bytes of the same
        // length, which a server must not take on trust.
        let pack = encode_blob_pack(&[(&digest, b"swapped contents".as_slice())]);

        let response = app
            .clone()
            .oneshot(pack_request(pack, 1, digest.size))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let uri = format!(
            "/v1/blobs/{}/{}/{}",
            digest.algorithm, digest.hash, digest.size
        );
        let response = app
            .oneshot(request("GET", uri, Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn refuses_a_pack_that_contradicts_its_headers() {
        let (app, _directory) = test_app().await;
        let bytes = b"one blob";
        let digest = digest(bytes);
        let pack = encode_blob_pack(&[(&digest, bytes.as_slice())]);

        let response = app
            .clone()
            .oneshot(pack_request(pack.clone(), 2, digest.size))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .oneshot(pack_request(pack, 1, digest.size + 1))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// Framing overhead does not consume the advertised payload allowance.
    #[tokio::test]
    async fn accepts_an_uploaded_pack_at_the_configured_payload_limit() {
        let bytes = b"exactly-16-bytes";
        assert_eq!(bytes.len(), 16);
        let (app, _directory) = app_with_blob_limit(bytes.len() as u64).await;
        let digest = digest(bytes);
        let pack = encode_blob_pack(&[(&digest, bytes.as_slice())]);
        assert_eq!(
            pack.len(),
            BLOB_PACK_MAGIC.len() + BLOB_PACK_HEADER_BYTES + bytes.len()
        );

        let response = app
            .oneshot(pack_request(pack, 1, digest.size))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn refuses_an_uploaded_pack_over_the_configured_limit() {
        let (app, _directory) = app_with_blob_limit(16).await;
        let bytes = b"larger than the configured blob limit";
        let digest = digest(bytes);
        let pack = encode_blob_pack(&[(&digest, bytes.as_slice())]);

        let response = app
            .oneshot(pack_request(pack, 1, digest.size))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn answers_batched_action_lookups_for_what_it_holds() {
        let (app, _directory) = test_app().await;
        let held = publish_rustc_action(&app, 1).await;
        let absent = digest(b"absent action");
        let body = serde_json::json!({ "digests": [held, absent] });

        let response = app
            .oneshot(request(
                "POST",
                "/v1/action-results:batch".into(),
                Body::from(serde_json::to_vec(&body).unwrap()),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(ACTION_RESULT_BATCH_MEDIA_TYPE)
        );
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["action"]["hash"], held.hash.as_str());
    }

    #[tokio::test]
    async fn batched_action_lookups_do_not_disclose_other_namespaces() {
        let (app, _directory) = test_app().await;
        let held = publish_rustc_action(&app, 1).await;
        let body = serde_json::json!({ "digests": [held] });
        let mut lookup = request(
            "POST",
            "/v1/action-results:batch".into(),
            Body::from(serde_json::to_vec(&body).unwrap()),
        );
        lookup
            .headers_mut()
            .insert("mbx-cache-namespace", "other/project".parse().unwrap());

        let response = app.oneshot(lookup).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(body["results"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn refuses_batched_action_lookups_that_are_malformed() {
        let (app, _directory) = test_app().await;
        // A batch is a JSON route, so it keeps the strict body of the others.
        let unknown = serde_json::json!({ "digests": [], "extra": true });
        let response = app
            .clone()
            .oneshot(request(
                "POST",
                "/v1/action-results:batch".into(),
                Body::from(serde_json::to_vec(&unknown).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        // Action keys are blake3, as they are on the single-lookup route.
        let sha256 = serde_json::json!({
            "digests": [{"algorithm":"sha256","hash":"0".repeat(64),"size":1}],
        });
        let response = app
            .oneshot(request(
                "POST",
                "/v1/action-results:batch".into(),
                Body::from(serde_json::to_vec(&sha256).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn action_manifests_use_optimistic_concurrency() {
        let (app, _directory) = test_app().await;
        let task = "a".repeat(64);
        let first = action_manifest(&task, &[b"first action"]);
        let key = first.selector_digest();
        let uri = format!(
            "/v1/action-manifests/{}/{}/{}",
            key.algorithm, key.hash, key.size
        );
        let response = app
            .clone()
            .oneshot(request(
                "PUT",
                uri.clone(),
                Body::from(serde_json::to_vec(&first).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let first_etag = response.headers()[header::ETAG].clone();

        let response = app
            .clone()
            .oneshot(request("GET", uri.clone(), Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::ETAG], first_etag);

        let second = action_manifest(&task, &[b"first action", b"second action"]);
        let mut update = request(
            "PUT",
            uri.clone(),
            Body::from(serde_json::to_vec(&second).unwrap()),
        );
        update.headers_mut().remove(header::IF_NONE_MATCH);
        update
            .headers_mut()
            .insert(header::IF_MATCH, first_etag.clone());
        let response = app.clone().oneshot(update).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let mut stale = request(
            "PUT",
            uri.clone(),
            Body::from(serde_json::to_vec(&first).unwrap()),
        );
        stale.headers_mut().remove(header::IF_NONE_MATCH);
        stale.headers_mut().insert(header::IF_MATCH, first_etag);
        assert_eq!(
            app.clone().oneshot(stale).await.unwrap().status(),
            StatusCode::PRECONDITION_FAILED
        );

        let response = app
            .oneshot(request("GET", uri, Body::empty()))
            .await
            .unwrap();
        let stored: TaskActionManifest =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(stored, second);
    }

    #[tokio::test]
    async fn publishes_rustc_results_without_uploading_source_inputs() {
        let (app, _directory) = test_app().await;
        // Action input digests identify local source content; only result
        // metadata, diagnostics, and output artifacts are CAS references.
        let source = digest(b"pub fn widget() {}\n");
        let action = upload_blob(&app, &rustc_action(&source, 1)).await;
        let stdout = upload_blob(&app, b"").await;
        let stderr = upload_blob(&app, b"warning: cached diagnostic\n").await;
        let metadata = upload_blob(&app, &compiler_metadata("rustc", &stdout, &stderr, 1)).await;
        let artifact = upload_blob(&app, b"rlib artifact").await;
        let output_root = upload_blob(&app, &output_directory(&artifact)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: Some(metadata),
            output_root: Some(output_root),
            version: 1,
        })
        .unwrap();

        assert_eq!(
            app.oneshot(request("PUT", result_uri, Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn publishes_native_link_results_without_uploading_linker_inputs() {
        let (app, _directory) = test_app().await;
        // Linker identity digests key host CRT inputs; like source-input
        // digests, they are not remote CAS references.
        let source = digest(b"fn main() {}\n");
        let crt = digest(b"host crt object");
        let mut action: serde_json::Value =
            serde_json::from_slice(&rustc_action(&source, 1)).unwrap();
        action["linker"] = serde_json::json!({
            "crt_objects":{"crt1.o":crt},
            "deployment_target":null,
            "driver":"/usr/bin/cc",
            "driver_version":"cc 15.2.0",
            "linker_version":"GNU ld 2.45",
            "sdk":null
        });
        let action = upload_blob(&app, &canonical(action)).await;
        let stdout = upload_blob(&app, b"").await;
        let stderr = upload_blob(&app, b"linker diagnostic\n").await;
        let metadata = upload_blob(&app, &compiler_metadata("rustc", &stdout, &stderr, 1)).await;
        let executable = upload_blob(&app, b"linked executable").await;
        let output_root = upload_blob(&app, &output_directory(&executable)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: Some(metadata),
            output_root: Some(output_root),
            version: 1,
        })
        .unwrap();

        assert_eq!(
            app.oneshot(request("PUT", result_uri, Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn rejects_incomplete_rustc_results() {
        let (app, _directory) = test_app().await;
        let source = digest(b"pub fn widget() {}\n");
        let action = upload_blob(&app, &rustc_action(&source, 1)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: None,
            output_root: None,
            version: 1,
        })
        .unwrap();

        assert_eq!(
            app.oneshot(request("PUT", result_uri, Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[tokio::test]
    async fn rejects_missing_rustc_diagnostic_blobs() {
        let (app, _directory) = test_app().await;
        let source = digest(b"pub fn widget() {}\n");
        let action = upload_blob(&app, &rustc_action(&source, 1)).await;
        let stdout = upload_blob(&app, b"").await;
        let missing_stderr = digest(b"missing diagnostic\n");
        let metadata = upload_blob(
            &app,
            &compiler_metadata("rustc", &stdout, &missing_stderr, 1),
        )
        .await;
        let artifact = upload_blob(&app, b"rlib artifact").await;
        let output_root = upload_blob(&app, &output_directory(&artifact)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: Some(metadata),
            output_root: Some(output_root),
            version: 1,
        })
        .unwrap();

        assert_eq!(
            app.oneshot(request("PUT", result_uri, Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[tokio::test]
    async fn rejects_invalid_rustc_action_inputs() {
        let (app, _directory) = test_app().await;
        let source = digest(b"pub fn widget() {}\n");
        let mut action: serde_json::Value =
            serde_json::from_slice(&rustc_action(&source, 1)).unwrap();
        action["inputs"][0]["path"] = "../src/lib.rs".into();
        let action = upload_blob(&app, &canonical(action)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: None,
            output_root: None,
            version: 1,
        })
        .unwrap();

        let response = app
            .oneshot(request("PUT", result_uri, Body::from(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["error"], "invalid rustc action values");
    }

    #[tokio::test]
    async fn rejects_unknown_rustc_action_fields() {
        let (app, _directory) = test_app().await;
        let source = digest(b"pub fn widget() {}\n");
        let mut action: serde_json::Value =
            serde_json::from_slice(&rustc_action(&source, 1)).unwrap();
        action["unknown"] = true.into();
        let action = upload_blob(&app, &canonical(action)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: None,
            output_root: None,
            version: 1,
        })
        .unwrap();

        let response = app
            .oneshot(request("PUT", result_uri, Body::from(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .starts_with("invalid rustc action:")
        );
    }

    #[tokio::test]
    async fn rejects_unknown_rustc_metadata_fields() {
        let (app, _directory) = test_app().await;
        let source = digest(b"pub fn widget() {}\n");
        let action = upload_blob(&app, &rustc_action(&source, 1)).await;
        let stdout = upload_blob(&app, b"").await;
        let stderr = upload_blob(&app, b"warning\n").await;
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&compiler_metadata("rustc", &stdout, &stderr, 1)).unwrap();
        metadata["unknown"] = true.into();
        let metadata = upload_blob(&app, &canonical(metadata)).await;
        let artifact = upload_blob(&app, b"rlib artifact").await;
        let output_root = upload_blob(&app, &output_directory(&artifact)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: Some(metadata),
            output_root: Some(output_root),
            version: 1,
        })
        .unwrap();

        let response = app
            .oneshot(request("PUT", result_uri, Body::from(body)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .starts_with("invalid rustc metadata:")
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_action_schema() {
        let (app, _directory) = test_app().await;
        let action = upload_blob(&app, &task_action(2)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: None,
            output_root: None,
            version: 1,
        })
        .unwrap();
        assert_eq!(
            app.oneshot(request("PUT", result_uri, Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[tokio::test]
    async fn rejects_metadata_kind_mismatch() {
        let (app, _directory) = test_app().await;
        let action = upload_blob(&app, &task_action(1)).await;
        let metadata = upload_blob(&app, &task_metadata("rustc", 1)).await;
        let result_uri = format!(
            "/v1/action-results/{}/{}/{}",
            action.algorithm, action.hash, action.size
        );
        let body = serde_json::to_vec(&ActionResult {
            action,
            metadata: Some(metadata),
            output_root: None,
            version: 1,
        })
        .unwrap();
        assert_eq!(
            app.oneshot(request("PUT", result_uri, Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[tokio::test]
    async fn rejects_sha256_action_keys() {
        let (app, _directory) = test_app().await;
        let action = task_action(1);
        let hash = hex::encode(sha2::Sha256::digest(&action));
        let result_uri = format!("/v1/action-results/sha256/{hash}/{}", action.len());
        let body = serde_json::to_vec(&ActionResult {
            action: Digest {
                algorithm: Algorithm::Sha256.into(),
                hash,
                size: action.len() as u64,
            },
            metadata: None,
            output_root: None,
            version: 1,
        })
        .unwrap();
        assert_eq!(
            app.oneshot(request("PUT", result_uri, Body::from(body)))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
}
