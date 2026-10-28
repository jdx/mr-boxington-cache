use async_trait::async_trait;
use aws_sdk_s3::{
    Client,
    error::{ProvideErrorMetadata, SdkError},
    primitives::ByteStream,
    types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart},
};
use base64::Engine as _;
use futures_util::StreamExt;
use http::{HeaderValue, Method};
use sha2::Digest as _;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::io::ReaderStream;

use super::{
    Blob, BlobDownload, BlobStore, DownloadMode, PutOutcome, UploadError, VerifiedUpload,
    upload::UploadSink,
};
use crate::{config::Config, model::Digest};

/// Fixed multipart geometry. The admission bound is per process, not a
/// distributed lock; conditional final writes are what arbitrate replicas.
const PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_PARTS: usize = 10_000;
const MAX_S3_OBJECT_BYTES: u64 = (PART_SIZE as u64) * (MAX_PARTS as u64);
/// Total per-process upload buffer budget. Each session reserves
/// `min(digest.size, PART_SIZE)` bytes, so empty uploads reserve nothing and
/// small uploads do not queue behind a fixed session count.
const UPLOAD_BUFFER_BYTES: usize = 64 * 1024 * 1024;

const COMMAND_QUEUE: usize = 2;
const PRESIGN_EXPIRY: Duration = Duration::from_secs(300);
const CLEANUP_BUDGET: Duration = Duration::from_secs(30);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(300);

const CACHE_CONTROL_VALUE: &str = "private, no-store";
const CONTENT_TYPE_VALUE: &str = "application/octet-stream";

pub struct S3Store {
    client: Client,
    bucket: String,
    prefix: String,
    redirect_downloads: bool,
    upload_slots: Arc<Semaphore>,
}

impl S3Store {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        validate_max_blob_bytes(config.max_blob_bytes)?;
        let bucket = config
            .s3_bucket
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--s3-bucket is required with S3 storage"))?;
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.s3_region.clone()));
        if let Some(endpoint) = &config.s3_endpoint {
            loader = loader.endpoint_url(endpoint);
        }
        let sdk_config = loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(config.s3_path_style)
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(OPERATION_TIMEOUT)
                    .build(),
            )
            .build();
        Ok(Self::from_client(
            Client::from_conf(s3_config),
            bucket,
            &config.s3_prefix,
            config.s3_redirect_downloads,
            UPLOAD_BUFFER_BYTES,
        ))
    }

    fn from_client(
        client: Client,
        bucket: impl Into<String>,
        prefix: &str,
        redirect_downloads: bool,
        upload_limit: usize,
    ) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: prefix.trim_matches('/').to_owned(),
            redirect_downloads,
            upload_slots: Arc::new(Semaphore::new(upload_limit)),
        }
    }

    fn key(&self, digest: &Digest) -> String {
        format!("{}/blobs/{}", self.prefix, digest.key())
    }

    async fn head_size(&self, digest: &Digest) -> anyhow::Result<Option<u64>> {
        let output = match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.key(digest))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error)
                if error.as_service_error().is_some_and(|e| e.is_not_found())
                    || error
                        .raw_response()
                        .is_some_and(|response| response.status().as_u16() == 404) =>
            {
                return Ok(None);
            }
            // A missing object can be reported as 403 when the signer cannot
            // list the bucket. Never turn a permission failure into absence.
            Err(error) => return Err(error.into()),
        };
        let size = output
            .content_length()
            .and_then(|size| u64::try_from(size).ok())
            .ok_or_else(|| {
                anyhow::anyhow!("S3 object response is missing a valid content length")
            })?;
        if size != digest.size {
            anyhow::bail!("S3 object size does not match the requested digest");
        }
        Ok(Some(size))
    }

    async fn presigned_get(&self, digest: &Digest) -> anyhow::Result<HeaderValue> {
        // This is the secret boundary. Never let the SDK's signed request or
        // its nested error context reach ApiError::internal logging.
        let config = aws_sdk_s3::presigning::PresigningConfig::expires_in(PRESIGN_EXPIRY)
            .map_err(|_| anyhow::anyhow!("S3 presigning configuration failed"))?;
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(digest))
            .response_content_type(CONTENT_TYPE_VALUE)
            .response_cache_control(CACHE_CONTROL_VALUE)
            .presigned(config)
            .await
            .map_err(|_| anyhow::anyhow!("S3 GET presigning failed"))?;

        if request.method() != Method::GET.as_str() {
            anyhow::bail!("S3 presigned request is not a GET");
        }
        // Presigning is expected to sign only the automatically supplied Host.
        // A cache-server Authorization or namespace header here would make the
        // redirect unusable, so fail instead of handing out a broken URL.
        for (name, _) in request.headers() {
            if !name.eq_ignore_ascii_case("host") {
                anyhow::bail!("S3 presigned request requires an unexpected header");
            }
        }

        let uri = request.uri();
        let mut location = HeaderValue::from_str(uri).map_err(|_| {
            anyhow::anyhow!("S3 presigned request cannot be used as a redirect location")
        })?;
        location.set_sensitive(true);
        Ok(location)
    }
}

fn validate_max_blob_bytes(max_blob_bytes: u64) -> anyhow::Result<()> {
    if max_blob_bytes > MAX_S3_OBJECT_BYTES {
        anyhow::bail!(
            "S3 uploads support at most {MAX_S3_OBJECT_BYTES} bytes (fixed {PART_SIZE}-byte parts, {MAX_PARTS} parts), \
             but --max-blob-bytes is {max_blob_bytes}"
        );
    }
    Ok(())
}

#[async_trait]
impl BlobStore for S3Store {
    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(digest))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error)
                if error.as_service_error().is_some_and(|e| e.is_no_such_key())
                    || error
                        .raw_response()
                        .is_some_and(|response| response.status().as_u16() == 404) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let size = output
            .content_length()
            .and_then(|size| u64::try_from(size).ok())
            .ok_or_else(|| anyhow::anyhow!("S3 object response is missing content length"))?;
        let stream = ReaderStream::new(output.body.into_async_read()).boxed();
        Ok(Some(Blob { size, stream }))
    }

    async fn begin_upload(&self, digest: &Digest) -> anyhow::Result<VerifiedUpload> {
        // Empty uploads reserve nothing; larger ones reserve only the part
        // bytes they can actually buffer. Wait for admission before allocating
        // that buffer or reading another request byte.
        let weight = usize::try_from(digest.size.min(PART_SIZE as u64)).unwrap_or(PART_SIZE);
        let permit = Arc::clone(&self.upload_slots)
            .acquire_many_owned(u32::try_from(weight).unwrap_or(u32::MAX))
            .await
            .map_err(|_| anyhow::anyhow!("S3 upload admission is closed"))?;
        let (commands, receiver) = mpsc::channel(COMMAND_QUEUE);
        let worker = UploadWorker::new(
            self.client.clone(),
            self.bucket.clone(),
            self.key(digest),
            permit,
            weight,
        );
        tokio::spawn(worker.run(receiver));
        VerifiedUpload::new(digest.clone(), Box::new(S3Upload { commands }))
    }

    async fn download(
        &self,
        digest: &Digest,
        mode: DownloadMode,
    ) -> anyhow::Result<Option<BlobDownload>> {
        if mode == DownloadMode::Stream || !self.redirect_downloads {
            return Ok(self.get(digest).await?.map(BlobDownload::Stream));
        }
        let Some(size) = self.head_size(digest).await? else {
            return Ok(None);
        };
        match mode {
            DownloadMode::Head => Ok(Some(BlobDownload::Head { size })),
            DownloadMode::Redirect => {
                let location = self.presigned_get(digest).await?;
                Ok(Some(BlobDownload::Redirect { location }))
            }
            DownloadMode::Stream => unreachable!("stream was handled above"),
        }
    }
}

struct S3Upload {
    commands: mpsc::Sender<UploadCommand>,
}

#[async_trait]
impl UploadSink for S3Upload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), UploadError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(UploadCommand::Write {
                data: chunk.to_vec(),
                reply,
            })
            .await
            .map_err(|_| UploadError::Cancelled)?;
        answer.await.map_err(|_| UploadError::Cancelled)?
    }

    async fn commit(&mut self) -> Result<PutOutcome, UploadError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(UploadCommand::Commit { reply })
            .await
            .map_err(|_| UploadError::Cancelled)?;
        answer.await.map_err(|_| UploadError::Cancelled)?
    }

    async fn abort(&mut self) {
        let (reply, answer) = oneshot::channel();
        if self
            .commands
            .send(UploadCommand::Abort { reply })
            .await
            .is_ok()
        {
            let _ = answer.await;
        }
    }
}

enum UploadCommand {
    Write {
        data: Vec<u8>,
        reply: oneshot::Sender<Result<(), UploadError>>,
    },
    Commit {
        reply: oneshot::Sender<Result<PutOutcome, UploadError>>,
    },
    Abort {
        reply: oneshot::Sender<()>,
    },
}

struct UploadWorker {
    client: Client,
    bucket: String,
    key: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
    part: Vec<u8>,
    upload_id: Option<String>,
    parts: Vec<CompletedPart>,
    next_part: i32,
}

impl UploadWorker {
    fn new(
        client: Client,
        bucket: String,
        key: String,
        permit: tokio::sync::OwnedSemaphorePermit,
        capacity: usize,
    ) -> Self {
        Self {
            client,
            bucket,
            key,
            _permit: permit,
            part: Vec::with_capacity(capacity),
            upload_id: None,
            parts: Vec::new(),
            next_part: 1,
        }
    }

    async fn run(mut self, mut commands: mpsc::Receiver<UploadCommand>) {
        while let Some(command) = commands.recv().await {
            match command {
                UploadCommand::Write { data, reply } => {
                    let result = self.write(&data).await;
                    let failed = result.is_err();
                    if reply.send(result).is_err() {
                        // The caller dropped just its write future but may
                        // still hold the session. No one can acknowledge this
                        // part, so abort any known upload now rather than
                        // waiting for that retained sender to be dropped.
                        self.cleanup().await;
                        return;
                    }
                    if failed {
                        self.cleanup().await;
                        return;
                    }
                }
                UploadCommand::Commit { reply } => {
                    let result = self.commit().await;
                    let _ = reply.send(result);
                    return;
                }
                UploadCommand::Abort { reply } => {
                    self.cleanup().await;
                    let _ = reply.send(());
                    return;
                }
            }
        }
        // The caller dropped its session without an explicit abort. A known
        // multipart upload still belongs to this worker, so clean it here.
        self.cleanup().await;
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), UploadError> {
        let mut rest = data;
        while !rest.is_empty() {
            let space = PART_SIZE - self.part.len();
            let take = space.min(rest.len());
            self.part.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.part.len() == PART_SIZE {
                let part = std::mem::take(&mut self.part);
                // No next-part allocation happens until this request has
                // settled, so a buffer replacement cannot transiently double
                // the per-session part memory.
                self.upload_part(part).await?;
                self.part = Vec::with_capacity(PART_SIZE);
            }
        }
        Ok(())
    }

    async fn upload_part(&mut self, data: Vec<u8>) -> Result<(), UploadError> {
        if self.upload_id.is_none() {
            self.create_upload().await?;
        }
        if self.next_part > MAX_PARTS as i32 {
            return Err(UploadError::Storage(anyhow::anyhow!(
                "S3 multipart upload exceeded {MAX_PARTS} parts"
            )));
        }
        let part_number = self.next_part;
        self.next_part += 1;
        let length = i64::try_from(data.len())
            .map_err(|_| UploadError::Storage(anyhow::anyhow!("S3 part is too large")))?;
        let checksum =
            base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(&data));
        let upload_id = self
            .upload_id
            .as_deref()
            .ok_or_else(|| UploadError::Storage(anyhow::anyhow!("S3 upload ID is missing")))?;
        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(length)
            .checksum_sha256(checksum.clone())
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(storage_error)?;
        let etag = output
            .e_tag()
            .filter(|etag| !etag.is_empty())
            .ok_or_else(|| {
                UploadError::Storage(anyhow::anyhow!(
                    "S3 upload part response is missing an ETag"
                ))
            })?;
        if output
            .checksum_sha256()
            .is_some_and(|returned| returned != checksum)
        {
            return Err(UploadError::Storage(anyhow::anyhow!(
                "S3 upload part checksum does not match the submitted checksum"
            )));
        }
        self.parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(etag)
                .checksum_sha256(checksum)
                .build(),
        );
        Ok(())
    }

    async fn create_upload(&mut self) -> Result<(), UploadError> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .map_err(storage_error)?;
        let upload_id = output
            .upload_id()
            .filter(|upload_id| !upload_id.is_empty())
            .ok_or_else(|| {
                UploadError::Storage(anyhow::anyhow!(
                    "S3 create multipart upload response is missing an upload ID"
                ))
            })?;
        // Take ownership immediately. If the caller disappears before this
        // point returns, the worker still knows what cleanup must abort.
        self.upload_id = Some(upload_id.to_owned());
        Ok(())
    }

    async fn commit(&mut self) -> Result<PutOutcome, UploadError> {
        let Some(upload_id) = self.upload_id.clone() else {
            let data = std::mem::take(&mut self.part);
            return match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .if_none_match("*")
                .body(ByteStream::from(data))
                .send()
                .await
            {
                Ok(_) => Ok(PutOutcome::Created),
                Err(error) if is_precondition(&error) => Ok(PutOutcome::AlreadyExists),
                Err(error) => Err(storage_error(error)),
            };
        };

        if !self.part.is_empty() {
            let tail = std::mem::take(&mut self.part);
            if let Err(error) = self.upload_part(tail).await {
                self.cleanup().await;
                return Err(error);
            }
        }
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(std::mem::take(&mut self.parts)))
            .build();
        let result = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .if_none_match("*")
            .send()
            .await;
        match result {
            Ok(output) => {
                // A real completion carries the final ETag. Requiring a
                // nonempty one prevents a malformed or empty success body from
                // being mistaken for a commit.
                if output.e_tag().filter(|etag| !etag.is_empty()).is_none() {
                    self.cleanup().await;
                    return Err(UploadError::Storage(anyhow::anyhow!(
                        "S3 complete multipart upload response is missing an ETag"
                    )));
                }
                self.upload_id = None;
                Ok(PutOutcome::Created)
            }
            Err(error) if is_precondition(&error) => {
                self.cleanup().await;
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => {
                self.cleanup().await;
                Err(storage_error(error))
            }
        }
    }

    async fn cleanup(&mut self) {
        let Some(upload_id) = self.upload_id.take() else {
            return;
        };
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let cleanup = async move {
            match client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await
            {
                Ok(_) => {}
                Err(error) if is_no_such_upload(&error) => {}
                Err(error) => {
                    let code = error.as_service_error().and_then(|e| e.code());
                    let status = error.raw_response().map(|r| r.status().as_u16());
                    tracing::warn!(
                        ?code,
                        ?status,
                        "could not abort an S3 multipart upload; the bucket lifecycle rule \
                         AbortIncompleteMultipartUpload is the remaining cleanup"
                    );
                }
            }
        };
        if tokio::time::timeout(CLEANUP_BUDGET, cleanup).await.is_err() {
            tracing::warn!(
                "timed out aborting an S3 multipart upload after {} seconds; \
                 the bucket lifecycle rule is the remaining cleanup",
                CLEANUP_BUDGET.as_secs()
            );
        }
    }
}

fn storage_error<E>(error: SdkError<E>) -> UploadError
where
    E: std::error::Error + Send + Sync + 'static,
{
    UploadError::Storage(anyhow::anyhow!(error))
}

fn is_precondition<E>(error: &SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "PreconditionFailed")
        || error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 412)
}

fn is_no_such_upload<E>(error: &SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "NoSuchUpload")
        || error
            .raw_response()
            .is_some_and(|response| response.status().as_u16() == 404)
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
