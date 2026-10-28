mod azure;
mod filesystem;
mod s3;
mod upload;

use async_trait::async_trait;
use axum::body::Bytes;
use futures_util::stream::BoxStream;
use http::HeaderValue;

use crate::{
    config::{Config, StorageKind},
    model::Digest,
};

pub use azure::AzureStore;
pub use filesystem::FilesystemStore;
pub use s3::S3Store;
pub use upload::{MAX_UPLOAD_CHUNK_BYTES, UploadError, VerifiedUpload};

pub type BlobStream = BoxStream<'static, std::io::Result<Bytes>>;

pub struct Blob {
    pub size: u64,
    pub stream: BlobStream,
}

/// How a download should be answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownloadMode {
    /// Proxy the object body through this service. Internal readers and
    /// requests that need the existing range/conditional behavior use this.
    Stream,
    /// Return only object metadata, never a body.
    Head,
    /// A full, unconditional GET may be redirected to the object store.
    Redirect,
}

pub enum BlobDownload {
    Stream(Blob),
    Head { size: u64 },
    Redirect { location: HeaderValue },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    AlreadyExists,
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>>;

    async fn begin_upload(&self, digest: &Digest) -> anyhow::Result<VerifiedUpload>;

    /// Resolve a download without changing internal reads.
    ///
    /// The default is the historical streaming GET. Only a backend that can
    /// safely hand out a short-lived object URL overrides it.
    async fn download(
        &self,
        digest: &Digest,
        _mode: DownloadMode,
    ) -> anyhow::Result<Option<BlobDownload>> {
        Ok(self.get(digest).await?.map(BlobDownload::Stream))
    }
}

pub async fn from_config(config: &Config) -> anyhow::Result<std::sync::Arc<dyn BlobStore>> {
    Ok(match config.storage {
        StorageKind::Azure => std::sync::Arc::new(AzureStore::new(config).await?),
        StorageKind::Filesystem => {
            std::sync::Arc::new(FilesystemStore::new(&config.data_dir).await?)
        }
        StorageKind::S3 => std::sync::Arc::new(S3Store::new(config).await?),
    })
}
