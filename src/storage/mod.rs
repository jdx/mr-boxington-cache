mod azure;
mod filesystem;
mod s3;

use async_trait::async_trait;
use axum::body::Bytes;
use futures_util::stream::BoxStream;
use std::path::Path;

use crate::{
    config::{Config, StorageKind},
    model::Digest,
};

pub use azure::AzureStore;
pub use filesystem::FilesystemStore;
pub use s3::S3Store;

pub type BlobStream = BoxStream<'static, std::io::Result<Bytes>>;

pub struct Blob {
    pub size: u64,
    pub stream: BlobStream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    AlreadyExists,
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>>;
    async fn put(&self, digest: &Digest, source: &Path) -> anyhow::Result<PutOutcome>;
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
