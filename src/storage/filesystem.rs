use async_trait::async_trait;
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use tokio_util::io::ReaderStream;

use super::{
    Blob, BlobStore, PutOutcome, UploadError, VerifiedUpload,
    upload::{Spool, UploadSink},
};
use crate::model::Digest;

pub struct FilesystemStore {
    root: PathBuf,
}

impl FilesystemStore {
    pub async fn new(root: &Path) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(root.join("blobs")).await?;
        Ok(Self {
            root: root.to_owned(),
        })
    }

    /// The one mapping from a digest to its on-disk path.
    fn path(&self, digest: &Digest) -> anyhow::Result<PathBuf> {
        digest
            .validate()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let algorithm = digest
            .algorithm_kind()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(self
            .root
            .join("blobs")
            .join(algorithm.as_str())
            .join(&digest.hash[..2])
            .join(&digest.hash)
            .join(digest.size.to_string()))
    }

    async fn put_file(&self, digest: &Digest, source: &Path) -> anyhow::Result<PutOutcome> {
        let destination = self.path(digest)?;
        if tokio::fs::try_exists(&destination).await? {
            return Ok(PutOutcome::AlreadyExists);
        }
        tokio::fs::create_dir_all(destination.parent().expect("blob path has parent")).await?;
        let staging =
            tempfile::NamedTempFile::new_in(destination.parent().expect("blob path has parent"))?;
        tokio::fs::copy(source, staging.path()).await?;
        match staging.persist_noclobber(&destination) {
            Ok(_) => Ok(PutOutcome::Created),
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => Err(error.error.into()),
        }
    }
}

struct FilesystemUpload {
    root: PathBuf,
    digest: Digest,
    spool: Spool,
}

#[async_trait]
impl UploadSink for FilesystemUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), UploadError> {
        self.spool
            .write_all(chunk)
            .await
            .map_err(UploadError::Storage)
    }

    async fn commit(&mut self) -> Result<PutOutcome, UploadError> {
        self.spool.flush().await.map_err(UploadError::Storage)?;
        FilesystemStore {
            root: self.root.clone(),
        }
        .put_file(&self.digest, self.spool.path())
        .await
        .map_err(UploadError::Storage)
    }

    async fn abort(&mut self) {}
}

#[async_trait]
impl BlobStore for FilesystemStore {
    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>> {
        let file = match tokio::fs::File::open(self.path(digest)?).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let size = file.metadata().await?.len();
        Ok(Some(Blob {
            size,
            stream: ReaderStream::new(file).boxed(),
        }))
    }

    async fn begin_upload(&self, digest: &Digest) -> anyhow::Result<VerifiedUpload> {
        let spool = Spool::new().await?;
        VerifiedUpload::new(
            digest.clone(),
            Box::new(FilesystemUpload {
                root: self.root.clone(),
                digest: digest.clone(),
                spool,
            }),
        )
    }
}
