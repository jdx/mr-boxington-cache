use async_trait::async_trait;
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use tokio_util::io::ReaderStream;

use super::{Blob, BlobStore, PutOutcome};
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

    fn path(&self, digest: &Digest) -> PathBuf {
        self.root
            .join("blobs")
            .join(&digest.algorithm)
            .join(&digest.hash[..2])
            .join(&digest.hash)
            .join(digest.size.to_string())
    }
}

#[async_trait]
impl BlobStore for FilesystemStore {
    async fn size(&self, digest: &Digest) -> anyhow::Result<Option<u64>> {
        match tokio::fs::metadata(self.path(digest)).await {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>> {
        let file = match tokio::fs::File::open(self.path(digest)).await {
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

    async fn put(&self, digest: &Digest, source: &Path) -> anyhow::Result<PutOutcome> {
        let destination = self.path(digest);
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
