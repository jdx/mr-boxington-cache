use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::{
    CopyMode, CopyOptions, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    azure::MicrosoftAzureBuilder, buffered::BufWriter, path::Path as ObjectPath,
};
use std::{io, path::Path, sync::Arc};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use super::{Blob, BlobStore, PutOutcome};
use crate::{config::Config, model::Digest};

/// An Azure Blob Storage-backed content-addressed blob store.
pub struct AzureStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl AzureStore {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let account = config
            .azure_account
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--azure-account is required with Azure storage"))?;
        let container = config
            .azure_container
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--azure-container is required with Azure storage"))?;
        let mut builder = MicrosoftAzureBuilder::from_env()
            .with_account(account)
            .with_container_name(container)
            .with_credential_type(&config.azure_credential_type)
            .with_allow_http(config.azure_allow_http);
        if let Some(endpoint) = config
            .azure_endpoint
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            builder = builder.with_endpoint(endpoint.to_owned());
        }
        Ok(Self::from_store(
            Arc::new(builder.build()?),
            &config.azure_prefix,
        ))
    }

    fn from_store(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_owned(),
        }
    }

    fn key(&self, digest: &Digest) -> ObjectPath {
        ObjectPath::from(format!("{}/blobs/{}", self.prefix, digest.key()))
    }

    fn upload_key(&self) -> ObjectPath {
        ObjectPath::from(format!("{}/uploads/{}", self.prefix, Uuid::new_v4()))
    }

    async fn wait_until_readable(&self, key: &ObjectPath, size: u64) -> anyhow::Result<()> {
        // Copy Blob may return while the destination is still an empty pending
        // blob. Reading its final byte proves the block blob has committed
        // without downloading it again.
        let last_byte = size - 1..size;
        for _ in 0..120 {
            if self.store.get_range(key, last_byte.clone()).await.is_ok() {
                return Ok(());
            }
            sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!("Azure blob did not become readable after copy: {key}")
    }
}

#[async_trait]
impl BlobStore for AzureStore {
    async fn size(&self, digest: &Digest) -> anyhow::Result<Option<u64>> {
        match self.store.head(&self.key(digest)).await {
            Ok(metadata) => Ok(Some(metadata.size)),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>> {
        let result = match self.store.get(&self.key(digest)).await {
            Ok(result) => result,
            Err(Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let size = result.meta.size;
        let stream = result
            .into_stream()
            .map(|result| result.map_err(io::Error::other))
            .boxed();
        Ok(Some(Blob { size, stream }))
    }

    async fn put(&self, digest: &Digest, source: &Path) -> anyhow::Result<PutOutcome> {
        let destination_key = self.key(digest);
        if digest.size == 0 {
            let result = self
                .store
                .put_opts(
                    &destination_key,
                    Vec::new().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await;
            return match result {
                Ok(_) => Ok(PutOutcome::Created),
                Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {
                    Ok(PutOutcome::AlreadyExists)
                }
                Err(error) => Err(error.into()),
            };
        }

        let upload_key = self.upload_key();
        let file = tokio::fs::File::open(source).await?;
        let mut reader = BufReader::new(file);
        let mut writer = BufWriter::new(Arc::clone(&self.store), upload_key.clone());

        if let Err(error) = tokio::io::copy(&mut reader, &mut writer).await {
            let _ = writer.abort().await;
            return Err(error.into());
        }
        if let Err(error) = writer.shutdown().await {
            let _ = self.store.delete(&upload_key).await;
            return Err(error.into());
        }

        match self
            .store
            .copy_opts(
                &upload_key,
                &destination_key,
                CopyOptions {
                    mode: CopyMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            // Keep the source until the uploads lifecycle removes it because
            // Azure may still be reading it after accepting Copy Blob.
            Ok(_) => {
                self.wait_until_readable(&destination_key, digest.size)
                    .await?;
                Ok(PutOutcome::Created)
            }
            Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {
                let _ = self.store.delete(&upload_key).await;
                self.wait_until_readable(&destination_key, digest.size)
                    .await?;
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => {
                let _ = self.store.delete(&upload_key).await;
                Err(error.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Algorithm;
    use futures_util::TryStreamExt;
    use object_store::memory::InMemory;
    use sha2::{Digest as _, Sha256};

    #[tokio::test]
    async fn streams_blobs_and_does_not_replace_existing_content() {
        exercise(AzureStore::from_store(Arc::new(InMemory::new()), "/v1/")).await;
    }

    #[tokio::test]
    async fn round_trips_with_azurite_when_configured() {
        if std::env::var_os("MBX_CACHE_TEST_AZURITE").is_none() {
            return;
        }
        let backend = MicrosoftAzureBuilder::new()
            .with_use_emulator(true)
            .with_container_name("cache")
            .build()
            .unwrap();
        exercise(AzureStore::from_store(
            Arc::new(backend),
            &format!("tests/{}/v1", Uuid::new_v4()),
        ))
        .await;
    }

    async fn exercise(store: AzureStore) {
        let source = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(source.path(), b"azure blob")
            .await
            .unwrap();
        let digest = Digest {
            algorithm: Algorithm::Sha256.into(),
            hash: hex::encode(Sha256::digest(b"azure blob")),
            size: 10,
        };

        assert_eq!(store.size(&digest).await.unwrap(), None);
        assert_eq!(
            store.put(&digest, source.path()).await.unwrap(),
            PutOutcome::Created
        );
        assert_eq!(store.size(&digest).await.unwrap(), Some(10));
        assert_eq!(
            store
                .get(&digest)
                .await
                .unwrap()
                .unwrap()
                .stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .concat(),
            b"azure blob"[..]
        );

        tokio::fs::write(source.path(), b"different").await.unwrap();
        assert_eq!(
            store.put(&digest, source.path()).await.unwrap(),
            PutOutcome::AlreadyExists
        );
        assert_eq!(store.size(&digest).await.unwrap(), Some(10));

        tokio::fs::write(source.path(), b"").await.unwrap();
        let empty_digest = Digest {
            algorithm: Algorithm::Sha256.into(),
            hash: hex::encode(Sha256::digest(b"")),
            size: 0,
        };
        assert_eq!(
            store.put(&empty_digest, source.path()).await.unwrap(),
            PutOutcome::Created
        );
        assert_eq!(store.size(&empty_digest).await.unwrap(), Some(0));
        assert_eq!(
            store.put(&empty_digest, source.path()).await.unwrap(),
            PutOutcome::AlreadyExists
        );
    }
}
