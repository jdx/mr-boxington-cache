use async_trait::async_trait;
use std::{
    collections::{HashMap, HashSet},
    sync::RwLock,
};

use super::{CommitOutcome, ManifestCommitOutcome, ManifestRecord, MetadataStore};
use crate::model::{ActionResult, Digest, TaskActionManifest};
use crate::storage::PutOutcome;

#[derive(Default)]
pub struct MemoryMetadata {
    entries: RwLock<HashMap<(String, Digest), Vec<u8>>>,
    blobs: RwLock<HashSet<(String, Digest)>>,
    manifests: RwLock<HashMap<(String, Digest), (String, TaskActionManifest)>>,
}

#[async_trait]
impl MetadataStore for MemoryMetadata {
    async fn visible_blobs(
        &self,
        namespace: &str,
        digests: &[Digest],
    ) -> anyhow::Result<Vec<Digest>> {
        let blobs = self.blobs.read().expect("metadata lock poisoned");
        Ok(digests
            .iter()
            .filter(|digest| blobs.contains(&(namespace.to_owned(), (*digest).clone())))
            .cloned()
            .collect())
    }

    /// Nothing here ages out, so what the put did to the object does not matter.
    async fn register_blob(
        &self,
        namespace: &str,
        digest: &Digest,
        _stored: PutOutcome,
    ) -> anyhow::Result<()> {
        self.blobs
            .write()
            .expect("metadata lock poisoned")
            .insert((namespace.to_owned(), digest.clone()));
        Ok(())
    }

    async fn forget_blob(&self, namespace: &str, digest: &Digest) -> anyhow::Result<()> {
        self.blobs
            .write()
            .expect("metadata lock poisoned")
            .remove(&(namespace.to_owned(), digest.clone()));
        Ok(())
    }

    async fn get(&self, namespace: &str, action: &Digest) -> anyhow::Result<Option<ActionResult>> {
        let entries = self.entries.read().expect("metadata lock poisoned");
        entries
            .get(&(namespace.to_owned(), action.clone()))
            .map(|value| serde_json::from_slice(value).map_err(Into::into))
            .transpose()
    }

    async fn commit(
        &self,
        namespace: &str,
        action: &Digest,
        result: &ActionResult,
    ) -> anyhow::Result<CommitOutcome> {
        let encoded = serde_json::to_vec(result)?;
        let mut entries = self.entries.write().expect("metadata lock poisoned");
        match entries.get(&(namespace.to_owned(), action.clone())) {
            None => {
                entries.insert((namespace.to_owned(), action.clone()), encoded);
                Ok(CommitOutcome::Created)
            }
            Some(existing) if *existing == encoded => Ok(CommitOutcome::AlreadyExists),
            Some(_) => Ok(CommitOutcome::Conflict),
        }
    }

    async fn get_manifest(
        &self,
        namespace: &str,
        key: &Digest,
    ) -> anyhow::Result<Option<ManifestRecord>> {
        Ok(self
            .manifests
            .read()
            .expect("metadata lock poisoned")
            .get(&(namespace.to_owned(), key.clone()))
            .map(|(etag, manifest)| ManifestRecord {
                etag: etag.clone(),
                manifest: manifest.clone(),
            }))
    }

    async fn commit_manifest(
        &self,
        namespace: &str,
        key: &Digest,
        expected_etag: Option<&str>,
        etag: &str,
        manifest: &TaskActionManifest,
    ) -> anyhow::Result<ManifestCommitOutcome> {
        let mut manifests = self.manifests.write().expect("metadata lock poisoned");
        let entry = manifests.get(&(namespace.to_owned(), key.clone()));
        let matches = match (entry, expected_etag) {
            (None, None) => true,
            (Some((current, _)), Some(expected)) => current == expected,
            _ => false,
        };
        if !matches {
            return Ok(ManifestCommitOutcome::PreconditionFailed);
        }
        let outcome = if entry.is_some() {
            ManifestCommitOutcome::Updated
        } else {
            ManifestCommitOutcome::Created
        };
        manifests.insert(
            (namespace.to_owned(), key.clone()),
            (etag.to_owned(), manifest.clone()),
        );
        Ok(outcome)
    }
}
