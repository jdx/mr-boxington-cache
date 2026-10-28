//! Shared verification for every blob upload.
//!
//! A backend starts an unpublished sink, and this wrapper is the only thing
//! that can feed it bytes and ask it to publish. Verification therefore cannot
//! be forgotten by an individual backend or HTTP route.
//!
//! `UploadSink` and `Spool` are private to the storage module tree. The public
//! surface is `VerifiedUpload`, which always hashes and counts exactly the
//! bytes it forwards.

use std::path::Path;

use async_trait::async_trait;
use sha2::Digest as _;
use tokio::io::AsyncWriteExt;

use super::PutOutcome;
use crate::model::{Algorithm, Digest};

/// Largest slice forwarded to a backend in one write.
///
/// HTTP handlers use the same constant to slice transport chunks, and
/// `VerifiedUpload` enforces it again so an internal caller cannot bypass the
/// bound by handing over a whole object.
pub const MAX_UPLOAD_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("blob exceeds declared or configured size")]
    TooLarge,
    #[error("content does not match the requested digest")]
    Mismatch,
    #[error("upload storage failed: {0}")]
    Storage(anyhow::Error),
    #[error("upload session is no longer usable")]
    Cancelled,
}

/// A private, backend-owned destination for unpublished bytes.
#[async_trait]
pub(super) trait UploadSink: Send {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), UploadError>;
    async fn commit(&mut self) -> Result<PutOutcome, UploadError>;
    async fn abort(&mut self);
}

/// A temporary file shared only by the backends that need to spool.
///
/// S3 deliberately does not use this and must never require a writable temp
/// directory.
pub(super) struct Spool {
    temp: tempfile::NamedTempFile,
    file: tokio::fs::File,
}

impl Spool {
    pub(super) async fn new() -> anyhow::Result<Self> {
        let temp = tempfile::NamedTempFile::new()?;
        let file = tokio::fs::File::create(temp.path()).await?;
        Ok(Self { temp, file })
    }

    pub(super) async fn write_all(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.file.write_all(bytes).await?;
        Ok(())
    }

    pub(super) async fn flush(&mut self) -> anyhow::Result<()> {
        self.file.flush().await?;
        Ok(())
    }

    pub(super) fn path(&self) -> &Path {
        self.temp.path()
    }
}

enum Verifier {
    Blake3(Box<blake3::Hasher>),
    Sha256(sha2::Sha256),
}

impl Verifier {
    fn new(algorithm: Algorithm) -> Self {
        match algorithm {
            Algorithm::Blake3 => Self::Blake3(Box::new(blake3::Hasher::new())),
            Algorithm::Sha256 => Self::Sha256(sha2::Sha256::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Blake3(hasher) => {
                hasher.update(bytes);
            }
            Self::Sha256(hasher) => {
                hasher.update(bytes);
            }
        }
    }

    fn finish(&mut self) -> String {
        let verifier = std::mem::replace(self, Self::Blake3(Box::new(blake3::Hasher::new())));
        match verifier {
            Self::Blake3(hasher) => hasher.finalize().to_hex().to_string(),
            Self::Sha256(hasher) => hex::encode(hasher.finalize()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Open,
    /// A write future is in progress. If that future is cancelled, the state
    /// deliberately stays here: the hasher and at least one backend write may
    /// already have advanced, so the session can never be resumed safely.
    Writing,
    Failed,
}

/// One verified, unpublished upload.
///
/// Dropping it abandons the upload. Backends implement that cleanup: the
/// filesystem and Azure spools remove themselves, and the S3 worker owns
/// multipart cleanup even when its caller disappears.
pub struct VerifiedUpload {
    expected: Digest,
    verifier: Verifier,
    written: u64,
    state: State,
    sink: Option<Box<dyn UploadSink>>,
}

impl VerifiedUpload {
    /// Storage-module only: callers outside storage cannot construct a session
    /// around an unverified sink.
    pub(super) fn new(digest: Digest, sink: Box<dyn UploadSink>) -> anyhow::Result<Self> {
        digest
            .validate()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let algorithm = digest
            .algorithm_kind()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(Self {
            expected: digest,
            verifier: Verifier::new(algorithm),
            written: 0,
            state: State::Open,
            sink: Some(sink),
        })
    }

    /// Hash and forward one chunk, rejecting input past the declared size
    /// before any of it reaches storage.
    pub async fn write(&mut self, chunk: &[u8]) -> Result<(), UploadError> {
        if self.state != State::Open {
            return Err(UploadError::Cancelled);
        }
        if chunk.is_empty() {
            return Ok(());
        }
        let new_size = match self.written.checked_add(chunk.len() as u64) {
            Some(size) => size,
            None => return self.fail(UploadError::TooLarge).await,
        };
        if new_size > self.expected.size {
            return self.fail(UploadError::TooLarge).await;
        }

        // Nothing before the last await may leave this state behind. If the
        // caller drops this borrowed write future, `Writing` remains and every
        // later write/finish is refused rather than reusing a half-advanced
        // verifier or a partially written backend.
        self.state = State::Writing;
        self.verifier.update(chunk);
        for slice in chunk.chunks(MAX_UPLOAD_CHUNK_BYTES) {
            let sink = self.sink.as_mut().ok_or_else(|| UploadError::Cancelled)?;
            if let Err(error) = sink.write(slice).await {
                return self.fail(error).await;
            }
        }
        self.written = new_size;
        self.state = State::Open;
        Ok(())
    }

    /// Verify the complete content and only then publish it.
    pub async fn finish(mut self) -> Result<PutOutcome, UploadError> {
        if self.state != State::Open {
            self.abort_sink().await;
            return Err(UploadError::Cancelled);
        }
        let actual = self.verifier.finish();
        if self.written != self.expected.size || actual != self.expected.hash {
            self.abort_sink().await;
            return Err(UploadError::Mismatch);
        }
        let Some(mut sink) = self.sink.take() else {
            return Err(UploadError::Cancelled);
        };
        match sink.commit().await {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                sink.abort().await;
                Err(error)
            }
        }
    }

    /// Explicitly abandon the upload and await observable backend cleanup.
    pub async fn abort(mut self) {
        self.abort_sink().await;
    }

    async fn fail(&mut self, error: UploadError) -> Result<(), UploadError> {
        self.state = State::Failed;
        self.abort_sink().await;
        Err(error)
    }

    async fn abort_sink(&mut self) {
        if let Some(mut sink) = self.sink.take() {
            sink.abort().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        writes: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
        commits: usize,
        aborts: usize,
    }

    #[async_trait]
    impl UploadSink for RecordingSink {
        async fn write(&mut self, chunk: &[u8]) -> Result<(), UploadError> {
            self.writes.lock().unwrap().push(chunk.len());
            Ok(())
        }

        async fn commit(&mut self) -> Result<PutOutcome, UploadError> {
            self.commits += 1;
            Ok(PutOutcome::Created)
        }

        async fn abort(&mut self) {
            self.aborts += 1;
        }
    }

    fn digest(bytes: &[u8]) -> Digest {
        Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: blake3::hash(bytes).to_hex().to_string(),
            size: bytes.len() as u64,
        }
    }

    #[tokio::test]
    async fn bounds_every_sink_write_even_for_one_large_caller_slice() {
        let bytes = vec![b'x'; MAX_UPLOAD_CHUNK_BYTES * 3 + 17];
        let sink = RecordingSink::default();
        let writes = std::sync::Arc::clone(&sink.writes);
        let mut session = VerifiedUpload::new(digest(&bytes), Box::new(sink)).unwrap();
        session.write(&bytes).await.unwrap();
        assert_eq!(session.finish().await.unwrap(), PutOutcome::Created);
        let writes = writes.lock().unwrap();
        assert_eq!(writes.iter().sum::<usize>(), bytes.len());
        assert!(writes.iter().all(|size| *size <= MAX_UPLOAD_CHUNK_BYTES));
    }

    #[tokio::test]
    async fn oversized_input_never_reaches_the_sink_and_is_terminal() {
        let expected = digest(b"0123456789");
        let mut session = VerifiedUpload::new(expected, Box::<RecordingSink>::default()).unwrap();
        assert!(matches!(
            session.write(b"0123456789!").await,
            Err(UploadError::TooLarge)
        ));
        assert!(matches!(
            session.finish().await,
            Err(UploadError::Cancelled)
        ));
    }

    #[derive(Default)]
    struct BlockingSink {
        started: std::sync::Arc<tokio::sync::Notify>,
        release: std::sync::Arc<tokio::sync::Notify>,
        writes: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
        commits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        aborts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl UploadSink for BlockingSink {
        async fn write(&mut self, chunk: &[u8]) -> Result<(), UploadError> {
            self.writes.lock().unwrap().push(chunk.len());
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        }

        async fn commit(&mut self) -> Result<PutOutcome, UploadError> {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(PutOutcome::Created)
        }

        async fn abort(&mut self) {
            self.aborts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn cancelling_a_borrowed_write_makes_the_retained_session_terminal() {
        let expected = digest(b"0123456789");
        let sink = BlockingSink::default();
        let started = std::sync::Arc::clone(&sink.started);
        let release = std::sync::Arc::clone(&sink.release);
        let writes = std::sync::Arc::clone(&sink.writes);
        let commits = std::sync::Arc::clone(&sink.commits);
        let aborts = std::sync::Arc::clone(&sink.aborts);
        let mut session = VerifiedUpload::new(expected, Box::new(sink)).unwrap();

        let mut write = Box::pin(session.write(b"0123456789"));
        tokio::select! {
            result = &mut write => panic!("write unexpectedly completed: {result:?}"),
            _ = started.notified() => {}
        }
        // Drop only the borrowed write future. The session itself stays alive.
        drop(write);
        release.notify_one();

        assert!(matches!(
            session.write(b"x").await,
            Err(UploadError::Cancelled)
        ));
        assert_eq!(writes.lock().unwrap().len(), 1);
        assert!(matches!(
            session.finish().await,
            Err(UploadError::Cancelled)
        ));
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn selects_sha256_for_sha256_digests() {
        let bytes = b"sha256 selection";
        let expected = Digest {
            algorithm: Algorithm::Sha256.into(),
            hash: hex::encode(sha2::Sha256::digest(bytes)),
            size: bytes.len() as u64,
        };
        let mut session = VerifiedUpload::new(expected, Box::<RecordingSink>::default()).unwrap();
        session.write(bytes).await.unwrap();
        assert_eq!(session.finish().await.unwrap(), PutOutcome::Created);
    }

    #[tokio::test]
    async fn a_mismatch_finishes_without_committing() {
        let expected = digest(b"0123456789");
        let mut session = VerifiedUpload::new(expected, Box::<RecordingSink>::default()).unwrap();
        session.write(b"9876543210").await.unwrap();
        assert!(matches!(session.finish().await, Err(UploadError::Mismatch)));
    }
}
