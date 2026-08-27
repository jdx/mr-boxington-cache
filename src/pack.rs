//! Framing shared by both directions of the blob-pack extension.
//!
//! A pack is the magic prefix `MBXPACK1` followed by one frame per blob: a
//! one-byte algorithm tag, the raw 32-byte hash, the payload length as a
//! big-endian `u64`, and then that many payload bytes.
//!
//! Reading it is separated from the handler that serves it so the parser can be
//! exercised without a request, and fuzzed without a server. It is a state
//! machine over arbitrary chunk boundaries: a client streams a pack, so a frame
//! header can arrive split across as many chunks as the network chooses.

use crate::model::{Algorithm, Digest};
use mbx_cache_protocol::BLOB_PACK_MAGIC;

const HEADER_BYTES: usize = mbx_cache_protocol::BLOB_PACK_HEADER_BYTES as usize;

/// What a caller should do with the bytes a chunk contained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackEvent {
    /// A frame header was read; its payload follows.
    Started(Digest),
    /// Payload bytes for the frame that started, in arrival order.
    Payload(Vec<u8>),
    /// The payload of the current frame is complete.
    Complete,
}

/// A pack that is not a pack: the caller rejects the request rather than
/// storing part of it.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PackError {
    #[error("blob pack does not start with the expected magic prefix")]
    Magic,
    #[error("blob pack declares an unknown digest algorithm {0}")]
    Algorithm(u8),
    #[error("blob pack ended inside a frame")]
    Truncated,
    #[error("blob pack declares a blob of {0} bytes, over the {1} byte limit")]
    BlobTooLarge(u64, u64),
    #[error("blob pack carries more than the declared {0} blobs")]
    TooManyBlobs(u64),
    #[error("blob pack carries more than the declared {0} payload bytes")]
    TooManyBytes(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Magic,
    Header,
    Payload,
}

/// Incremental reader for one uploaded pack.
///
/// Constructed with the limits the request declared and the service enforces,
/// so an overlong pack is refused as it arrives rather than after it is stored.
pub struct PackReader {
    state: State,
    buffer: Vec<u8>,
    max_blob_bytes: u64,
    max_blobs: u64,
    max_payload_bytes: u64,
    blobs: u64,
    payload_bytes: u64,
    remaining: u64,
}

impl PackReader {
    /// A reader bounded by one blob's ceiling and the request's declaration.
    pub fn new(max_blob_bytes: u64, max_blobs: u64, max_payload_bytes: u64) -> Self {
        Self {
            state: State::Magic,
            buffer: Vec::new(),
            max_blob_bytes,
            max_blobs,
            max_payload_bytes,
            blobs: 0,
            payload_bytes: 0,
            remaining: 0,
        }
    }

    /// Blobs whose frames have been read in full.
    pub fn blobs(&self) -> u64 {
        self.blobs
    }

    /// Payload bytes read across those frames.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Consume one chunk of the request body.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<PackEvent>, PackError> {
        let mut events = Vec::new();
        let mut rest = chunk;
        loop {
            match self.state {
                State::Magic => {
                    if !self.fill(&mut rest, BLOB_PACK_MAGIC.len()) {
                        return Ok(events);
                    }
                    if self.buffer != BLOB_PACK_MAGIC {
                        return Err(PackError::Magic);
                    }
                    self.buffer.clear();
                    self.state = State::Header;
                }
                State::Header => {
                    if rest.is_empty() && self.buffer.is_empty() {
                        return Ok(events);
                    }
                    if !self.fill(&mut rest, HEADER_BYTES) {
                        return Ok(events);
                    }
                    let digest = self.parse_header()?;
                    self.buffer.clear();
                    self.remaining = digest.size;
                    self.blobs += 1;
                    if self.blobs > self.max_blobs {
                        return Err(PackError::TooManyBlobs(self.max_blobs));
                    }
                    events.push(PackEvent::Started(digest));
                    self.state = State::Payload;
                    if self.remaining == 0 {
                        events.push(PackEvent::Complete);
                        self.state = State::Header;
                    }
                }
                State::Payload => {
                    if rest.is_empty() {
                        return Ok(events);
                    }
                    let take = usize::try_from(self.remaining)
                        .unwrap_or(usize::MAX)
                        .min(rest.len());
                    let (payload, remainder) = rest.split_at(take);
                    rest = remainder;
                    self.remaining -= take as u64;
                    self.payload_bytes += take as u64;
                    if self.payload_bytes > self.max_payload_bytes {
                        return Err(PackError::TooManyBytes(self.max_payload_bytes));
                    }
                    events.push(PackEvent::Payload(payload.to_vec()));
                    if self.remaining == 0 {
                        events.push(PackEvent::Complete);
                        self.state = State::Header;
                    }
                }
            }
        }
    }

    /// Assert the pack ended on a frame boundary.
    pub fn finish(&self) -> Result<(), PackError> {
        if self.state == State::Header && self.buffer.is_empty() {
            Ok(())
        } else {
            Err(PackError::Truncated)
        }
    }

    /// Buffer until `wanted` bytes are held, reporting whether they are.
    fn fill(&mut self, rest: &mut &[u8], wanted: usize) -> bool {
        if self.buffer.len() < wanted {
            let take = (wanted - self.buffer.len()).min(rest.len());
            self.buffer.extend_from_slice(&rest[..take]);
            *rest = &rest[take..];
        }
        self.buffer.len() == wanted
    }

    fn parse_header(&self) -> Result<Digest, PackError> {
        let algorithm = match self.buffer[0] {
            1 => Algorithm::Blake3,
            2 => Algorithm::Sha256,
            other => return Err(PackError::Algorithm(other)),
        };
        let hash = hex::encode(&self.buffer[1..33]);
        let size = u64::from_be_bytes(
            self.buffer[33..HEADER_BYTES]
                .try_into()
                .expect("a filled header holds eight length bytes"),
        );
        if size > self.max_blob_bytes {
            return Err(PackError::BlobTooLarge(size, self.max_blob_bytes));
        }
        Ok(Digest {
            algorithm: algorithm.into(),
            hash,
            size,
        })
    }
}

/// Parse a whole pack in one call, for tests.
#[cfg(test)]
pub fn read_pack(
    bytes: &[u8],
    max_blob_bytes: u64,
    max_blobs: u64,
    max_payload_bytes: u64,
) -> Result<Vec<(Digest, Vec<u8>)>, PackError> {
    let mut reader = PackReader::new(max_blob_bytes, max_blobs, max_payload_bytes);
    let mut blobs: Vec<(Digest, Vec<u8>)> = Vec::new();
    for event in reader.push(bytes)? {
        match event {
            PackEvent::Started(digest) => blobs.push((digest, Vec::new())),
            PackEvent::Payload(payload) => {
                blobs
                    .last_mut()
                    .expect("payload follows a started blob")
                    .1
                    .extend(payload);
            }
            PackEvent::Complete => {}
        }
    }
    reader.finish()?;
    Ok(blobs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(digest: &Digest, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![match digest.algorithm_kind().unwrap() {
            Algorithm::Blake3 => 1,
            Algorithm::Sha256 => 2,
        }];
        frame.extend(hex::decode(&digest.hash).unwrap());
        frame.extend(digest.size.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn digest(payload: &[u8]) -> Digest {
        Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: blake3::hash(payload).to_hex().to_string(),
            size: payload.len() as u64,
        }
    }

    fn pack(entries: &[(&Digest, &[u8])]) -> Vec<u8> {
        let mut bytes = BLOB_PACK_MAGIC.to_vec();
        for (digest, payload) in entries {
            bytes.extend(frame(digest, payload));
        }
        bytes
    }

    #[test]
    fn reads_every_frame_in_a_pack() {
        let first_payload = b"first blob";
        let second_payload = b"second blob";
        let first = digest(first_payload);
        let second = digest(second_payload);
        let bytes = pack(&[
            (&first, first_payload.as_slice()),
            (&second, second_payload.as_slice()),
        ]);

        let blobs = read_pack(&bytes, 1024, 10, 1024).unwrap();

        assert_eq!(
            blobs,
            vec![
                (first, first_payload.to_vec()),
                (second, second_payload.to_vec())
            ]
        );
    }

    /// A client streams a pack, so a header may arrive split anywhere.
    #[test]
    fn reassembles_frames_split_across_chunks() {
        let payload = b"a blob split across chunks";
        let digest = digest(payload);
        let bytes = pack(&[(&digest, payload.as_slice())]);
        let mut reader = PackReader::new(1024, 10, 1024);
        let mut started = Vec::new();
        let mut received = Vec::new();

        for chunk in bytes.chunks(1) {
            for event in reader.push(chunk).unwrap() {
                match event {
                    PackEvent::Started(digest) => started.push(digest),
                    PackEvent::Payload(bytes) => received.extend(bytes),
                    PackEvent::Complete => {}
                }
            }
        }
        reader.finish().unwrap();

        assert_eq!(started, vec![digest]);
        assert_eq!(received, payload);
        assert_eq!(reader.blobs(), 1);
        assert_eq!(reader.payload_bytes(), payload.len() as u64);
    }

    #[test]
    fn reads_an_empty_pack() {
        assert!(
            read_pack(BLOB_PACK_MAGIC, 1024, 10, 1024)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn refuses_a_pack_without_the_magic_prefix() {
        assert_eq!(
            read_pack(b"NOTAPACK", 1024, 10, 1024).unwrap_err(),
            PackError::Magic
        );
    }

    #[test]
    fn refuses_an_unknown_algorithm() {
        let payload = b"blob";
        let mut bytes = pack(&[(&digest(payload), payload.as_slice())]);
        bytes[BLOB_PACK_MAGIC.len()] = 9;
        assert_eq!(
            read_pack(&bytes, 1024, 10, 1024).unwrap_err(),
            PackError::Algorithm(9)
        );
    }

    #[test]
    fn refuses_a_truncated_frame() {
        let payload = b"truncated blob";
        let bytes = pack(&[(&digest(payload), payload.as_slice())]);
        assert_eq!(
            read_pack(&bytes[..bytes.len() - 1], 1024, 10, 1024).unwrap_err(),
            PackError::Truncated
        );
        assert_eq!(
            read_pack(&bytes[..BLOB_PACK_MAGIC.len() + 3], 1024, 10, 1024).unwrap_err(),
            PackError::Truncated
        );
    }

    #[test]
    fn refuses_a_blob_over_the_limit() {
        let payload = b"oversized blob";
        let bytes = pack(&[(&digest(payload), payload.as_slice())]);
        assert_eq!(
            read_pack(&bytes, 4, 10, 1024).unwrap_err(),
            PackError::BlobTooLarge(payload.len() as u64, 4)
        );
    }

    #[test]
    fn refuses_more_than_the_declared_contents() {
        let first_payload = b"first blob";
        let second_payload = b"second blob";
        let first = digest(first_payload);
        let second = digest(second_payload);
        let bytes = pack(&[
            (&first, first_payload.as_slice()),
            (&second, second_payload.as_slice()),
        ]);
        assert_eq!(
            read_pack(&bytes, 1024, 1, 1024).unwrap_err(),
            PackError::TooManyBlobs(1)
        );
        assert_eq!(
            read_pack(&bytes, 1024, 10, 12).unwrap_err(),
            PackError::TooManyBytes(12)
        );
    }
}
