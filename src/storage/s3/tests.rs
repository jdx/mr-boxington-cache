use super::test_support::{MockS3, Op, Operation, object_key, test_digest};
use super::*;
use crate::{
    auth::Authorizer,
    metadata::{MemoryMetadata, MetadataStore},
    model::{Algorithm, Digest},
    server::{AppState, router},
};
use axum::body::Bytes;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use futures_util::stream;
use std::{
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::mpsc;
use tower::ServiceExt;

const NAMESPACE: &str = "test/project";

fn digest(bytes: &[u8]) -> Digest {
    test_digest(bytes)
}

async fn upload(store: &S3Store, digest: &Digest, bytes: &[u8]) -> Result<PutOutcome, UploadError> {
    let mut upload = store.begin_upload(digest).await.unwrap();
    upload.write(bytes).await?;
    upload.finish().await
}

async fn make_app(
    store: S3Store,
    metadata: Arc<dyn MetadataStore>,
    max_blob_bytes: u64,
) -> axum::Router {
    let auth = Authorizer::new(None, None, true, None).await.unwrap();
    router(AppState::new(
        Arc::new(store),
        metadata,
        auth,
        max_blob_bytes,
    ))
}

fn put_request(namespace: &str, digest: &Digest, body: Body) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(format!(
            "/v1/blobs/{}/{}/{}",
            digest.algorithm, digest.hash, digest.size
        ))
        .header("mbx-cache-protocol", "1")
        .header("mbx-cache-namespace", namespace)
        .header(header::IF_NONE_MATCH, "*")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .unwrap()
}

async fn router_upload(
    app: &axum::Router,
    namespace: &str,
    digest: &Digest,
    bytes: &[u8],
) -> StatusCode {
    app.clone()
        .oneshot(put_request(namespace, digest, Body::from(bytes.to_vec())))
        .await
        .unwrap()
        .status()
}

async fn poll_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition did not become true");
}

#[test]
fn multipart_constants_are_legal_and_bound_the_advertised_size() {
    const {
        assert!(PART_SIZE >= 5 * 1024 * 1024);
        assert!(PART_SIZE <= 5 * 1024 * 1024 * 1024);
        assert!(MAX_PARTS <= 10_000);
    }
    assert_eq!(MAX_S3_OBJECT_BYTES, PART_SIZE as u64 * MAX_PARTS as u64);
    assert_eq!(UPLOAD_BUFFER_BYTES, 8 * PART_SIZE);
    assert!(validate_max_blob_bytes(MAX_S3_OBJECT_BYTES).is_ok());
    assert!(validate_max_blob_bytes(MAX_S3_OBJECT_BYTES + 1).is_err());
}

#[tokio::test]
async fn small_and_multipart_uploads_use_exact_parts_and_bytes() {
    let mock = MockS3::start().await;
    // Multipart uploads must also work when a compatible provider accepts the
    // submitted checksum but does not echo it back.
    mock.omit_part_checksum();
    let store = mock.store();

    let cases: Vec<Vec<u8>> = vec![
        Vec::new(),
        b"x".to_vec(),
        vec![b'a'; PART_SIZE - 1],
        vec![b'b'; PART_SIZE],
        vec![b'c'; PART_SIZE + 1],
        vec![b'd'; PART_SIZE * 2],
        vec![b'e'; PART_SIZE * 2 + 123],
    ];
    for contents in cases {
        let expected = digest(&contents);
        assert_eq!(
            upload(&store, &expected, &contents).await.unwrap(),
            PutOutcome::Created
        );
        assert_eq!(mock.object(&object_key(&expected)).unwrap(), contents);
        assert!(mock.upload_ids().is_empty());
    }

    let parts = mock.part_operations();
    let lengths: Vec<usize> = parts
        .iter()
        .map(|part| match part {
            Operation::UploadPart { len, .. } => *len,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(
        lengths,
        vec![
            PART_SIZE, PART_SIZE, 1, PART_SIZE, PART_SIZE, PART_SIZE, PART_SIZE, 123
        ]
    );
    assert_eq!(mock.put_count(), 3);
    assert_eq!(mock.create_count(), 4);

    let first = digest(b"x");
    assert_eq!(
        upload(&store, &first, b"x").await.unwrap(),
        PutOutcome::AlreadyExists
    );
}

#[tokio::test]
async fn blocked_upload_part_stops_polling_the_http_body() {
    let mock = MockS3::start().await;
    mock.block_next(Op::UploadPart);
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let app = make_app(mock.store(), Arc::clone(&metadata), u64::MAX).await;
    let first = vec![b'B'; PART_SIZE];
    let tail = b"tail after the blocked part".to_vec();
    let mut contents = first.clone();
    contents.extend_from_slice(&tail);
    let expected = digest(&contents);

    let (sender, receiver) = mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let yielded = Arc::new(AtomicUsize::new(0));
    let producer_yielded = Arc::clone(&yielded);
    let body = Body::from_stream(stream::unfold(
        (receiver, producer_yielded),
        |(mut receiver, yielded)| async move {
            receiver.recv().await.map(|item| {
                yielded.fetch_add(1, Ordering::SeqCst);
                (item, (receiver, yielded))
            })
        },
    ));
    sender.send(Ok(Bytes::from(first))).await.unwrap();
    sender.send(Ok(Bytes::from(tail))).await.unwrap();
    drop(sender);

    let request_digest = expected.clone();
    let task = tokio::spawn(async move {
        app.oneshot(put_request(NAMESPACE, &request_digest, body))
            .await
            .unwrap()
    });
    mock.wait_until_blocked(Op::UploadPart).await;

    // The second body chunk is already available to the producer, but the
    // handler must not poll for it while the first part is backpressured.
    assert_eq!(yielded.load(Ordering::SeqCst), 1);
    assert_eq!(mock.part_arrivals(), 1);
    assert_eq!(mock.part_operations().len(), 0);
    assert!(!mock.upload_ids().is_empty());
    assert!(mock.object(&object_key(&expected)).is_none());
    assert!(!metadata.blob_visible(NAMESPACE, &expected).await.unwrap());

    mock.release();
    assert_eq!(task.await.unwrap().status(), StatusCode::CREATED);
    assert_eq!(yielded.load(Ordering::SeqCst), 2);
    assert_eq!(mock.object(&object_key(&expected)).unwrap(), contents);
    assert!(metadata.blob_visible(NAMESPACE, &expected).await.unwrap());
}

#[tokio::test]
async fn rejects_mismatched_short_and_oversized_bodies_without_publishing() {
    let mock = MockS3::start().await;
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let app = make_app(mock.store(), Arc::clone(&metadata), 1024).await;

    let claimed = digest(b"claimed contents");
    assert_eq!(
        router_upload(&app, NAMESPACE, &claimed, b"swapped contents").await,
        StatusCode::BAD_REQUEST
    );
    assert!(mock.object(&object_key(&claimed)).is_none());
    assert!(!metadata.blob_visible(NAMESPACE, &claimed).await.unwrap());

    assert_eq!(
        router_upload(&app, NAMESPACE, &claimed, b"short").await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        router_upload(&app, NAMESPACE, &claimed, b"claimed contents!").await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert!(mock.object(&object_key(&claimed)).is_none());

    let too_large = digest(&vec![b'l'; 2048]);
    let before = mock.operations().len();
    assert_eq!(
        router_upload(&app, NAMESPACE, &too_large, b"unused").await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(mock.operations().len(), before);
}

#[tokio::test]
async fn reupload_of_an_existing_key_still_verifies_every_byte() {
    let mock = MockS3::start().await;
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let app = make_app(mock.store(), Arc::clone(&metadata), u64::MAX).await;
    let contents = b"immutable contents";
    let expected = digest(contents);
    assert_eq!(
        router_upload(&app, NAMESPACE, &expected, contents).await,
        StatusCode::CREATED
    );
    assert_eq!(
        router_upload(&app, NAMESPACE, &expected, contents).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        router_upload(&app, "test/other", &expected, b"swapped contents").await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(mock.object(&object_key(&expected)).unwrap(), contents);
    assert!(
        !metadata
            .blob_visible("test/other", &expected)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_stores_publish_once_and_abort_the_loser() {
    let mock = MockS3::start().await;
    mock.block_next(Op::Complete);
    let first_store = mock.store();
    let second_store = mock.store();
    let first_metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let second_metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let first = make_app(first_store, Arc::clone(&first_metadata), u64::MAX).await;
    let second = make_app(second_store, Arc::clone(&second_metadata), u64::MAX).await;
    let contents = vec![b'z'; PART_SIZE + 1];
    let expected = digest(&contents);

    let first_task = tokio::spawn({
        let app = first.clone();
        let expected = expected.clone();
        let contents = contents.clone();
        async move { router_upload(&app, NAMESPACE, &expected, &contents).await }
    });
    let second_task = tokio::spawn({
        let app = second.clone();
        let expected = expected.clone();
        let contents = contents.clone();
        async move { router_upload(&app, NAMESPACE, &expected, &contents).await }
    });

    mock.wait_until_blocked(Op::Complete).await;
    mock.release();
    let (first_status, second_status) = tokio::join!(first_task, second_task);
    let statuses = [first_status.unwrap(), second_status.unwrap()];
    assert!(statuses.contains(&StatusCode::CREATED));
    assert!(statuses.contains(&StatusCode::NO_CONTENT));
    assert_eq!(mock.object(&object_key(&expected)).unwrap(), contents);
    poll_until(|| mock.upload_ids().is_empty()).await;
}

#[tokio::test]
async fn retries_replay_the_same_part_including_a_short_tail() {
    let mock = MockS3::start().await;
    let contents = vec![b'r'; PART_SIZE * 2 + 77];
    let expected = digest(&contents);
    mock.fail_part(3, 1, 500);
    let store = mock.store();
    assert_eq!(
        upload(&store, &expected, &contents).await.unwrap(),
        PutOutcome::Created
    );
    assert_eq!(mock.object(&object_key(&expected)).unwrap(), contents);
    let parts = mock.part_operations();
    let part_three: Vec<_> = parts
        .iter()
        .filter_map(|part| match part {
            Operation::UploadPart {
                part_number,
                len,
                checksum,
            } if *part_number == 3 => Some((*len, checksum.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(part_three.len(), 2);
    assert_eq!(part_three[0], part_three[1]);
    assert_eq!(part_three[0].0, 77);
}

#[tokio::test]
async fn completion_failures_never_register_or_report_success() {
    for (name, configure) in [
        (
            "409",
            Box::new(|mock: &MockS3| mock.fail_complete(409, "ConditionalRequestConflict"))
                as Box<dyn Fn(&MockS3)>,
        ),
        (
            "embedded-200",
            Box::new(|mock: &MockS3| mock.fail_complete_embedded_200()),
        ),
        (
            "malformed",
            Box::new(|mock: &MockS3| mock.fail_complete_malformed()),
        ),
    ] {
        let mock = MockS3::start().await;
        configure(&mock);
        let store = mock.store();
        let contents = vec![b'f'; PART_SIZE + 9];
        let expected = digest(&contents);
        let error = upload(&store, &expected, &contents).await.unwrap_err();
        assert!(
            matches!(error, UploadError::Storage(_)),
            "{name}: unexpected {error:?}"
        );
        assert!(mock.object(&object_key(&expected)).is_none(), "{name}");
        poll_until(|| mock.upload_ids().is_empty()).await;
    }
}

#[tokio::test]
async fn abort_transient_failures_are_retried_and_the_primary_error_survives() {
    let mock = MockS3::start().await;
    mock.fail_abort(2, 503);
    let store = mock.store();
    let contents = vec![b'a'; PART_SIZE];
    let expected = digest(&contents);
    let mut session = store.begin_upload(&expected).await.unwrap();
    session.write(&vec![b'x'; PART_SIZE]).await.unwrap();
    assert!(matches!(session.finish().await, Err(UploadError::Mismatch)));
    poll_until(|| mock.upload_ids().is_empty()).await;
    assert!(mock.abort_count() >= 3);
}

#[tokio::test]
async fn cancellation_cleans_up_each_in_flight_phase() {
    // Cancelled while CreateMultipartUpload is in flight: the worker must
    // remember the returned upload ID and abort it.
    let mock = MockS3::start().await;
    mock.block_next(Op::Create);
    let store = mock.store();
    let contents = vec![b'c'; PART_SIZE];
    let expected = digest(&contents);
    let task = tokio::spawn({
        let contents = contents.clone();
        let expected = expected.clone();
        async move {
            let mut session = store.begin_upload(&expected).await.unwrap();
            session.write(&contents).await
        }
    });
    mock.wait_until_blocked(Op::Create).await;
    task.abort();
    mock.release();
    poll_until(|| mock.upload_ids().is_empty()).await;

    // Cancelled while UploadPart is in flight.
    let mock = MockS3::start().await;
    mock.block_next(Op::UploadPart);
    let store = mock.store();
    let task = tokio::spawn({
        let contents = contents.clone();
        let expected = expected.clone();
        async move {
            let mut session = store.begin_upload(&expected).await.unwrap();
            session.write(&contents).await
        }
    });
    mock.wait_until_blocked(Op::UploadPart).await;
    task.abort();
    mock.release();
    poll_until(|| mock.upload_ids().is_empty()).await;

    // Cancelled while the request is waiting for more body input after a part
    // already exists.
    mock.block_next(Op::UploadPart);
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let app = make_app(mock.store(), Arc::clone(&metadata), u64::MAX).await;
    let (sender, receiver) = mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let body = Body::from_stream(stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    }));
    let request = put_request(NAMESPACE, &expected, body);
    let task = tokio::spawn(async move { app.oneshot(request).await });
    sender
        .send(Ok(Bytes::from(contents.clone())))
        .await
        .unwrap();
    mock.wait_until_blocked(Op::UploadPart).await;
    drop(sender);
    task.abort();
    mock.release();
    poll_until(|| mock.upload_ids().is_empty()).await;
    assert!(!metadata.blob_visible(NAMESPACE, &expected).await.unwrap());

    // Cancelled while CompleteMultipartUpload is in flight. Completion may
    // still succeed, but it must never manufacture metadata for the caller.
    let mock = MockS3::start().await;
    mock.block_next(Op::Complete);
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let app = make_app(mock.store(), Arc::clone(&metadata), u64::MAX).await;
    let request = put_request(NAMESPACE, &expected, Body::from(contents.clone()));
    let task = tokio::spawn(async move { app.oneshot(request).await });
    mock.wait_until_blocked(Op::Complete).await;
    task.abort();
    mock.release();
    poll_until(|| mock.object(&object_key(&expected)).is_some()).await;
    assert!(!metadata.blob_visible(NAMESPACE, &expected).await.unwrap());
    poll_until(|| mock.upload_ids().is_empty()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_is_weighted_by_reserved_bytes_and_released_after_cleanup() {
    let mock = MockS3::start().await;
    mock.block_next(Op::UploadPart);
    // One full part of budget, plus room for exactly four one-byte uploads.
    let store = Arc::new(mock.store_with_budget(PART_SIZE + 4));
    let large = vec![b'l'; PART_SIZE];
    let large_digest = digest(&large);

    let first = tokio::spawn({
        let store = Arc::clone(&store);
        let large = large.clone();
        let large_digest = large_digest.clone();
        async move {
            let mut session = store.begin_upload(&large_digest).await.unwrap();
            session.write(&large).await
        }
    });
    mock.wait_until_blocked(Op::UploadPart).await;

    // Empty uploads reserve zero bytes, so they are admitted even while the
    // whole byte budget is held by a blocked large upload.
    let empty = b"";
    let empty_digest = digest(empty);
    let mut empty_session =
        tokio::time::timeout(Duration::from_secs(2), store.begin_upload(&empty_digest))
            .await
            .expect("empty upload should not need admission bytes")
            .unwrap();
    empty_session.write(empty).await.unwrap();
    assert_eq!(empty_session.finish().await.unwrap(), PutOutcome::Created);

    // Four one-byte reservations fit; a fifth waits because the remaining
    // budget is measured in bytes, not sessions.
    let one_byte = b"y";
    let one_digest = digest(one_byte);
    let mut smalls = Vec::new();
    for _ in 0..4 {
        smalls.push(
            tokio::time::timeout(Duration::from_secs(2), store.begin_upload(&one_digest))
                .await
                .expect("one-byte upload should fit the remaining budget")
                .unwrap(),
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), store.begin_upload(&one_digest))
            .await
            .is_err(),
        "a fifth byte reservation must wait for budget"
    );
    for mut session in smalls {
        session.write(one_byte).await.unwrap();
        assert!(matches!(
            session.finish().await.unwrap(),
            PutOutcome::Created | PutOutcome::AlreadyExists
        ));
    }

    // Releasing the large upload returns its full part reservation.
    mock.release();
    first.await.unwrap().unwrap();

    // More than eight tiny sessions are no longer throttled by a session
    // count, and every completed session releases its byte reservation.
    let mut many = Vec::new();
    for _ in 0..16 {
        many.push(
            tokio::time::timeout(Duration::from_secs(2), store.begin_upload(&one_digest))
                .await
                .expect("small uploads should not queue behind a fixed session count")
                .unwrap(),
        );
    }
    for mut session in many {
        session.write(one_byte).await.unwrap();
        assert!(matches!(
            session.finish().await.unwrap(),
            PutOutcome::Created | PutOutcome::AlreadyExists
        ));
    }
    assert!(mock.upload_ids().is_empty());
}

#[tokio::test]
async fn pack_uploads_commit_earlier_frames_and_abort_a_later_bad_frame() {
    let mock = MockS3::start().await;
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let app = make_app(mock.store(), Arc::clone(&metadata), u64::MAX).await;
    let first = b"valid packed frame";
    let first_digest = digest(first);
    let bad_digest = digest(b"claims these bytes");
    let pack = encode_pack(&[
        (&first_digest, first.as_slice()),
        (&bad_digest, b"other same-size!!".as_slice()),
    ]);
    let request = Request::builder()
        .method("POST")
        .uri("/v1/blobs:pack-upload")
        .header("mbx-cache-protocol", "1")
        .header("mbx-cache-namespace", NAMESPACE)
        .header(
            header::CONTENT_TYPE,
            mbx_cache_protocol::BLOB_PACK_MEDIA_TYPE,
        )
        .header(mbx_cache_protocol::BLOB_PACK_BLOBS_HEADER, 2)
        .header(
            mbx_cache_protocol::BLOB_PACK_BYTES_HEADER,
            first.len() + bad_digest.size as usize,
        )
        .body(Body::from(pack))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        metadata
            .blob_visible(NAMESPACE, &first_digest)
            .await
            .unwrap()
    );
    assert!(!metadata.blob_visible(NAMESPACE, &bad_digest).await.unwrap());
    assert!(mock.object(&object_key(&bad_digest)).is_none());
    assert!(mock.upload_ids().is_empty());
}

fn encode_pack(entries: &[(&Digest, &[u8])]) -> Vec<u8> {
    let mut pack = mbx_cache_protocol::BLOB_PACK_MAGIC.to_vec();
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

#[test]
fn s3_uploads_do_not_require_a_writable_tempdir() {
    if std::env::var_os("MBX_CACHE_S3_TMPDIR_CHILD").is_some() {
        return;
    }
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("storage::s3::tests::s3_uploads_need_no_tempdir_child")
        .arg("--nocapture")
        .env("MBX_CACHE_S3_TMPDIR_CHILD", "1")
        .env("TMPDIR", "/nonexistent/mbx-cache-test-tmpdir")
        .status()
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn s3_uploads_need_no_tempdir_child() {
    if std::env::var_os("MBX_CACHE_S3_TMPDIR_CHILD").is_none() {
        return;
    }
    let mock = MockS3::start().await;
    let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadata::default());
    let app = make_app(mock.store(), Arc::clone(&metadata), u64::MAX).await;
    let contents = b"no temporary file";
    let expected = digest(contents);
    assert_eq!(
        router_upload(&app, NAMESPACE, &expected, contents).await,
        StatusCode::CREATED
    );
    let second = b"packed without a temporary file";
    let second_digest = digest(second);
    let pack = encode_pack(&[(&second_digest, second.as_slice())]);
    let request = Request::builder()
        .method("POST")
        .uri("/v1/blobs:pack-upload")
        .header("mbx-cache-protocol", "1")
        .header("mbx-cache-namespace", NAMESPACE)
        .header(
            header::CONTENT_TYPE,
            mbx_cache_protocol::BLOB_PACK_MEDIA_TYPE,
        )
        .header(mbx_cache_protocol::BLOB_PACK_BLOBS_HEADER, 1)
        .header(mbx_cache_protocol::BLOB_PACK_BYTES_HEADER, second.len())
        .body(Body::from(pack))
        .unwrap();
    assert_eq!(app.oneshot(request).await.unwrap().status(), StatusCode::OK);
    assert!(
        metadata
            .blob_visible(NAMESPACE, &second_digest)
            .await
            .unwrap()
    );
}
