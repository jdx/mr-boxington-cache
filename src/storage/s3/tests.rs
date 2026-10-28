use super::{
    test_support::{MockS3, object_key},
    *,
};
use crate::{
    auth::Authorizer,
    metadata::{MemoryMetadata, MetadataStore},
    model::Digest,
    server::{AppState, router},
};
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

const NAMESPACE: &str = "test/project";

fn get_request(namespace: &str, digest: &Digest) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!(
            "/v1/blobs/{}/{}/{}",
            digest.algorithm, digest.hash, digest.size
        ))
        .header("mbx-cache-protocol", "1")
        .header("mbx-cache-namespace", namespace)
        .body(Body::empty())
        .unwrap()
}

async fn app(store: S3Store, metadata: Arc<dyn MetadataStore>) -> axum::Router {
    let auth = Authorizer::new(None, None, true, None).await.unwrap();
    router(AppState::new(Arc::new(store), metadata, auth, u64::MAX))
}

async fn visible(metadata: &MemoryMetadata, digest: &Digest) {
    metadata
        .register_blob(NAMESPACE, digest, PutOutcome::Created)
        .await
        .unwrap();
}

#[tokio::test]
async fn default_off_proxies_the_body_without_a_head() {
    let mock = MockS3::start().await;
    let contents = b"proxied by default";
    let digest = Digest::blake3(contents);
    mock.insert(&object_key(&digest), contents);
    let metadata = Arc::new(MemoryMetadata::default());
    visible(&metadata, &digest).await;
    let app = app(mock.store(false), metadata).await;

    let response = app.oneshot(get_request(NAMESPACE, &digest)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(header::LOCATION).is_none());
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        contents.as_slice()
    );
    assert_eq!(mock.head_count(), 0);
    assert_eq!(mock.get_count(), 1);
}

#[tokio::test]
async fn redirect_get_is_gated_and_does_not_fetch_the_body_first() {
    let mock = MockS3::start().await;
    let contents = b"redirected contents";
    let digest = Digest::blake3(contents);
    mock.insert(&object_key(&digest), contents);
    let metadata = Arc::new(MemoryMetadata::default());
    visible(&metadata, &digest).await;
    let app = app(
        mock.store(true),
        Arc::clone(&metadata) as Arc<dyn MetadataStore>,
    )
    .await;

    let response = app
        .clone()
        .oneshot(get_request(NAMESPACE, &digest))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(mock.head_count(), 1);
    assert_eq!(mock.get_count(), 0);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("private, no-store")
    );
    assert_eq!(
        response
            .headers()
            .get(header::REFERRER_POLICY)
            .and_then(|value| value.to_str().ok()),
        Some("no-referrer")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("0")
    );
    let location = response.headers().get(header::LOCATION).unwrap();
    assert!(location.is_sensitive());
    let url = reqwest::Url::parse(location.to_str().unwrap()).unwrap();
    let query: BTreeMap<String, String> = url
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect();
    assert!(
        query
            .get("X-Amz-Signature")
            .is_some_and(|signature| !signature.is_empty())
    );
    assert!(
        query
            .get("X-Amz-Expires")
            .and_then(|expires| expires.parse::<u64>().ok())
            .is_some_and(|expires| expires <= 300)
    );
    assert_eq!(
        query.get("response-content-type").map(String::as_str),
        Some("application/octet-stream")
    );
    assert_eq!(
        query.get("response-cache-control").map(String::as_str),
        Some("private, no-store")
    );

    let followed = reqwest::get(location.to_str().unwrap()).await.unwrap();
    assert_eq!(followed.status(), StatusCode::OK);
    assert_eq!(followed.bytes().await.unwrap().as_ref(), contents);
    assert_eq!(mock.get_count(), 1);

    // Hidden from another namespace: no extra HEAD or URL issuance.
    let before = (mock.head_count(), mock.get_count());
    let response = app
        .clone()
        .oneshot(get_request("test/other", &digest))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!((mock.head_count(), mock.get_count()), before);

    // Stale metadata for a missing object: no URL.
    let missing = Digest::blake3(b"missing object");
    visible(&metadata, &missing).await;
    let response = app
        .clone()
        .oneshot(get_request(NAMESPACE, &missing))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(response.headers().get(header::LOCATION).is_none());

    // A permission failure is not invented absence and still yields no URL.
    mock.fail_head(403);
    let response = app.oneshot(get_request(NAMESPACE, &digest)).await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get(header::LOCATION).is_none());

    // Cache authentication still runs before storage lookup.
    let auth = Authorizer::new(
        Some(r#"[{"token":"secret","read":["test/*"],"write":["test/project"]}]"#),
        None,
        false,
        None,
    )
    .await
    .unwrap();
    let auth_app = router(AppState::new(
        Arc::new(mock.store(true)),
        Arc::clone(&metadata) as Arc<dyn MetadataStore>,
        auth,
        u64::MAX,
    ));
    let before = (mock.head_count(), mock.get_count());
    let response = auth_app
        .oneshot(get_request(NAMESPACE, &digest))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!((mock.head_count(), mock.get_count()), before);
}

#[tokio::test]
async fn head_is_local_and_range_or_conditional_gets_keep_the_proxy_path() {
    let mock = MockS3::start().await;
    let contents = b"range and condition object";
    let digest = Digest::blake3(contents);
    mock.insert(&object_key(&digest), contents);
    let metadata = Arc::new(MemoryMetadata::default());
    visible(&metadata, &digest).await;
    let app = app(mock.store(true), metadata).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!(
                    "/v1/blobs/{}/{}/{}",
                    digest.algorithm, digest.hash, digest.size
                ))
                .header("mbx-cache-protocol", "1")
                .header("mbx-cache-namespace", NAMESPACE)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some(contents.len().to_string().as_str())
    );
    assert!(response.headers().get(header::LOCATION).is_none());
    assert_eq!(mock.head_count(), 1);
    assert_eq!(mock.get_count(), 0);
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );

    for conditional in [
        (header::RANGE, "bytes=0-3"),
        (header::IF_NONE_MATCH, "\"other\""),
    ] {
        let mut request = get_request(NAMESPACE, &digest);
        request
            .headers_mut()
            .insert(conditional.0, conditional.1.parse().unwrap());
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::LOCATION).is_none());
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            contents.as_slice()
        );
    }
    assert_eq!(mock.head_count(), 1);
    assert_eq!(mock.get_count(), 2);
    assert!(
        mock.last_get_headers()
            .iter()
            .all(|(name, _)| name != "range" && name != "if-none-match")
    );
}
