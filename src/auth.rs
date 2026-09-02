use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow, bail};
use axum::http::HeaderMap;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyOperations, PublicKeyUse},
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

use crate::server::ApiError;

const DEFAULT_JWKS_REFRESH_SECONDS: u64 = 300;
const DEFAULT_CLOCK_SKEW_SECONDS: u64 = 60;
const MIN_JWKS_REFRESH_SECONDS: u64 = 30;
const INITIAL_FETCH_ATTEMPTS: u32 = 3;
const INITIAL_FETCH_BACKOFF_MILLIS: u64 = 250;
const DEFAULT_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenGrant {
    pub token: String,
    #[serde(default)]
    pub read: Vec<String>,
    #[serde(default)]
    pub write: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcProviderConfig {
    issuer: String,
    audiences: Vec<String>,
    #[serde(default)]
    discovery_uri: Option<String>,
    #[serde(default)]
    jwks_uri: Option<String>,
    #[serde(default)]
    algorithms: Vec<String>,
    #[serde(default = "default_jwks_refresh_seconds")]
    jwks_refresh_seconds: u64,
    #[serde(default = "default_clock_skew_seconds")]
    clock_skew_seconds: u64,
    rules: Vec<OidcRule>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OidcRule {
    claims: BTreeMap<String, ClaimRequirement>,
    #[serde(default)]
    read: Vec<String>,
    #[serde(default)]
    write: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum ClaimRequirement {
    One(ScalarValue),
    Any(Vec<ScalarValue>),
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
enum ScalarValue {
    String(String),
    Number(serde_json::Number),
    Bool(bool),
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    jwks_uri: String,
    #[serde(default)]
    id_token_signing_alg_values_supported: Vec<String>,
}

#[derive(Clone)]
struct OidcProvider {
    config: Arc<OidcProviderConfig>,
    algorithms: Arc<Vec<Algorithm>>,
    jwks_uri: Arc<str>,
    client: reqwest::Client,
    keys: Arc<RwLock<CachedKeys>>,
    refresh: Arc<Mutex<()>>,
    min_refresh_interval: Duration,
}

struct CachedKeys {
    set: JwkSet,
    fetched_at: Instant,
    attempted_at: Option<Instant>,
}

#[derive(Clone)]
pub struct Authorizer {
    grants: Vec<TokenGrant>,
    oidc: Vec<OidcProvider>,
    allow_anonymous: bool,
    anonymous_read_namespaces: Vec<String>,
}

#[derive(Clone, Copy)]
pub enum Access {
    Read,
    Write,
}

impl Authorizer {
    pub async fn new(
        tokens_json: Option<&str>,
        oidc_providers_json: Option<&str>,
        allow_anonymous: bool,
        anonymous_read_namespaces_json: Option<&str>,
    ) -> anyhow::Result<Self> {
        let grants: Vec<TokenGrant> = parse_json_array(tokens_json, "token grants")?;
        let configs: Vec<OidcProviderConfig> =
            parse_json_array(oidc_providers_json, "OIDC providers")?;
        let anonymous_read_namespaces: Vec<String> = parse_json_array(
            anonymous_read_namespaces_json,
            "anonymous read namespace patterns",
        )?;
        let mut issuers = std::collections::BTreeSet::new();
        let mut oidc = Vec::with_capacity(configs.len());
        for config in configs {
            if !issuers.insert(config.issuer.clone()) {
                bail!(
                    "OIDC issuer {:?} is configured more than once",
                    config.issuer
                );
            }
            oidc.push(OidcProvider::new(config).await?);
        }
        Ok(Self {
            grants,
            oidc,
            allow_anonymous,
            anonymous_read_namespaces,
        })
    }

    pub async fn authorize(&self, headers: &HeaderMap, access: Access) -> Result<String, ApiError> {
        if headers
            .get("mbx-cache-protocol")
            .and_then(|value| value.to_str().ok())
            != Some("1")
        {
            return Err(ApiError::upgrade_required());
        }
        let namespace = headers
            .get("mbx-cache-namespace")
            .and_then(|value| value.to_str().ok())
            .filter(|value| valid_namespace(value))
            .ok_or_else(|| {
                ApiError::bad_request("a valid Mbx-Cache-Namespace header is required")
            })?;

        if self.allow_anonymous && self.grants.is_empty() && self.oidc.is_empty() {
            return Ok(namespace.to_owned());
        }
        if matches!(access, Access::Read)
            && self
                .anonymous_read_namespaces
                .iter()
                .any(|pattern| matches_namespace(pattern, namespace))
        {
            return Ok(namespace.to_owned());
        }
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| ApiError::unauthorized("a bearer token is required"))?;

        if let Some(grant) = self
            .grants
            .iter()
            .find(|grant| constant_time_eq(grant.token.as_bytes(), token.as_bytes()))
        {
            return authorize_patterns(patterns(grant, access), namespace);
        }

        let issuer =
            unverified_issuer(token).map_err(|_| ApiError::unauthorized("invalid bearer token"))?;
        let provider = self
            .oidc
            .iter()
            .find(|provider| provider.config.issuer == issuer)
            .ok_or_else(|| ApiError::unauthorized("invalid bearer token"))?;
        let claims = provider.verify(token).await.map_err(|error| {
            tracing::warn!(%error, issuer, "OIDC token validation failed");
            ApiError::unauthorized("invalid bearer token")
        })?;
        let authorized = provider.config.rules.iter().any(|rule| {
            rule.matches_claims(&claims)
                && rule
                    .patterns(access)
                    .iter()
                    .any(|pattern| matches_namespace(pattern, namespace))
        });
        if authorized {
            Ok(namespace.to_owned())
        } else {
            Err(ApiError::forbidden(
                "identity is not authorized for this namespace",
            ))
        }
    }
}

impl OidcProvider {
    async fn new(config: OidcProviderConfig) -> anyhow::Result<Self> {
        validate_provider_config(&config)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        let (jwks_uri, advertised_algorithms, set) =
            fetch_initial_provider_data(&client, &config).await?;
        let algorithms = configured_algorithms(&config.algorithms, &advertised_algorithms)?;
        let now = Instant::now();
        Ok(Self {
            config: Arc::new(config),
            algorithms: Arc::new(algorithms),
            jwks_uri: jwks_uri.into(),
            client,
            keys: Arc::new(RwLock::new(CachedKeys {
                set,
                fetched_at: now,
                attempted_at: None,
            })),
            refresh: Arc::new(Mutex::new(())),
            min_refresh_interval: Duration::from_secs(MIN_JWKS_REFRESH_SECONDS),
        })
    }

    async fn verify(&self, token: &str) -> anyhow::Result<Value> {
        let header = decode_header(token).context("decode JWT header")?;
        let kid = header.kid.as_deref().context("JWT header is missing kid")?;
        if !self.algorithms.contains(&header.alg) {
            bail!("JWT uses a disallowed signing algorithm");
        }

        let keys = self.keys.read().await;
        let stale =
            keys.fetched_at.elapsed() >= Duration::from_secs(self.config.jwks_refresh_seconds);
        let known_key = keys.set.find(kid).is_some();
        drop(keys);
        if (stale || !known_key)
            && let Err(error) = self.refresh_keys(kid).await
        {
            if !known_key {
                return Err(error);
            }
            tracing::warn!(%error, issuer = self.config.issuer, "using stale OIDC keys after refresh failed");
        }
        let key = {
            let keys = self.keys.read().await;
            let jwk = keys
                .set
                .find(kid)
                .context("JWT signing key was not found")?;
            if jwk.common.key_algorithm.is_some_and(|algorithm| {
                Algorithm::from_str(&algorithm.to_string()).ok() != Some(header.alg)
            }) || jwk
                .common
                .public_key_use
                .as_ref()
                .is_some_and(|usage| usage != &PublicKeyUse::Signature)
                || jwk
                    .common
                    .key_operations
                    .as_ref()
                    .is_some_and(|operations| !operations.contains(&KeyOperations::Verify))
            {
                bail!("JWT signing key is not valid for this algorithm");
            }
            DecodingKey::from_jwk(jwk).context("decode JWT signing key")?
        };

        let mut validation = Validation::new(header.alg);
        validation.set_audience(&self.config.audiences);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_required_spec_claims(&["exp", "aud", "iss", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = self.config.clock_skew_seconds;
        Ok(decode::<Value>(token, &key, &validation)
            .context("validate JWT")?
            .claims)
    }

    async fn refresh_keys(&self, kid: &str) -> anyhow::Result<()> {
        let _guard = self.refresh.lock().await;
        {
            let keys = self.keys.read().await;
            if keys
                .attempted_at
                .is_some_and(|attempted_at| attempted_at.elapsed() < self.min_refresh_interval)
                || (keys.fetched_at.elapsed()
                    < Duration::from_secs(self.config.jwks_refresh_seconds)
                    && keys.set.find(kid).is_some())
            {
                return Ok(());
            }
        }
        self.keys.write().await.attempted_at = Some(Instant::now());
        let set = fetch_jwks(&self.client, &self.jwks_uri).await?;
        let mut keys = self.keys.write().await;
        keys.set = set;
        keys.fetched_at = Instant::now();
        Ok(())
    }
}

impl OidcRule {
    fn patterns(&self, access: Access) -> &[String] {
        match access {
            Access::Read => &self.read,
            Access::Write => &self.write,
        }
    }

    fn matches_claims(&self, claims: &Value) -> bool {
        self.claims.iter().all(|(name, requirement)| {
            claims
                .get(name)
                .is_some_and(|actual| requirement.matches(actual))
        })
    }
}

impl ClaimRequirement {
    fn matches(&self, actual: &Value) -> bool {
        let expected = match self {
            Self::One(value) => std::slice::from_ref(value),
            Self::Any(values) => values,
        };
        match actual {
            Value::Array(values) => values
                .iter()
                .any(|actual| expected.iter().any(|value| value.matches(actual))),
            actual => expected.iter().any(|value| value.matches(actual)),
        }
    }
}

impl ScalarValue {
    fn matches(&self, actual: &Value) -> bool {
        match (self, actual) {
            (Self::String(expected), Value::String(actual)) => expected == actual,
            (Self::Number(expected), Value::Number(actual)) => expected == actual,
            (Self::Bool(expected), Value::Bool(actual)) => expected == actual,
            _ => false,
        }
    }
}

fn parse_json_array<T: for<'de> Deserialize<'de>>(
    json: Option<&str>,
    label: &str,
) -> anyhow::Result<Vec<T>> {
    json.map(serde_json::from_str)
        .transpose()
        .with_context(|| format!("parse {label} JSON"))
        .map(Option::unwrap_or_default)
}

fn validate_provider_config(config: &OidcProviderConfig) -> anyhow::Result<()> {
    if config.issuer.is_empty() || config.audiences.is_empty() || config.rules.is_empty() {
        bail!("each OIDC provider requires issuer, audiences, and rules");
    }
    if config.jwks_refresh_seconds == 0 {
        bail!("OIDC jwks_refresh_seconds must be greater than zero");
    }
    if config.rules.iter().any(|rule| rule.claims.is_empty()) {
        bail!("each OIDC authorization rule must require at least one claim");
    }
    Ok(())
}

fn configured_algorithms(
    configured: &[String],
    advertised: &[String],
) -> anyhow::Result<Vec<Algorithm>> {
    let mut algorithms = if configured.is_empty() {
        DEFAULT_ALGORITHMS.to_vec()
    } else {
        configured
            .iter()
            .map(|name| {
                Algorithm::from_str(name)
                    .map_err(|_| anyhow!("unsupported OIDC algorithm {name:?}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    };
    algorithms.retain(|algorithm| DEFAULT_ALGORITHMS.contains(algorithm));
    if !advertised.is_empty() {
        let advertised = advertised
            .iter()
            .filter_map(|name| Algorithm::from_str(name).ok())
            .collect::<Vec<_>>();
        algorithms.retain(|algorithm| advertised.contains(algorithm));
    }
    if algorithms.is_empty() {
        bail!("OIDC provider has no mutually supported asymmetric signing algorithm");
    }
    Ok(algorithms)
}

async fn fetch_initial_provider_data(
    client: &reqwest::Client,
    config: &OidcProviderConfig,
) -> anyhow::Result<(String, Vec<String>, JwkSet)> {
    let mut last_error = None;
    for attempt in 1..=INITIAL_FETCH_ATTEMPTS {
        match fetch_provider_data(client, config).await {
            Ok(data) => return Ok(data),
            Err(error) => {
                tracing::warn!(
                    %error,
                    issuer = config.issuer,
                    attempt,
                    max_attempts = INITIAL_FETCH_ATTEMPTS,
                    "initial OIDC provider fetch failed"
                );
                last_error = Some(error);
                if attempt < INITIAL_FETCH_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(
                        INITIAL_FETCH_BACKOFF_MILLIS * u64::from(attempt),
                    ))
                    .await;
                }
            }
        }
    }
    Err(last_error.expect("at least one OIDC fetch attempt"))
        .with_context(|| format!("initialize OIDC issuer {}", config.issuer))
}

async fn fetch_provider_data(
    client: &reqwest::Client,
    config: &OidcProviderConfig,
) -> anyhow::Result<(String, Vec<String>, JwkSet)> {
    let (jwks_uri, advertised_algorithms) = if let Some(uri) = &config.jwks_uri {
        (uri.clone(), Vec::new())
    } else {
        let discovery_uri = config.discovery_uri.clone().unwrap_or_else(|| {
            format!(
                "{}/.well-known/openid-configuration",
                config.issuer.trim_end_matches('/')
            )
        });
        let document: DiscoveryDocument = fetch_json(client, &discovery_uri)
            .await
            .with_context(|| format!("discover OIDC issuer {}", config.issuer))?;
        if document.issuer != config.issuer {
            bail!(
                "OIDC discovery issuer mismatch: configured {:?}, received {:?}",
                config.issuer,
                document.issuer
            );
        }
        (
            document.jwks_uri,
            document.id_token_signing_alg_values_supported,
        )
    };
    let set = fetch_jwks(client, &jwks_uri)
        .await
        .with_context(|| format!("fetch OIDC keys for {}", config.issuer))?;
    Ok((jwks_uri, advertised_algorithms, set))
}

async fn fetch_jwks(client: &reqwest::Client, uri: &str) -> anyhow::Result<JwkSet> {
    fetch_json(client, uri).await.context("fetch JWKS")
}

async fn fetch_json<T: for<'de> Deserialize<'de>>(
    client: &reqwest::Client,
    uri: &str,
) -> anyhow::Result<T> {
    Ok(client
        .get(uri)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

fn unverified_issuer(token: &str) -> anyhow::Result<String> {
    let mut parts = token.split('.');
    let _header = parts.next().context("JWT is missing header")?;
    let payload = parts.next().context("JWT is missing payload")?;
    let _signature = parts.next().context("JWT is missing signature")?;
    if parts.next().is_some() {
        bail!("JWT has too many segments");
    }
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
    claims
        .get("iss")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .context("JWT is missing issuer")
}

fn patterns(grant: &TokenGrant, access: Access) -> &[String] {
    match access {
        Access::Read => &grant.read,
        Access::Write => &grant.write,
    }
}

fn authorize_patterns(patterns: &[String], namespace: &str) -> Result<String, ApiError> {
    if patterns
        .iter()
        .any(|pattern| matches_namespace(pattern, namespace))
    {
        Ok(namespace.to_owned())
    } else {
        Err(ApiError::forbidden(
            "token is not authorized for this namespace",
        ))
    }
}

const fn default_jwks_refresh_seconds() -> u64 {
    DEFAULT_JWKS_REFRESH_SECONDS
}

const fn default_clock_skew_seconds() -> u64 {
    DEFAULT_CLOCK_SKEW_SECONDS
}

fn valid_namespace(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
        && value
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..")
}

fn matches_namespace(pattern: &str, namespace: &str) -> bool {
    pattern == "*"
        || pattern == namespace
        || pattern.strip_suffix("/*").is_some_and(|prefix| {
            namespace.starts_with(prefix) && namespace.as_bytes().get(prefix.len()) == Some(&b'/')
        })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get,
    };
    use jsonwebtoken::{EncodingKey, Header, encode};
    use rsa::{
        RsaPrivateKey, RsaPublicKey,
        pkcs8::{EncodePrivateKey, LineEnding},
        rand_core::OsRng,
        traits::PublicKeyParts,
    };
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::net::TcpListener;

    #[test]
    fn namespace_patterns_do_not_overmatch() {
        assert!(matches_namespace("acme/*", "acme/project"));
        assert!(!matches_namespace("acme/*", "acme"));
        assert!(!matches_namespace("acme/*", "acme-other/project"));
        assert!(matches_namespace("*", "anything"));
    }

    #[test]
    fn rejects_unsafe_namespaces() {
        assert!(valid_namespace("acme/project-a"));
        assert!(!valid_namespace("../project"));
        assert!(!valid_namespace("project name"));
    }

    #[tokio::test]
    async fn anonymous_read_patterns_never_allow_writes() {
        let authorizer = Authorizer::new(None, None, false, Some(r#"["jdx/*"]"#))
            .await
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("mbx-cache-protocol", "1".parse().unwrap());
        headers.insert("mbx-cache-namespace", "jdx/mise".parse().unwrap());

        assert!(authorizer.authorize(&headers, Access::Read).await.is_ok());
        assert!(authorizer.authorize(&headers, Access::Write).await.is_err());

        headers.insert("mbx-cache-namespace", "private/mise".parse().unwrap());
        assert!(authorizer.authorize(&headers, Access::Read).await.is_err());
    }

    #[test]
    fn claim_requirements_match_scalars_and_arrays() {
        let claims = serde_json::json!({"repository":"jdx/mise", "groups":["dev", "release"]});
        let rule: OidcRule = serde_json::from_value(serde_json::json!({
            "claims":{"repository":"jdx/mise", "groups":["ops", "release"]},
            "read":["jdx/mise"]
        }))
        .unwrap();
        assert!(rule.matches_claims(&claims));
    }

    #[test]
    fn rejects_hmac_algorithms() {
        assert!(configured_algorithms(&["HS256".into()], &[]).is_err());
    }

    #[tokio::test]
    async fn retries_initial_jwks_fetches() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/jwks",
                get(|State(attempts): State<Arc<AtomicUsize>>| async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        Json(serde_json::json!({"keys":[]})).into_response()
                    }
                }),
            )
            .with_state(attempts.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let providers = serde_json::json!([{
            "issuer":"https://issuer.example",
            "audiences":["https://cache.example"],
            "jwks_uri":format!("http://{address}/jwks"),
            "rules":[{"claims":{"repository":"jdx/mise"}, "read":["jdx/mise"]}]
        }]);

        Authorizer::new(None, Some(&providers.to_string()), false, None)
            .await
            .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn validates_oidc_signature_audience_claims_and_permissions() {
        let private = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let public = RsaPublicKey::from(&private);
        let jwks = Arc::new(RwLock::new(serde_json::json!({"keys":[{
            "kty":"RSA",
            "use":"sig",
            "alg":"RS256",
            "kid":"test-key",
            "n":URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),
            "e":URL_SAFE_NO_PAD.encode(public.e().to_bytes_be())
        }]})));
        let app = Router::new()
            .route(
                "/jwks",
                get(|State(jwks): State<Arc<RwLock<Value>>>| async move {
                    Json(jwks.read().await.clone())
                }),
            )
            .with_state(jwks.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let issuer = "https://issuer.example";
        let providers = serde_json::json!([{
            "issuer":issuer,
            "audiences":["https://cache.example"],
            "jwks_uri":format!("http://{address}/jwks"),
            "rules":[{
                "claims":{"repository":"jdx/mise", "repository_owner_id":"216188"},
                "read":["jdx/mise"],
                "write":["jdx/mise"]
            }]
        }]);
        let mut authorizer = Authorizer::new(None, Some(&providers.to_string()), false, None)
            .await
            .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = serde_json::json!({
            "iss":issuer,
            "aud":"https://cache.example",
            "sub":"repo:jdx/mise:ref:refs/heads/main",
            "exp":now + 300,
            "nbf":now - 1,
            "repository":"jdx/mise",
            "repository_owner_id":"216188"
        });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".into());
        let pem = private.to_pkcs8_pem(LineEnding::LF).unwrap();
        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
        )
        .unwrap();
        let verified = authorizer.oidc[0].verify(&token).await.unwrap();
        assert!(authorizer.oidc[0].config.rules[0].matches_claims(&verified));
        let headers = request_headers(&token, "jdx/mise");
        assert!(authorizer.authorize(&headers, Access::Write).await.is_ok());

        let denied_headers = request_headers(&token, "another/project");
        assert!(
            authorizer
                .authorize(&denied_headers, Access::Read)
                .await
                .is_err()
        );

        let wrong_audience = serde_json::json!({
            "iss":issuer,
            "aud":"https://attacker.example",
            "sub":"repo:jdx/mise:ref:refs/heads/main",
            "exp":now + 300,
            "repository":"jdx/mise",
            "repository_owner_id":"216188"
        });
        let token = encode(
            &header,
            &wrong_audience,
            &EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
        )
        .unwrap();
        assert!(
            authorizer
                .authorize(&request_headers(&token, "jdx/mise"), Access::Read)
                .await
                .is_err()
        );

        let rotated_private = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let rotated_public = RsaPublicKey::from(&rotated_private);
        *jwks.write().await = serde_json::json!({"keys":[{
            "kty":"RSA",
            "use":"sig",
            "alg":"RS256",
            "kid":"rotated-key",
            "n":URL_SAFE_NO_PAD.encode(rotated_public.n().to_bytes_be()),
            "e":URL_SAFE_NO_PAD.encode(rotated_public.e().to_bytes_be())
        }]});
        let mut rotated_header = Header::new(Algorithm::RS256);
        rotated_header.kid = Some("rotated-key".into());
        let rotated_pem = rotated_private.to_pkcs8_pem(LineEnding::LF).unwrap();
        let rotated_token = encode(
            &rotated_header,
            &claims,
            &EncodingKey::from_rsa_pem(rotated_pem.as_bytes()).unwrap(),
        )
        .unwrap();
        assert!(
            authorizer
                .authorize(&request_headers(&rotated_token, "jdx/mise"), Access::Write)
                .await
                .is_ok()
        );

        *jwks.write().await = serde_json::json!({"keys":[{
            "kty":"RSA",
            "use":"sig",
            "alg":"RS256",
            "kid":"second-rotation",
            "n":URL_SAFE_NO_PAD.encode(rotated_public.n().to_bytes_be()),
            "e":URL_SAFE_NO_PAD.encode(rotated_public.e().to_bytes_be())
        }]});
        let mut second_header = Header::new(Algorithm::RS256);
        second_header.kid = Some("second-rotation".into());
        let second_token = encode(
            &second_header,
            &claims,
            &EncodingKey::from_rsa_pem(rotated_pem.as_bytes()).unwrap(),
        )
        .unwrap();
        assert!(
            authorizer
                .authorize(&request_headers(&second_token, "jdx/mise"), Access::Write)
                .await
                .is_err()
        );
        authorizer.oidc[0].min_refresh_interval = Duration::ZERO;
        assert!(
            authorizer
                .authorize(&request_headers(&second_token, "jdx/mise"), Access::Write)
                .await
                .is_ok()
        );
        server.abort();
    }

    fn request_headers(token: &str, namespace: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("mbx-cache-protocol", "1".parse().unwrap());
        headers.insert("mbx-cache-namespace", namespace.parse().unwrap());
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers
    }
}
