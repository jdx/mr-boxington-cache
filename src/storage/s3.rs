use async_trait::async_trait;
use aws_sdk_s3::{Client, primitives::ByteStream};
use futures_util::StreamExt;
use http::{HeaderValue, Method};
use std::{path::Path, time::Duration};
use tokio_util::io::ReaderStream;

use super::{Blob, BlobDownload, BlobStore, DownloadMode, PutOutcome};
use crate::{config::Config, model::Digest};

const PRESIGN_EXPIRY: Duration = Duration::from_secs(300);
const CACHE_CONTROL_VALUE: &str = "private, no-store";
const CONTENT_TYPE_VALUE: &str = "application/octet-stream";

pub struct S3Store {
    client: Client,
    bucket: String,
    prefix: String,
    redirect_downloads: bool,
}

impl S3Store {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let bucket = config
            .s3_bucket
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--s3-bucket is required with S3 storage"))?;
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.s3_region.clone()));
        if let Some(endpoint) = &config.s3_endpoint {
            loader = loader.endpoint_url(endpoint);
        }
        let sdk_config = loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(config.s3_path_style)
            .build();
        Ok(Self::from_client(
            Client::from_conf(s3_config),
            bucket,
            &config.s3_prefix,
            config.s3_redirect_downloads,
        ))
    }

    fn from_client(
        client: Client,
        bucket: impl Into<String>,
        prefix: &str,
        redirect_downloads: bool,
    ) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: prefix.trim_matches('/').to_owned(),
            redirect_downloads,
        }
    }

    fn key(&self, digest: &Digest) -> String {
        format!("{}/blobs/{}", self.prefix, digest.key())
    }

    async fn head_size(&self, digest: &Digest) -> anyhow::Result<Option<u64>> {
        let output = match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.key(digest))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error)
                if error.as_service_error().is_some_and(|e| e.is_not_found())
                    || error
                        .raw_response()
                        .is_some_and(|response| response.status().as_u16() == 404) =>
            {
                return Ok(None);
            }
            // A missing object can be reported as 403 when the signer cannot
            // list the bucket. Never turn a permission failure into absence.
            Err(error) => return Err(error.into()),
        };
        let size = output
            .content_length()
            .and_then(|size| u64::try_from(size).ok())
            .ok_or_else(|| {
                anyhow::anyhow!("S3 object response is missing a valid content length")
            })?;
        if size != digest.size {
            anyhow::bail!("S3 object size does not match the requested digest");
        }
        Ok(Some(size))
    }

    async fn presigned_get(&self, digest: &Digest) -> anyhow::Result<HeaderValue> {
        let config = aws_sdk_s3::presigning::PresigningConfig::expires_in(PRESIGN_EXPIRY)
            .map_err(|_| anyhow::anyhow!("S3 presigning configuration failed"))?;
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(digest))
            .response_content_type(CONTENT_TYPE_VALUE)
            .response_cache_control(CACHE_CONTROL_VALUE)
            .presigned(config)
            .await
            .map_err(|_| anyhow::anyhow!("S3 GET presigning failed"))?;

        if request.method() != Method::GET.as_str() {
            anyhow::bail!("S3 presigned request is not a GET");
        }
        for (name, _) in request.headers() {
            if !name.eq_ignore_ascii_case("host") {
                anyhow::bail!("S3 presigned request requires an unexpected header");
            }
        }

        let uri = request.uri();
        let mut location = HeaderValue::from_str(uri)
            .map_err(|_| anyhow::anyhow!("S3 redirect location is invalid"))?;
        location.set_sensitive(true);
        Ok(location)
    }
}

#[async_trait]
impl BlobStore for S3Store {
    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key(digest))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error)
                if error.as_service_error().is_some_and(|e| e.is_no_such_key())
                    || error
                        .raw_response()
                        .is_some_and(|response| response.status().as_u16() == 404) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let size = output
            .content_length()
            .and_then(|size| u64::try_from(size).ok())
            .ok_or_else(|| anyhow::anyhow!("S3 object response is missing content length"))?;
        let stream = ReaderStream::new(output.body.into_async_read()).boxed();
        Ok(Some(Blob { size, stream }))
    }

    async fn download(
        &self,
        digest: &Digest,
        mode: DownloadMode,
    ) -> anyhow::Result<Option<BlobDownload>> {
        if mode == DownloadMode::Stream || !self.redirect_downloads {
            return Ok(self.get(digest).await?.map(BlobDownload::Stream));
        }
        let Some(size) = self.head_size(digest).await? else {
            return Ok(None);
        };
        match mode {
            DownloadMode::Head => Ok(Some(BlobDownload::Head { size })),
            DownloadMode::Redirect => {
                let location = self.presigned_get(digest).await?;
                Ok(Some(BlobDownload::Redirect { location }))
            }
            DownloadMode::Stream => unreachable!("stream was handled above"),
        }
    }

    async fn put(&self, digest: &Digest, source: &Path) -> anyhow::Result<PutOutcome> {
        let body = ByteStream::from_path(source).await?;
        match self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(digest))
            .if_none_match("*")
            .body(body)
            .send()
            .await
        {
            Ok(_) => Ok(PutOutcome::Created),
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|response| response.status().as_u16() == 412) =>
            {
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
