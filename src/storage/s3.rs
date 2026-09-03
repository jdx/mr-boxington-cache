use async_trait::async_trait;
use aws_sdk_s3::{Client, primitives::ByteStream};
use futures_util::StreamExt;
use std::path::Path;
use tokio_util::io::ReaderStream;

use super::{Blob, BlobStore, PutOutcome};
use crate::{config::Config, model::Digest};

pub struct S3Store {
    client: Client,
    bucket: String,
    prefix: String,
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
        Ok(Self {
            client: Client::from_conf(s3_config),
            bucket,
            prefix: config.s3_prefix.trim_matches('/').to_owned(),
        })
    }

    fn key(&self, digest: &Digest) -> String {
        format!("{}/blobs/{}", self.prefix, digest.key())
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
