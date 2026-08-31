use async_trait::async_trait;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

use super::{CommitOutcome, ManifestCommitOutcome, ManifestRecord, MetadataStore, SweepOutcome};
use crate::model::{ActionResult, Digest, TaskActionManifest};
use crate::storage::PutOutcome;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// How stale a blob's recorded access has to be before a read refreshes it.
const ACCESS_REFRESH_INTERVAL: &str = "1 hour";

pub struct PostgresMetadata {
    pool: PgPool,
}

impl PostgresMetadata {
    pub async fn connect(url: &str, max_connections: u32) -> anyhow::Result<Self> {
        anyhow::ensure!(
            max_connections > 0,
            "database max connections must be at least 1"
        );
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }
}

fn representable_digests(digests: &[Digest]) -> Vec<(&Digest, i64)> {
    digests
        .iter()
        .filter_map(|digest| i64::try_from(digest.size).ok().map(|size| (digest, size)))
        .collect()
}

#[async_trait]
impl MetadataStore for PostgresMetadata {
    async fn visible_blobs(
        &self,
        namespace: &str,
        digests: &[Digest],
    ) -> anyhow::Result<Vec<Digest>> {
        let digests = representable_digests(digests);
        if digests.is_empty() {
            return Ok(Vec::new());
        }
        let algorithms = digests
            .iter()
            .map(|(digest, _)| digest.algorithm.to_string())
            .collect::<Vec<_>>();
        let hashes = digests
            .iter()
            .map(|(digest, _)| digest.hash.clone())
            .collect::<Vec<_>>();
        let sizes = digests.iter().map(|(_, size)| *size).collect::<Vec<_>>();
        let rows = sqlx::query(
            "SELECT requested.ordinality \
             FROM UNNEST($2::text[], $3::text[], $4::bigint[]) WITH ORDINALITY \
                  AS requested(algorithm, hash, size, ordinality) \
             JOIN namespace_blobs AS blobs \
               ON blobs.algorithm = requested.algorithm \
              AND blobs.hash = requested.hash \
              AND blobs.size = requested.size \
             WHERE blobs.namespace = $1 \
             ORDER BY requested.ordinality",
        )
        .bind(namespace)
        .bind(algorithms)
        .bind(hashes)
        .bind(sizes)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let ordinal: i64 = row.try_get("ordinality")?;
                let index = usize::try_from(ordinal - 1)?;
                digests
                    .get(index)
                    .map(|(digest, _)| (*digest).clone())
                    .ok_or_else(|| anyhow::anyhow!("database returned an invalid blob ordinal"))
            })
            .collect()
    }

    async fn sweep(&self, older_than_days: u32) -> anyhow::Result<SweepOutcome> {
        let age = format!("{older_than_days} days");
        // One transaction, so no reader sees a result whose blobs this sweep
        // has already removed. Blobs go first; the dangling query below then
        // observes the post-delete state.
        let mut transaction = self.pool.begin().await?;
        let blobs =
            sqlx::query("DELETE FROM namespace_blobs WHERE created_at < now() - $1::interval")
                .bind(&age)
                .execute(&mut *transaction)
                .await?
                .rows_affected();

        // A result whose objects are gone can only produce a miss, and it costs
        // a wasted round trip every time a client asks for it. Only the
        // top-level objects are checked: a directory that lost a nested object
        // still restores as a miss, which is safe.
        let action_results = sqlx::query(
            "DELETE FROM action_results AS results \
             WHERE EXISTS ( \
               SELECT 1 \
               FROM (VALUES (results.result -> 'metadata'), \
                            (results.result -> 'output_root')) AS referenced(digest) \
               WHERE jsonb_typeof(referenced.digest) = 'object' \
                 AND NOT EXISTS ( \
                   SELECT 1 FROM namespace_blobs AS blobs \
                   WHERE blobs.namespace = results.namespace \
                     AND blobs.algorithm = referenced.digest ->> 'algorithm' \
                     AND blobs.hash = referenced.digest ->> 'hash' \
                     AND blobs.size = (referenced.digest ->> 'size')::bigint \
                 ) \
             )",
        )
        .execute(&mut *transaction)
        .await?
        .rows_affected();

        // Manifests only predict actions, so an old one costs a cold prefetch
        // rather than a wrong answer. They are aged out on their own use.
        let manifests =
            sqlx::query("DELETE FROM action_manifests WHERE updated_at < now() - $1::interval")
                .bind(&age)
                .execute(&mut *transaction)
                .await?
                .rows_affected();

        transaction.commit().await?;
        Ok(SweepOutcome {
            blobs,
            action_results,
            manifests,
        })
    }

    async fn touch_blobs(&self, namespace: &str, digests: &[Digest]) -> anyhow::Result<()> {
        let digests = representable_digests(digests);
        if digests.is_empty() {
            return Ok(());
        }
        let algorithms = digests
            .iter()
            .map(|(digest, _)| digest.algorithm.to_string())
            .collect::<Vec<_>>();
        let hashes = digests
            .iter()
            .map(|(digest, _)| digest.hash.clone())
            .collect::<Vec<_>>();
        let sizes = digests.iter().map(|(_, size)| *size).collect::<Vec<_>>();
        // Skip blobs touched recently so a frequently served blob costs at most
        // one write per interval rather than one per read.
        sqlx::query(
            "UPDATE namespace_blobs AS blobs \
             SET last_accessed_at = now() \
             FROM UNNEST($2::text[], $3::text[], $4::bigint[]) \
                  AS requested(algorithm, hash, size) \
             WHERE blobs.namespace = $1 \
               AND blobs.algorithm = requested.algorithm \
               AND blobs.hash = requested.hash \
               AND blobs.size = requested.size \
               AND blobs.last_accessed_at < now() - $5::interval",
        )
        .bind(namespace)
        .bind(algorithms)
        .bind(hashes)
        .bind(sizes)
        .bind(ACCESS_REFRESH_INTERVAL)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn register_blob(
        &self,
        namespace: &str,
        digest: &Digest,
        stored: PutOutcome,
    ) -> anyhow::Result<()> {
        // `created_at` has to track the object's age in storage, since that is
        // what a lifecycle rule expires and what `sweep` follows. An upload
        // that wrote the object restarts both, so the row follows it; one
        // refused because the object was already there restarts neither, and
        // moving the row forward would leave it visible long after the
        // lifecycle deleted the object it names.
        //
        // A first registration of an object another namespace uploaded earlier
        // is the one case this cannot get right: the row starts at now() while
        // the object is already partway to its expiry, so it can outlive the
        // object by up to the retention window. The next sweep after that
        // removes it.
        sqlx::query(
            "INSERT INTO namespace_blobs (namespace, algorithm, hash, size) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (namespace, algorithm, hash, size) DO UPDATE \
             SET created_at = CASE WHEN $5 THEN now() ELSE namespace_blobs.created_at END, \
                 last_accessed_at = now()",
        )
        .bind(namespace)
        .bind(digest.algorithm.to_string())
        .bind(&digest.hash)
        .bind(digest.size as i64)
        .bind(matches!(stored, PutOutcome::Created))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get(&self, namespace: &str, action: &Digest) -> anyhow::Result<Option<ActionResult>> {
        let row = sqlx::query("SELECT result FROM action_results WHERE namespace = $1 AND algorithm = $2 AND hash = $3 AND size = $4")
            .bind(namespace).bind(action.algorithm.to_string()).bind(&action.hash).bind(action.size as i64)
            .fetch_optional(&self.pool).await?;
        row.map(|row| serde_json::from_value(row.get("result")).map_err(Into::into))
            .transpose()
    }

    async fn get_batch(
        &self,
        namespace: &str,
        actions: &[Digest],
    ) -> anyhow::Result<Vec<ActionResult>> {
        let actions = representable_digests(actions);
        if actions.is_empty() {
            return Ok(Vec::new());
        }
        let algorithms = actions
            .iter()
            .map(|(digest, _)| digest.algorithm.to_string())
            .collect::<Vec<_>>();
        let hashes = actions
            .iter()
            .map(|(digest, _)| digest.hash.clone())
            .collect::<Vec<_>>();
        let sizes = actions.iter().map(|(_, size)| *size).collect::<Vec<_>>();
        // Keep this as one round trip, but make every requested digest an
        // explicit primary-key probe. A join against the whole action-results
        // relation can turn into a hash join as a namespace grows, scanning and
        // decoding every cached row to answer a few hundred requested keys.
        // The correlated scalar subquery is unique by the primary key and keeps
        // the cost proportional to the request instead of the namespace.
        let rows = sqlx::query(
            "SELECT ( \
                 SELECT results.result \
                 FROM action_results AS results \
                 WHERE results.namespace = $1 \
                   AND results.algorithm = requested.algorithm \
                   AND results.hash = requested.hash \
                   AND results.size = requested.size \
             ) AS result \
             FROM UNNEST($2::text[], $3::text[], $4::bigint[]) WITH ORDINALITY \
                  AS requested(algorithm, hash, size, ordinality) \
             ORDER BY requested.ordinality",
        )
        .bind(namespace)
        .bind(algorithms)
        .bind(hashes)
        .bind(sizes)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .filter_map(
                |row| match row.try_get::<Option<serde_json::Value>, _>("result") {
                    Ok(Some(result)) => Some(serde_json::from_value(result).map_err(Into::into)),
                    Ok(None) => None,
                    Err(error) => Some(Err(error.into())),
                },
            )
            .collect()
    }

    async fn commit(
        &self,
        namespace: &str,
        action: &Digest,
        result: &ActionResult,
    ) -> anyhow::Result<CommitOutcome> {
        let encoded = serde_json::to_value(result)?;
        let inserted = sqlx::query("INSERT INTO action_results (namespace, algorithm, hash, size, result) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
            .bind(namespace).bind(action.algorithm.to_string()).bind(&action.hash).bind(action.size as i64).bind(&encoded)
            .execute(&self.pool).await?.rows_affected();
        if inserted == 1 {
            return Ok(CommitOutcome::Created);
        }
        let existing = self
            .get(namespace, action)
            .await?
            .ok_or_else(|| anyhow::anyhow!("action result disappeared after conflict"))?;
        if serde_json::to_value(existing)? == encoded {
            Ok(CommitOutcome::AlreadyExists)
        } else {
            Ok(CommitOutcome::Conflict)
        }
    }

    async fn get_manifest(
        &self,
        namespace: &str,
        key: &Digest,
    ) -> anyhow::Result<Option<ManifestRecord>> {
        let row = sqlx::query("SELECT etag, manifest FROM action_manifests WHERE namespace = $1 AND algorithm = $2 AND hash = $3 AND size = $4")
            .bind(namespace).bind(key.algorithm.to_string()).bind(&key.hash).bind(key.size as i64)
            .fetch_optional(&self.pool).await?;
        row.map(|row| {
            Ok(ManifestRecord {
                etag: row.get("etag"),
                manifest: serde_json::from_value(row.get("manifest"))?,
            })
        })
        .transpose()
    }

    async fn commit_manifest(
        &self,
        namespace: &str,
        key: &Digest,
        expected_etag: Option<&str>,
        etag: &str,
        manifest: &TaskActionManifest,
    ) -> anyhow::Result<ManifestCommitOutcome> {
        let manifest = serde_json::to_value(manifest)?;
        let rows = if let Some(expected_etag) = expected_etag {
            sqlx::query("UPDATE action_manifests SET etag = $5, manifest = $6, updated_at = now() WHERE namespace = $1 AND algorithm = $2 AND hash = $3 AND size = $4 AND etag = $7")
                .bind(namespace).bind(key.algorithm.to_string()).bind(&key.hash).bind(key.size as i64)
                .bind(etag).bind(&manifest).bind(expected_etag)
                .execute(&self.pool).await?.rows_affected()
        } else {
            sqlx::query("INSERT INTO action_manifests (namespace, algorithm, hash, size, etag, manifest) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING")
                .bind(namespace).bind(key.algorithm.to_string()).bind(&key.hash).bind(key.size as i64)
                .bind(etag).bind(&manifest)
                .execute(&self.pool).await?.rows_affected()
        };
        Ok(if rows == 0 {
            ManifestCommitOutcome::PreconditionFailed
        } else if expected_etag.is_some() {
            ManifestCommitOutcome::Updated
        } else {
            ManifestCommitOutcome::Created
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::model::Algorithm;

    /// Connect to the database CI provides for the tests below.
    ///
    /// A local run without one skips them, but CI must never skip silently:
    /// these are the only coverage this backend has.
    async fn store() -> Option<PostgresMetadata> {
        match std::env::var("MBX_CACHE_TEST_DATABASE_URL") {
            Ok(url) => Some(
                PostgresMetadata::connect(&url, 4)
                    .await
                    .expect("the test database should accept connections"),
            ),
            Err(_) => {
                assert!(
                    std::env::var_os("CI").is_none(),
                    "MBX_CACHE_TEST_DATABASE_URL must be set so CI exercises this backend"
                );
                eprintln!(
                    "skipping: set MBX_CACHE_TEST_DATABASE_URL to run the PostgreSQL metadata tests"
                );
                None
            }
        }
    }

    #[tokio::test]
    async fn an_empty_connection_pool_is_rejected_before_connecting() {
        let error = PostgresMetadata::connect("postgres://unreachable.invalid/cache", 0)
            .await
            .err()
            .expect("a zero-sized pool must be rejected");
        assert_eq!(
            error.to_string(),
            "database max connections must be at least 1"
        );
    }

    /// A namespace no other test shares, so one database serves them all.
    fn namespace(label: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!(
            "test/{label}/{}/{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn test_digest(fill: &str, size: u64) -> Digest {
        Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: fill.repeat(64),
            size,
        }
    }

    fn action_result(action: &Digest, metadata: Option<&Digest>) -> ActionResult {
        ActionResult {
            action: action.clone(),
            metadata: metadata.cloned(),
            output_root: None,
            version: 1,
        }
    }

    /// The recorded access as text, so a test can assert it did not move.
    async fn access_timestamp(store: &PostgresMetadata, namespace: &str) -> String {
        sqlx::query(
            "SELECT last_accessed_at::text AS stamp FROM namespace_blobs WHERE namespace = $1",
        )
        .bind(namespace)
        .fetch_one(&store.pool)
        .await
        .expect("the blob row should exist")
        .try_get::<String, _>("stamp")
        .expect("stamp should be text")
    }

    async fn access_age_seconds(store: &PostgresMetadata, namespace: &str) -> f64 {
        sqlx::query(
            "SELECT EXTRACT(EPOCH FROM (now() - last_accessed_at))::float8 AS age \
             FROM namespace_blobs WHERE namespace = $1",
        )
        .bind(namespace)
        .fetch_one(&store.pool)
        .await
        .expect("the blob row should exist")
        .try_get::<f64, _>("age")
        .expect("age should be a float")
    }

    #[test]
    fn embedded_migration_versions_are_unique() {
        let mut versions = BTreeSet::new();
        for migration in MIGRATOR.iter() {
            assert!(
                versions.insert(migration.version),
                "migration version {} is duplicated",
                migration.version
            );
        }
    }

    #[test]
    fn unrepresentable_blob_sizes_are_not_queried() {
        let representable = Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: "0".repeat(64),
            size: i64::MAX as u64,
        };
        let unrepresentable = Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: "1".repeat(64),
            size: i64::MAX as u64 + 1,
        };

        assert_eq!(
            representable_digests(&[representable.clone(), unrepresentable]),
            vec![(&representable, i64::MAX)]
        );
    }
    /// A sweep deletes across every namespace, so tests that age rows and count
    /// what went have to take turns.
    static SWEEP: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Age a namespace's rows so the sweep sees them as expired.
    async fn backdate(store: &PostgresMetadata, namespace: &str, days: u32) {
        for statement in [
            "UPDATE namespace_blobs SET created_at = now() - ($2 || ' days')::interval WHERE namespace = $1",
            "UPDATE action_results SET created_at = now() - ($2 || ' days')::interval WHERE namespace = $1",
            "UPDATE action_manifests SET updated_at = now() - ($2 || ' days')::interval WHERE namespace = $1",
        ] {
            sqlx::query(statement)
                .bind(namespace)
                .bind(days.to_string())
                .execute(&store.pool)
                .await
                .expect("backdating should succeed");
        }
    }

    #[tokio::test]
    async fn a_sweep_leaves_recent_metadata_alone() {
        let Some(store) = store().await else { return };
        let _serialized = SWEEP.lock().await;
        let namespace = namespace("sweep-recent");
        let action = test_digest("2", 10);
        store
            .register_blob(&namespace, &action, PutOutcome::Created)
            .await
            .unwrap();
        store
            .commit(&namespace, &action, &action_result(&action, None))
            .await
            .unwrap();

        let swept = store.sweep(30).await.unwrap();

        assert_eq!(swept.blobs, 0, "a fresh blob is not expired");
        assert!(store.blob_visible(&namespace, &action).await.unwrap());
        assert!(store.get(&namespace, &action).await.unwrap().is_some());
        let _ = swept.manifests;
    }

    #[tokio::test]
    async fn a_sweep_drops_expired_blobs_and_the_results_left_dangling() {
        let Some(store) = store().await else { return };
        let _serialized = SWEEP.lock().await;
        let namespace = namespace("sweep-expired");
        let action = test_digest("3", 20);
        let metadata = test_digest("4", 30);
        store
            .register_blob(&namespace, &action, PutOutcome::Created)
            .await
            .unwrap();
        store
            .register_blob(&namespace, &metadata, PutOutcome::Created)
            .await
            .unwrap();
        store
            .commit(
                &namespace,
                &action,
                &action_result(&action, Some(&metadata)),
            )
            .await
            .unwrap();
        backdate(&store, &namespace, 40).await;

        let swept = store.sweep(30).await.unwrap();

        assert_eq!(swept.blobs, 2, "both expired blobs go");
        assert_eq!(
            swept.action_results, 1,
            "the result referencing them cannot survive them"
        );
        assert!(!store.blob_visible(&namespace, &action).await.unwrap());
        assert!(store.get(&namespace, &action).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_sweep_keeps_a_blob_that_was_uploaded_again() {
        let Some(store) = store().await else { return };
        let _serialized = SWEEP.lock().await;
        let namespace = namespace("sweep-reuploaded");
        let blob = test_digest("9", 70);
        store
            .register_blob(&namespace, &blob, PutOutcome::Created)
            .await
            .unwrap();
        backdate(&store, &namespace, 40).await;

        // Storage expired the object and the upload wrote it again, so its age
        // there restarted and the row has to follow.
        store
            .register_blob(&namespace, &blob, PutOutcome::Created)
            .await
            .unwrap();

        assert_eq!(
            store.sweep(30).await.unwrap().blobs,
            0,
            "a re-uploaded object is not expired"
        );
        assert!(store.blob_visible(&namespace, &blob).await.unwrap());
    }

    #[tokio::test]
    async fn a_sweep_expires_a_blob_the_upload_did_not_rewrite() {
        let Some(store) = store().await else { return };
        let _serialized = SWEEP.lock().await;
        let namespace = namespace("sweep-upload-refused");
        let blob = test_digest("9", 80);
        store
            .register_blob(&namespace, &blob, PutOutcome::Created)
            .await
            .unwrap();
        backdate(&store, &namespace, 40).await;

        // The put was refused because the object was already there, so its age
        // in storage did not move and neither may the row's. Refreshing here
        // would keep the row past the lifecycle rule that deletes the object.
        store
            .register_blob(&namespace, &blob, PutOutcome::AlreadyExists)
            .await
            .unwrap();

        assert_eq!(
            store.sweep(30).await.unwrap().blobs,
            1,
            "an upload that rewrote nothing does not extend the row"
        );
        assert!(!store.blob_visible(&namespace, &blob).await.unwrap());
    }

    #[tokio::test]
    async fn a_sweep_keeps_a_result_whose_objects_are_still_there() {
        let Some(store) = store().await else { return };
        let _serialized = SWEEP.lock().await;
        let namespace = namespace("sweep-live-result");
        let action = test_digest("5", 40);
        let metadata = test_digest("6", 50);
        store
            .register_blob(&namespace, &action, PutOutcome::Created)
            .await
            .unwrap();
        store
            .register_blob(&namespace, &metadata, PutOutcome::Created)
            .await
            .unwrap();
        store
            .commit(
                &namespace,
                &action,
                &action_result(&action, Some(&metadata)),
            )
            .await
            .unwrap();
        // Age only the result. Its objects are current, so it is still useful.
        sqlx::query(
            "UPDATE action_results SET created_at = now() - interval '40 days' WHERE namespace = $1",
        )
        .bind(&namespace)
        .execute(&store.pool)
        .await
        .unwrap();

        let swept = store.sweep(30).await.unwrap();

        assert_eq!(
            swept.action_results, 0,
            "age alone does not expire a result"
        );
        assert!(store.get(&namespace, &action).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_sweep_expires_manifests_by_their_last_update() {
        let Some(store) = store().await else { return };
        let _serialized = SWEEP.lock().await;
        let namespace = namespace("sweep-manifests");
        let key = test_digest("7", 60);
        let manifest = TaskActionManifest {
            predictions: Vec::new(),
            task: "8".repeat(64),
            version: 1,
        };
        store
            .commit_manifest(&namespace, &key, None, "etag-1", &manifest)
            .await
            .unwrap();

        assert_eq!(store.sweep(30).await.unwrap().manifests, 0);
        backdate(&store, &namespace, 40).await;
        assert_eq!(store.sweep(30).await.unwrap().manifests, 1);
        assert!(
            store
                .get_manifest(&namespace, &key)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn blobs_are_visible_only_inside_their_namespace() {
        let Some(store) = store().await else { return };
        let mine = namespace("blobs-mine");
        let theirs = namespace("blobs-theirs");
        let present = test_digest("a", 10);
        let absent = test_digest("b", 20);

        store
            .register_blob(&mine, &present, PutOutcome::Created)
            .await
            .unwrap();

        assert_eq!(
            store
                .visible_blobs(&mine, &[present.clone(), absent.clone()])
                .await
                .unwrap(),
            vec![present.clone()],
            "only the registered blob is visible"
        );
        assert!(
            store
                .visible_blobs(&theirs, std::slice::from_ref(&present))
                .await
                .unwrap()
                .is_empty(),
            "another namespace must not see it"
        );
        assert!(store.blob_visible(&mine, &present).await.unwrap());
        assert!(!store.blob_visible(&mine, &absent).await.unwrap());
    }

    #[tokio::test]
    async fn serving_a_blob_refreshes_a_stale_access_time() {
        let Some(store) = store().await else { return };
        let namespace = namespace("touch-stale");
        let blob = test_digest("c", 30);
        store
            .register_blob(&namespace, &blob, PutOutcome::Created)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE namespace_blobs SET last_accessed_at = now() - interval '3 days' \
             WHERE namespace = $1",
        )
        .bind(&namespace)
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(access_age_seconds(&store, &namespace).await > 86_400.0);

        let before = access_timestamp(&store, &namespace).await;

        store.touch_blobs(&namespace, &[blob]).await.unwrap();

        assert_ne!(
            before,
            access_timestamp(&store, &namespace).await,
            "a served blob should record the access"
        );
        assert!(
            access_age_seconds(&store, &namespace).await < 60.0,
            "and the recorded access should be recent"
        );
    }

    #[tokio::test]
    async fn serving_a_blob_again_leaves_a_fresh_access_time_alone() {
        let Some(store) = store().await else { return };
        let namespace = namespace("touch-fresh");
        let blob = test_digest("d", 40);
        // Registration already recorded now(), so a read is inside the refresh
        // interval and must not write.
        store
            .register_blob(&namespace, &blob, PutOutcome::Created)
            .await
            .unwrap();

        let before = access_timestamp(&store, &namespace).await;
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        store.touch_blobs(&namespace, &[blob]).await.unwrap();
        let after = access_timestamp(&store, &namespace).await;

        assert_eq!(
            before, after,
            "a blob served inside the refresh interval must cost no write"
        );
    }

    #[tokio::test]
    async fn an_action_result_commits_once_and_reports_conflicts() {
        let Some(store) = store().await else { return };
        let elsewhere = namespace("actions-elsewhere");
        let namespace = namespace("actions");
        let action = test_digest("e", 50);
        let result = action_result(&action, None);

        assert_eq!(
            store.commit(&namespace, &action, &result).await.unwrap(),
            CommitOutcome::Created
        );
        assert_eq!(
            store.commit(&namespace, &action, &result).await.unwrap(),
            CommitOutcome::AlreadyExists,
            "committing identical bytes is not a conflict"
        );
        assert_eq!(
            store
                .commit(
                    &namespace,
                    &action,
                    &action_result(&action, Some(&test_digest("f", 60)))
                )
                .await
                .unwrap(),
            CommitOutcome::Conflict,
            "a different result for the same action is a conflict"
        );

        let stored = store.get(&namespace, &action).await.unwrap().unwrap();
        assert_eq!(stored.action, action);
        assert!(stored.metadata.is_none(), "the first commit wins");
        assert!(
            store.get(&elsewhere, &action).await.unwrap().is_none(),
            "action results do not leak across namespaces"
        );
    }

    #[tokio::test]
    async fn a_batched_lookup_returns_only_the_results_a_namespace_holds() {
        let Some(store) = store().await else { return };
        let elsewhere = namespace("action-batch-elsewhere");
        let namespace = namespace("action-batch");
        let first = test_digest("1", 51);
        let second = test_digest("2", 52);
        let absent = test_digest("3", 53);
        for action in [&first, &second] {
            store
                .commit(&namespace, action, &action_result(action, None))
                .await
                .unwrap();
        }
        // Held by another namespace, so this one must not see it.
        store
            .commit(&elsewhere, &absent, &action_result(&absent, None))
            .await
            .unwrap();

        let results = store
            .get_batch(
                &namespace,
                &[first.clone(), absent.clone(), second.clone(), first.clone()],
            )
            .await
            .unwrap();

        let found: Vec<&Digest> = results.iter().map(|result| &result.action).collect();
        assert_eq!(found, vec![&first, &second, &first]);
        assert!(
            store
                .get_batch(&namespace, &[absent])
                .await
                .unwrap()
                .is_empty(),
            "a batch answers for nothing it does not hold"
        );
        assert!(
            store.get_batch(&namespace, &[]).await.unwrap().is_empty(),
            "an empty batch asks the database nothing"
        );
    }

    #[tokio::test]
    async fn manifest_commits_respect_the_expected_etag() {
        let Some(store) = store().await else { return };
        let namespace = namespace("manifests");
        let key = test_digest("1", 70);
        let manifest = TaskActionManifest {
            predictions: Vec::new(),
            task: "2".repeat(64),
            version: 1,
        };

        assert_eq!(
            store
                .commit_manifest(&namespace, &key, None, "etag-1", &manifest)
                .await
                .unwrap(),
            ManifestCommitOutcome::Created
        );
        assert_eq!(
            store
                .commit_manifest(&namespace, &key, Some("etag-1"), "etag-2", &manifest)
                .await
                .unwrap(),
            ManifestCommitOutcome::Updated
        );
        assert_eq!(
            store
                .commit_manifest(&namespace, &key, Some("etag-1"), "etag-3", &manifest)
                .await
                .unwrap(),
            ManifestCommitOutcome::PreconditionFailed,
            "a stale etag must not overwrite a newer manifest"
        );

        assert_eq!(
            store
                .get_manifest(&namespace, &key)
                .await
                .unwrap()
                .unwrap()
                .etag,
            "etag-2"
        );
    }
}
