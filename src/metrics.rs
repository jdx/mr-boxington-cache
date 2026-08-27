use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram, info::Info},
    registry::Registry,
};
use std::{sync::Arc, time::Duration, time::Instant};

type HistogramFamily = Family<OutcomeLabels, Histogram, fn() -> Histogram>;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: &'static str,
    revision: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KindLabels {
    kind: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabels {
    outcome: &'static str,
}

pub struct Metrics {
    registry: Registry,
    blob_hits: Counter,
    blob_uploads: Counter,
    blob_pack_uploads: Counter,
    action_hits: Counter,
    action_batches: Counter,
    action_misses: Counter,
    action_commits: Counter,
    pack_requests: Family<OutcomeLabels, Counter>,
    pack_in_flight: Gauge,
    pack_blobs: Family<KindLabels, Counter>,
    pack_bytes: Family<KindLabels, Counter>,
    pack_duration: HistogramFamily,
    pack_time_to_first_byte: Histogram,
    pack_metadata_query_duration: HistogramFamily,
    pack_storage_gets: Family<OutcomeLabels, Counter>,
    pack_storage_get_duration: HistogramFamily,
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("mbx_cache");
        registry.register(
            "build",
            "Build information for the running mbx-cache process",
            Info::new(BuildLabels {
                version: env!("CARGO_PKG_VERSION"),
                revision: option_env!("MBX_CACHE_BUILD_REVISION").unwrap_or("unknown"),
            }),
        );

        let blob_hits = Counter::default();
        registry.register(
            "blob_hits",
            "Blob reads served by single or packed transfers",
            blob_hits.clone(),
        );
        let blob_uploads = Counter::default();
        registry.register(
            "blob_uploads",
            "Blob uploads accepted by the service",
            blob_uploads.clone(),
        );
        let blob_pack_uploads = Counter::default();
        registry.register(
            "blob_pack_uploads",
            "Framed multi-blob uploads accepted by the service",
            blob_pack_uploads.clone(),
        );
        let action_batches = Counter::default();
        registry.register(
            "action_batches",
            "Batched action-result lookups answered by the service",
            action_batches.clone(),
        );
        let action_hits = Counter::default();
        registry.register(
            "action_hits",
            "Action-result lookups that found a result",
            action_hits.clone(),
        );
        let action_misses = Counter::default();
        registry.register(
            "action_misses",
            "Action-result lookups that did not find a result",
            action_misses.clone(),
        );
        let action_commits = Counter::default();
        registry.register(
            "action_commits",
            "Action-result commit attempts",
            action_commits.clone(),
        );

        let pack_requests = Family::default();
        registry.register(
            "pack_requests",
            "Accepted blob-pack response streams by terminal outcome",
            pack_requests.clone(),
        );
        let pack_in_flight = Gauge::default();
        registry.register(
            "pack_in_flight",
            "Blob-pack response streams currently in flight",
            pack_in_flight.clone(),
        );
        let pack_blobs = Family::default();
        registry.register(
            "pack_blobs",
            "Unique blobs requested, served, or missing in blob packs",
            pack_blobs.clone(),
        );
        let pack_bytes = Family::default();
        registry.register(
            "pack_bytes",
            "Declared blob bytes requested or payload bytes served in blob packs",
            pack_bytes.clone(),
        );
        let pack_duration = histogram_family();
        registry.register(
            "pack_duration_seconds",
            "Blob-pack request duration through response-body completion",
            pack_duration.clone(),
        );
        let pack_time_to_first_byte = duration_histogram();
        registry.register(
            "pack_time_to_first_byte_seconds",
            "Time from entering the blob-pack handler until its first response byte",
            pack_time_to_first_byte.clone(),
        );
        let pack_metadata_query_duration = histogram_family();
        registry.register(
            "pack_metadata_query_duration_seconds",
            "Namespace visibility query duration for blob packs",
            pack_metadata_query_duration.clone(),
        );
        let pack_storage_gets = Family::default();
        registry.register(
            "pack_storage_gets",
            "Blob-store GET attempts made for blob packs",
            pack_storage_gets.clone(),
        );
        let pack_storage_get_duration = histogram_family();
        registry.register(
            "pack_storage_get_duration_seconds",
            "Time to receive blob-store GET response headers for blob packs",
            pack_storage_get_duration.clone(),
        );

        Self {
            registry,
            blob_hits,
            blob_uploads,
            blob_pack_uploads,
            action_batches,
            action_hits,
            action_misses,
            action_commits,
            pack_requests,
            pack_in_flight,
            pack_blobs,
            pack_bytes,
            pack_duration,
            pack_time_to_first_byte,
            pack_metadata_query_duration,
            pack_storage_gets,
            pack_storage_get_duration,
        }
    }

    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)?;
        Ok(buffer)
    }

    pub fn inc_blob_hit(&self) {
        self.blob_hits.inc();
    }

    pub fn inc_blob_upload(&self) {
        self.blob_uploads.inc();
    }

    /// Record one batched lookup, and the results it did and did not find.
    ///
    /// The hit and miss counters stay comparable with the single-lookup
    /// endpoint's, so a batching client does not make them read as a collapse
    /// in traffic.
    pub fn inc_action_batch(&self, hits: u64, misses: u64) {
        self.action_batches.inc();
        self.action_hits.inc_by(hits);
        self.action_misses.inc_by(misses);
    }

    pub fn inc_blob_pack_upload(&self, blobs: u64) {
        self.blob_pack_uploads.inc();
        self.pack_blobs
            .get_or_create(&KindLabels { kind: "uploaded" })
            .inc_by(blobs);
    }

    pub fn inc_action_hit(&self) {
        self.action_hits.inc();
    }

    pub fn inc_action_miss(&self) {
        self.action_misses.inc();
    }

    pub fn inc_action_commit(&self) {
        self.action_commits.inc();
    }

    pub fn observe_pack_metadata_query(&self, outcome: &'static str, duration: Duration) {
        self.pack_metadata_query_duration
            .get_or_create(&OutcomeLabels { outcome })
            .observe(duration.as_secs_f64());
    }

    pub fn observe_pack_storage_get(&self, outcome: &'static str, duration: Duration) {
        let labels = OutcomeLabels { outcome };
        self.pack_storage_gets.get_or_create(&labels).inc();
        self.pack_storage_get_duration
            .get_or_create(&labels)
            .observe(duration.as_secs_f64());
    }

    pub fn start_pack(
        self: &Arc<Self>,
        started: Instant,
        requested_blobs: u64,
        requested_bytes: u64,
        missing_blobs: u64,
    ) -> PackGuard {
        self.pack_in_flight.inc();
        self.pack_blobs
            .get_or_create(&KindLabels { kind: "requested" })
            .inc_by(requested_blobs);
        self.pack_blobs
            .get_or_create(&KindLabels { kind: "missing" })
            .inc_by(missing_blobs);
        self.pack_bytes
            .get_or_create(&KindLabels { kind: "requested" })
            .inc_by(requested_bytes);
        PackGuard {
            metrics: self.clone(),
            started,
            finished: false,
            first_byte_recorded: false,
        }
    }
}

pub struct PackGuard {
    metrics: Arc<Metrics>,
    started: Instant,
    finished: bool,
    first_byte_recorded: bool,
}

impl PackGuard {
    pub fn record_first_byte(&mut self) {
        if !self.first_byte_recorded {
            self.metrics
                .pack_time_to_first_byte
                .observe(self.started.elapsed().as_secs_f64());
            self.first_byte_recorded = true;
        }
    }

    pub fn add_served_bytes(&self, bytes: u64) {
        self.metrics
            .pack_bytes
            .get_or_create(&KindLabels { kind: "served" })
            .inc_by(bytes);
    }

    pub fn blob_served(&self) {
        self.metrics
            .pack_blobs
            .get_or_create(&KindLabels { kind: "served" })
            .inc();
    }

    pub fn complete(&mut self) {
        self.finish("completed");
    }

    pub fn error(&mut self) {
        self.finish("error");
    }

    fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        let labels = OutcomeLabels { outcome };
        self.metrics.pack_requests.get_or_create(&labels).inc();
        self.metrics
            .pack_duration
            .get_or_create(&labels)
            .observe(self.started.elapsed().as_secs_f64());
        self.metrics.pack_in_flight.dec();
    }
}

impl Drop for PackGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.finish("cancelled");
        }
    }
}

fn duration_histogram() -> Histogram {
    Histogram::new([
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
        120.0, 300.0,
    ])
}

fn histogram_family() -> HistogramFamily {
    Family::new_with_constructor(duration_histogram as fn() -> Histogram)
}
