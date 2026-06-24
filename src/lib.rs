//! A [`metrics`] recorder that emits aggregated periodic snapshots — and, optionally, filtered
//! per-call events — as structured [`tracing`] events.
//!
//! # Two complementary outputs
//!
//! * **Snapshot (always on):** every metric is stored in an in-process registry. A background
//!   poller drains it on a fixed interval and emits one `event="snapshot"` tracing event per
//!   metric (`count, sum, p50, p90, p99` for histograms; `value` for counters/gauges). Histogram
//!   storage is bounded by draining raw observations into a per-key DDSketch each tick, so memory
//!   is bounded by the number of distinct keys, not observation volume.
//! * **Per-call stream (opt-in):** when a [`StreamFilter`] is configured, keys that pass the filter
//!   also emit an `event="emit"` tracing event on every update. Histograms only stream when their
//!   key matches an explicit histogram allow set, so a hot histogram can't become a firehose.
//!
//! # Fixed target, runtime scope
//!
//! `tracing` targets must be compile-time literals, so the target is the fixed const `"metrics"`
//! and the event name is `"metric"`. The logical scope the consumer cares about (`"server"`,
//! `"client"`, ...) is carried as the runtime field `scope`.
//!
//! # Composition
//!
//! [`TracingRecorder::builder`]`().build()` returns the recorder and its [`Poller`] separately so
//! the recorder can be fanned out alongside other recorders before `set_global_recorder`. The
//! [`Builder::install`] convenience does build + set-global + spawn in one call.
//!
//! ```no_run
//! use std::time::Duration;
//! use metrics_exporter_tracing::TracingRecorder;
//!
//! TracingRecorder::builder()
//!     .scope("server")
//!     .snapshot_interval(Duration::from_secs(3))
//!     .install()
//!     .unwrap();
//! ```

mod emit;
#[cfg(feature = "stream")]
mod filter;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};
use metrics_util::storage::Summary;
use tracing::Level;

#[cfg(feature = "stream")]
pub use filter::{StreamFilter, StreamFilterBuilder};

use emit::TARGET;
#[cfg(feature = "stream")]
use emit::{StreamContext, StreamingCounter, StreamingGauge, StreamingHistogram};

/// One computed metric sample produced by a snapshot poll. Exposed so consumers (and tests) can
/// observe what a tick would emit without capturing tracing output.
#[derive(Debug, Clone, PartialEq)]
pub enum Sample {
    Counter {
        metric: String,
        value: u64,
    },
    Gauge {
        metric: String,
        value: f64,
    },
    Histogram {
        metric: String,
        count: usize,
        sum: f64,
        p50: f64,
        p90: f64,
        p99: f64,
    },
}

/// Registry-backed [`metrics::Recorder`] that emits structured tracing events.
pub struct TracingRecorder {
    inner: Arc<Inner>,
}

/// Shared registry plus the optional streaming context. Held by both the recorder and its poller.
struct Inner {
    registry: Registry<Key, AtomicStorage>,
    // Summary exposes no sum accessor, so we accumulate the running sum beside each sketch.
    summaries: Mutex<HashMap<Key, (Summary, f64)>>,
    #[cfg(feature = "stream")]
    stream: Option<Arc<StreamContext>>,
}

#[cfg(feature = "stream")]
impl Inner {
    fn streaming(&self, metric: &str, is_histogram: bool) -> Option<Arc<StreamContext>> {
        let ctx = self.stream.as_ref()?;
        let allowed = if is_histogram {
            ctx.filter.allows_histogram(metric)
        } else {
            ctx.filter.allows(metric)
        };
        allowed.then(|| ctx.clone())
    }
}

fn render_key(key: &Key) -> String {
    let mut labels = key.labels().peekable();
    if labels.peek().is_none() {
        return key.name().to_string();
    }
    let rendered: Vec<String> = labels
        .map(|l| format!("{}={}", l.key(), l.value()))
        .collect();
    format!("{}{{{}}}", key.name(), rendered.join(","))
}

impl Recorder for TracingRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        #[cfg(feature = "stream")]
        {
            let rendered = render_key(key);
            // Wrap the registry's own Arc so streamed updates and snapshots see the same atomic.
            if let Some(ctx) = self.inner.streaming(&rendered, false) {
                return self.inner.registry.get_or_create_counter(key, |c| {
                    Counter::from_arc(Arc::new(StreamingCounter::new(
                        c.clone(),
                        rendered.into(),
                        ctx,
                    )))
                });
            }
        }
        self.inner
            .registry
            .get_or_create_counter(key, |c| Counter::from_arc(c.clone()))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        #[cfg(feature = "stream")]
        {
            let rendered = render_key(key);
            if let Some(ctx) = self.inner.streaming(&rendered, false) {
                return self.inner.registry.get_or_create_gauge(key, |g| {
                    Gauge::from_arc(Arc::new(StreamingGauge::new(
                        g.clone(),
                        rendered.into(),
                        ctx,
                    )))
                });
            }
        }
        self.inner
            .registry
            .get_or_create_gauge(key, |g| Gauge::from_arc(g.clone()))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        #[cfg(feature = "stream")]
        {
            let rendered = render_key(key);
            if let Some(ctx) = self.inner.streaming(&rendered, true) {
                return self.inner.registry.get_or_create_histogram(key, |h| {
                    Histogram::from_arc(Arc::new(StreamingHistogram::new(
                        h.clone(),
                        rendered.into(),
                        ctx,
                    )))
                });
            }
        }
        self.inner
            .registry
            .get_or_create_histogram(key, |h| Histogram::from_arc(h.clone()))
    }
}

/// Builds a [`TracingRecorder`] and its [`Poller`].
pub struct Builder {
    interval: Duration,
    level: Level,
    scope: String,
    #[cfg(feature = "stream")]
    stream: Option<StreamFilter>,
    on_tick: Option<Box<dyn Fn() + Send + 'static>>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3),
            level: Level::INFO,
            scope: String::new(),
            #[cfg(feature = "stream")]
            stream: None,
            on_tick: None,
        }
    }
}

impl Builder {
    /// Interval between snapshot polls (and `on_tick` calls). Default 3s.
    pub fn snapshot_interval(mut self, d: Duration) -> Self {
        self.interval = d;
        self
    }

    /// Level for every emitted event. Default `INFO`. Only `INFO`/`DEBUG`/`TRACE` are distinct;
    /// higher levels fall back to `INFO` (tracing requires a literal level).
    pub fn level(mut self, l: Level) -> Self {
        self.level = l;
        self
    }

    /// Value of the runtime `scope` field on every event. Default `""`.
    pub fn scope(mut self, s: impl Into<String>) -> Self {
        self.scope = s.into();
        self
    }

    /// Enable the per-call event stream, filtered by `f`.
    #[cfg(feature = "stream")]
    pub fn stream(mut self, f: StreamFilter) -> Self {
        self.stream = Some(f);
        self
    }

    /// Run `f` at the start of every tick, before the snapshot emit, so pull-style metrics
    /// (process/heap) can be refreshed into the registry first. Without this, pull metrics never
    /// update.
    pub fn on_tick(mut self, f: impl Fn() + Send + 'static) -> Self {
        self.on_tick = Some(Box::new(f));
        self
    }

    /// Build the recorder and poller for manual composition (e.g. behind a `Fanout`).
    pub fn build(self) -> (TracingRecorder, Poller) {
        let scope: Arc<str> = Arc::from(self.scope.as_str());
        #[cfg(feature = "stream")]
        let stream = self.stream.map(|filter| {
            Arc::new(StreamContext {
                level: self.level,
                scope: scope.clone(),
                filter: Arc::new(filter),
            })
        });
        let inner = Arc::new(Inner {
            registry: Registry::atomic(),
            summaries: Mutex::new(HashMap::new()),
            #[cfg(feature = "stream")]
            stream,
        });
        let recorder = TracingRecorder {
            inner: inner.clone(),
        };
        let poller = Poller {
            inner,
            interval: self.interval,
            level: self.level,
            scope,
            on_tick: self.on_tick,
        };
        (recorder, poller)
    }

    /// Build, install as the global recorder, and spawn the poller thread.
    pub fn install(self) -> Result<(), BuildError> {
        let (recorder, poller) = self.build();
        metrics::set_global_recorder(recorder).map_err(|_| BuildError::AlreadyInstalled)?;
        poller.spawn()?;
        Ok(())
    }
}

impl TracingRecorder {
    /// Start configuring a recorder.
    pub fn builder() -> Builder {
        Builder::default()
    }
}

/// Drives the periodic snapshot. Holds the registry `Arc` so it can snapshot even when the recorder
/// itself is not the global recorder (the consumer may have fanned it out, or installed only the
/// `on_tick` refresh path).
pub struct Poller {
    inner: Arc<Inner>,
    interval: Duration,
    level: Level,
    scope: Arc<str>,
    on_tick: Option<Box<dyn Fn() + Send + 'static>>,
}

impl Poller {
    /// Compute the current snapshot, draining histogram buckets into their per-key sketches.
    /// Exposed for tests so a tick's output can be asserted without capturing tracing.
    pub fn poll_once(&self) -> Vec<Sample> {
        let mut samples = Vec::new();

        for (key, counter) in self.inner.registry.get_counter_handles() {
            samples.push(Sample::Counter {
                metric: render_key(&key),
                value: counter.load(Ordering::Relaxed),
            });
        }

        for (key, gauge) in self.inner.registry.get_gauge_handles() {
            samples.push(Sample::Gauge {
                metric: render_key(&key),
                value: f64::from_bits(gauge.load(Ordering::Relaxed)),
            });
        }

        let mut summaries = self.inner.summaries.lock().expect("summaries lock");
        for (key, bucket) in self.inner.registry.get_histogram_handles() {
            let (summary, sum) = summaries
                .entry(key.clone())
                .or_insert_with(|| (Summary::with_defaults(), 0.0));
            bucket.clear_with(|block| {
                for v in block {
                    summary.add(*v);
                    *sum += *v;
                }
            });
            if summary.count() == 0 {
                continue;
            }
            samples.push(Sample::Histogram {
                metric: render_key(&key),
                count: summary.count(),
                sum: *sum,
                p50: summary.quantile(0.5).unwrap_or(f64::NAN),
                p90: summary.quantile(0.9).unwrap_or(f64::NAN),
                p99: summary.quantile(0.99).unwrap_or(f64::NAN),
            });
        }

        samples
    }

    fn emit_snapshot(&self) {
        for sample in self.poll_once() {
            match sample {
                Sample::Counter { metric, value } => {
                    emit_snapshot_counter(self.level, &self.scope, &metric, value)
                }
                Sample::Gauge { metric, value } => {
                    emit_snapshot_gauge(self.level, &self.scope, &metric, value)
                }
                Sample::Histogram {
                    metric,
                    count,
                    sum,
                    p50,
                    p90,
                    p99,
                } => emit_snapshot_histogram(
                    self.level,
                    &self.scope,
                    &metric,
                    count,
                    sum,
                    p50,
                    p90,
                    p99,
                ),
            }
        }
    }

    /// Spawn the daemon thread that runs `on_tick` then emits a snapshot every interval.
    pub fn spawn(self) -> std::io::Result<()> {
        thread::Builder::new()
            .name("metrics-snapshot".into())
            .spawn(move || loop {
                thread::sleep(self.interval);
                if let Some(tick) = &self.on_tick {
                    tick();
                }
                self.emit_snapshot();
            })?;
        Ok(())
    }
}

fn emit_snapshot_counter(level: Level, scope: &str, metric: &str, value: u64) {
    macro_rules! e {
        ($lvl:expr) => {
            tracing::event!(
                target: TARGET, $lvl,
                event = "snapshot", scope, metric, kind = "counter", value
            )
        };
    }
    match level {
        Level::TRACE => e!(Level::TRACE),
        Level::DEBUG => e!(Level::DEBUG),
        _ => e!(Level::INFO),
    }
}

fn emit_snapshot_gauge(level: Level, scope: &str, metric: &str, value: f64) {
    macro_rules! e {
        ($lvl:expr) => {
            tracing::event!(
                target: TARGET, $lvl,
                event = "snapshot", scope, metric, kind = "gauge", value
            )
        };
    }
    match level {
        Level::TRACE => e!(Level::TRACE),
        Level::DEBUG => e!(Level::DEBUG),
        _ => e!(Level::INFO),
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_snapshot_histogram(
    level: Level,
    scope: &str,
    metric: &str,
    count: usize,
    sum: f64,
    p50: f64,
    p90: f64,
    p99: f64,
) {
    macro_rules! e {
        ($lvl:expr) => {
            tracing::event!(
                target: TARGET, $lvl,
                event = "snapshot", scope, metric, kind = "histogram",
                count, sum, p50, p90, p99
            )
        };
    }
    match level {
        Level::TRACE => e!(Level::TRACE),
        Level::DEBUG => e!(Level::DEBUG),
        _ => e!(Level::INFO),
    }
}

/// Error from [`Builder::install`].
#[derive(Debug)]
pub enum BuildError {
    /// A global `metrics` recorder was already installed.
    AlreadyInstalled,
    /// The poller thread could not be spawned.
    Spawn(std::io::Error),
}

impl From<std::io::Error> for BuildError {
    fn from(e: std::io::Error) -> Self {
        BuildError::Spawn(e)
    }
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::AlreadyInstalled => {
                write!(f, "a global metrics recorder is already installed")
            }
            BuildError::Spawn(e) => write!(f, "failed to spawn the snapshot poller thread: {e}"),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BuildError::Spawn(e) => Some(e),
            BuildError::AlreadyInstalled => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;

    fn metric_names(samples: &[Sample]) -> Vec<&str> {
        samples
            .iter()
            .map(|s| match s {
                Sample::Counter { metric, .. } => metric.as_str(),
                Sample::Gauge { metric, .. } => metric.as_str(),
                Sample::Histogram { metric, .. } => metric.as_str(),
            })
            .collect()
    }

    #[test]
    fn snapshot_aggregates_counter_gauge_histogram() {
        let (recorder, poller) = TracingRecorder::builder().build();

        with_local_recorder(&recorder, || {
            metrics::counter!("hits").increment(3);
            metrics::counter!("hits").increment(4);
            metrics::gauge!("temp").set(21.5);
            let h = metrics::histogram!("lat");
            for v in [1.0, 2.0, 3.0, 4.0] {
                h.record(v);
            }
        });

        let samples = poller.poll_once();

        let counter = samples
            .iter()
            .find(|s| matches!(s, Sample::Counter { metric, .. } if metric == "hits"))
            .expect("counter sample");
        assert_eq!(
            *counter,
            Sample::Counter {
                metric: "hits".into(),
                value: 7
            }
        );

        let gauge = samples
            .iter()
            .find(|s| matches!(s, Sample::Gauge { metric, .. } if metric == "temp"))
            .expect("gauge sample");
        assert_eq!(
            *gauge,
            Sample::Gauge {
                metric: "temp".into(),
                value: 21.5
            }
        );

        match samples
            .iter()
            .find(|s| matches!(s, Sample::Histogram { metric, .. } if metric == "lat"))
            .expect("histogram sample")
        {
            Sample::Histogram { count, sum, .. } => {
                assert_eq!(*count, 4);
                assert_eq!(*sum, 10.0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn labels_render_into_metric_name() {
        let (recorder, poller) = TracingRecorder::builder().build();
        with_local_recorder(&recorder, || {
            metrics::counter!("frames", "display" => "0").increment(1);
        });
        assert_eq!(metric_names(&poller.poll_once()), vec!["frames{display=0}"]);
    }

    #[test]
    fn histogram_sketch_persists_across_polls() {
        // Each poll drains the bucket; the sketch must keep the prior count so cumulative quantiles
        // stay correct across ticks.
        let (recorder, poller) = TracingRecorder::builder().build();
        let h = with_local_recorder(&recorder, || metrics::histogram!("lat"));

        with_local_recorder(&recorder, || h.record(1.0));
        let first = poller.poll_once();
        assert!(matches!(
            first.iter().find(|s| matches!(s, Sample::Histogram { .. })),
            Some(Sample::Histogram { count: 1, .. })
        ));

        with_local_recorder(&recorder, || h.record(2.0));
        let second = poller.poll_once();
        match second
            .iter()
            .find(|s| matches!(s, Sample::Histogram { .. }))
            .unwrap()
        {
            Sample::Histogram { count, sum, .. } => {
                assert_eq!(*count, 2);
                assert_eq!(*sum, 3.0);
            }
            _ => unreachable!(),
        }
    }
}
