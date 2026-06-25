//! Tracing emission helpers.
//!
//! `tracing`'s `target:` and `level:` must be compile-time literals, so neither can be threaded
//! through as a runtime value. The target is a fixed const and the level is dispatched through a
//! `match` over the small supported set. The logical "scope" the consumer wants is carried as a
//! runtime field instead of a target.

/// Fixed tracing target for all events from this recorder.
pub const TARGET: &str = "metrics";

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use metrics::{CounterFn, GaugeFn, HistogramFn};
use tracing::Level;

use crate::filter::StreamFilter;

/// Emit a per-call ("emit") event at the configured level. `op` and `value` describe the single
/// operation that just hit the registry.
macro_rules! emit_event {
    ($level:expr, $scope:expr, $metric:expr, $labels:expr, $kind:expr, $op:expr, $value:expr) => {{
        match $level {
            Level::TRACE => tracing::event!(
                target: TARGET, Level::TRACE,
                event = "emit", scope = $scope, metric = $metric,
                labels = $labels, kind = $kind, op = $op, value = $value
            ),
            Level::DEBUG => tracing::event!(
                target: TARGET, Level::DEBUG,
                event = "emit", scope = $scope, metric = $metric,
                labels = $labels, kind = $kind, op = $op, value = $value
            ),
            _ => tracing::event!(
                target: TARGET, Level::INFO,
                event = "emit", scope = $scope, metric = $metric,
                labels = $labels, kind = $kind, op = $op, value = $value
            ),
        }
    }};
}

/// Shared context every streaming handle needs to render an event. Holds filter + scope only;
/// per-metric level is carried on each handle so different metrics can emit at their own level.
pub(crate) struct StreamContext {
    pub(crate) scope: Arc<str>,
    pub(crate) filter: Arc<StreamFilter>,
}

/// Counter handle that updates the registry atomic and emits a per-call event. Only constructed
/// when the key already passed the filter, so the hot path here never re-checks.
pub(crate) struct StreamingCounter {
    inner: Arc<AtomicU64>,
    metric: Arc<str>,
    labels: Arc<str>,
    level: Level,
    ctx: Arc<StreamContext>,
}

impl StreamingCounter {
    pub(crate) fn new(
        inner: Arc<AtomicU64>,
        metric: Arc<str>,
        labels: Arc<str>,
        level: Level,
        ctx: Arc<StreamContext>,
    ) -> Self {
        Self {
            inner,
            metric,
            labels,
            level,
            ctx,
        }
    }
}

impl CounterFn for StreamingCounter {
    fn increment(&self, value: u64) {
        self.inner.fetch_add(value, Ordering::Relaxed);
        emit_event!(
            self.level,
            &*self.ctx.scope,
            &*self.metric,
            &*self.labels,
            "counter",
            "increment",
            value
        );
    }

    fn absolute(&self, value: u64) {
        // monotonic max so out-of-order callers can't roll the value back
        self.inner.fetch_max(value, Ordering::Relaxed);
        emit_event!(
            self.level,
            &*self.ctx.scope,
            &*self.metric,
            &*self.labels,
            "counter",
            "absolute",
            value
        );
    }
}

/// Gauge handle that updates the registry atomic (f64 bit-encoded) and emits a per-call event.
pub(crate) struct StreamingGauge {
    inner: Arc<AtomicU64>,
    metric: Arc<str>,
    labels: Arc<str>,
    level: Level,
    ctx: Arc<StreamContext>,
}

impl StreamingGauge {
    pub(crate) fn new(
        inner: Arc<AtomicU64>,
        metric: Arc<str>,
        labels: Arc<str>,
        level: Level,
        ctx: Arc<StreamContext>,
    ) -> Self {
        Self {
            inner,
            metric,
            labels,
            level,
            ctx,
        }
    }

    fn update<F: Fn(f64) -> f64>(&self, op: F) {
        let mut current = self.inner.load(Ordering::Relaxed);
        loop {
            let next = op(f64::from_bits(current)).to_bits();
            match self.inner.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

impl GaugeFn for StreamingGauge {
    fn increment(&self, value: f64) {
        self.update(|v| v + value);
        emit_event!(
            self.level,
            &*self.ctx.scope,
            &*self.metric,
            &*self.labels,
            "gauge",
            "increment",
            value
        );
    }

    fn decrement(&self, value: f64) {
        self.update(|v| v - value);
        emit_event!(
            self.level,
            &*self.ctx.scope,
            &*self.metric,
            &*self.labels,
            "gauge",
            "decrement",
            value
        );
    }

    fn set(&self, value: f64) {
        self.inner.store(value.to_bits(), Ordering::Relaxed);
        emit_event!(
            self.level,
            &*self.ctx.scope,
            &*self.metric,
            &*self.labels,
            "gauge",
            "set",
            value
        );
    }
}

/// Histogram handle that records into the registry bucket and emits a per-observation event.
/// Only constructed for keys matching the explicit histogram allow set (firehose guard).
pub(crate) struct StreamingHistogram {
    inner: Arc<metrics_util::storage::AtomicBucket<f64>>,
    metric: Arc<str>,
    labels: Arc<str>,
    level: Level,
    ctx: Arc<StreamContext>,
}

impl StreamingHistogram {
    pub(crate) fn new(
        inner: Arc<metrics_util::storage::AtomicBucket<f64>>,
        metric: Arc<str>,
        labels: Arc<str>,
        level: Level,
        ctx: Arc<StreamContext>,
    ) -> Self {
        Self {
            inner,
            metric,
            labels,
            level,
            ctx,
        }
    }
}

impl HistogramFn for StreamingHistogram {
    fn record(&self, value: f64) {
        self.inner.push(value);
        emit_event!(
            self.level,
            &*self.ctx.scope,
            &*self.metric,
            &*self.labels,
            "histogram",
            "record",
            value
        );
    }
}
