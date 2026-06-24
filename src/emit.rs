//! Tracing emission helpers.
//!
//! `tracing`'s `target:` and `level:` must be compile-time literals, so neither can be threaded
//! through as a runtime value. The target is a fixed const and the level is dispatched through a
//! `match` over the small supported set. The logical "scope" the consumer wants is carried as a
//! runtime field instead of a target.

/// Fixed tracing target for all events from this recorder.
pub const TARGET: &str = "metrics";

#[cfg(feature = "stream")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "stream")]
use std::sync::Arc;

#[cfg(feature = "stream")]
use metrics::{CounterFn, GaugeFn, HistogramFn};
#[cfg(feature = "stream")]
use tracing::Level;

#[cfg(feature = "stream")]
use crate::filter::StreamFilter;

/// Emit a per-call ("emit") event at the configured level. `op` and `value` describe the single
/// operation that just hit the registry.
#[cfg(feature = "stream")]
macro_rules! emit_event {
    ($level:expr, $scope:expr, $metric:expr, $kind:expr, $op:expr, $value:expr) => {{
        match $level {
            Level::TRACE => tracing::event!(
                target: TARGET, Level::TRACE,
                event = "emit", scope = $scope, metric = $metric,
                kind = $kind, op = $op, value = $value
            ),
            Level::DEBUG => tracing::event!(
                target: TARGET, Level::DEBUG,
                event = "emit", scope = $scope, metric = $metric,
                kind = $kind, op = $op, value = $value
            ),
            _ => tracing::event!(
                target: TARGET, Level::INFO,
                event = "emit", scope = $scope, metric = $metric,
                kind = $kind, op = $op, value = $value
            ),
        }
    }};
}

/// Shared context every streaming handle needs to render an event.
#[cfg(feature = "stream")]
pub(crate) struct StreamContext {
    pub(crate) level: Level,
    pub(crate) scope: Arc<str>,
    pub(crate) filter: Arc<StreamFilter>,
}

/// Counter handle that updates the registry atomic and emits a per-call event. Only constructed
/// when the key already passed the filter, so the hot path here never re-checks.
#[cfg(feature = "stream")]
pub(crate) struct StreamingCounter {
    inner: Arc<AtomicU64>,
    metric: Arc<str>,
    ctx: Arc<StreamContext>,
}

#[cfg(feature = "stream")]
impl StreamingCounter {
    pub(crate) fn new(inner: Arc<AtomicU64>, metric: Arc<str>, ctx: Arc<StreamContext>) -> Self {
        Self { inner, metric, ctx }
    }
}

#[cfg(feature = "stream")]
impl CounterFn for StreamingCounter {
    fn increment(&self, value: u64) {
        self.inner.fetch_add(value, Ordering::Relaxed);
        emit_event!(
            self.ctx.level,
            &*self.ctx.scope,
            &*self.metric,
            "counter",
            "increment",
            value
        );
    }

    fn absolute(&self, value: u64) {
        // monotonic max so out-of-order callers can't roll the value back
        self.inner.fetch_max(value, Ordering::Relaxed);
        emit_event!(
            self.ctx.level,
            &*self.ctx.scope,
            &*self.metric,
            "counter",
            "absolute",
            value
        );
    }
}

/// Gauge handle that updates the registry atomic (f64 bit-encoded) and emits a per-call event.
#[cfg(feature = "stream")]
pub(crate) struct StreamingGauge {
    inner: Arc<AtomicU64>,
    metric: Arc<str>,
    ctx: Arc<StreamContext>,
}

#[cfg(feature = "stream")]
impl StreamingGauge {
    pub(crate) fn new(inner: Arc<AtomicU64>, metric: Arc<str>, ctx: Arc<StreamContext>) -> Self {
        Self { inner, metric, ctx }
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

#[cfg(feature = "stream")]
impl GaugeFn for StreamingGauge {
    fn increment(&self, value: f64) {
        self.update(|v| v + value);
        emit_event!(
            self.ctx.level,
            &*self.ctx.scope,
            &*self.metric,
            "gauge",
            "increment",
            value
        );
    }

    fn decrement(&self, value: f64) {
        self.update(|v| v - value);
        emit_event!(
            self.ctx.level,
            &*self.ctx.scope,
            &*self.metric,
            "gauge",
            "decrement",
            value
        );
    }

    fn set(&self, value: f64) {
        self.inner.store(value.to_bits(), Ordering::Relaxed);
        emit_event!(
            self.ctx.level,
            &*self.ctx.scope,
            &*self.metric,
            "gauge",
            "set",
            value
        );
    }
}

/// Histogram handle that records into the registry bucket and emits a per-observation event.
/// Only constructed for keys matching the explicit histogram allow set (firehose guard).
#[cfg(feature = "stream")]
pub(crate) struct StreamingHistogram {
    inner: Arc<metrics_util::storage::AtomicBucket<f64>>,
    metric: Arc<str>,
    ctx: Arc<StreamContext>,
}

#[cfg(feature = "stream")]
impl StreamingHistogram {
    pub(crate) fn new(
        inner: Arc<metrics_util::storage::AtomicBucket<f64>>,
        metric: Arc<str>,
        ctx: Arc<StreamContext>,
    ) -> Self {
        Self { inner, metric, ctx }
    }
}

#[cfg(feature = "stream")]
impl HistogramFn for StreamingHistogram {
    fn record(&self, value: f64) {
        self.inner.push(value);
        emit_event!(
            self.ctx.level,
            &*self.ctx.scope,
            &*self.metric,
            "histogram",
            "record",
            value
        );
    }
}
