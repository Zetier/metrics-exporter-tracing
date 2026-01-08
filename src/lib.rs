//! A `metrics` recorder that emits metric updates as `tracing` events.
//!
//! This crate is intended for developer-focused debugging and lightweight
//! visibility during development. Metrics are emitted synchronously on the
//! metrics call path as structured `tracing` events.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::{DashMap, DashSet};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use tracing::callsite::{self, Callsite};
use tracing::field::{self, FieldSet, Value};
use tracing::metadata;
use tracing::subscriber::Interest;
use tracing::{Event, Level as TracingLevel, Metadata as TracingMetadata};

const DEFAULT_TARGET: &str = "metrics";
const EVENT_NAME: &str = "metric";

/// A `metrics` recorder that emits `tracing` events.
#[derive(Clone)]
pub struct TracingRecorder {
    inner: Arc<Inner>,
}

impl TracingRecorder {
    /// Creates a recorder with defaults: level TRACE and target "metrics".
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Returns a builder to configure a [`TracingRecorder`].
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl Default for TracingRecorder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for [`TracingRecorder`].
#[derive(Clone, Debug)]
pub struct Builder {
    default_level: TracingLevel,
    default_target: Arc<str>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            default_level: TracingLevel::TRACE,
            default_target: Arc::from(DEFAULT_TARGET),
        }
    }
}

impl Builder {
    /// Sets the default tracing level for emitted events.
    pub fn default_level(mut self, level: TracingLevel) -> Self {
        self.default_level = level;
        self
    }

    /// Sets the default target for emitted events.
    pub fn default_target<T: Into<String>>(mut self, target: T) -> Self {
        self.default_target = Arc::from(target.into());
        self
    }

    /// Builds a [`TracingRecorder`].
    pub fn build(self) -> TracingRecorder {
        TracingRecorder {
            inner: Arc::new(Inner {
                default_level: self.default_level,
                default_target: self.default_target,
                described: DashSet::new(),
                callsites: DashMap::new(),
            }),
        }
    }
}

impl Recorder for TracingRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe(MetricKind::Counter, key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe(MetricKind::Gauge, key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe(MetricKind::Histogram, key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        let target = self.inner.resolve_target(metadata.target());
        let level = map_level(*metadata.level());
        let handle = CounterHandle {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
            target,
            level,
        };
        Counter::from_arc(Arc::new(handle))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        let target = self.inner.resolve_target(metadata.target());
        let level = map_level(*metadata.level());
        let handle = GaugeHandle {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
            target,
            level,
        };
        Gauge::from_arc(Arc::new(handle))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        let target = self.inner.resolve_target(metadata.target());
        let level = map_level(*metadata.level());
        let handle = HistogramHandle {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
            target,
            level,
        };
        Histogram::from_arc(Arc::new(handle))
    }
}

struct Inner {
    default_level: TracingLevel,
    default_target: Arc<str>,
    described: DashSet<SeenKey>,
    callsites: DashMap<CallsiteKey, &'static DynamicCallsite>,
}

impl Inner {
    fn resolve_target(&self, target: &str) -> Arc<str> {
        if target.is_empty() {
            return Arc::clone(&self.default_target);
        }
        if target == &*self.default_target {
            return Arc::clone(&self.default_target);
        }
        Arc::from(target.to_string())
    }

    fn describe(
        &self,
        kind: MetricKind,
        key: KeyName,
        unit: Option<Unit>,
        description: SharedString,
    ) {
        if !self.mark_described(&key, kind) {
            return;
        }

        let target = Arc::clone(&self.default_target);
        let level = self.default_level;

        let event_value = EVENT_DESCRIBE.to_string();
        let name_value = key.as_str().to_string();
        let kind_value = kind.as_ref().to_string();
        let description_value = description.to_string();
        let unit_value = unit.map(|u| u.as_str().to_string());

        let values: [Option<&dyn Value>; 5] = [
            Some(&event_value as &dyn Value),
            Some(&name_value as &dyn Value),
            Some(&kind_value as &dyn Value),
            Some(&description_value as &dyn Value),
            unit_value.as_ref().map(|value| value as &dyn Value),
        ];

        self.dispatch_event(Schema::Describe, &target, level, &values);
    }

    fn emit_counter(
        &self,
        key: &Key,
        target: &Arc<str>,
        level: TracingLevel,
        op: CounterOp,
        value: u64,
    ) {
        let event_value = EVENT_EMIT.to_string();
        let name_value = key.name().to_string();
        let kind_value = MetricKind::Counter.as_ref().to_string();
        let op_value = op.as_ref().to_string();
        let labels_debug = LabelsDebug::from_key(key);
        let labels_value = field::debug(labels_debug);
        let values: [Option<&dyn Value>; 6] = [
            Some(&event_value as &dyn Value),
            Some(&name_value as &dyn Value),
            Some(&kind_value as &dyn Value),
            Some(&labels_value as &dyn Value),
            Some(&value as &dyn Value),
            Some(&op_value as &dyn Value),
        ];

        self.dispatch_event(Schema::Emit, target, level, &values);
    }

    fn emit_gauge(
        &self,
        key: &Key,
        target: &Arc<str>,
        level: TracingLevel,
        op: GaugeOp,
        value: f64,
    ) {
        let event_value = EVENT_EMIT.to_string();
        let name_value = key.name().to_string();
        let kind_value = MetricKind::Gauge.as_ref().to_string();
        let op_value = op.as_ref().to_string();
        let labels_debug = LabelsDebug::from_key(key);
        let labels_value = field::debug(labels_debug);
        let values: [Option<&dyn Value>; 6] = [
            Some(&event_value as &dyn Value),
            Some(&name_value as &dyn Value),
            Some(&kind_value as &dyn Value),
            Some(&labels_value as &dyn Value),
            Some(&value as &dyn Value),
            Some(&op_value as &dyn Value),
        ];

        self.dispatch_event(Schema::Emit, target, level, &values);
    }

    fn emit_histogram(&self, key: &Key, target: &Arc<str>, level: TracingLevel, value: f64) {
        let event_value = EVENT_EMIT.to_string();
        let name_value = key.name().to_string();
        let kind_value = MetricKind::Histogram.as_ref().to_string();
        let op_value = HistogramOp::Sample.as_ref().to_string();
        let labels_debug = LabelsDebug::from_key(key);
        let labels_value = field::debug(labels_debug);
        let values: [Option<&dyn Value>; 6] = [
            Some(&event_value as &dyn Value),
            Some(&name_value as &dyn Value),
            Some(&kind_value as &dyn Value),
            Some(&labels_value as &dyn Value),
            Some(&value as &dyn Value),
            Some(&op_value as &dyn Value),
        ];

        self.dispatch_event(Schema::Emit, target, level, &values);
    }

    fn dispatch_event(
        &self,
        schema: Schema,
        target: &Arc<str>,
        level: TracingLevel,
        values: &[Option<&dyn Value>],
    ) {
        if !tracing::level_enabled!(level) {
            return;
        }

        let callsite = self.callsite_for(schema, level, target);
        let meta = callsite.metadata_static();

        let interest = callsite.interest();
        if interest.is_never() {
            return;
        }
        if !interest.is_always() && !tracing::dispatcher::get_default(|d| d.enabled(meta)) {
            return;
        }

        let value_set = meta.fields().value_set_all(values);
        Event::dispatch(meta, &value_set);
    }

    fn callsite_for(
        &self,
        schema: Schema,
        level: TracingLevel,
        target: &Arc<str>,
    ) -> &'static DynamicCallsite {
        let key = CallsiteKey {
            schema,
            level: level_key(level),
            target: Arc::clone(target),
        };

        match self.callsites.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let callsite = build_callsite(schema, level, target.as_ref());
                entry.insert(callsite);
                callsite
            }
        }
    }

    fn mark_described(&self, name: &KeyName, kind: MetricKind) -> bool {
        let key = SeenKey {
            name: name.clone(),
            kind,
        };

        self.described.insert(key)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Schema {
    Emit,
    Describe,
}

impl Schema {
    fn field_names(self) -> &'static [&'static str] {
        match self {
            Schema::Emit => &EMIT_FIELDS,
            Schema::Describe => &DESCRIBE_FIELDS,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum CounterOp {
    Increment,
    Absolute,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum GaugeOp {
    Set,
    Increment,
    Decrement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum HistogramOp {
    Sample,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct SeenKey {
    name: KeyName,
    kind: MetricKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CallsiteKey {
    schema: Schema,
    level: u8,
    target: Arc<str>,
}

impl Hash for CallsiteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.level.hash(state);
        self.target.hash(state);
    }
}

const EVENT_EMIT: &str = "emit";
const EVENT_DESCRIBE: &str = "describe";

const EMIT_FIELDS: [&str; 6] = ["event", "name", "kind", "labels", "value", "op"];
const DESCRIBE_FIELDS: [&str; 5] = ["event", "name", "kind", "description", "unit"];

struct DynamicCallsite {
    metadata: OnceLock<TracingMetadata<'static>>,
    interest: AtomicU8,
}

impl DynamicCallsite {
    const INTEREST_NEVER: u8 = 0;
    const INTEREST_SOMETIMES: u8 = 1;
    const INTEREST_ALWAYS: u8 = 2;

    fn new() -> Self {
        Self {
            metadata: OnceLock::new(),
            interest: AtomicU8::new(Self::INTEREST_SOMETIMES),
        }
    }

    fn set_metadata(&self, metadata: TracingMetadata<'static>) {
        let _ = self.metadata.set(metadata);
    }

    fn metadata_static(&'static self) -> &'static TracingMetadata<'static> {
        self.metadata
            .get()
            .expect("callsite metadata set before registration")
    }

    fn interest(&self) -> Interest {
        match self.interest.load(Ordering::Relaxed) {
            Self::INTEREST_NEVER => Interest::never(),
            Self::INTEREST_ALWAYS => Interest::always(),
            _ => Interest::sometimes(),
        }
    }
}

impl Callsite for DynamicCallsite {
    fn set_interest(&self, interest: Interest) {
        let value = if interest.is_never() {
            Self::INTEREST_NEVER
        } else if interest.is_always() {
            Self::INTEREST_ALWAYS
        } else {
            Self::INTEREST_SOMETIMES
        };
        self.interest.store(value, Ordering::Relaxed);
    }

    fn metadata(&self) -> &TracingMetadata<'_> {
        self.metadata
            .get()
            .expect("callsite metadata set before registration")
    }
}

fn build_callsite(schema: Schema, level: TracingLevel, target: &str) -> &'static DynamicCallsite {
    let callsite = Box::leak(Box::new(DynamicCallsite::new()));
    let target_static: &'static str = Box::leak(target.to_string().into_boxed_str());
    let fields = FieldSet::new(schema.field_names(), callsite::Identifier(callsite));
    let meta = TracingMetadata::new(
        EVENT_NAME,
        target_static,
        level,
        None,
        None,
        None,
        fields,
        metadata::Kind::EVENT,
    );
    callsite.set_metadata(meta);
    callsite::register(callsite);
    callsite
}

fn map_level(level: metrics::Level) -> TracingLevel {
    match level {
        metrics::Level::TRACE => TracingLevel::TRACE,
        metrics::Level::DEBUG => TracingLevel::DEBUG,
        metrics::Level::INFO => TracingLevel::INFO,
        metrics::Level::WARN => TracingLevel::WARN,
        metrics::Level::ERROR => TracingLevel::ERROR,
    }
}

fn level_key(level: TracingLevel) -> u8 {
    match level {
        TracingLevel::TRACE => 0,
        TracingLevel::DEBUG => 1,
        TracingLevel::INFO => 2,
        TracingLevel::WARN => 3,
        TracingLevel::ERROR => 4,
    }
}

struct LabelsDebug<'a> {
    key: &'a Key,
}

impl<'a> LabelsDebug<'a> {
    fn from_key(key: &'a Key) -> Self {
        Self { key }
    }
}

impl fmt::Debug for LabelsDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for label in self.key.labels() {
            map.entry(&label.key(), &label.value());
        }
        map.finish()
    }
}

struct CounterHandle {
    inner: Arc<Inner>,
    key: Key,
    target: Arc<str>,
    level: TracingLevel,
}

impl metrics::CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        self.inner.emit_counter(
            &self.key,
            &self.target,
            self.level,
            CounterOp::Increment,
            value,
        );
    }

    fn absolute(&self, value: u64) {
        self.inner.emit_counter(
            &self.key,
            &self.target,
            self.level,
            CounterOp::Absolute,
            value,
        );
    }
}

struct GaugeHandle {
    inner: Arc<Inner>,
    key: Key,
    target: Arc<str>,
    level: TracingLevel,
}

impl metrics::GaugeFn for GaugeHandle {
    fn increment(&self, value: f64) {
        self.inner.emit_gauge(
            &self.key,
            &self.target,
            self.level,
            GaugeOp::Increment,
            value,
        );
    }

    fn decrement(&self, value: f64) {
        self.inner.emit_gauge(
            &self.key,
            &self.target,
            self.level,
            GaugeOp::Decrement,
            value,
        );
    }

    fn set(&self, value: f64) {
        self.inner
            .emit_gauge(&self.key, &self.target, self.level, GaugeOp::Set, value);
    }
}

struct HistogramHandle {
    inner: Arc<Inner>,
    key: Key,
    target: Arc<str>,
    level: TracingLevel,
}

impl metrics::HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        self.inner
            .emit_histogram(&self.key, &self.target, self.level, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::io;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::prelude::*;

    #[derive(Clone, Default)]
    struct JsonBuffer {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    impl JsonBuffer {
        fn new(buf: Arc<Mutex<Vec<u8>>>) -> Self {
            Self { buf }
        }
    }

    struct JsonWriter {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    impl<'a> MakeWriter<'a> for JsonBuffer {
        type Writer = JsonWriter;

        fn make_writer(&'a self) -> Self::Writer {
            JsonWriter {
                buf: Arc::clone(&self.buf),
            }
        }
    }

    impl io::Write for JsonWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let mut guard = self
                .buf
                .lock()
                .map_err(|_| io::Error::other("log buffer poisoned"))?;
            guard.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn capture_events(f: impl FnOnce()) -> Vec<Value> {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = JsonBuffer::new(Arc::clone(&buffer));
        let layer = tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(writer);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, f);

        let bytes = buffer.lock().expect("log buffer").clone();
        let text = String::from_utf8(bytes).expect("log output utf8");
        text.lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<Value>(line).expect("valid json"))
            .collect()
    }

    fn event_fields(event: &Value) -> &serde_json::Map<String, Value> {
        event
            .get("fields")
            .and_then(Value::as_object)
            .expect("fields object")
    }

    #[test]
    fn emits_counter_event_fields() {
        let recorder = TracingRecorder::new();
        let key = Key::from_parts("my_counter", &[("label", "value")]);
        let metadata = Metadata::new("my.target", metrics::Level::INFO, None);
        let counter = recorder.register_counter(&key, &metadata);

        let events = capture_events(|| {
            counter.increment(3);
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        let fields = event_fields(event);

        assert_eq!(event.get("level").and_then(Value::as_str), Some("INFO"));
        assert_eq!(
            event.get("target").and_then(Value::as_str),
            Some("my.target")
        );
        assert_eq!(fields.get("event").and_then(Value::as_str), Some("emit"));
        assert_eq!(
            fields.get("name").and_then(Value::as_str),
            Some("my_counter")
        );
        assert_eq!(fields.get("kind").and_then(Value::as_str), Some("counter"));
        assert_eq!(fields.get("op").and_then(Value::as_str), Some("increment"));
        assert_eq!(fields.get("value"), Some(&Value::from(3)));

        let expected_labels = format!("{:?}", LabelsDebug::from_key(&key));
        assert_eq!(
            fields.get("labels").and_then(Value::as_str),
            Some(expected_labels.as_str())
        );
    }

    #[test]
    fn emits_gauge_set_with_level() {
        let recorder = TracingRecorder::new();
        let key = Key::from_name("my_gauge");
        let metadata = Metadata::new("gauge.target", metrics::Level::WARN, None);
        let gauge = recorder.register_gauge(&key, &metadata);

        let events = capture_events(|| {
            gauge.set(2.5);
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        let fields = event_fields(event);

        assert_eq!(event.get("level").and_then(Value::as_str), Some("WARN"));
        assert_eq!(
            event.get("target").and_then(Value::as_str),
            Some("gauge.target")
        );
        assert_eq!(fields.get("event").and_then(Value::as_str), Some("emit"));
        assert_eq!(fields.get("kind").and_then(Value::as_str), Some("gauge"));
        assert_eq!(fields.get("op").and_then(Value::as_str), Some("set"));
        assert_eq!(fields.get("value"), Some(&Value::from(2.5)));
    }

    #[test]
    fn emits_histogram_sample_op() {
        let recorder = TracingRecorder::new();
        let key = Key::from_name("my_histogram");
        let metadata = Metadata::new("hist.target", metrics::Level::DEBUG, None);
        let histogram = recorder.register_histogram(&key, &metadata);

        let events = capture_events(|| {
            histogram.record(7.25);
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        let fields = event_fields(event);

        assert_eq!(event.get("level").and_then(Value::as_str), Some("DEBUG"));
        assert_eq!(
            event.get("target").and_then(Value::as_str),
            Some("hist.target")
        );
        assert_eq!(fields.get("event").and_then(Value::as_str), Some("emit"));
        assert_eq!(
            fields.get("kind").and_then(Value::as_str),
            Some("histogram")
        );
        assert_eq!(fields.get("op").and_then(Value::as_str), Some("sample"));
        assert_eq!(fields.get("value"), Some(&Value::from(7.25)));
    }

    #[test]
    fn describe_is_emitted_once() {
        let recorder = TracingRecorder::new();

        let events = capture_events(|| {
            recorder.describe_counter(
                KeyName::from("described_counter"),
                Some(Unit::Seconds),
                "desc".into(),
            );
            recorder.describe_counter(
                KeyName::from("described_counter"),
                Some(Unit::Seconds),
                "desc".into(),
            );
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        let fields = event_fields(event);
        assert_eq!(
            fields.get("event").and_then(Value::as_str),
            Some("describe")
        );
        assert_eq!(
            fields.get("description").and_then(Value::as_str),
            Some("desc")
        );
        assert_eq!(fields.get("unit").and_then(Value::as_str), Some("seconds"));
    }

    #[test]
    fn describe_is_emitted_per_kind() {
        let recorder = TracingRecorder::new();

        let events = capture_events(|| {
            recorder.describe_counter(
                KeyName::from("shared_name"),
                Some(Unit::Seconds),
                "counter".into(),
            );
            recorder.describe_gauge(KeyName::from("shared_name"), None, "gauge".into());
        });

        assert_eq!(events.len(), 2);
        let mut kinds: Vec<_> = events
            .iter()
            .map(|event| {
                event_fields(event)
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        kinds.sort();
        assert_eq!(kinds, vec!["counter".to_string(), "gauge".to_string()]);
    }

    #[test]
    fn describe_uses_default_level_and_target() {
        let recorder = TracingRecorder::new();

        let events = capture_events(|| {
            recorder.describe_gauge(KeyName::from("gauge"), None, "desc".into());
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.get("level").and_then(Value::as_str), Some("TRACE"));
        assert_eq!(
            event.get("target").and_then(Value::as_str),
            Some(DEFAULT_TARGET)
        );
    }

    #[test]
    fn emit_uses_default_target_when_metadata_empty() {
        let recorder = TracingRecorder::new();
        let key = Key::from_name("empty_target");
        let metadata = Metadata::new("", metrics::Level::INFO, None);
        let counter = recorder.register_counter(&key, &metadata);

        let events = capture_events(|| {
            counter.increment(1);
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(
            event.get("target").and_then(Value::as_str),
            Some(DEFAULT_TARGET)
        );
    }

    #[test]
    fn emit_does_not_include_unit() {
        let recorder = TracingRecorder::new();
        let key = Key::from_name("my_histogram");
        let metadata = Metadata::new("my.target", metrics::Level::INFO, None);
        let histogram = recorder.register_histogram(&key, &metadata);

        let events = capture_events(|| {
            histogram.record(1.5);
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        let fields = event_fields(event);
        assert_eq!(fields.get("event").and_then(Value::as_str), Some("emit"));
        assert!(fields.get("unit").is_none());
    }
}
