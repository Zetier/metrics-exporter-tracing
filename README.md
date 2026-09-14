# metrics-exporter-tracing

`metrics-exporter-tracing` is a `metrics` recorder that emits metric updates as
structured `tracing` events. It is meant for debugging and lightweight visibility
during development.

## Quick start

```rust
use metrics_exporter_tracing::TracingRecorder;

fn main() {
    // Install a tracing subscriber first (example only).
    tracing_subscriber::fmt::init();

    // Install the recorder.
    let recorder = TracingRecorder::new();
    metrics::set_global_recorder(recorder).expect("install recorder");

    // Emit metrics.
    metrics::counter!("requests_total", 1, "route" => "/health");
    metrics::gauge!("in_flight", 42.0);
    metrics::histogram!("latency_ms", 12.7);
}
```

## Configuration

The recorder defaults to:

- level: `TRACE`
- target: `"metrics"`

Override defaults with the builder:

```rust
use metrics_exporter_tracing::TracingRecorder;
use tracing::Level;

let recorder = TracingRecorder::builder()
    .default_level(Level::DEBUG)
    .default_target("my.metrics")
    .build();
```

Per-metric metadata levels and targets from the `metrics` macros are respected.
If a metric does not specify a target, the recorder uses the default target.

## Event schema

All events use the `tracing` event name `metric` and emit structured fields.

Emit events (`event = "emit"`):

- `event`: `"emit"`
- `name`: metric name
- `kind`: `"counter" | "gauge" | "histogram"`
- `labels`: debug-formatted map of label key/value pairs
- `value`: sample value
- `op`: operation
  - counter: `"increment" | "absolute"`
  - gauge: `"set" | "increment" | "decrement"`
  - histogram: `"sample"`

Describe events (`event = "describe"`):

- `event`: `"describe"`
- `name`: metric name
- `kind`: `"counter" | "gauge" | "histogram"`
- `description`: description string
- `unit`: unit string (if provided)

Description events are emitted only once per `(name, kind)` pair.

## Aggregated snapshots

Enable `prometheus` to emit snapshots using `prometheus-client`'s types:

```rust
use metrics_exporter_tracing::TracingRecorder;
use prometheus_client::encoding::prometheus_protobuf::prometheus_data_model::MetricFamily;

fn export(recorder: &TracingRecorder, metrics: &[MetricFamily]) {
    recorder.emit_snapshot(metrics);
}
```

Calls emit `event="snapshot"` synchronously at the recorder's default level and
target, preserving source labels. The caller controls collection and scheduling.
Counter and gauge values are numeric. Each histogram emits one event with numeric
count, sum, schema, and zero-bucket fields, plus debug-formatted bucket collections
in their original Prometheus encoding. Other metric types are ignored.

## Notes

- Events are emitted synchronously on the metrics call path.
- Labels are emitted verbatim (as a debug map).
- This crate is intended for debugging and development workflows rather than
  production-grade metrics export.
