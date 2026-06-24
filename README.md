# metrics-exporter-tracing

A [`metrics`](https://crates.io/crates/metrics) recorder that emits aggregated periodic
**snapshots** — and, optionally, a **filtered per-call stream** — as structured
[`tracing`](https://crates.io/crates/tracing) events. It is meant for debugging and lightweight
visibility during development, where you already have a `tracing` subscriber and would rather read
metrics through it than stand up a separate metrics pipeline.

## Two complementary outputs

- **Snapshot (always on).** Every metric is stored in an in-process registry. A background poller
  drains it on a fixed interval and emits one `event = "snapshot"` event per metric. Histograms
  carry `count, sum, p50, p90, p99`; counters and gauges carry `value`. Histogram storage stays
  bounded because raw observations are drained into a per-key DDSketch each tick, so memory scales
  with the number of distinct keys, not with observation volume.
- **Per-call stream (opt-in).** When a [`StreamFilter`](#stream-filtering) is configured, keys that
  pass the filter *also* emit an `event = "emit"` event on every update. Keys that don't match (or
  when no filter is set) use the bare registry handle with zero per-call overhead — snapshot only.

## Fixed target, runtime scope

`tracing` requires the event `target:` to be a compile-time literal, so a configurable target is
impossible. The target is the fixed const `"metrics"` and the event name is `"metric"`. The logical
scope you care about (`"server"`, `"client"`, ...) is carried as the runtime field `scope`.

The event level is `INFO` by default and may be lowered to `DEBUG` or `TRACE`. (Other levels fall
back to `INFO`; `tracing` requires a literal level, so only this small set is dispatched.)

## Quick start

```rust
use std::time::Duration;
use metrics_exporter_tracing::TracingRecorder;

fn main() {
    // Install a tracing subscriber first (example only).
    tracing_subscriber::fmt::init();

    // Install the recorder as the global metrics recorder and spawn its snapshot poller.
    TracingRecorder::builder()
        .scope("server")
        .snapshot_interval(Duration::from_secs(3))
        .install()
        .expect("install recorder");

    // Emit metrics anywhere via the `metrics` facade.
    metrics::counter!("requests_total", "route" => "/health").increment(1);
    metrics::gauge!("in_flight").set(42.0);
    metrics::histogram!("latency_ms").record(12.7);
}
```

## Composing with other recorders

`install()` sets the global recorder for you. When you need to fan out to several recorders, use
`build()` instead: it returns the recorder and its poller separately so you can wrap the recorder in
a `Fanout` before installing, then spawn the poller.

```rust
use metrics_exporter_tracing::TracingRecorder;

let (recorder, poller) = TracingRecorder::builder().scope("server").build();
// add `recorder` to your Fanout, set_global_recorder(fanout), then:
poller.spawn().expect("spawn poller");
```

## The `on_tick` hook

The poller runs an optional `on_tick` closure at the start of every tick, *before* the snapshot is
emitted. Use it to refresh pull-style metrics (process stats, heap usage) into the registry so the
snapshot — and any other installed recorder, such as Prometheus — sees current values. Without it,
pull metrics never update.

```rust
# use metrics_exporter_tracing::TracingRecorder;
let (recorder, poller) = TracingRecorder::builder()
    .on_tick(|| {
        // refresh process/heap gauges here
        metrics::gauge!("heap_bytes").set(current_heap() as f64);
    })
    .build();
# fn current_heap() -> u64 { 0 }
```

## Stream filtering

A `StreamFilter` controls which keys produce per-call `event = "emit"` events. It is built from
[`globset`](https://crates.io/crates/globset) patterns matched against the rendered key
(`name` or `name{k=v,...}`):

- `allow` — globs that opt counter/gauge keys into streaming. **Empty = allow all.**
- `deny` — globs that suppress streaming for matching keys. **Deny always wins.**
- `histograms` — globs that opt specific histogram keys into per-observation streaming. Histograms
  never stream on the general `allow` list; they require an explicit `histograms` match. This is a
  firehose guard so a hot histogram cannot flood your logs just because its name matches `allow`.

```rust
use metrics_exporter_tracing::{StreamFilter, TracingRecorder};

let filter = StreamFilter::new()
    .allow(["requests_*", "in_flight"])
    .deny(["requests_internal"])
    .histograms(["latency_ms"])
    .build();

let (recorder, poller) = TracingRecorder::builder()
    .scope("server")
    .stream(filter)
    .build();
```

## Event schema

All events use the `tracing` event name `metric`, the target `metrics`, and a runtime `scope`
field.

Snapshot events (`event = "snapshot"`), one per metric per tick:

| field    | meaning                                            |
| -------- | -------------------------------------------------- |
| `event`  | `"snapshot"`                                       |
| `scope`  | configured scope string                            |
| `metric` | rendered key (`name` or `name{k=v,...}`)           |
| `kind`   | `"counter" \| "gauge" \| "histogram"`              |
| `value`  | counter total (u64) or gauge value (f64)           |
| `count`, `sum`, `p50`, `p90`, `p99` | histogram aggregates             |

Emit events (`event = "emit"`), one per matching update:

| field    | meaning                                                              |
| -------- | ------------------------------------------------------------------- |
| `event`  | `"emit"`                                                            |
| `scope`  | configured scope string                                             |
| `metric` | rendered key                                                        |
| `kind`   | `"counter" \| "gauge" \| "histogram"`                               |
| `op`     | counter: `"increment" \| "absolute"`; gauge: `"set" \| "increment" \| "decrement"`; histogram: `"record"` |
| `value`  | the value of this single operation                                  |

## API surface

```rust
pub struct TracingRecorder;           // impls metrics::Recorder
pub struct Builder;
pub struct Poller;
pub struct StreamFilter;
pub struct StreamFilterBuilder;
pub enum   Sample;                    // Counter | Gauge | Histogram (for poll_once)
pub enum   BuildError;                // AlreadyInstalled | Spawn(io::Error)

impl TracingRecorder { pub fn builder() -> Builder; }
impl Builder {
    pub fn snapshot_interval(self, d: Duration) -> Self;   // default 3s
    pub fn level(self, l: tracing::Level) -> Self;         // default INFO
    pub fn scope(self, s: impl Into<String>) -> Self;      // default ""
    pub fn stream(self, f: StreamFilter) -> Self;          // optional
    pub fn on_tick(self, f: impl Fn() + Send + 'static) -> Self;
    pub fn build(self) -> (TracingRecorder, Poller);       // compose, then set_global_recorder
    pub fn install(self) -> Result<(), BuildError>;        // build + set_global + poller.spawn
}
impl Poller {
    pub fn poll_once(&self) -> Vec<Sample>;                // compute a snapshot without emitting
    pub fn spawn(self) -> std::io::Result<()>;             // spawn the daemon poller thread
}
impl StreamFilter { pub fn new() -> StreamFilterBuilder; }
```

## Notes

- Snapshot events are emitted from the poller thread, off the metrics call path.
- Per-call (`emit`) events *are* emitted synchronously on the metrics call path, so keep the stream
  filter tight in hot code.
- This crate targets debugging and development workflows rather than production-grade metrics
  export. For production, pair it with a real exporter (e.g. Prometheus) via a `Fanout`.

## License

Licensed under either of Apache-2.0 or MIT at your option.
