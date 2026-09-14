use metrics::{Key, Label};
use prometheus_client::encoding::prometheus_protobuf::prometheus_data_model::{
    Histogram, MetricFamily, MetricType,
};
use tracing::field::{self, Value};

use crate::{LabelsDebug, Schema, TracingRecorder};

pub fn emit(recorder: &TracingRecorder, metrics: &[MetricFamily]) {
    for metric in metrics {
        for series in &metric.metric {
            let labels: Vec<_> = series
                .label
                .iter()
                .map(|label| Label::new(label.name.clone(), label.value.clone()))
                .collect();
            let key = Key::from_parts(metric.name.clone(), labels);
            let emit = |kind, statistic, value: &dyn Value| {
                recorder.emit_statistic(&key, kind, statistic, value);
            };
            match MetricType::try_from(metric.r#type) {
                Ok(MetricType::Counter) => {
                    if let Some(counter) = &series.counter {
                        emit("counter", "value", &counter.value);
                    }
                }
                Ok(MetricType::Gauge) => {
                    if let Some(gauge) = &series.gauge {
                        emit("gauge", "value", &gauge.value);
                    }
                }
                Ok(MetricType::Histogram) => {
                    if let Some(histogram) = &series.histogram {
                        recorder.emit_histogram_snapshot(&key, histogram);
                    }
                }
                _ => {}
            }
        }
    }
}

impl TracingRecorder {
    fn emit_histogram_snapshot(&self, key: &Key, histogram: &Histogram) {
        let count: &dyn Value = if histogram.sample_count_float > 0.0 {
            &histogram.sample_count_float
        } else {
            &histogram.sample_count
        };
        let zero_count: &dyn Value = if histogram.zero_count_float > 0.0 {
            &histogram.zero_count_float
        } else {
            &histogram.zero_count
        };
        let values: [Option<&dyn Value>; 16] = [
            Some(&"snapshot"),
            Some(&key.name()),
            Some(&"histogram"),
            Some(&field::debug(LabelsDebug::from_key(key))),
            Some(count),
            Some(&histogram.sample_sum),
            Some(&histogram.schema),
            Some(&histogram.zero_threshold),
            Some(zero_count),
            Some(&field::debug(&histogram.bucket)),
            Some(&field::debug(&histogram.positive_span)),
            Some(&field::debug(&histogram.positive_delta)),
            Some(&field::debug(&histogram.positive_count)),
            Some(&field::debug(&histogram.negative_span)),
            Some(&field::debug(&histogram.negative_delta)),
            Some(&field::debug(&histogram.negative_count)),
        ];
        self.inner.dispatch_event(
            Schema::HistogramSnapshot,
            &self.inner.default_target,
            self.inner.default_level,
            &values,
        );
    }
}
