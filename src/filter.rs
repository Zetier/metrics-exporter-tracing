//! Compile a set of glob patterns into a fast key matcher for the per-call stream.

use globset::{Glob, GlobSet, GlobSetBuilder};

/// Decides which metric keys produce per-call `tracing` events.
///
/// Built from globset patterns matched against the rendered key (`name` or `name{k=v,..}`).
/// An empty `key_allow` allows every key; `key_deny` always wins over allow. Histograms are gated
/// separately by `histogram_keys` so a hot histogram cannot turn into a per-observation firehose
/// just because its name matches the general allow list.
#[derive(Debug, Clone)]
pub struct StreamFilter {
    key_allow: GlobSet,
    key_deny: GlobSet,
    histogram_keys: GlobSet,
    allow_empty: bool,
}

impl StreamFilter {
    /// Start building a filter (allow-all for counters/gauges, no histogram streaming).
    pub fn builder() -> StreamFilterBuilder {
        StreamFilterBuilder::default()
    }

    /// Whether a counter/gauge key should stream per-call events.
    pub fn allows(&self, rendered_key: &str) -> bool {
        if self.key_deny.is_match(rendered_key) {
            return false;
        }
        self.allow_empty || self.key_allow.is_match(rendered_key)
    }

    /// Whether a histogram key should stream per-observation events. Requires an explicit
    /// `histograms()` match and must still survive the deny list.
    pub fn allows_histogram(&self, rendered_key: &str) -> bool {
        if self.key_deny.is_match(rendered_key) {
            return false;
        }
        self.histogram_keys.is_match(rendered_key)
    }
}

/// Builder for [`StreamFilter`]. Invalid glob patterns are silently dropped so a bad pattern cannot
/// break recorder install; this is a debug-visibility feature, not a security boundary.
#[derive(Debug, Default)]
pub struct StreamFilterBuilder {
    allow: Vec<String>,
    deny: Vec<String>,
    histograms: Vec<String>,
}

impl StreamFilterBuilder {
    /// Globs that allow counter/gauge keys to stream. Empty list = allow all.
    pub fn allow<I, S>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allow.extend(patterns.into_iter().map(Into::into));
        self
    }

    /// Globs that suppress streaming for any matching key (wins over allow and histograms).
    pub fn deny<I, S>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.deny.extend(patterns.into_iter().map(Into::into));
        self
    }

    /// Globs that opt specific histogram keys into per-observation streaming.
    pub fn histograms<I, S>(mut self, patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.histograms.extend(patterns.into_iter().map(Into::into));
        self
    }

    /// Finish the filter, compiling the glob sets.
    pub fn build(self) -> StreamFilter {
        let allow_empty = self.allow.is_empty();
        StreamFilter {
            key_allow: compile(&self.allow),
            key_deny: compile(&self.deny),
            histogram_keys: compile(&self.histograms),
            allow_empty,
        }
    }
}

fn compile(patterns: &[String]) -> GlobSet {
    let mut builder = GlobSetBuilder::new();
    for p in patterns {
        let Ok(glob) = Glob::new(p) else { continue };
        builder.add(glob);
    }
    builder.build().unwrap_or_else(|_| GlobSet::empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_allow_matches_everything() {
        let f = StreamFilter::builder().build();
        assert!(f.allows("anything"));
        assert!(f.allows("frame_count{display=0}"));
    }

    #[test]
    fn allow_restricts_to_matching_keys() {
        let f = StreamFilter::builder().allow(["frame_*"]).build();
        assert!(f.allows("frame_count"));
        assert!(!f.allows("bytes_sent"));
    }

    #[test]
    fn deny_wins_over_allow() {
        let f = StreamFilter::builder()
            .allow(["frame_*"])
            .deny(["frame_internal"])
            .build();
        assert!(f.allows("frame_count"));
        assert!(!f.allows("frame_internal"));
    }

    #[test]
    fn histograms_require_explicit_opt_in() {
        // General allow does not enable histogram streaming.
        let f = StreamFilter::builder().allow(["*"]).build();
        assert!(!f.allows_histogram("latency_ms"));

        let f = StreamFilter::builder().histograms(["latency_*"]).build();
        assert!(f.allows_histogram("latency_ms"));
        assert!(!f.allows_histogram("queue_depth"));
    }

    #[test]
    fn deny_also_suppresses_histogram_streaming() {
        let f = StreamFilter::builder()
            .histograms(["latency_*"])
            .deny(["latency_debug"])
            .build();
        assert!(f.allows_histogram("latency_ms"));
        assert!(!f.allows_histogram("latency_debug"));
    }

    #[test]
    fn rendered_key_with_labels_matches() {
        let f = StreamFilter::builder().allow(["frame_count{*}"]).build();
        assert!(f.allows("frame_count{display=0}"));
    }
}
