use hdrhistogram::Histogram;
use std::time::Duration;

pub struct EngineMetrics {
    hist: Histogram<u64>,
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineMetrics {
    pub fn new() -> Self {
        Self {
            hist: Histogram::new(3).unwrap(),
        }
    }

    pub fn record(&mut self, dur: Duration) {
        self.hist.record(dur.as_micros() as u64).ok();
    }

    pub fn summary(&self) -> (f64, f64) {
        (
            self.hist.value_at_percentile(50.0) as f64,
            self.hist.value_at_percentile(99.0) as f64,
        )
    }

    pub fn print_summary(&self) {
        let (p50, p99) = self.summary();
        println!("[metrics] p50={p50:.1}us p99={p99:.1}us");
    }
}
