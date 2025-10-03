use std::time::{Duration, Instant};

pub struct EngineConfig {
    pub symbol: String,
    pub venue: String,
    pub core_id: usize,
}

/// What the engine expects from a venue-specific handler.
pub trait EngineHandler {
    /// Do one non-blocking (or short-blocking) poll step.
    /// Return the time spent in the *apply/parse* part so we can histogram it.
    fn poll_once(&mut self) -> anyhow::Result<Option<Duration>>;

    /// Flush metrics (print/export) on a cadence.
    fn flush_metrics(&mut self);
}

/// Generic single-owner engine.
/// The handler owns its sockets, book, and hot path buffers.
/// We just pin the thread and spin the loop.
pub struct Engine<H: EngineHandler> {
    pub cfg: EngineConfig,
    pub handler: H,
}

impl<H: EngineHandler> Engine<H> {
    pub fn run(mut self) -> anyhow::Result<()> {
        let core = core_affinity::CoreId { id: self.cfg.core_id };
        core_affinity::set_for_current(core);
        eprintln!(
            "[engine] starting {} on core {} ({})",
            self.cfg.symbol, self.cfg.core_id, self.cfg.venue
        );

        let mut last_flush = Instant::now();
        loop {
            let _maybe_latency = self.handler.poll_once()?;
            if last_flush.elapsed() >= Duration::from_secs(5) {
                self.handler.flush_metrics();
                last_flush = Instant::now();
            }
            // light backoff if nothing arrived (handler keeps this small)
            std::hint::spin_loop();
        }
    }
}