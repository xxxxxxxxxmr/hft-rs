use std::thread;
use std::time::{Duration, Instant};

use spsc::{channel, Consumer, Producer};

pub struct EngineConfig {
    pub symbol: String,
    pub venue: String,
    pub core_id: usize,
}

const LOG_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub enum LogEventKind {
    EngineStarted { venue: String, symbol: String },
    EngineApplyLatency { micros: u64 },
    EngineMetricsFlushed,
    HandlerInfo { source: &'static str, detail: String },
    HandlerError { source: &'static str, detail: String },
    Text(String),
}

#[derive(Debug, Clone)]
pub struct LogEvent {
    timestamp: Instant,
    kind: LogEventKind,
}

impl LogEvent {
    fn new(kind: LogEventKind) -> Self {
        Self {
            timestamp: Instant::now(),
            kind,
        }
    }
}

#[derive(Clone)]
pub struct Logger<const N: usize> {
    tx: Producer<LogEvent, N>,
}

impl<const N: usize> Logger<N> {
    pub fn emit(&self, kind: LogEventKind) {
        if !self.tx.push(LogEvent::new(kind.clone())) {
            eprintln!("[log drop] {:?}", kind);
        }
    }

    pub fn with_producer(tx: Producer<LogEvent, N>) -> Self {
        Self { tx }
    }
}

fn logging_worker<const N: usize>(consumer: Consumer<LogEvent, N>) {
    let backoff = Duration::from_millis(1);
    loop {
        if let Some(event) = consumer.pop() {
            let delta = event.timestamp.elapsed();
            match event.kind {
                LogEventKind::EngineStarted { venue, symbol } => {
                    eprintln!("[engine] {}:{} started (+{}us)", venue, symbol, delta.as_micros());
                }
                LogEventKind::EngineApplyLatency { micros } => {
                    eprintln!("[latency] apply {}us (+{}us)", micros, delta.as_micros());
                }
                LogEventKind::EngineMetricsFlushed => {
                    eprintln!("[metrics] flushed (+{}us)", delta.as_micros());
                }
                LogEventKind::HandlerInfo { source, detail } => {
                    eprintln!("[{}] {} (+{}us)", source, detail, delta.as_micros());
                }
                LogEventKind::HandlerError { source, detail } => {
                    eprintln!("[{} ERROR] {} (+{}us)", source, detail, delta.as_micros());
                }
                LogEventKind::Text(text) => {
                    eprintln!("[log] {} (+{}us)", text, delta.as_micros());
                }
            }
        } else {
            thread::sleep(backoff);
        }
    }
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
        let (log_tx, log_rx) = channel::<LogEvent, LOG_CAPACITY>();
        let logger = Logger::with_producer(log_tx.clone());
        thread::Builder::new()
            .name("logger".into())
            .spawn(move || logging_worker(log_rx))
            .expect("spawn logger thread");

        let core = core_affinity::CoreId { id: self.cfg.core_id };
        core_affinity::set_for_current(core);
        logger.emit(LogEventKind::EngineStarted {
            venue: self.cfg.venue.clone(),
            symbol: self.cfg.symbol.clone(),
        });

        let mut last_flush = Instant::now();
        loop {
            if let Some(latency) = self.handler.poll_once()? {
                logger.emit(LogEventKind::EngineApplyLatency {
                    micros: latency.as_micros() as u64,
                });
            }
            if last_flush.elapsed() >= Duration::from_secs(5) {
                self.handler.flush_metrics();
                logger.emit(LogEventKind::EngineMetricsFlushed);
                last_flush = Instant::now();
            }
            // light backoff if nothing arrived (handler keeps this small)
            std::hint::spin_loop();
        }
    }
}
