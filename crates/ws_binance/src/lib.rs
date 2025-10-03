use tungstenite::client::connect_with_config;
use anyhow::{anyhow, Context, Result};
use arrayvec::ArrayVec;
use ahash::AHashMap;
use engine_core::EngineHandler;
use hdrhistogram::Histogram;
use reqwest::blocking::Client;
use std::io;
use std::net::TcpStream;
use std::time::{Duration, Instant};
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::WebSocketConfig;
use tungstenite::{Message, WebSocket};
use tungstenite::stream::MaybeTlsStream;
use simd_json::prelude::{ValueAsContainer, ValueAsScalar};

/// Fixed-cap working buffers for zero/low-alloc parsing on hot path.
const MAX_LEVELS_PER_MSG: usize = 256;

/// Minimal order book using price -> qty map.
/// For a fast PoC, we pre-reserve and reuse; we avoid rehashing.
/// (True zero-alloc per tick would need a custom slab; this is good enough to start.)
#[derive(Default)]
struct Book {
    bids: AHashMap<u64, f64>,
    asks: AHashMap<u64, f64>,
}

impl Book {
    fn with_capacity(cap: usize) -> Self {
        Self {
            bids: AHashMap::with_capacity(cap),
            asks: AHashMap::with_capacity(cap),
        }
    }
    #[inline]
    fn set_bid(&mut self, px: u64, qty: f64) {
        if qty == 0.0 { self.bids.remove(&px); } else { self.bids.insert(px, qty); }
    }
    #[inline]
    fn set_ask(&mut self, px: u64, qty: f64) {
        if qty == 0.0 { self.asks.remove(&px); } else { self.asks.insert(px, qty); }
    }
}

/// Convert Binance "price as string" into integer ticks by multiplying by 1e4 (tick 0.0001).
/// Adjust if you want exact tick size; for BTCUSDT, 1e2 or 1e1 may be enough. Keep it simple here.
#[inline]
fn px_to_ticks(s: &str) -> Result<u64> {
    let v: f64 = lexical_core::parse(s.as_bytes())
        .map_err(|_| anyhow!("bad price"))?;
    Ok((v * 10_000.0).round() as u64)
}

#[inline]
fn qty_from_str(s: &str) -> Result<f64> {
    let v: f64 = lexical_core::parse(s.as_bytes())
        .map_err(|_| anyhow!("bad qty"))?;
    Ok(v)
}

/// Binance diff-depth JSON (subset)
#[derive(Debug)]
struct DiffOwned {
    first_id: u64,  // U
    final_id: u64,  // u
    bids: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
    asks: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
}

fn parse_diff_owned(buf: &mut [u8]) -> Result<DiffOwned> {
    let v: simd_json::BorrowedValue<'_> = simd_json::to_borrowed_value(buf)?;
    let obj = v.as_object().ok_or_else(|| anyhow!("not obj"))?;

    let first_id = obj.get("U").and_then(|x| x.as_u64()).ok_or_else(|| anyhow!("no U"))?;
    let final_id = obj.get("u").and_then(|x| x.as_u64()).ok_or_else(|| anyhow!("no u"))?;

    let mut bids = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();
    let mut asks = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();

    if let Some(bb) = obj.get("b").and_then(|x| x.as_array()) {
        for e in bb.iter().take(MAX_LEVELS_PER_MSG) {
            if let Some(pq) = e.as_array() {
                if pq.len() >= 2 {
                    if let (Some(p), Some(q)) = (pq[0].as_str(), pq[1].as_str()) {
                        bids.push((px_to_ticks(p)?, qty_from_str(q)?));
                    }
                }
            }
        }
    }
    if let Some(aa) = obj.get("a").and_then(|x| x.as_array()) {
        for e in aa.iter().take(MAX_LEVELS_PER_MSG) {
            if let Some(pq) = e.as_array() {
                if pq.len() >= 2 {
                    if let (Some(p), Some(q)) = (pq[0].as_str(), pq[1].as_str()) {
                        asks.push((px_to_ticks(p)?, qty_from_str(q)?));
                    }
                }
            }
        }
    }

    Ok(DiffOwned { first_id, final_id, bids, asks })
}

/// REST snapshot shape (subset)
#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct DepthSnap {
    lastUpdateId: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

pub struct BinanceHandler {
    symbol: String,              // e.g. BTCUSDT
    ws: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    http: Client,
    book: Book,
    last_update_id: u64,
    synced: bool,
    // metrics
    hist: Histogram<u64>,
    // buffers
    rx_buf: Vec<u8>,
}

impl BinanceHandler {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            ws: None,
            http: Client::builder()
                .user_agent("hft-rs/0.1 binance")
                .build()
                .expect("http client"),
            book: Book::with_capacity(4096),
            last_update_id: 0,
            synced: false,
            hist: Histogram::new(3).unwrap(),
            rx_buf: vec![0u8; 1 << 16], // 64 KiB
        }
    }

    fn testnet_ws_url(&self) -> String {
        // depth stream at ~100ms; we’ll use single-stream endpoint
        let stream = format!("{}@depth@100ms", self.symbol.to_lowercase());
        format!("wss://testnet.binance.com/ws/{}", stream)
    }

    fn snapshot_url(&self) -> String {
        // 1000 levels snapshot to minimize early resyncs
        format!(
            "https://testnet.binance.com/api/v3/depth?symbol={}&limit=1000",
            self.symbol.to_uppercase()
        )
    }

    fn ensure_ws(&mut self) -> Result<()> {
        if self.ws.is_some() { return Ok(()); }

        // 1) Connect WS with small read timeout and TCP_NODELAY
        let url = self.testnet_ws_url();
        let req = url.clone().into_client_request()?;
        let ws_cfg = WebSocketConfig {
            max_message_size: Some(1 << 22),  // 4 MiB
            max_frame_size: Some(1 << 22),
            ..Default::default()
        };
        let (ws, _resp) = connect_with_config(req, Some(ws_cfg), 0)
            .context("ws connect")?;

        self.ws = Some(ws);

        // 2) Take REST snapshot
        let snap: DepthSnap = self.http.get(self.snapshot_url())
            .send()?.error_for_status()?
            .json()?;
        self.apply_snapshot(&snap)?;
        self.last_update_id = snap.lastUpdateId;
        self.synced = false; // need to roll forward until we see a diff with u >= lastUpdateId

        Ok(())
    }

    fn apply_snapshot(&mut self, snap: &DepthSnap) -> Result<()> {
        self.book.bids.clear();
        self.book.asks.clear();
        for (p, q) in &snap.bids {
            let px = px_to_ticks(p)?;
            let qty = qty_from_str(q)?;
            if qty != 0.0 { self.book.set_bid(px, qty); }
        }
        for (p, q) in &snap.asks {
            let px = px_to_ticks(p)?;
            let qty = qty_from_str(q)?;
            if qty != 0.0 { self.book.set_ask(px, qty); }
        }
        Ok(())
    }

    fn apply_diff(&mut self, d: &DiffOwned) -> Result<()> {
        if !self.synced {
            if d.final_id < self.last_update_id + 1 {
                return Ok(());
            }
            if d.first_id <= self.last_update_id + 1 && self.last_update_id + 1 <= d.final_id {
                self.synced = true;
            } else {
                return Err(anyhow!(
                    "seq gap before sync: have={}, diff[{}..{}]",
                    self.last_update_id, d.first_id, d.final_id
                ));
            }
        }
    
        for (px, qty) in d.bids.iter() {
            self.book.set_bid(*px, *qty);
        }
        for (px, qty) in d.asks.iter() {
            self.book.set_ask(*px, *qty);
        }
        self.last_update_id = d.final_id;
        Ok(())
    }
}

impl EngineHandler for BinanceHandler {
    fn poll_once(&mut self) -> Result<Option<Duration>> {
        self.ensure_ws()?;

        let ws = self.ws.as_mut().unwrap();
        // Read one frame with a small timeout. We set read_timeout on the socket,
        // so tungstenite will surface WouldBlock / io::ErrorKind::WouldBlock via the TLS layer.
        let msg = match ws.read() {
            Ok(m) => m,
            Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
                // nothing to do
                return Ok(None);
            }
            Err(tungstenite::Error::AlreadyClosed) => {
                self.ws = None;
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if let Message::Text(txt) = msg {
            let len = txt.len().min(self.rx_buf.len());
            self.rx_buf[..len].copy_from_slice(&txt.as_bytes()[..len]);
        
            let t0 = Instant::now();
            let diff = parse_diff_owned(&mut self.rx_buf[..len])?;
            self.apply_diff(&diff)?;
            let elapsed = t0.elapsed();
            self.hist.record(elapsed.as_micros() as u64).ok();
            return Ok(Some(elapsed));
        } else if let Message::Binary(bin) = msg {
            let len = bin.len().min(self.rx_buf.len());
            self.rx_buf[..len].copy_from_slice(&bin[..len]);
        
            let t0 = Instant::now();
            let diff = parse_diff_owned(&mut self.rx_buf[..len])?;
            self.apply_diff(&diff)?;
            let elapsed = t0.elapsed();
            self.hist.record(elapsed.as_micros() as u64).ok();
            return Ok(Some(elapsed));
        }
        // Ignore pings/pongs/others for now
        Ok(None)
    }

    fn flush_metrics(&mut self) {
        let p50 = self.hist.value_at_percentile(50.0);
        let p99 = self.hist.value_at_percentile(99.0);
        eprintln!("[binance:{}] packet→apply: p50={}us p99={}us lastU={}",
            self.symbol, p50, p99, self.last_update_id);
    }
}
