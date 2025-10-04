use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use arrayvec::ArrayVec;
use engine_core::{EngineHandler, EngineLogger, HandlerEvent};
use hdrhistogram::Histogram;
use simd_json::prelude::{ValueAsContainer, ValueAsScalar};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use tungstenite::client::connect_with_config;
use tungstenite::http;
use tungstenite::protocol::WebSocketConfig;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

/// HL WS (testnet): wss://api.hyperliquid-testnet.xyz/ws
/// Subscriptions doc: l2Book, trades, etc. (we'll use l2Book "BTC").
/// https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
/// https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
const MAX_LEVELS_PER_MSG: usize = 512;

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
        if qty == 0.0 {
            self.bids.remove(&px);
        } else {
            self.bids.insert(px, qty);
        }
    }
    #[inline]
    fn set_ask(&mut self, px: u64, qty: f64) {
        if qty == 0.0 {
            self.asks.remove(&px);
        } else {
            self.asks.insert(px, qty);
        }
    }
}

#[inline]
fn px_to_ticks(s: &str) -> Result<u64> {
    // HL prices are strings; pick 1e4 ticks for now.
    let v: f64 = lexical_core::parse(s.as_bytes()).map_err(|_| anyhow!("bad px"))?;
    Ok((v * 10_000.0).round() as u64)
}
#[inline]
fn sz_from_str(s: &str) -> Result<f64> {
    lexical_core::parse(s.as_bytes()).map_err(|_| anyhow!("bad sz"))
}

/// Minimal decoded WsBook: { coin, levels: [ [ {px, sz, n}... ], [ ... ] ], time }
/// We parse directly into owned (u64,f64) arrays for borrow freedom.
struct BookUpdate {
    bids: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
    asks: ArrayVec<(u64, f64), MAX_LEVELS_PER_MSG>,
}

fn parse_wsbook_owned(buf: &mut [u8]) -> Result<Option<BookUpdate>> {
    let v: simd_json::BorrowedValue<'_> = simd_json::to_borrowed_value(buf)?;
    let root = if let Some(obj) = v.as_object() {
        obj
    } else {
        return Ok(None);
    };

    let payload = root.get("data").and_then(|d| d.as_object()).unwrap_or(root);

    let levels = match payload.get("levels").and_then(|x| x.as_array()) {
        Some(lvls) => lvls,
        None => return Ok(None),
    };
    let bids_a = match levels.first().and_then(|x| x.as_array()) {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let asks_a = match levels.get(1).and_then(|x| x.as_array()) {
        Some(arr) => arr,
        None => return Ok(None),
    };

    let mut bids = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();
    let mut asks = ArrayVec::<(u64, f64), MAX_LEVELS_PER_MSG>::new();

    for e in bids_a.iter().take(MAX_LEVELS_PER_MSG) {
        if let Some(lvl) = e.as_object() {
            if let (Some(px), Some(sz)) = (
                lvl.get("px").and_then(|x| x.as_str()),
                lvl.get("sz").and_then(|x| x.as_str()),
            ) {
                bids.push((px_to_ticks(px)?, sz_from_str(sz)?));
            }
        }
    }
    for e in asks_a.iter().take(MAX_LEVELS_PER_MSG) {
        if let Some(lvl) = e.as_object() {
            if let (Some(px), Some(sz)) = (
                lvl.get("px").and_then(|x| x.as_str()),
                lvl.get("sz").and_then(|x| x.as_str()),
            ) {
                asks.push((px_to_ticks(px)?, sz_from_str(sz)?));
            }
        }
    }
    Ok(Some(BookUpdate { bids, asks }))
}

pub struct HyperHandler {
    coin: String, // e.g. "BTC"
    ws: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    book: Book,
    rx_buf: Vec<u8>,
    hist: Histogram<u64>,
    subscribed: bool,
    got_initial: bool,
}

impl HyperHandler {
    pub fn new(coin: String) -> Self {
        Self {
            coin,
            ws: None,
            book: Book::with_capacity(4096),
            rx_buf: vec![0u8; 1 << 16],
            hist: Histogram::new(3).unwrap(),
            subscribed: false,
            got_initial: false,
        }
    }

    fn ws_url(&self) -> String {
        "wss://api.hyperliquid-testnet.xyz/ws".to_string()
    }

    fn ensure_ws(&mut self, log: &EngineLogger) -> Result<()> {
        if self.ws.is_some() {
            return Ok(());
        }

        let url = self.ws_url();
        // explicit request with Origin tends to be robust
        let uri = url.parse::<http::Uri>().context("parse ws url")?;
        let mut builder = http::Request::builder()
            .method("GET")
            .uri(uri.clone())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", "hl-hft-rs")
            .header("Origin", "https://hyperliquid.xyz");

        if let Some(host) = uri.authority() {
            builder = builder.header("Host", host.as_str());
        }

        let req = builder.body(()).map_err(|e| anyhow!("{e}"))?;

        let ws_cfg = WebSocketConfig {
            max_message_size: Some(1 << 22),
            max_frame_size: Some(1 << 22),
            ..Default::default()
        };
        let (ws, _resp) = connect_with_config(req, Some(ws_cfg), 0).map_err(|e| {
            log.handler_event(
                "hyperliquid",
                &self.coin,
                HandlerEvent::Error {
                    detail: format!("ws connect failure: {e}"),
                },
            );
            e
        })?;
        self.ws = Some(ws);
        self.subscribed = false;
        log.handler_event("hyperliquid", &self.coin, HandlerEvent::Connected);
        Ok(())
    }

    fn subscribe_l2(&mut self, log: &EngineLogger) -> Result<()> {
        if self.subscribed {
            return Ok(());
        }
        let ws = self.ws.as_mut().ok_or_else(|| anyhow!("no ws"))?;

        // { "method": "subscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
        // per docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
        let sub = format!(
            r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"{}"}}}}"#,
            self.coin
        );
        ws.send(Message::Text(sub)).context("send sub")?;
        self.subscribed = true;
        log.handler_event("hyperliquid", &self.coin, HandlerEvent::SubscriptionSent);
        Ok(())
    }

    fn apply_book(&mut self, up: &BookUpdate) -> (usize, usize, bool) {
        // HL sends full snapshot-ish levels each push; treat as set operations
        for (px, sz) in up.bids.iter() {
            self.book.set_bid(*px, *sz);
        }
        for (px, sz) in up.asks.iter() {
            self.book.set_ask(*px, *sz);
        }
        let was_initial = !self.got_initial;
        if was_initial {
            self.got_initial = true;
        }
        (up.bids.len(), up.asks.len(), was_initial)
    }
}

impl EngineHandler for HyperHandler {
    fn poll_once(&mut self, log: &EngineLogger) -> Result<Option<Duration>> {
        self.ensure_ws(log)?;
        self.subscribe_l2(log)?;

        let ws = self.ws.as_mut().unwrap();
        let msg = match ws.read() {
            Ok(m) => m,
            Err(tungstenite::Error::AlreadyClosed) => {
                log.handler_event(
                    "hyperliquid",
                    &self.coin,
                    HandlerEvent::Warning {
                        detail: "ws closed by peer".into(),
                    },
                );
                self.ws = None;
                return Ok(None);
            }
            Err(tungstenite::Error::Io(e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Ok(None);
            }
            Err(e) => {
                log.handler_event(
                    "hyperliquid",
                    &self.coin,
                    HandlerEvent::Error {
                        detail: format!("ws read error: {e}"),
                    },
                );
                return Err(e.into());
            }
        };

        match msg {
            Message::Text(txt) => {
                let len = txt.len().min(self.rx_buf.len());
                self.rx_buf[..len].copy_from_slice(&txt.as_bytes()[..len]);
                // Only handle WsBook channel frames
                if txt.contains(r#""channel":"l2Book""#)
                    || txt.contains(r#""levels":"#)
                    || txt.contains(r#""levels":[["#)
                {
                    let t0 = Instant::now();
                    if let Some(up) = parse_wsbook_owned(&mut self.rx_buf[..len])? {
                        let (bids, asks, first) = self.apply_book(&up);
                        let dt = t0.elapsed();
                        self.hist.record(dt.as_micros() as u64).ok();
                        if first {
                            log.handler_event(
                                "hyperliquid",
                                &self.coin,
                                HandlerEvent::SnapshotApplied { bids, asks },
                            );
                        }
                        Ok(Some(dt))
                    } else {
                        log.handler_event(
                            "hyperliquid",
                            &self.coin,
                            HandlerEvent::Warning {
                                detail: "unexpected message without levels".into(),
                            },
                        );
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Message::Binary(bin) => {
                let len = bin.len().min(self.rx_buf.len());
                self.rx_buf[..len].copy_from_slice(&bin[..len]);
                let t0 = Instant::now();
                if let Some(up) = parse_wsbook_owned(&mut self.rx_buf[..len])? {
                    let (bids, asks, first) = self.apply_book(&up);
                    let dt = t0.elapsed();
                    self.hist.record(dt.as_micros() as u64).ok();
                    if first {
                        log.handler_event(
                            "hyperliquid",
                            &self.coin,
                            HandlerEvent::SnapshotApplied { bids, asks },
                        );
                    }
                    Ok(Some(dt))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn flush_metrics(&mut self, log: &EngineLogger) {
        let p50 = self.hist.value_at_percentile(50.0);
        let p99 = self.hist.value_at_percentile(99.0);
        log.handler_event(
            "hyperliquid",
            &self.coin,
            HandlerEvent::Metrics {
                p50,
                p99,
                last_seq: None,
            },
        );
    }
}
