use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct DepthDiff {
    #[serde(rename = "U")]
    pub first_id: u64,
    #[serde(rename = "u")]
    pub final_id: u64,
    #[serde(rename = "pu", default)]
    pub previous_final_id: Option<u64>,
    #[serde(rename = "E", default)]
    pub event_time: Option<u64>,
    #[serde(rename = "T", default)]
    pub transaction_time: Option<u64>,
    #[serde(rename = "b", default)]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a", default)]
    pub asks: Vec<[String; 2]>,
}

pub fn load_snapshot(path: &Path) -> Result<Snapshot> {
    let file = File::open(path).with_context(|| format!("opening snapshot {path:?}"))?;
    let snapshot =
        serde_json::from_reader(file).with_context(|| format!("parsing snapshot {path:?}"))?;
    Ok(snapshot)
}

pub fn price_to_ticks(raw: &str, scale: u64) -> Result<u64> {
    let price: f64 =
        lexical_core::parse(raw.as_bytes()).map_err(|_| anyhow!("invalid price: {raw}"))?;
    if price < 0.0 {
        return Err(anyhow!("negative price: {price}"));
    }
    let ticks = (price * scale as f64).round();
    Ok(ticks as u64)
}

pub fn qty_from_str(raw: &str) -> Result<f64> {
    let qty: f64 =
        lexical_core::parse(raw.as_bytes()).map_err(|_| anyhow!("invalid quantity: {raw}"))?;
    if qty < 0.0 {
        return Err(anyhow!("negative quantity: {qty}"));
    }
    Ok(qty)
}

pub fn convert_levels(levels: &[[String; 2]], scale: u64) -> Result<Vec<(u64, f64)>> {
    let mut out = Vec::with_capacity(levels.len());
    for pair in levels {
        let price_ticks = price_to_ticks(&pair[0], scale)?;
        let qty = qty_from_str(&pair[1])?;
        out.push((price_ticks, qty));
    }
    Ok(out)
}

pub struct DiffStream<R> {
    reader: BufReader<R>,
    buffer: String,
}

impl DiffStream<File> {
    pub fn from_path(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("opening diffs {path:?}"))?;
        Ok(Self::new(file))
    }
}

impl<R: std::io::Read> DiffStream<R> {
    pub fn new(inner: R) -> Self {
        Self {
            reader: BufReader::new(inner),
            buffer: String::new(),
        }
    }

    pub fn next_diff(&mut self) -> Result<Option<DepthDiff>> {
        loop {
            self.buffer.clear();
            let len = self.reader.read_line(&mut self.buffer)?;
            if len == 0 {
                return Ok(None);
            }
            let trimmed = self.buffer.trim();
            if trimmed.is_empty() {
                continue;
            }
            let diff: DepthDiff = serde_json::from_str(trimmed)
                .with_context(|| format!("parsing diff line: {trimmed}"))?;
            return Ok(Some(diff));
        }
    }
}
