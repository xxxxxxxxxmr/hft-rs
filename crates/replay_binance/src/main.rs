mod book;
mod capture;

use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use book::{L3Book, Side};
use capture::{DepthDiff, DiffStream, convert_levels, load_snapshot};

#[derive(Debug)]
struct Args {
    snapshot: PathBuf,
    diffs: PathBuf,
    price_precision: u32,
    speed: ReplaySpeed,
    validate_sequence: bool,
    output: Option<PathBuf>,
    output_format: OutputFormat,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ReplaySpeed {
    Max,
    Real,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum OutputFormat {
    Jsonl,
    Bincode,
}

impl Args {
    fn from_env() -> Result<Self> {
        let mut snapshot: Option<PathBuf> = None;
        let mut diffs: Option<PathBuf> = None;
        let mut price_precision: u32 = 4;
        let mut speed = ReplaySpeed::Max;
        let mut validate_sequence = false;
        let mut output: Option<PathBuf> = None;
        let mut output_format = OutputFormat::Jsonl;
        let mut format_specified = false;

        let mut iter = env::args().skip(1);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--snapshot" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--snapshot requires a path"))?;
                    snapshot = Some(PathBuf::from(value));
                }
                "--diffs" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--diffs requires a path"))?;
                    diffs = Some(PathBuf::from(value));
                }
                "--price-precision" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--price-precision requires a value"))?;
                    price_precision = value
                        .parse::<u32>()
                        .map_err(|_| anyhow!("invalid price precision: {value}"))?;
                }
                "--speed" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--speed requires a value"))?;
                    speed = ReplaySpeed::from_str(&value)?;
                }
                "--validate-sequence" => {
                    validate_sequence = true;
                }
                "--out" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--out requires a path"))?;
                    output = Some(PathBuf::from(value));
                }
                "--out-format" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| anyhow!("--out-format requires a value"))?;
                    output_format = OutputFormat::from_str(&value)?;
                    format_specified = true;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                unknown => {
                    return Err(anyhow!("unknown argument: {unknown}"));
                }
            }
        }

        let snapshot = snapshot.ok_or_else(|| anyhow!("missing required --snapshot"))?;
        let diffs = diffs.ok_or_else(|| anyhow!("missing required --diffs"))?;

        if format_specified && output.is_none() {
            return Err(anyhow!("--out-format requires --out"));
        }

        Ok(Self {
            snapshot,
            diffs,
            price_precision,
            speed,
            validate_sequence,
            output,
            output_format,
        })
    }
}

impl ReplaySpeed {
    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "max" => Ok(ReplaySpeed::Max),
            "real" => Ok(ReplaySpeed::Real),
            other => Err(anyhow!("invalid speed: {other}")),
        }
    }
}

impl OutputFormat {
    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "jsonl" => Ok(OutputFormat::Jsonl),
            "bincode" => Ok(OutputFormat::Bincode),
            other => Err(anyhow!("invalid output format: {other}")),
        }
    }
}

fn print_usage() {
    eprintln!(
        "Usage: replay_binance --snapshot <file> --diffs <file> [--price-precision <n>] [--speed <max|real>] [--validate-sequence] [--out <file>] [--out-format <jsonl|bincode>]"
    );
}

fn main() -> Result<()> {
    let args = Args::from_env()?;
    run(args)
}

fn run(args: Args) -> Result<()> {
    let price_scale = 10u64.checked_pow(args.price_precision).ok_or_else(|| {
        anyhow!(
            "price precision {precision} too large",
            precision = args.price_precision
        )
    })?;

    let snapshot = load_snapshot(&args.snapshot)?;
    let mut book = L3Book::new();
    let mut sink = EventSink::new(args.output.as_deref(), args.output_format)?;

    let bid_snapshot = convert_levels(&snapshot.bids, price_scale)?;
    let ask_snapshot = convert_levels(&snapshot.asks, price_scale)?;

    emit_events(
        &mut sink,
        book.seed_from_snapshot(Side::Bid, &bid_snapshot, snapshot.last_update_id, None),
    )?;
    emit_events(
        &mut sink,
        book.seed_from_snapshot(Side::Ask, &ask_snapshot, snapshot.last_update_id, None),
    )?;

    let mut last_update_id = snapshot.last_update_id;
    let mut diff_stream = DiffStream::from_path(&args.diffs)?;
    let mut previous_timestamp_ms: Option<u64> = None;

    while let Some(diff) = diff_stream.next_diff()? {
        if !should_apply_diff(&diff, last_update_id, args.validate_sequence)? {
            continue;
        }

        if args.speed == ReplaySpeed::Real {
            if let Some(ts) = diff.transaction_time.or(diff.event_time) {
                if let Some(prev) = previous_timestamp_ms {
                    if ts > prev {
                        let delta = ts - prev;
                        thread::sleep(Duration::from_millis(delta));
                    }
                }
                previous_timestamp_ms = Some(ts);
            }
        }

        let bid_updates = convert_levels(&diff.bids, price_scale)?;
        let ask_updates = convert_levels(&diff.asks, price_scale)?;
        let timestamp_ms = diff.transaction_time.or(diff.event_time);

        let mut events = Vec::new();
        events.extend(book.apply_updates(Side::Bid, &bid_updates, diff.final_id, timestamp_ms));
        events.extend(book.apply_updates(Side::Ask, &ask_updates, diff.final_id, timestamp_ms));
        emit_events(&mut sink, events)?;

        last_update_id = diff.final_id;
    }

    sink.finalize()?;
    Ok(())
}

enum EventSink {
    Stdout,
    Jsonl(BufWriter<File>),
    Bincode(BufWriter<File>),
}

impl EventSink {
    fn new(path: Option<&Path>, format: OutputFormat) -> Result<Self> {
        if let Some(p) = path {
            if let Some(parent) = p.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("creating output directory {parent:?}"))?;
                }
            }
            let file = File::create(p).with_context(|| format!("creating output file {p:?}"))?;
            let writer = BufWriter::new(file);
            match format {
                OutputFormat::Jsonl => Ok(EventSink::Jsonl(writer)),
                OutputFormat::Bincode => Ok(EventSink::Bincode(writer)),
            }
        } else {
            Ok(EventSink::Stdout)
        }
    }

    fn write(&mut self, event: &book::InferredEvent) -> Result<()> {
        match self {
            EventSink::Stdout => {
                let line = serde_json::to_string(event).context("serializing event")?;
                println!("{line}");
                Ok(())
            }
            EventSink::Jsonl(writer) => {
                serde_json::to_writer(&mut *writer, event).context("serializing event")?;
                writer.write_all(b"\n")?;
                Ok(())
            }
            EventSink::Bincode(writer) => {
                bincode::serialize_into(writer, event).context("writing bincode event")?;
                Ok(())
            }
        }
    }

    fn finalize(&mut self) -> Result<()> {
        match self {
            EventSink::Stdout => Ok(()),
            EventSink::Jsonl(writer) | EventSink::Bincode(writer) => {
                writer.flush().context("flushing output")
            }
        }
    }
}

fn should_apply_diff(diff: &DepthDiff, last_update_id: u64, strict: bool) -> Result<bool> {
    if diff.final_id <= last_update_id {
        return Ok(false);
    }

    if strict {
        let expected = last_update_id + 1;
        if diff.first_id > expected {
            return Err(anyhow!(
                "gap detected: expected first diff id <= {expected}, got {}",
                diff.first_id
            ));
        }
        if let Some(prev) = diff.previous_final_id {
            if prev != last_update_id {
                return Err(anyhow!(
                    "previous final id mismatch: expected {last_update_id}, got {prev}"
                ));
            }
        }
    } else if diff.first_id > last_update_id + 1 {
        eprintln!(
            "[warn] sequence gap detected: last={} first={} final={}",
            last_update_id, diff.first_id, diff.final_id
        );
    }

    Ok(true)
}

fn emit_events(sink: &mut EventSink, events: Vec<book::InferredEvent>) -> Result<()> {
    for event in events {
        sink.write(&event)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_diff(first: u64, final_id: u64, previous: Option<u64>) -> DepthDiff {
        DepthDiff {
            first_id: first,
            final_id,
            previous_final_id: previous,
            event_time: None,
            transaction_time: None,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    #[test]
    fn skip_outdated_diff() {
        let diff = make_diff(10, 15, Some(9));
        let apply = should_apply_diff(&diff, 20, true).unwrap();
        assert!(!apply);
    }

    #[test]
    fn detect_gap_in_strict_mode() {
        let diff = make_diff(25, 30, Some(24));
        let err = should_apply_diff(&diff, 20, true).unwrap_err();
        assert!(err.to_string().contains("gap"));
    }

    #[test]
    fn warn_on_gap_in_relaxed_mode() {
        let diff = make_diff(25, 30, Some(24));
        let apply = should_apply_diff(&diff, 20, false).unwrap();
        assert!(apply);
    }
}
