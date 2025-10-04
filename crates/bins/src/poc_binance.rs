use anyhow::{Result, anyhow};
use engine_core::{Engine, EngineConfig};
use std::env;
use std::path::PathBuf;
use ws_binance::{BinanceConfig, BinanceHandler, CaptureConfig};

fn print_usage() {
    eprintln!("Usage: poc_binance [--capture-dir <path>] [--capture-session <label>]");
}

fn main() -> Result<()> {
    let mut capture_dir: Option<PathBuf> = None;
    let mut session_label: Option<String> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--capture-dir" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow!("--capture-dir requires a path"))?;
                capture_dir = Some(PathBuf::from(path));
            }
            "--capture-session" => {
                let label = args
                    .next()
                    .ok_or_else(|| anyhow!("--capture-session requires a label"))?;
                session_label = Some(label);
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            other => {
                return Err(anyhow!("unknown argument: {other}"));
            }
        }
    }

    if capture_dir.is_none() && session_label.is_some() {
        return Err(anyhow!("--capture-session requires --capture-dir"));
    }

    let cfg = EngineConfig {
        symbol: "BTCUSDT".into(),
        venue: "binance".into(),
        core_id: 2, // pick your core
    };
    let handler_cfg = BinanceConfig {
        symbol: cfg.symbol.clone(),
        capture: capture_dir.map(|root| CaptureConfig {
            root_dir: root,
            session_label,
        }),
    };
    let handler = BinanceHandler::with_config(handler_cfg)?;
    let engine = Engine { cfg, handler };
    engine.run()
}
