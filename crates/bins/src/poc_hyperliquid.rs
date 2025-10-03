use engine_core::{Engine, EngineConfig};
use ws_hyperliquid::HyperHandler;

fn main() -> anyhow::Result<()> {
    let cfg = EngineConfig {
        symbol: "BTC".into(),        // HL coin symbol
        venue: "hyperliquid".into(),
        core_id: 3,                  // pick your core
    };
    let handler = HyperHandler::new(cfg.symbol.clone());
    let engine = Engine { cfg, handler };
    engine.run()
}