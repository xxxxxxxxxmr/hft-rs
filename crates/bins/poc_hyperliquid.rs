use engine_core::*;
use ws_hyperliquid::HyperHandler;

fn main() -> anyhow::Result<()> {
    let cfg = EngineConfig {
        symbol: "HYPE-PERP".into(),
        venue: "hyperliquid".into(),
        core_id: 3,
    };
    let handler = HyperHandler::new(cfg.symbol.clone());
    let engine = Engine { cfg, handler };
    engine.run()
}