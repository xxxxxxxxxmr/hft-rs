use engine_core::{Engine, EngineConfig};
use ws_binance::BinanceHandler;

fn main() -> anyhow::Result<()> {
    let cfg = EngineConfig {
        symbol: "BTCUSDT".into(),
        venue: "binance".into(),
        core_id: 2, // pick your core
    };
    let handler = BinanceHandler::new(cfg.symbol.clone());
    let engine = Engine { cfg, handler };
    engine.run()
}
