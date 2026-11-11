# PAPER_TRADING_PHASE2

## Paper Execution Model
- Modes: `passive_maker_sim`, `aggressive_taker_sim`, `maker_then_taker_sim`, `no_trade`.
- Queue approximation includes queue-ahead pressure and touch-fill penalty.
- Partial fills and no-fill outcomes are explicit.
- Taker slippage proxy uses depth pressure + near-expiry deterioration.
- Cancel/replace delay contributes additional execution cost.

## Limitations
- Queue position is proxy, not exact exchange queue state.
- No full matching-engine reconstruction yet.
- Venue-specific microstructure quirks are not fully parameterized.

## Realistic vs Approximate
- Realistic: conservative fill assumptions, risk gating, stale-feed blocking.
- Approximate: queue ordering details, hidden liquidity, full latency distribution.
- All approximations should remain visible in config and report outputs.

