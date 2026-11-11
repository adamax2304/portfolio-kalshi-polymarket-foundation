# STRATEGY_SHORTDATED_CRYPTO_PHASE2

## Hypothesis
Short-dated BTC/ETH threshold markets can contain tradable dislocations after conservative execution assumptions.

## Edge Sources
- Fair value dislocation vs implied probability.
- Cross-venue lead/lag response.
- Synthetic arb candidates with explicit uncertainty and settlement mismatch guardrails.

## Fair Value Design
- Base probability approximation with distance-to-strike, time-to-expiry, and realized vol windows (30s, 60s, 180s, 300s).
- Microstructure adjustment (L1/L2 imbalance, microprice deviation, trade pressure, reprice speed), bounded by cap.
- Regime adjustment from `market-regimes` to widen band and penalize confidence in stress.

## Signal Design
- Dynamic required edge threshold, not static.
- `estimated_net_edge_bps > dynamic_required_edge_bps` is mandatory.
- Confidence, spread, vol, depth, fill uncertainty, and regime all included.

## Paper Execution Assumptions
- Touch != fill.
- Partial/no fill outcomes enabled.
- Queue pressure and cancel/replace delay costs included.
- Near-expiry deterioration penalty included.

## Risks
- Approximate queue model.
- Incomplete settlement metadata can invalidate synthetic candidates.
- Replay overfitting risk if parameter sweeps are not out-of-sample validated.

