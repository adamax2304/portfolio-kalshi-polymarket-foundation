# CALIBRATION_GUIDE_PHASE2

## Parameter Sweep
Use config profiles in `infrastructure/config-profiles/*.json`.

```powershell
cargo run -p ops-cli -- run parameter-sweep --profile replay_btc_15m_baseline
```

## Calibrate
- Realized vol windows
- Dynamic edge multipliers
- Slippage assumptions
- Fill probability assumptions
- Near-expiry penalties
- Microstructure adjustment caps

## Avoid Overfitting
- Never optimize on one short window.
- Keep at least one holdout interval.
- Prefer parameter ranges that remain stable across BTC and ETH.

## Interpret Markout and Capture
- Good headline PnL with poor 1s/5s markout often indicates execution luck.
- Edge capture ratio < 1 with high gross edge often indicates slippage leakage.
- High rejected-signal rate can be healthy if it removes noise regimes.
