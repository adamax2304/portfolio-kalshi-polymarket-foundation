# REPLAY_WORKFLOW_PHASE2

## Run Replay
```powershell
cargo run -p replay-runner -- --from 2026-01-10T00:00:00Z --to 2026-01-10T02:00:00Z --underlying BTC --mode paper --speed x20
```

## Interpret Results
- Check accepted vs rejected signals.
- Check traded vs accepted (fill realism).
- Check realized PnL and compare with markout quality.
- Inspect rejected reasons for threshold/risk over-filtering.

## Compare Configs
```powershell
cargo run -p ops-cli -- replay compare --run-a <run_a> --run-b <run_b>
```

## Recommended First Slice
- BTC 15m baseline.
- ETH 15m baseline.
- Combined BTC+ETH after single-asset sanity checks.
