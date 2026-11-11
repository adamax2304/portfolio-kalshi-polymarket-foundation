export type JsonValue = Record<string, unknown>;

const defaults = {
  metadata: process.env.METADATA_SERVICE_URL || "http://localhost:7012",
  recorder: process.env.RECORDER_SERVICE_URL || "http://localhost:7010",
  normalizer: process.env.NORMALIZER_SERVICE_URL || "http://localhost:7011",
  fair: process.env.FAIR_VALUE_ENGINE_URL || "http://localhost:7013",
  signal: process.env.SIGNAL_ENGINE_URL || "http://localhost:7014",
  paper: process.env.PAPER_EXECUTION_URL || "http://localhost:7015",
  analytics: process.env.ANALYTICS_ENGINE_URL || "http://localhost:7016",
  binance: process.env.GATEWAY_BINANCE_URL || "http://localhost:7001",
  polymarket: process.env.GATEWAY_POLYMARKET_URL || "http://localhost:7002",
  kalshi: process.env.GATEWAY_KALSHI_URL || "http://localhost:7003",
  deribit: process.env.GATEWAY_DERIBIT_URL || "http://localhost:7004"
};

export async function fetchJson(url: string): Promise<JsonValue> {
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return { error: `status_${response.status}` };
    }
    return (await response.json()) as JsonValue;
  } catch (error) {
    return { error: String(error) };
  }
}

export async function getGatewayHealth() {
  const [binance, polymarket, kalshi, deribit] = await Promise.all([
    fetchJson(`${defaults.binance}/health`),
    fetchJson(`${defaults.polymarket}/health`),
    fetchJson(`${defaults.kalshi}/health`),
    fetchJson(`${defaults.deribit}/health`)
  ]);
  return { binance, polymarket, kalshi, deribit };
}

export async function getFoundationSnapshot() {
  const [metadataHealth, recorderHealth, normalizerHealth, contracts, unresolved, recorderStats, services, gateways] =
    await Promise.all([
      fetchJson(`${defaults.metadata}/health`),
      fetchJson(`${defaults.recorder}/health`),
      fetchJson(`${defaults.normalizer}/health`),
      fetchJson(`${defaults.metadata}/contracts`),
      fetchJson(`${defaults.metadata}/mappings/unresolved`),
      fetchJson(`${defaults.recorder}/stats`),
      fetchJson(`${defaults.metadata}/services`),
      getGatewayHealth()
    ]);
  return {
    metadataHealth,
    recorderHealth,
    normalizerHealth,
    contracts,
    unresolved,
    recorderStats,
    services,
    gateways
  };
}

export async function getPhase2Snapshot() {
  const [fair, signals, signalStats, paperStatus, paperPositions, paperDecisions, analyticsOverview, replayRuns] =
    await Promise.all([
      fetchJson(`${defaults.fair}/fair/list`),
      fetchJson(`${defaults.signal}/signals/tail`),
      fetchJson(`${defaults.signal}/signals/stats`),
      fetchJson(`${defaults.paper}/paper/status`),
      fetchJson(`${defaults.paper}/paper/positions`),
      fetchJson(`${defaults.paper}/paper/decisions`),
      fetchJson(`${defaults.analytics}/analytics/overview`),
      fetchJson(`${defaults.analytics}/analytics/runs`)
    ]);
  return {
    fair,
    signals,
    signalStats,
    paperStatus,
    paperPositions,
    paperDecisions,
    analyticsOverview,
    replayRuns
  };
}
