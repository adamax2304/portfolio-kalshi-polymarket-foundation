import { fetchJson, getPhase2Snapshot } from "@/lib/api";

export default async function AnalyticsPage() {
  const snapshot = await getPhase2Snapshot();
  const analyticsBase = process.env.ANALYTICS_ENGINE_URL || "http://localhost:7016";
  const [rejected, pnlByUnderlying, signalPerformance, perRegime] = await Promise.all([
    fetchJson(`${analyticsBase}/analytics/rejected-signals`),
    fetchJson(`${analyticsBase}/analytics/pnl-by-underlying`),
    fetchJson(`${analyticsBase}/analytics/signal-performance`),
    fetchJson(`${analyticsBase}/analytics/per-regime`)
  ]);
  const rejectedRows = ((rejected as { data?: Array<Record<string, unknown>> }).data || []).slice(0, 200);

  return (
    <main className="grid">
      <section className="card">
        <h3>PnL Decomposition / Edge Capture (Overview)</h3>
        <pre className="mono">{JSON.stringify(snapshot.analyticsOverview, null, 2)}</pre>
        <pre className="mono">{JSON.stringify(pnlByUnderlying, null, 2)}</pre>
        <pre className="mono">{JSON.stringify(signalPerformance, null, 2)}</pre>
        <pre className="mono">{JSON.stringify(perRegime, null, 2)}</pre>
      </section>

      <section className="card">
        <h3>Rejected Signal Analysis</h3>
        <table>
          <thead>
            <tr>
              <th>Signal</th>
              <th>Contract</th>
              <th>Payload</th>
              <th>Created At</th>
            </tr>
          </thead>
          <tbody>
            {rejectedRows.map((row, i) => (
              <tr key={i}>
                <td className="mono">{String(row.signal_id)}</td>
                <td className="mono">{String(row.canonical_contract_key)}</td>
                <td className="mono">{JSON.stringify(row.payload_json || {}, null, 0)}</td>
                <td className="mono">{String(row.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}
