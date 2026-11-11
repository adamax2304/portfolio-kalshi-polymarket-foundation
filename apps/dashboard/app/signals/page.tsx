import { getPhase2Snapshot } from "@/lib/api";

export default async function SignalsPage() {
  const snapshot = await getPhase2Snapshot();
  const rows = ((snapshot.signals as { data?: Array<Record<string, unknown>> }).data || []).slice(0, 300);

  return (
    <main className="grid">
      <section className="card">
        <h3>Signal Tape</h3>
        <pre className="mono">{JSON.stringify(snapshot.signalStats, null, 2)}</pre>
        <table>
          <thead>
            <tr>
              <th>Signal ID</th>
              <th>Type</th>
              <th>Contract</th>
              <th>Edge bps</th>
              <th>Confidence</th>
              <th>Regime</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((item) => (
              <tr key={String(item.signal_id)}>
                <td className="mono">{String(item.signal_id)}</td>
                <td>{String(item.signal_type)}</td>
                <td className="mono">{String(item.canonical_contract_key)}</td>
                <td>{String(item.estimated_net_edge_bps)}</td>
                <td>{String(item.confidence)}</td>
                <td className="mono">{JSON.stringify(item.market_regime_flags || [])}</td>
                <td className={item.accepted ? "ok" : "warn"}>
                  {item.accepted ? "accepted" : String(item.rejected_reason || "rejected")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

