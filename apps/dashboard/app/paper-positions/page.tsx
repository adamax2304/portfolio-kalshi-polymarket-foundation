import { getPhase2Snapshot } from "@/lib/api";

export default async function PaperPositionsPage() {
  const snapshot = await getPhase2Snapshot();
  const positions = ((snapshot.paperPositions as { data?: Array<Record<string, unknown>> }).data || []).slice(0, 200);
  const decisions = ((snapshot.paperDecisions as { data?: Array<Record<string, unknown>> }).data || []).slice(0, 100);

  return (
    <main className="grid">
      <section className="card">
        <h3>Paper Positions</h3>
        <pre className="mono">{JSON.stringify(snapshot.paperStatus, null, 2)}</pre>
        <table>
          <thead>
            <tr>
              <th>Contract</th>
              <th>Qty</th>
              <th>Avg Price</th>
              <th>Realized PnL</th>
              <th>Fees</th>
            </tr>
          </thead>
          <tbody>
            {positions.map((item) => (
              <tr key={String(item.canonical_contract_key)}>
                <td className="mono">{String(item.canonical_contract_key)}</td>
                <td>{String(item.qty)}</td>
                <td>{String(item.avg_price)}</td>
                <td>{String(item.realized_pnl)}</td>
                <td>{String(item.fees)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="card">
        <h3>Recent Paper Decisions</h3>
        <table>
          <thead>
            <tr>
              <th>Traded</th>
              <th>Fill Ratio</th>
              <th>Executed Qty</th>
              <th>Fill Prob</th>
              <th>Reasons</th>
            </tr>
          </thead>
          <tbody>
            {decisions.map((item, i) => (
              <tr key={i}>
                <td>{String(item.traded)}</td>
                <td>{String(item.fill_ratio)}</td>
                <td>{String(item.executed_qty)}</td>
                <td>{String(item.fill_probability_score)}</td>
                <td className="mono">{JSON.stringify(item.reasons || [])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

