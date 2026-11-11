import { getPhase2Snapshot } from "@/lib/api";

export default async function FairValuePage() {
  const snapshot = await getPhase2Snapshot();
  const rows = ((snapshot.fair as { data?: Array<Record<string, unknown>> }).data || []).slice(0, 200);

  return (
    <main className="grid">
      <section className="card">
        <h3>Fair Value Monitor</h3>
        <table>
          <thead>
            <tr>
              <th>Contract</th>
              <th>Underlying</th>
              <th>Fair Prob</th>
              <th>Band</th>
              <th>Implied</th>
              <th>Confidence</th>
              <th>Regime</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((item) => {
              const fair = (item.fair as Record<string, unknown>) || {};
              return (
                <tr key={String(item.event_id)}>
                  <td className="mono">{String(item.canonical_contract_key)}</td>
                  <td>{String(item.underlying)}</td>
                  <td>{String(fair.fair_prob)}</td>
                  <td>
                    {String(fair.fair_prob_low)} .. {String(fair.fair_prob_high)}
                  </td>
                  <td>{String(item.market_implied_prob)}</td>
                  <td>{String(fair.confidence_score)}</td>
                  <td className="mono">{JSON.stringify(fair.market_regime_flags || [])}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>
    </main>
  );
}

