import { getPhase2Snapshot } from "@/lib/api";

export default async function ReplayRunsPage() {
  const snapshot = await getPhase2Snapshot();
  const rows = ((snapshot.replayRuns as { data?: Array<Record<string, unknown>> }).data || []).slice(0, 200);

  return (
    <main className="grid">
      <section className="card">
        <h3>Replay Runs</h3>
        <pre className="mono">{JSON.stringify(snapshot.analyticsOverview, null, 2)}</pre>
        <table>
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Mode</th>
              <th>Underlying</th>
              <th>Range</th>
              <th>Speed</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((item) => (
              <tr key={String(item.run_id)}>
                <td className="mono">{String(item.run_id)}</td>
                <td>{String(item.mode)}</td>
                <td>{String(item.underlying)}</td>
                <td className="mono">
                  {String(item.from_ts)} to {String(item.to_ts)}
                </td>
                <td>{String(item.speed)}</td>
                <td>{String(item.status)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

