import { getGatewayHealth } from "@/lib/api";

export default async function StreamsPage() {
  const data = await getGatewayHealth();
  const rows = [
    { venue: "Polymarket", payload: data.polymarket },
    { venue: "Kalshi", payload: data.kalshi },
    { venue: "Binance", payload: data.binance },
    { venue: "Deribit", payload: data.deribit }
  ];

  return (
    <main className="grid">
      <section className="card">
        <h3>Streams</h3>
        <table>
          <thead>
            <tr>
              <th>Venue</th>
              <th>Connection</th>
              <th>Reconnects</th>
              <th>Last Message</th>
              <th>Latency ms</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const stream = (row.payload as { stream?: Record<string, unknown> }).stream || {};
              return (
                <tr key={row.venue}>
                  <td>{row.venue}</td>
                  <td>{String(stream.connection_status || "unknown")}</td>
                  <td>{String(stream.reconnect_count || 0)}</td>
                  <td className="mono">{String(stream.last_message_ts || "n/a")}</td>
                  <td>{String(stream.current_latency_ms || "n/a")}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>
    </main>
  );
}

