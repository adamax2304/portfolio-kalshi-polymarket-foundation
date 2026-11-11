import { getFoundationSnapshot } from "@/lib/api";

function asRows(data: unknown): Array<Record<string, unknown>> {
  if (!data || typeof data !== "object") return [];
  const maybe = (data as { data?: unknown }).data;
  return Array.isArray(maybe) ? (maybe as Array<Record<string, unknown>>) : [];
}

export default async function OverviewPage() {
  const snapshot = await getFoundationSnapshot();
  const contracts = asRows(snapshot.contracts);
  const services = asRows(snapshot.services);
  const unresolved = asRows(snapshot.unresolved);

  return (
    <main className="grid">
      <section className="card">
        <h3>System Overview</h3>
        <div className="kpi">
          <div>
            <div className="sub">Services registered</div>
            <div className="title">{services.length}</div>
          </div>
          <div>
            <div className="sub">Contracts normalized</div>
            <div className="title">{contracts.length}</div>
          </div>
          <div>
            <div className="sub">Unresolved mappings</div>
            <div className={unresolved.length > 0 ? "title warn" : "title ok"}>{unresolved.length}</div>
          </div>
          <div>
            <div className="sub">Recorder raw persisted</div>
            <div className="title">{String((snapshot.recorderStats as { raw_persisted?: number })?.raw_persisted ?? 0)}</div>
          </div>
        </div>
      </section>

      <section className="card">
        <h3>Service Health</h3>
        <table>
          <thead>
            <tr>
              <th>Service</th>
              <th>Status</th>
              <th>Instance</th>
              <th>Build</th>
            </tr>
          </thead>
          <tbody>
            {[
              snapshot.gateways.binance,
              snapshot.gateways.polymarket,
              snapshot.gateways.kalshi,
              snapshot.gateways.deribit,
              snapshot.recorderHealth,
              snapshot.normalizerHealth,
              snapshot.metadataHealth
            ].map((item, idx) => {
              const service = (item as { service?: Record<string, string> }).service ?? {};
              const status = String(service.status || "unknown");
              return (
                <tr key={idx}>
                  <td>{String(service.service_name || "n/a")}</td>
                  <td className={status.includes("healthy") || status === "ok" ? "ok" : "warn"}>{status}</td>
                  <td className="mono">{String(service.instance_id || "n/a")}</td>
                  <td className="mono">{String(service.build_version || "n/a")}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>
    </main>
  );
}

