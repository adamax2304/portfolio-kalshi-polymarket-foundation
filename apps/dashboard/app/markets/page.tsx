import { fetchJson } from "@/lib/api";

function rowsOf(value: unknown) {
  if (!value || typeof value !== "object") return [];
  const data = (value as { data?: unknown }).data;
  return Array.isArray(data) ? data : [];
}

export default async function MarketsPage() {
  const [contractsPayload, unresolvedPayload] = await Promise.all([
    fetchJson(`${process.env.METADATA_SERVICE_URL || "http://localhost:7012"}/contracts`),
    fetchJson(`${process.env.METADATA_SERVICE_URL || "http://localhost:7012"}/mappings/unresolved`)
  ]);
  const contracts = rowsOf(contractsPayload) as Array<Record<string, unknown>>;
  const unresolved = rowsOf(unresolvedPayload) as Array<Record<string, unknown>>;

  return (
    <main className="grid">
      <section className="card">
        <h3>Normalized Contracts</h3>
        <table>
          <thead>
            <tr>
              <th>Canonical Key</th>
              <th>Venue</th>
              <th>Venue Market ID</th>
              <th>Metadata</th>
            </tr>
          </thead>
          <tbody>
            {contracts.map((contract) => (
              <tr key={String(contract.canonical_contract_key)}>
                <td className="mono">{String(contract.canonical_contract_key)}</td>
                <td>{String(contract.venue)}</td>
                <td className="mono">{String(contract.venue_market_id)}</td>
                <td className="mono">{JSON.stringify(contract.metadata_json || {}, null, 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="card">
        <h3>Unresolved Mappings</h3>
        <table>
          <thead>
            <tr>
              <th>Id</th>
              <th>Event Type</th>
              <th>Payload</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {unresolved.map((item) => (
              <tr key={String(item.id)}>
                <td>{String(item.id)}</td>
                <td>{String(item.event_type)}</td>
                <td className="mono">{JSON.stringify(item.payload_json || {}, null, 0)}</td>
                <td className="mono">{String(item.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

