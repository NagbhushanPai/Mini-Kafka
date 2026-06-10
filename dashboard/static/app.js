const kpiDefs = [
  ["topics", "Topics"],
  ["partitions", "Partitions"],
  ["active_groups", "Groups"],
  ["active_consumers", "Consumers"],
  ["messages_stored", "Stored msgs"],
];

const fmt = (value) => new Intl.NumberFormat().format(value ?? 0);

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function loadSnapshot() {
  const res = await fetch("/api/snapshot", { cache: "no-store" });
  const data = await res.json();
  if (!res.ok || data.status === "error") {
    throw new Error(data.message || `Request failed with ${res.status}`);
  }
  return data;
}

function render(snapshot) {
  const { metrics, topics } = snapshot;
  document.getElementById("connectionState").textContent = "Connected";

  document.getElementById("kpis").innerHTML = kpiDefs.map(([key, label]) => `
    <article class="kpi">
      <div class="label">${label}</div>
      <div class="num">${fmt(metrics[key])}</div>
    </article>
  `).join("");

  document.getElementById("topics").innerHTML = topics.map((topic) => `
    <article class="topic-card">
      <div class="topic-title">
        <h3>${escapeHtml(topic.name)}</h3>
        <span class="muted">${topic.partitions.length} partitions</span>
      </div>
      <div class="partition-list">
        ${topic.partitions.map((partition) => `
          <div class="partition-pill">
            <strong>Partition ${partition.partition_id} - next offset ${fmt(partition.next_offset)}</strong>
            <div class="muted">${fmt(partition.message_count)} sample messages shown</div>
            <div class="sample">${escapeHtml(JSON.stringify(partition.sample_messages, null, 2))}</div>
          </div>
        `).join("")}
      </div>
    </article>
  `).join("") || `<p class="muted">No topics found yet. Create one and produce a few messages to bring the demo to life.</p>`;

  document.getElementById("groups").innerHTML = Object.entries(metrics.committed_offsets || {}).map(([groupId, offsets]) => `
    <div class="group-card">
      <h3>${escapeHtml(groupId)}</h3>
      <div class="muted">${Object.keys(offsets).length} tracked partitions</div>
      <pre class="sample">${escapeHtml(JSON.stringify(offsets, null, 2))}</pre>
    </div>
  `).join("") || `<p class="muted">No consumer groups are active yet.</p>`;

  document.getElementById("metrics").innerHTML = [
    ["Throughput", metrics.throughput],
    ["Avg producer latency ms", metrics.avg_producer_latency_ms],
    ["P95 producer latency ms", metrics.p95_producer_latency_ms],
    ["Avg end-to-end latency ms", metrics.avg_e2e_latency_ms],
    ["Consumer lag", metrics.consumer_lag],
  ].map(([label, value]) => `
    <div class="metric-row">
      <div class="muted">${label}</div>
      <div class="value">${typeof value === "number" ? fmt(Number(value.toFixed(2))) : escapeHtml(value)}</div>
    </div>
  `).join("");
}

async function refresh() {
  const state = document.getElementById("connectionState");
  state.textContent = "Refreshing...";
  try {
    const snapshot = await loadSnapshot();
    render(snapshot);
  } catch (error) {
    state.textContent = "Broker unavailable";
    document.getElementById("topics").innerHTML = `<p class="muted">Could not load live data: ${escapeHtml(error.message)}</p>`;
  }
}

document.getElementById("refreshBtn").addEventListener("click", refresh);
refresh();
setInterval(refresh, 5000);
