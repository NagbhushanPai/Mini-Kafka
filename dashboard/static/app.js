const kpiDefs = [
  ["topics", "Topics"],
  ["partitions", "Partitions"],
  ["active_groups", "Groups"],
  ["active_consumers", "Consumers"],
  ["messages_stored", "Stored msgs"],
];

const fmt = (value) => new Intl.NumberFormat().format(Number(value ?? 0));

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function normalizeMessages(messages) {
  if (Array.isArray(messages)) return messages;
  if (messages && Array.isArray(messages.messages)) return messages.messages;
  return [];
}

async function loadSnapshot() {
  const res = await fetch("/api/snapshot", { cache: "no-store" });
  const data = await res.json();
  if (!res.ok || data.status === "error") {
    throw new Error(data.message || `Request failed with ${res.status}`);
  }
  return data;
}

function renderNarrative(metrics, topics) {
  const story = document.getElementById("systemNarrative");
  const topicCount = Number(metrics?.topics ?? topics.length ?? 0);
  const partitionCount = Number(metrics?.partitions ?? topics.reduce((sum, topic) => sum + (topic.partitions?.length || 0), 0));
  const groupCount = Number(metrics?.active_groups ?? 0);
  const consumerCount = Number(metrics?.active_consumers ?? 0);
  const storedMessages = Number(metrics?.messages_stored ?? 0);

  story.innerHTML = `
    <article class="story-card">
      <span>Write path</span>
      <strong>${fmt(storedMessages)}</strong>
      <p>Append-only events currently stored across ${fmt(partitionCount)} partitions in ${fmt(topicCount)} topic(s).</p>
    </article>
    <article class="story-card">
      <span>Coordination</span>
      <strong>${fmt(groupCount)}</strong>
      <p>Consumer groups are tracking progress and rebalancing work to keep reads distributed.</p>
    </article>
    <article class="story-card">
      <span>Delivery</span>
      <strong>${fmt(consumerCount)}</strong>
      <p>Active consumer members are polling assigned partitions and advancing offsets from the latest commit.</p>
    </article>
  `;
}

function flash(id) {
  const el = document.getElementById(id);
  el.classList.remove("refreshed");
  void el.offsetWidth;
  el.classList.add("refreshed");
}

function render(snapshot) {
  const { metrics = {}, topics = [] } = snapshot;
  document.getElementById("connectionState").textContent = "Connected";
  document.getElementById("statusDot").classList.remove("offline");
  document.getElementById("lastUpdated").textContent = `Last updated ${new Date().toLocaleTimeString()}`;
  renderNarrative(metrics, topics);

  document.getElementById("kpis").innerHTML = kpiDefs.map(([key, label]) => `
    <article class="kpi">
      <div class="label">${label}</div>
      <div class="num">${fmt(metrics[key])}</div>
    </article>
  `).join("");
  flash("kpis");

  document.getElementById("topics").innerHTML = topics.map((topic) => `
    <article class="topic-card">
      <div class="topic-title">
        <h3>${escapeHtml(topic.name)}</h3>
        <span class="muted">${topic.partitions.length} partitions</span>
      </div>
      <div class="partition-list">
        ${topic.partitions.map((partition) => {
          const sampleMessages = normalizeMessages(partition.sample_messages);
          return `
            <div class="partition-pill">
              <strong>Partition ${partition.partition_id} — next offset ${fmt(partition.next_offset)}</strong>
              <div class="muted">${fmt(partition.message_count ?? sampleMessages.length)} sample messages shown</div>
              <pre class="sample">${escapeHtml(JSON.stringify(sampleMessages, null, 2)) || "[]"}</pre>
            </div>
          `;
        }).join("")}
      </div>
    </article>
  `).join("") || `<p class="muted">No topics found yet. Create one and produce a few messages to bring the demo to life.</p>`;

  const committedOffsets = metrics.committed_offsets || {};
  document.getElementById("groups").innerHTML = Object.entries(committedOffsets).map(([groupId, offsets]) => `
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
      <div class="value">${typeof value === "number" ? fmt(Number(value.toFixed(2))) : escapeHtml(value ?? "—")}</div>
    </div>
  `).join("");
  flash("metrics");
  flash("topics");
  flash("groups");
}

async function refresh() {
  try {
    const snapshot = await loadSnapshot();
    render(snapshot);
  } catch (error) {
    document.getElementById("connectionState").textContent = "Broker unavailable";
    document.getElementById("statusDot").classList.add("offline");
    document.getElementById("lastUpdated").textContent = `Last attempt failed at ${new Date().toLocaleTimeString()}`;
    document.getElementById("topics").innerHTML = `<p class="muted">Could not load live data: ${escapeHtml(error.message)}</p>`;
    document.getElementById("groups").innerHTML = `<p class="muted">Consumer group state is unavailable while the broker is offline.</p>`;
    document.getElementById("metrics").innerHTML = `<div class="metric-row"><div class="muted">Status</div><div class="value">Offline</div></div>`;
  }
}

document.getElementById("refreshBtn").addEventListener("click", refresh);

async function loadBenchmarkScenarios() {
  try {
    const data = await (await fetch("/api/benchmarks/scenarios")).json();
    const select = document.getElementById("benchmarkScenario");
    select.innerHTML = Object.entries(data.scenarios).map(([name, scenario]) => `
      <option value="${name}">${name} — ${scenario.producers} producers, ${scenario.consumers} consumers, ${scenario.messages.toLocaleString()} messages</option>
    `).join("");
  } catch (error) {
    document.getElementById("benchmarkStatus").textContent = `Scenarios unavailable: ${error.message}`;
  }
}

const BENCHMARK_STAGE_LABELS = {
  queued: "Queued",
  starting: "Starting",
  setup: "Setting up",
  running: "Running",
  cancelling: "Cancelling",
  cancelled: "Cancelled",
  complete: "Complete",
  error: "Error",
};

let activeBenchmarkId = null;

function setBenchmarkRunning(running) {
  document.getElementById("runBenchmark").disabled = running;
  document.getElementById("cancelBenchmark").disabled = !running;
  document.getElementById("benchmarkScenario").disabled = running;
}

function renderBenchmarkStage(status) {
  const badge = document.getElementById("benchmarkStageBadge");
  badge.textContent = BENCHMARK_STAGE_LABELS[status] || status;
  badge.className = `stage-badge stage-${status}`;
}

function renderBenchmarkDetails(job) {
  const rows = [
    ["Messages", `${fmt(job.current || 0)} / ${fmt(job.total || 0)}`],
  ];
  if (job.report) {
    rows.push(
      ["Throughput", `${fmt(Number((job.report.throughput || 0).toFixed(2)))} msg/sec`],
      ["Avg producer latency ms", fmt(Number((job.report.avg_producer_latency_ms || 0).toFixed(2)))],
      ["Avg end-to-end latency ms", fmt(Number((job.report.avg_e2e_latency_ms || 0).toFixed(2)))],
      ["Consumer lag", fmt(job.report.consumer_lag || 0)],
      ["Duration", `${Number((job.report.duration_seconds || 0).toFixed(2))}s`],
    );
  }
  document.getElementById("benchmarkDetails").innerHTML = rows.map(([label, value]) => `
    <div class="metric-row">
      <div class="muted">${label}</div>
      <div class="value">${escapeHtml(String(value))}</div>
    </div>
  `).join("");
}

async function watchBenchmark(id) {
  try {
    const job = await (await fetch(`/api/benchmarks/${id}`)).json();
    document.getElementById("benchmarkStatus").textContent = `${job.message} (${job.progress || 0}%)`;
    const fill = document.getElementById("benchmarkProgressFill");
    fill.style.width = `${job.progress || 0}%`;
    fill.classList.toggle("running", !["complete", "error", "cancelled"].includes(job.status));
    renderBenchmarkStage(job.status);
    renderBenchmarkDetails(job);
    if (job.report) {
      document.getElementById("benchmarkReport").textContent = JSON.stringify(job.report, null, 2);
    }
    if (!["complete", "error", "cancelled"].includes(job.status)) {
      setTimeout(() => watchBenchmark(id), 750);
    } else {
      activeBenchmarkId = null;
      setBenchmarkRunning(false);
    }
  } catch (error) {
    document.getElementById("benchmarkStatus").textContent = error.message;
    renderBenchmarkStage("error");
    activeBenchmarkId = null;
    setBenchmarkRunning(false);
  }
}

document.getElementById("runBenchmark").addEventListener("click", async () => {
  setBenchmarkRunning(true);
  document.getElementById("benchmarkReport").textContent = "";
  document.getElementById("benchmarkDetails").innerHTML = "";
  renderBenchmarkStage("queued");
  document.getElementById("benchmarkProgressFill").style.width = "0%";
  try {
    const payload = { scenario: document.getElementById("benchmarkScenario").value };
    const response = await fetch("/api/benchmarks/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok || data.status === "error") {
      throw new Error(data.message || "Benchmark submission failed");
    }
    activeBenchmarkId = data.id;
    watchBenchmark(data.id);
  } catch (error) {
    document.getElementById("benchmarkStatus").textContent = error.message;
    renderBenchmarkStage("error");
    setBenchmarkRunning(false);
  }
});

document.getElementById("cancelBenchmark").addEventListener("click", async () => {
  if (!activeBenchmarkId) return;
  const button = document.getElementById("cancelBenchmark");
  button.disabled = true;
  renderBenchmarkStage("cancelling");
  try {
    await fetch(`/api/benchmarks/${activeBenchmarkId}/cancel`, { method: "POST" });
  } catch (error) {
    document.getElementById("benchmarkStatus").textContent = error.message;
  }
});

loadBenchmarkScenarios();

async function post(path, payload) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await res.json();
  if (!res.ok || data.status === "error") throw new Error(data.message || "Request failed");
  return data;
}

function formData(form) {
  return Object.fromEntries(new FormData(form));
}

function showResult(data) {
  document.getElementById("actionResult").textContent = JSON.stringify(data, null, 2);
  refresh();
}

document.getElementById("topicForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    showResult(await post("/api/topics", formData(event.target)));
  } catch (error) {
    showResult({ error: error.message });
  }
});

document.getElementById("messageForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    showResult(await post("/api/messages", formData(event.target)));
  } catch (error) {
    showResult({ error: error.message });
  }
});

document.querySelectorAll("#groupForm button").forEach((button) => {
  button.addEventListener("click", async (event) => {
    event.preventDefault();
    try {
      showResult(await post(`/api/groups/${button.dataset.action}`, formData(event.target.form)));
    } catch (error) {
      showResult({ error: error.message });
    }
  });
});

document.getElementById("browseBtn").addEventListener("click", async () => {
  const q = new URLSearchParams({
    topic: document.getElementById("browseTopic").value,
    partition: document.getElementById("browsePartition").value,
    offset: document.getElementById("browseOffset").value,
    limit: document.getElementById("browseLimit").value,
  });

  try {
    const response = await fetch(`/api/messages?${q}`);
    const data = await response.json();
    if (!response.ok || data.status === "error") {
      throw new Error(data.message || "Could not load messages");
    }
    document.getElementById("messages").textContent = JSON.stringify(data, null, 2);
  } catch (error) {
    document.getElementById("messages").textContent = error.message;
  }
});

document.getElementById("resetBtn").addEventListener("click", async () => {
  const button = document.getElementById("resetBtn");
  const status = document.getElementById("resetStatus");
  if (!confirm("Reset the live demo? Current topics, messages, and offsets will be archived as a session snapshot, then cleared.")) return;
  button.disabled = true;
  status.textContent = "Archiving current state and resetting broker...";
  try {
    const data = await post("/api/reset", {});
    const session = data.archived_session;
    status.textContent = session
      ? `Archived session ${session.session_id} (${fmt(session.total_messages)} messages across ${session.topics.length} topic(s)). Broker reset — ready for a fresh demo.`
      : "Broker reset.";
    refresh();
    loadSessions();
  } catch (error) {
    status.textContent = `Reset failed: ${error.message}`;
  } finally {
    button.disabled = false;
  }
});

function renderSessionDetail(session) {
  const detail = document.getElementById("sessionDetail");
  if (!session) {
    detail.textContent = "";
    return;
  }
  const lines = session.topics.map((topic) => {
    const partitionSummary = topic.partitions
      .map((p) => `  partition ${p.partition_id}: ${fmt(p.message_count)} messages`)
      .join("\n");
    return `${topic.name}\n${partitionSummary}`;
  });
  detail.textContent = lines.length
    ? `Session ${session.session_id} — archived ${session.archived_at}\n\n${lines.join("\n\n")}`
    : `Session ${session.session_id} — archived ${session.archived_at}\n\nNo topics were present at reset time.`;
}

async function loadSessions() {
  const list = document.getElementById("sessionList");
  try {
    const res = await fetch("/api/sessions", { cache: "no-store" });
    const data = await res.json();
    if (!res.ok || data.status === "error") throw new Error(data.message || "Could not load sessions");
    const sessions = data.sessions || [];
    list.innerHTML = sessions.length
      ? sessions.map((s) => `
        <button type="button" class="session-row" data-id="${escapeHtml(s.session_id)}">
          <span>${escapeHtml(s.archived_at)}</span>
          <span class="muted">${fmt(s.topic_count)} topics · ${fmt(s.total_messages)} messages</span>
        </button>
      `).join("")
      : `<p class="muted">No previous sessions yet. Click "Reset demo" to archive the current state.</p>`;
    list.querySelectorAll(".session-row").forEach((row) => {
      row.addEventListener("click", async () => {
        const res = await fetch(`/api/sessions/${encodeURIComponent(row.dataset.id)}`, { cache: "no-store" });
        const data = await res.json();
        if (res.ok && data.status !== "error") renderSessionDetail(data.session);
      });
    });
  } catch (error) {
    list.innerHTML = `<p class="muted">Could not load sessions: ${escapeHtml(error.message)}</p>`;
  }
}

document.getElementById("refreshSessionsBtn").addEventListener("click", loadSessions);
loadSessions();

refresh();
setInterval(refresh, 5000);
