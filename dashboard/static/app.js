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

// ---------- Partition log strips ----------

const STRIP_CELLS = 32;      // offsets visible per partition row
const STRIP_HEADROOM = 4;    // empty slots kept to the right of the log end
const prevLogEnds = new Map(); // "topic/partition" -> next_offset from previous refresh
const openDetails = new Set(); // keeps <details> open across re-renders

const PALETTE = ["#67e8f9", "#a78bfa", "#f472b6", "#fbbf24", "#34d399", "#fb923c", "#60a5fa", "#e879f9"];

function colorFor(name) {
  let hash = 0;
  for (const ch of String(name)) hash = (hash * 31 + ch.charCodeAt(0)) >>> 0;
  return PALETTE[hash % PALETTE.length];
}

function chip(name, extra = "") {
  return `<span class="chip" style="--chip:${colorFor(name)}"><i></i>${escapeHtml(name)}${extra}</span>`;
}

function groupsForTopic(groups, topicName) {
  return Object.entries(groups || {}).filter(([, g]) => g.topic === topicName);
}

function committedFor(committedOffsets, groupId, topic, partitionId) {
  const value = committedOffsets?.[groupId]?.[`${topic}-${partitionId}`];
  return value === undefined ? 0 : Number(value);
}

function renderPartitionStrip(topic, partition, topicGroups, committedOffsets) {
  const pid = partition.partition_id;
  const end = Number(partition.next_offset);
  const stripKey = `${topic.name}/${pid}`;
  const prevEnd = prevLogEnds.get(stripKey);
  prevLogEnds.set(stripKey, end);

  const windowStart = Math.max(0, end - (STRIP_CELLS - STRIP_HEADROOM));
  const samples = new Map(normalizeMessages(partition.sample_messages).map((m) => [Number(m.offset), m]));

  const groupStates = topicGroups.map(([groupId, g]) => {
    const committed = Math.min(committedFor(committedOffsets, groupId, topic.name, pid), end);
    return { groupId, owner: g.assignments?.[String(pid)] || null, committed, lag: Math.max(0, end - committed) };
  });
  const consumedByAll = groupStates.length ? Math.min(...groupStates.map((s) => s.committed)) : 0;

  const cells = [];
  for (let i = 0; i < STRIP_CELLS; i += 1) {
    const offset = windowStart + i;
    const classes = ["cell"];
    let title = `offset ${offset}`;
    if (offset < end) {
      classes.push(offset < consumedByAll ? "consumed" : "stored");
      if (prevEnd !== undefined && offset >= prevEnd) classes.push("fresh");
      const msg = samples.get(offset);
      if (msg) title += ` · key=${msg.key ?? ""} · ${JSON.stringify(msg.value)}`;
    } else if (offset === end) {
      classes.push("next");
      title += " · next write";
    } else {
      classes.push("empty");
    }
    cells.push(`<span class="${classes.join(" ")}" title="${escapeHtml(title)}"></span>`);
  }

  const markers = groupStates.map((s, idx) => {
    const clipped = s.committed < windowStart;
    const pos = clipped ? 0 : ((s.committed - windowStart) / STRIP_CELLS) * 100;
    return `<span class="marker${clipped ? " clipped" : ""}" style="left:${pos}%;--chip:${colorFor(s.groupId)};--row:${idx}"
      title="${escapeHtml(`${s.groupId} committed offset ${s.committed} · lag ${s.lag}`)}"></span>`;
  }).join("");

  const groupRows = groupStates.map((s) => `
    <div class="lag-line">
      ${chip(s.groupId)}
      <span class="muted">→</span>
      ${s.owner ? chip(s.owner) : `<span class="muted tiny-inline">unassigned</span>`}
      <span class="lag ${s.lag === 0 ? "caught-up" : ""}">${s.lag === 0 ? "caught up" : `lag ${fmt(s.lag)}`}</span>
    </div>
  `).join("");

  const detailsKey = `samples:${stripKey}`;
  const sampleList = [...samples.values()].reverse();

  return `
    <div class="pstrip">
      <div class="pstrip-head">
        <strong>P${pid}</strong>
        <span class="muted">${fmt(end)} message${end === 1 ? "" : "s"}${prevEnd !== undefined && end > prevEnd ? ` <span class="delta">+${fmt(end - prevEnd)}</span>` : ""}</span>
      </div>
      <div class="pstrip-track" style="--cells:${STRIP_CELLS};--marker-rows:${Math.max(1, groupStates.length)}">
        ${cells.join("")}
        ${markers}
      </div>
      <div class="pstrip-axis muted">
        <span>${windowStart > 0 ? `… ${fmt(windowStart)}` : "0"}</span>
        <span>next offset ${fmt(end)}</span>
      </div>
      ${groupRows}
      ${sampleList.length ? `
        <details data-key="${escapeHtml(detailsKey)}" ${openDetails.has(detailsKey) ? "open" : ""}>
          <summary class="muted">Latest ${sampleList.length} message${sampleList.length === 1 ? "" : "s"}</summary>
          <table class="msg-table">
            <thead><tr><th>Offset</th><th>Key</th><th>Value</th></tr></thead>
            <tbody>${sampleList.map((m) => `
              <tr><td>${fmt(m.offset)}</td><td>${escapeHtml(m.key ?? "")}</td><td><code>${escapeHtml(JSON.stringify(m.value))}</code></td></tr>
            `).join("")}</tbody>
          </table>
        </details>` : ""}
    </div>
  `;
}

function renderTopics(topics, metrics) {
  const groups = metrics.groups || {};
  const committedOffsets = metrics.committed_offsets || {};
  const legend = `
    <div class="strip-legend muted">
      <span><span class="cell stored"></span>stored</span>
      <span><span class="cell consumed"></span>consumed by every group</span>
      <span><span class="cell fresh-legend"></span>new since last refresh</span>
      <span><span class="cell next"></span>next write</span>
      <span><span class="marker-legend"></span>group's committed offset</span>
    </div>`;

  const cards = topics.map((topic) => {
    const topicGroups = groupsForTopic(groups, topic.name);
    const total = topic.partitions.reduce((sum, p) => sum + Number(p.next_offset), 0);
    return `
      <article class="topic-card">
        <div class="topic-title">
          <h3>${escapeHtml(topic.name)}</h3>
          <span class="muted">${topic.partitions.length} partitions · ${fmt(total)} messages${topicGroups.length ? ` · ${topicGroups.length} group${topicGroups.length === 1 ? "" : "s"}` : ""}</span>
        </div>
        <div class="partition-list">
          ${topic.partitions.map((p) => renderPartitionStrip(topic, p, topicGroups, committedOffsets)).join("")}
        </div>
      </article>`;
  }).join("");

  document.getElementById("topics").innerHTML = cards
    ? legend + cards
    : `<p class="muted">No topics found yet. Create one and produce a few messages to bring the demo to life.</p>`;
}

function renderGroups(metrics) {
  const groups = metrics.groups || {};
  const committedOffsets = metrics.committed_offsets || {};
  const partitionOffsets = metrics.partition_offsets || {};

  document.getElementById("groups").innerHTML = Object.entries(groups).map(([groupId, g]) => {
    const ends = partitionOffsets[g.topic] || {};
    const rows = Object.keys(ends).sort((a, b) => Number(a) - Number(b)).map((pid) => {
      const end = Number(ends[pid]);
      const committed = Math.min(committedFor(committedOffsets, groupId, g.topic, pid), end);
      const pct = end ? (committed / end) * 100 : 100;
      const owner = g.assignments?.[String(pid)];
      return `
        <div class="offset-row">
          <span class="muted">P${pid}</span>
          <div class="offset-bar"><div style="width:${pct}%;background:${colorFor(owner || groupId)}"></div></div>
          <span class="offset-num">${fmt(committed)} / ${fmt(end)}</span>
        </div>`;
    }).join("");
    const totalLag = Object.keys(ends).reduce((sum, pid) => sum + Math.max(0, Number(ends[pid]) - committedFor(committedOffsets, groupId, g.topic, pid)), 0);
    return `
      <div class="group-card">
        <div class="topic-title">
          <h3>${chip(groupId)}</h3>
          <span class="lag ${totalLag === 0 ? "caught-up" : ""}">${totalLag === 0 ? "caught up" : `lag ${fmt(totalLag)}`}</span>
        </div>
        <div class="muted tiny">topic <strong>${escapeHtml(g.topic)}</strong> · ${g.members.length} member${g.members.length === 1 ? "" : "s"}</div>
        <div class="chip-row">${g.members.map((m) => chip(m)).join("") || `<span class="muted">no active members</span>`}</div>
        ${rows}
      </div>`;
  }).join("") || `<p class="muted">No consumer groups are active yet. Join one from the Consumer group panel.</p>`;
}

document.getElementById("topics").addEventListener("toggle", (event) => {
  const key = event.target?.dataset?.key;
  if (!key) return;
  if (event.target.open) openDetails.add(key); else openDetails.delete(key);
}, true);

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

  renderTopics(topics, metrics);
  renderGroups(metrics);

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
