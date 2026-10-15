/**
 * Replenishment Dashboard Frontend Logic
 * Connects to REST APIs and WebSocket for live Kafka transaction stream & Chart.js forecasts.
 */

let forecastChart = null;
let streamTxnCount = 0;
let isStreamingActive = true;
let socket = null;

// Initialize on page load
document.addEventListener("DOMContentLoaded", () => {
  initChart();
  fetchSystemStatus();
  loadRecommendations(false);
  updateChart();
  connectWebSocket();

  // Periodic poll for recommendations & status
  setInterval(() => loadRecommendations(false), 10000);
  setInterval(fetchSystemStatus, 15000);
});

// Initialize Chart.js
function initChart() {
  const ctx = document.getElementById("forecastChart").getContext("2d");
  forecastChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Actual Daily Withdrawals ($)",
          data: [],
          borderColor: "#38bdf8",
          backgroundColor: "rgba(56, 189, 248, 0.1)",
          borderWidth: 2,
          pointRadius: 4,
          pointBackgroundColor: "#38bdf8",
          tension: 0.25,
          fill: true
        },
        {
          label: "Next-Day ML Forecast ($)",
          data: [],
          borderColor: "#f59e0b",
          backgroundColor: "rgba(245, 158, 11, 0.15)",
          borderWidth: 2.5,
          borderDash: [5, 5],
          pointRadius: 6,
          pointBackgroundColor: "#f59e0b",
          pointHoverRadius: 8,
          tension: 0.25
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      },
      plugins: {
        legend: {
          labels: {
            color: "#94a3b8",
            font: { family: "Inter", size: 12 }
          }
        },
        tooltip: {
          backgroundColor: "rgba(15, 23, 42, 0.95)",
          titleColor: "#f8fafc",
          bodyColor: "#cbd5e1",
          borderColor: "rgba(255, 255, 255, 0.1)",
          borderWidth: 1,
          padding: 12,
          callbacks: {
            label: function(context) {
              const val = context.raw;
              if (val === null || val === undefined) return null;
              return `${context.dataset.label}: $${val.toLocaleString()}`;
            }
          }
        }
      },
      scales: {
        x: {
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: { color: "#64748b", font: { family: "Inter", size: 11 } }
        },
        y: {
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: {
            color: "#64748b",
            font: { family: "Inter", size: 11 },
            callback: value => `$${(value / 1000).toFixed(0)}k`
          }
        }
      }
    }
  });
}

// Fetch System Status & Telemetry
async function fetchSystemStatus() {
  try {
    const res = await fetch("/api/status");
    const data = await res.json();

    // Update navbar badges dynamically
    const kafkaModeEl = document.getElementById("kafka-mode-label");
    const pillKafka = document.getElementById("pill-kafka");
    if (kafkaModeEl) {
      if (data.is_streaming) {
        kafkaModeEl.innerText = `Kafka: Streaming`;
        if (pillKafka) pillKafka.classList.remove("paused");
      } else {
        kafkaModeEl.innerText = `Kafka: Paused`;
        if (pillKafka) pillKafka.classList.add("paused");
      }
    }
    
    const hdfsStatusEl = document.getElementById("hdfs-status-label");
    if (hdfsStatusEl) {
      hdfsStatusEl.innerText = `HDFS: ${data.hdfs.partitions_count} Partitions (${Math.round(data.hdfs.total_size_kb).toLocaleString()} KB)`;
    }
    
    const modelStatusEl = document.getElementById("model-status-label");
    if (modelStatusEl) {
      modelStatusEl.innerText = `Model: ${data.forecasting_model.accuracy_percent}% Acc`;
    }

    // Update telemetry boxes
    const hdfsNamenodeEl = document.getElementById("hdfs-namenode");
    if (hdfsNamenodeEl) hdfsNamenodeEl.innerText = data.hdfs.namenode_uri;

    const hdfsNodesEl = document.getElementById("hdfs-datanodes");
    if (hdfsNodesEl) hdfsNodesEl.innerText = `${data.hdfs.active_datanodes_count} Active Nodes`;

    const hdfsPartitionsEl = document.getElementById("hdfs-partitions");
    if (hdfsPartitionsEl) hdfsPartitionsEl.innerText = `${data.hdfs.partitions_count} Daily Partitions`;

    const hdfsSizeEl = document.getElementById("hdfs-size");
    if (hdfsSizeEl) hdfsSizeEl.innerText = `${data.hdfs.total_size_kb.toLocaleString()} KB`;

    const modelAlgoEl = document.getElementById("model-algo");
    if (modelAlgoEl) modelAlgoEl.innerText = data.forecasting_model.name;

    const modelAccEl = document.getElementById("model-acc");
    if (modelAccEl) modelAccEl.innerText = `${data.forecasting_model.accuracy_percent}%`;

    const modelMaeEl = document.getElementById("model-mae");
    if (modelMaeEl) modelMaeEl.innerText = `$${Math.round(data.forecasting_model.mae).toLocaleString()}`;

    const modelMapeEl = document.getElementById("model-mape");
    if (modelMapeEl) modelMapeEl.innerText = `${data.forecasting_model.mape}%`;

    // Stream button sync
    isStreamingActive = data.is_streaming;
    updateStreamButtonUI();
  } catch (err) {
    console.error("Failed to fetch system status:", err);
  }
}

let isFetchingRecommendations = false;

// Load Recommendations & KPIs with Smart In-Place DOM Updates (No flickering/lag)
async function loadRecommendations(isUserClick = false) {
  if (isFetchingRecommendations && !isUserClick) return;
  isFetchingRecommendations = true;

  const refreshIcon = document.getElementById("refresh-icon");
  const refreshBtn = document.getElementById("btn-refresh-plan");
  
  if (isUserClick && refreshIcon) {
    refreshIcon.classList.add("spin-icon");
    if (refreshBtn) refreshBtn.disabled = true;
  }

  try {
    const res = await fetch("/api/recommendations");
    const data = await res.json();
    const kpis = data.kpis;
    const recs = data.recommendations;

    // Update KPIs
    const elStockout = document.getElementById("kpi-stockout");
    if (elStockout) elStockout.innerText = kpis.stockout_alerts;

    const elExcess = document.getElementById("kpi-excess");
    if (elExcess) elExcess.innerText = kpis.excess_cash_alerts;

    const elOptimal = document.getElementById("kpi-optimal");
    if (elOptimal) elOptimal.innerText = kpis.optimal_atms;

    const elNeeded = document.getElementById("kpi-total-needed");
    if (elNeeded) elNeeded.innerText = `$${Math.round(kpis.total_cash_needed).toLocaleString()}`;

    const elSaved = document.getElementById("kpi-idle-saved");
    if (elSaved) elSaved.innerText = `$${Math.round(kpis.idle_cash_saved_vs_fixed).toLocaleString()}`;

    // Render Table In-Place
    const tbody = document.getElementById("recommendations-tbody");
    if (tbody) {
      recs.forEach(r => {
        let tr = document.getElementById(`row-${r.atm_id}`);
        const isNewRow = !tr;
        if (isNewRow) {
          tr = document.createElement("tr");
          tr.id = `row-${r.atm_id}`;
        }

        let badgeClass = "badge-success";
        let badgeLabel = "🟢 Optimal";
        if (r.risk_status === "STOCKOUT_ALERT") {
          badgeClass = "badge-danger";
          badgeLabel = "🚨 Stockout Alert";
        } else if (r.risk_status === "EXCESS_CASH_ALERT") {
          badgeClass = "badge-warning";
          badgeLabel = "⚠️ Excess Cash";
        }

        const loadAmountFormatted = r.recommended_replenishment > 0
          ? `<strong>+$${Math.round(r.recommended_replenishment).toLocaleString()}</strong>`
          : `<span class="text-dim">$0 (No load)</span>`;

        // Context-aware action button
        let actionBtnHtml = "";
        if (r.risk_status === "STOCKOUT_ALERT") {
          const loadAmt = r.recommended_replenishment > 0 ? r.recommended_replenishment : 25000;
          actionBtnHtml = `
            <button class="btn btn-action" id="btn-action-${r.atm_id}" onclick="dispatchCashVan('${r.atm_id}', ${loadAmt})">
              🚚 Refill (+$${Math.round(loadAmt / 1000)}k)
            </button>
          `;
        } else if (r.risk_status === "EXCESS_CASH_ALERT") {
          actionBtnHtml = `
            <button class="btn btn-sweep" id="btn-action-${r.atm_id}" onclick="dispatchCashSweep('${r.atm_id}', 20000)">
              💰 Sweep ($20k)
            </button>
          `;
        } else {
          actionBtnHtml = `
            <button class="btn btn-optimal" id="btn-action-${r.atm_id}" onclick="dispatchCashVan('${r.atm_id}', 10000)">
              🚚 Top Up ($10k)
            </button>
          `;
        }

        tr.innerHTML = `
          <td>
            <div class="atm-cell-title">${r.atm_name}</div>
            <div class="atm-cell-sub"><code>${r.atm_id}</code></div>
          </td>
          <td>
            <div class="atm-cell-title">${r.location}</div>
            <div class="atm-cell-sub">${r.traffic_type}</div>
          </td>
          <td><strong id="cash-${r.atm_id}">$${Math.round(r.current_cash).toLocaleString()}</strong></td>
          <td><strong>$${Math.round(r.predicted_demand).toLocaleString()}</strong></td>
          <td class="text-dim">+$${Math.round(r.safety_stock).toLocaleString()}</td>
          <td>${loadAmountFormatted}</td>
          <td><span class="badge ${badgeClass}" id="badge-${r.atm_id}">${badgeLabel}</span></td>
          <td id="cell-action-${r.atm_id}">
            ${actionBtnHtml}
          </td>
        `;

        if (isNewRow) {
          tbody.appendChild(tr);
        }
      });
    }

    if (isUserClick) {
      showToast("🔄 Replenishment plan updated with real-time ATM balances!");
      fetchSystemStatus();
      updateChart();
    }
  } catch (err) {
    console.error("Failed to load recommendations:", err);
    if (isUserClick) {
      showToast("Error updating replenishment plan.");
    }
  } finally {
    isFetchingRecommendations = false;
    if (isUserClick && refreshIcon) {
      setTimeout(() => {
        refreshIcon.classList.remove("spin-icon");
        if (refreshBtn) refreshBtn.disabled = false;
      }, 400);
    }
  }
}

// Reset ATM Balances to problem statement scenario
async function resetAtmBalances() {
  const resetBtn = document.getElementById("btn-reset-balances");
  if (resetBtn) resetBtn.disabled = true;

  try {
    const res = await fetch("/api/atm/reset", { method: "POST" });
    const data = await res.json();
    showToast("⚡ ATMs reset: ATM_001 & ATM_002 low on cash, ATM_004 excess cash!");
    await loadRecommendations(false);
    await updateChart();
  } catch (err) {
    showToast("Failed to reset ATM balances.");
  } finally {
    if (resetBtn) resetBtn.disabled = false;
  }
}

// Emit Test Transaction immediately
async function emitTestTransaction() {
  try {
    const res = await fetch("/api/live-stream/emit-test", { method: "POST" });
    const data = await res.json();
    const txn = data.transaction;
    showToast(`⚡ Streamed txn: ${txn.transaction_id} on ${txn.atm_id} (-$${txn.amount_withdrawn})`);
  } catch (err) {
    showToast("Failed to emit test transaction.");
  }
}

// Update Forecast Chart
async function updateChart() {
  const select = document.getElementById("atm-select");
  if (!select) return;
  const atmId = select.value;

  try {
    const res = await fetch(`/api/chart-data/${atmId}`);
    const data = await res.json();

    forecastChart.data.labels = data.labels;
    forecastChart.data.datasets[0].data = data.actual_demands;
    forecastChart.data.datasets[1].data = data.forecast_demands;
    forecastChart.update();

    const summaryEl = document.getElementById("chart-summary");
    if (summaryEl) {
      summaryEl.innerHTML = `
        <strong>${data.atm_name} (${data.atm_id})</strong>: 
        Current Vault Cash: <strong>$${Math.round(data.current_cash).toLocaleString()}</strong> | 
        Tomorrow's Demand Forecast: <strong>$${Math.round(data.tomorrow_prediction).toLocaleString()}</strong>.
      `;
    }
  } catch (err) {
    console.error("Failed to update chart:", err);
  }
}

// Connect to WebSocket for Continuous Live Kafka Stream
function connectWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${protocol}//${window.location.host}/ws/transactions`;
  
  if (socket) {
    try { socket.close(); } catch (e) {}
  }

  socket = new WebSocket(wsUrl);
  const consoleEl = document.getElementById("stream-console");
  const countBadge = document.getElementById("stream-count-badge");

  socket.onopen = () => {
    console.log("[WebSocket] Connected to live Kafka transaction stream.");
    const placeholder = consoleEl ? consoleEl.querySelector(".terminal-placeholder") : null;
    if (placeholder) placeholder.style.display = "none";
  };

  socket.onmessage = event => {
    const msg = JSON.parse(event.data);
    streamTxnCount++;
    if (countBadge) countBadge.innerText = `${streamTxnCount} Streamed`;

    if (!consoleEl) return;

    const timeOnly = msg.timestamp ? (msg.timestamp.split(" ")[1] || msg.timestamp) : new Date().toLocaleTimeString();
    const line = document.createElement("div");

    // Live update the ATM's cash cell in the table if available
    if (msg.atm_id && msg.remaining_cash !== undefined) {
      const cashCell = document.getElementById(`cash-${msg.atm_id}`);
      if (cashCell) {
        cashCell.innerText = `$${Math.round(msg.remaining_cash).toLocaleString()}`;
      }
    }

    if (msg.event_type === "REPLENISHMENT") {
      line.className = "terminal-line replenish";
      line.innerHTML = `
        <div class="terminal-line-left">
          <span class="terminal-time">[${timeOnly}]</span>
          <span class="terminal-id" style="color:#10b981;">🚚 REPLENISH</span>
          <span class="terminal-atm">${msg.atm_name || msg.atm_id}</span>
        </div>
        <div>
          <span class="terminal-amount" style="color:#34d399;">+$${(msg.amount_replenished || 0).toLocaleString()}</span>
          <span class="terminal-bal">(Vault: $${(msg.remaining_cash || 0).toLocaleString()})</span>
        </div>
      `;
    } else if (msg.event_type === "CASH_SWEEP") {
      line.className = "terminal-line sweep";
      line.innerHTML = `
        <div class="terminal-line-left">
          <span class="terminal-time">[${timeOnly}]</span>
          <span class="terminal-id" style="color:#8b5cf6;">💰 CASH SWEEP</span>
          <span class="terminal-atm">${msg.atm_name || msg.atm_id}</span>
        </div>
        <div>
          <span class="terminal-amount" style="color:#c084fc;">-$${(msg.amount_swept || 0).toLocaleString()}</span>
          <span class="terminal-bal">(Vault: $${(msg.remaining_cash || 0).toLocaleString()})</span>
        </div>
      `;
    } else if (msg.event_type === "SYSTEM_RESET") {
      line.className = "terminal-line reset";
      line.innerHTML = `
        <div class="terminal-line-left">
          <span class="terminal-time">[${timeOnly}]</span>
          <span class="terminal-id" style="color:#f59e0b;">⚡ RESET</span>
          <span class="terminal-atm">${msg.alert || "Fleet Balances Reset"}</span>
        </div>
        <div>
          <span class="terminal-amount" style="color:#fbbf24;">Baseline Active</span>
        </div>
      `;
    } else {
      // Standard transaction
      const isStockout = msg.transaction_status === "CASH_OUT_FAILED";
      line.className = `terminal-line ${isStockout ? "stockout" : ""}`;

      line.innerHTML = `
        <div class="terminal-line-left">
          <span class="terminal-time">[${timeOnly}]</span>
          <span class="terminal-id">${msg.transaction_id}</span>
          <span class="terminal-atm">${msg.atm_name} (${msg.atm_id})</span>
        </div>
        <div>
          <span class="terminal-amount">${isStockout ? "🚨 FAILED (Cash Out)" : `-$${msg.amount_withdrawn}`}</span>
          <span class="terminal-bal">(Bal: $${msg.remaining_cash?.toLocaleString() || 0})</span>
        </div>
      `;
    }

    consoleEl.prepend(line);

    // Limit to 50 entries in DOM
    if (consoleEl.children.length > 50) {
      consoleEl.removeChild(consoleEl.lastChild);
    }
  };

  socket.onclose = () => {
    console.log("[WebSocket] Closed. Reconnecting in 2.5 seconds...");
    setTimeout(connectWebSocket, 2500);
  };

  socket.onerror = (err) => {
    console.error("[WebSocket] Error:", err);
  };
}

// Toggle Live Stream
async function toggleStream() {
  try {
    const res = await fetch("/api/live-stream/toggle", { method: "POST" });
    const data = await res.json();
    isStreamingActive = data.is_streaming;
    updateStreamButtonUI();
    showToast(isStreamingActive ? "Kafka Producer started streaming." : "Kafka Producer paused.");
  } catch (err) {
    console.error("Failed to toggle stream:", err);
  }
}

function updateStreamButtonUI() {
  const icon = document.getElementById("stream-btn-icon");
  const text = document.getElementById("stream-btn-text");
  if (!icon || !text) return;
  if (isStreamingActive) {
    icon.innerText = "⏸️";
    text.innerText = "Pause Stream";
  } else {
    icon.innerText = "▶️";
    text.innerText = "Start Stream";
  }
}

// Dispatch Cash Van Action
async function dispatchCashVan(atmId, amount) {
  const btn = document.getElementById(`btn-action-${atmId}`);
  const originalText = btn ? btn.innerHTML : "🚚 Refill";
  if (btn) {
    btn.innerHTML = "⏳ Refilling...";
    btn.disabled = true;
  }

  try {
    const res = await fetch("/api/atm/replenish", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ atm_id: atmId, amount: amount })
    });
    const data = await res.json();
    
    // Immediate visual feedback
    const cashCell = document.getElementById(`cash-${atmId}`);
    if (cashCell && data.updated_state?.current_cash) {
      cashCell.innerText = `$${Math.round(data.updated_state.current_cash).toLocaleString()}`;
    }

    showToast(data.message);
    await loadRecommendations(false);
    await updateChart();
  } catch (err) {
    showToast(`Failed to dispatch cash van to ${atmId}`);
    if (btn) {
      btn.innerHTML = originalText;
      btn.disabled = false;
    }
  }
}

// Dispatch Cash Sweep Action (Returns excess money to central bank vault)
async function dispatchCashSweep(atmId, amount) {
  const btn = document.getElementById(`btn-action-${atmId}`);
  const originalText = btn ? btn.innerHTML : "💰 Sweep";
  if (btn) {
    btn.innerHTML = "⏳ Sweeping...";
    btn.disabled = true;
  }

  try {
    const res = await fetch("/api/atm/sweep", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ atm_id: atmId, amount: amount })
    });
    const data = await res.json();

    const cashCell = document.getElementById(`cash-${atmId}`);
    if (cashCell && data.updated_state?.current_cash) {
      cashCell.innerText = `$${Math.round(data.updated_state.current_cash).toLocaleString()}`;
    }

    showToast(data.message);
    await loadRecommendations(false);
    await updateChart();
  } catch (err) {
    showToast(`Failed to sweep cash from ${atmId}`);
    if (btn) {
      btn.innerHTML = originalText;
      btn.disabled = false;
    }
  }
}

// Fleet Replenish: Dispatches cash vans to all at-risk machines in 1 click
async function dispatchFleetReplenish() {
  const btn = document.getElementById("btn-refill-all");
  const originalText = btn ? btn.innerHTML : "🚚 Refill All At-Risk";
  if (btn) {
    btn.innerHTML = "⏳ Dispatching Fleet...";
    btn.disabled = true;
  }

  try {
    const res = await fetch("/api/atm/replenish-all", { method: "POST" });
    const data = await res.json();
    showToast(`🚚 ${data.message}`);
    await loadRecommendations(false);
    await updateChart();
  } catch (err) {
    showToast("Failed to dispatch fleet replenishment.");
  } finally {
    if (btn) {
      btn.innerHTML = originalText;
      btn.disabled = false;
    }
  }
}

// Ingest New HDFS Partition
async function ingestHdfsPartition() {
  showToast("📦 Ingesting new daily partition into Hadoop HDFS...");
  try {
    const res = await fetch("/api/hdfs/ingest-partition", { method: "POST" });
    const data = await res.json();
    showToast(`📦 Created HDFS partition! Total partitions: ${data.total_partitions}`);
    await fetchSystemStatus();
    await loadRecommendations(false);
  } catch (err) {
    showToast("Failed to ingest HDFS partition.");
  }
}

// Retrain Model on HDFS Data
async function retrainModel() {
  showToast("Retraining time-series model on latest HDFS data...");
  try {
    const res = await fetch("/api/model/retrain", { method: "POST" });
    const data = await res.json();
    showToast(`Model retrained! New accuracy: ${data.metrics.accuracy_percent}% (MAE: $${Math.round(data.metrics.mae).toLocaleString()})`);
    await fetchSystemStatus();
    await loadRecommendations(false);
    await updateChart();
  } catch (err) {
    showToast("Model retraining failed.");
  }
}

// Toast Notification
function showToast(msg) {
  const toast = document.getElementById("toast");
  if (!toast) return;
  toast.innerText = msg;
  toast.classList.add("show");
  setTimeout(() => {
    toast.classList.remove("show");
  }, 3500);
}

