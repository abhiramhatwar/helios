// Helios dashboard — WebSocket client + Chart.js real-time visualization

const MAX_FEED   = 60;  // max items in live feed
const MAX_CHART  = 60;  // seconds of chart history

// --- State ---
let totalEvents   = 0;
let totalAnomalies = 0;
let eventsThisSec = 0;
let anomaliesThisSec = 0;
let feedCount     = 0;
const anomalyLogEmpty = document.querySelector('#anomaly-log .italic');

// Bucket arrays for the rate chart (one entry per second).
const chartLabels      = Array(MAX_CHART).fill('');
const chartDataAll     = Array(MAX_CHART).fill(0);
const chartDataAnomal  = Array(MAX_CHART).fill(0);

// --- Chart ---
const ctx = document.getElementById('rateChart').getContext('2d');
const rateChart = new Chart(ctx, {
  type: 'line',
  data: {
    labels: chartLabels,
    datasets: [
      {
        label: 'All events/s',
        data: chartDataAll,
        borderColor: 'rgba(59,130,246,0.8)',
        backgroundColor: 'rgba(59,130,246,0.08)',
        borderWidth: 1.5,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
      },
      {
        label: 'Anomalies/s',
        data: chartDataAnomal,
        borderColor: 'rgba(239,68,68,0.8)',
        backgroundColor: 'rgba(239,68,68,0.08)',
        borderWidth: 1.5,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
      },
    ],
  },
  options: {
    responsive: true,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: { legend: { display: false } },
    scales: {
      x: {
        ticks: { display: false },
        grid: { color: 'rgba(255,255,255,0.04)' },
      },
      y: {
        beginAtZero: true,
        ticks: { color: '#4b5563', maxTicksLimit: 5, precision: 0 },
        grid: { color: 'rgba(255,255,255,0.04)' },
      },
    },
  },
});

// Every second: push current bucket into chart, reset counters.
setInterval(() => {
  chartLabels.shift();   chartLabels.push('');
  chartDataAll.shift();  chartDataAll.push(eventsThisSec);
  chartDataAnomal.shift(); chartDataAnomal.push(anomaliesThisSec);
  eventsThisSec = 0;
  anomaliesThisSec = 0;
  rateChart.update('none');

  const rate = chartDataAll[chartDataAll.length - 1];
  document.getElementById('stat-rate').textContent = rate + ' ev/s';
}, 1000);

// --- Helpers ---
const LEVEL_STYLE = {
  info:     { dot: 'bg-blue-500',   text: 'text-blue-400',   badge: 'bg-blue-900/40 text-blue-300' },
  warning:  { dot: 'bg-yellow-500', text: 'text-yellow-400', badge: 'bg-yellow-900/40 text-yellow-300' },
  error:    { dot: 'bg-orange-500', text: 'text-orange-400', badge: 'bg-orange-900/40 text-orange-300' },
  critical: { dot: 'bg-red-500',    text: 'text-red-400',    badge: 'bg-red-900/40 text-red-300' },
};

function levelStyle(level) {
  return LEVEL_STYLE[level] || LEVEL_STYLE['info'];
}

function timeAgo(isoString) {
  const d = new Date(isoString);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function truncate(str, n) {
  return str.length > n ? str.slice(0, n) + '…' : str;
}

// --- Event handling ---
function onEvent(ev) {
  totalEvents++;
  eventsThisSec++;

  if (ev.is_anomaly) {
    totalAnomalies++;
    anomaliesThisSec++;
    addToAnomalyLog(ev);
  }

  updateStats(ev);
  addToFeed(ev);
}

function updateStats(ev) {
  document.getElementById('stat-total').textContent = totalEvents.toLocaleString();
  document.getElementById('stat-anomalies').textContent = totalAnomalies.toLocaleString();

  const pct = totalEvents > 0 ? ((totalAnomalies / totalEvents) * 100).toFixed(1) : '0.0';
  document.getElementById('stat-anomaly-pct').textContent = pct + '% of total';

  document.getElementById('stat-last-source').textContent = ev.source || '—';
  document.getElementById('stat-last-time').textContent = timeAgo(ev.timestamp);
}

function addToFeed(ev) {
  const feed = document.getElementById('event-feed');
  const style = levelStyle(ev.level);
  const isAnomaly = ev.is_anomaly;

  const item = document.createElement('div');
  item.className = `feed-item flex items-start gap-2 py-1 px-2 rounded text-xs ${isAnomaly ? 'anomaly-flash bg-red-900/10' : ''}`;
  item.innerHTML = `
    <div class="w-1.5 h-1.5 rounded-full ${style.dot} mt-1.5 flex-shrink-0"></div>
    <div class="flex-1 min-w-0">
      <div class="flex items-center gap-2">
        <span class="${style.text} uppercase font-bold text-[10px]">${ev.level}</span>
        <span class="text-gray-600 truncate">${ev.source}</span>
        ${isAnomaly ? '<span class="text-red-400 text-[10px]">⚠ ANOMALY</span>' : ''}
      </div>
      <div class="text-gray-400 truncate">${truncate(ev.message, 80)}</div>
    </div>
    <div class="text-gray-700 text-[10px] flex-shrink-0">${timeAgo(ev.timestamp)}</div>
  `;

  feed.insertBefore(item, feed.firstChild);
  feedCount++;
  document.getElementById('feed-count').textContent = Math.min(feedCount, MAX_FEED) + ' events';

  // Cap feed length.
  while (feed.children.length > MAX_FEED) {
    feed.removeChild(feed.lastChild);
  }
}

function addToAnomalyLog(ev) {
  const log = document.getElementById('anomaly-log');

  if (anomalyLogEmpty && anomalyLogEmpty.parentNode === log) {
    log.removeChild(anomalyLogEmpty);
  }

  const scoreBar = Math.round(ev.anomaly_score * 10);
  const scoreColor = ev.anomaly_score > 0.9 ? 'text-red-400' : ev.anomaly_score > 0.75 ? 'text-orange-400' : 'text-yellow-400';

  const entry = document.createElement('div');
  entry.className = 'feed-item bg-red-900/10 border border-red-900/30 rounded p-3 text-xs space-y-1';
  entry.innerHTML = `
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-2">
        <span class="text-red-400 font-bold uppercase">${ev.level}</span>
        <span class="text-gray-300">${ev.source}</span>
        <span class="text-gray-600 text-[10px]">${ev.classification}</span>
      </div>
      <div class="flex items-center gap-2">
        <span class="${scoreColor} font-mono">${(ev.anomaly_score * 100).toFixed(0)}%</span>
        <span class="text-gray-700 text-[10px]">${timeAgo(ev.timestamp)}</span>
      </div>
    </div>
    <div class="text-gray-300">${ev.summary || ev.message}</div>
    <div class="flex gap-0.5 mt-1">
      ${Array(10).fill(0).map((_, i) => `<div class="h-1 flex-1 rounded-sm ${i < scoreBar ? 'bg-red-500' : 'bg-gray-800'}"></div>`).join('')}
    </div>
  `;

  log.insertBefore(entry, log.firstChild);

  // Keep last 20 anomalies.
  while (log.children.length > 20) {
    log.removeChild(log.lastChild);
  }
}

// --- WebSocket ---
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    document.getElementById('ws-dot').className   = 'w-2 h-2 rounded-full bg-green-400 transition-colors duration-300';
    document.getElementById('ws-label').textContent = 'live';
  };

  ws.onmessage = (e) => {
    try {
      const ev = JSON.parse(e.data);
      onEvent(ev);
    } catch {
      console.warn('unparseable WS message', e.data);
    }
  };

  ws.onclose = () => {
    document.getElementById('ws-dot').className   = 'w-2 h-2 rounded-full bg-yellow-400 transition-colors duration-300';
    document.getElementById('ws-label').textContent = 'reconnecting...';
    setTimeout(connect, 2000);
  };

  ws.onerror = () => {
    document.getElementById('ws-dot').className   = 'w-2 h-2 rounded-full bg-red-500 transition-colors duration-300';
    document.getElementById('ws-label').textContent = 'error';
  };
}

connect();

// Poll status endpoint every 5s to show live circuit breaker state.
async function pollStatus() {
  try {
    const r = await fetch('/api/v1/status');
    if (!r.ok) return;
    const data = await r.json();
    const cb = (data.circuit_breaker || 'closed').toLowerCase();
    const el    = document.getElementById('stat-cb');
    const elSub = document.getElementById('stat-cb-sub');
    if (cb === 'open') {
      el.textContent    = 'OPEN';
      el.className      = 'text-lg font-bold text-red-400';
      elSub.textContent = 'AI paused';
    } else if (cb === 'half-open') {
      el.textContent    = 'HALF-OPEN';
      el.className      = 'text-lg font-bold text-yellow-400';
      elSub.textContent = 'probing AI';
    } else {
      el.textContent    = 'CLOSED';
      el.className      = 'text-lg font-bold text-green-400';
      elSub.textContent = 'AI active';
    }
  } catch { /* server not ready yet */ }
}
pollStatus();
setInterval(pollStatus, 5000);
