
const REFRESH_MS = 1000;
const WINDOW_MIN = 10;
const WINDOW_OPTIONS = [5, 10, 15, 30, 60, 120];
const DEVICES = [{"key": "dev1", "name": "Device 1", "kind": "switch"}];
const I18N = {"web.live.title": "Shelly Live Dashboard", "web.live.meta_refresh": "Aktualisierung: alle", "web.nav.live": "Live", "web.nav.control": "Steuerung", "web.pill.window": "Ausschnitt", "web.pill.url": "URL", "web.chart.power": "Leistung (W)", "web.chart.voltage": "Spannung (V) \u2013 L1/L2/L3", "web.chart.current": "Strom (A) \u2013 L1/L2/L3", "web.kv.power": "Leistung", "web.kv.kwh_today": "kWh heute", "web.kv.u": "U", "web.kv.i": "I", "web.switch": "Schalter", "web.switch.toggle": "Umschalten", "web.switch.on": "Ein", "web.switch.off": "Aus", "web.control.title": "Shelly Steuerung", "web.control.meta": "Von hier kannst du Sync/Plots/Exports starten.", "web.control.sync": "Sync", "web.control.plots": "Plots", "web.control.export": "Export", "web.control.jobs": "Jobs", "web.control.mode": "Modus", "web.control.start": "Start (TT.MM.JJJJ)", "web.control.btn.sync": "Sync starten", "web.control.plots.from": "von", "web.control.plots.to": "bis", "web.control.btn.plots": "Plots erzeugen", "web.control.invoice": "Rechnung", "web.control.anchor": "Anker", "web.control.custom_note": "(bei custom werden von/bis genutzt)", "web.control.btn.pdf": "PDF Summary + Plots", "web.control.btn.invoices": "Rechnungen je Shelly", "web.control.btn.bundle": "ZIP Bundle (48h)", "web.control.jobs.meta": "Letzte Aktionen (Browser \u2194 App). Aktualisiert automatisch."};
function t(k){ return (I18N && I18N[k]) ? I18N[k] : k; }

let windowMin = WINDOW_MIN;
let pendingWindow = null;

document.getElementById("refresh_s").textContent = (REFRESH_MS/1000).toFixed(1).replace(/\.0$/, "");
function qs() { return ""; }
document.getElementById("url").textContent = window.location.origin + "/";

// Navigation links are simple (no auth/token)

function escapeHtml(s) {
  return String(s || "").replace(/[&<>"']/g, c => ({
    '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'
  }[c]));
}

function makeId(key) {
  return String(key || "dev").replace(/[^a-zA-Z0-9_-]/g, "_");
}

const gridEl = document.getElementById("grid");
const UI = {}; // device_key -> {p,v,c,kv,swState,swBtn}

async function apiRun(action, params) {
  const r = await fetch("/api/run" + qs(), {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({action: String(action||""), params: params||{}})
  });
  return await r.json();
}

function buildCards() {
  if (!gridEl) return;
  gridEl.innerHTML = "";
  (DEVICES || []).forEach(dev => {
    const id = makeId(dev.key);
    const isSwitch = String(dev.kind || "").toLowerCase() === "switch";
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h2>${escapeHtml(dev.name || dev.key)}</h2>
      ${isSwitch ? `<div class="row" style="margin:6px 0 8px;gap:10px;">
        <div class="pill">${escapeHtml(t('web.switch'))}: <b id="sw_${id}">–</b></div>
        <button id="swbtn_${id}" type="button">${escapeHtml(t('web.switch.toggle'))}</button>
      </div>` : ``}
      <div class="row">
        <canvas id="p_${id}"></canvas>
        <canvas id="v_${id}"></canvas>
        <canvas id="c_${id}"></canvas>
      </div>
      <div class="kv" id="kv_${id}"></div>
    `;
    gridEl.appendChild(card);
    UI[dev.key] = {
      p: document.getElementById(`p_${id}`),
      v: document.getElementById(`v_${id}`),
      c: document.getElementById(`c_${id}`),
      kv: document.getElementById(`kv_${id}`),
      swState: document.getElementById(`sw_${id}`),
      swBtn: document.getElementById(`swbtn_${id}`),
    };

    // Wire switch toggle (if present)
    if (isSwitch) {
      const btn = UI[dev.key].swBtn;
      if (btn) {
        btn.addEventListener('click', async () => {
          try {
            btn.disabled = true;
            const res = await apiRun('toggle_switch', {device_key: dev.key});
            if (res && res.ok && UI[dev.key].swState) {
              UI[dev.key].swState.textContent = res.on ? t('web.switch.on') : t('web.switch.off');
            }
          } catch (e) {
            // ignore
          } finally {
            btn.disabled = false;
          }
        });
      }
    }
  });
}

buildCards();

const winSel = document.getElementById("win_sel");

function renderWindowOptions(opts, selected) {
  if (!winSel) return;
  winSel.innerHTML = "";
  const uniq = Array.from(new Set(opts.map(x=>parseInt(x,10)).filter(x=>x>0))).sort((a,b)=>a-b);
  uniq.forEach(m => {
    const o = document.createElement("option");
    o.value = String(m);
    o.textContent = String(m);
    if (m === selected) o.selected = true;
    winSel.appendChild(o);
  });
}

async function syncConfig() {
  try {
    const r = await fetch("/api/config" + qs(), {cache:"no-store"});
    const cfg = await r.json();
    // If the user just changed the window, keep the UI stable until the server confirms.
    if (pendingWindow !== null) {
      const sm = parseInt((cfg && cfg.window_minutes) ? cfg.window_minutes : pendingWindow, 10);
      if (!isNaN(sm) && sm === pendingWindow) {
        windowMin = pendingWindow;
        pendingWindow = null;
      }
    }
    if (pendingWindow === null && cfg && cfg.window_minutes) {
      const m = parseInt(cfg.window_minutes, 10);
      if (!isNaN(m) && m>0) windowMin = m;
    }
    const opts = (cfg && cfg.available_windows) ? cfg.available_windows : WINDOW_OPTIONS;
    renderWindowOptions(opts, pendingWindow !== null ? pendingWindow : windowMin);
  } catch (e) {
    renderWindowOptions(WINDOW_OPTIONS, windowMin);
  }
}

if (winSel) {
  winSel.addEventListener("change", async () => {
    const m = parseInt(winSel.value, 10);
    if (!m || isNaN(m)) return;
    // Optimistically apply locally so the UI doesn't "jump back" while the request is in-flight.
    windowMin = m;
    pendingWindow = m;
    try {
      await fetch("/api/set_window" + qs(), {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({minutes: m})
      });
    } catch (e) {}
    await syncConfig();
  });
}

// initial render + periodic sync for bidirectional updates
syncConfig();
setInterval(syncConfig, Math.max(2000, REFRESH_MS*3));

function fmt(n, d=1) {
  if (n === null || n === undefined || isNaN(n)) return "–";
  return Number(n).toFixed(d);
}

function drawLineChart(canvas, tsMs, seriesList, yLabel, colors) {
  const ctx = canvas.getContext("2d");
  const w = canvas.width  = canvas.clientWidth  * devicePixelRatio;
  const h = canvas.height = canvas.clientHeight * devicePixelRatio;
  ctx.clearRect(0, 0, w, h);

  const padL = 46 * devicePixelRatio;
  const padR = 10 * devicePixelRatio;
  const padT = 10 * devicePixelRatio;
  const padB = 38 * devicePixelRatio;

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  // compute min/max from all series
  let ymin = Infinity, ymax = -Infinity, n = 0;
  seriesList.forEach(s => {
    (s.values || []).forEach(v => {
      if (v === null || v === undefined || isNaN(v)) return;
      ymin = Math.min(ymin, v);
      ymax = Math.max(ymax, v);
      n++;
    });
  });

  if (!isFinite(ymin) || !isFinite(ymax) || n < 2 || !tsMs || tsMs.length < 2) {
    ctx.fillStyle = "rgba(255,255,255,0.55)";
    const fs = Math.round(12 * devicePixelRatio);
    ctx.font = fs + "px system-ui";
    ctx.textAlign = "left";
    ctx.textBaseline = "top";
    ctx.fillText("Warte auf Daten …", padL, padT + 8*devicePixelRatio);
    return;
  }

  // headroom
  const span = Math.max(1e-6, ymax - ymin);
  ymin -= 0.06 * span;
  ymax += 0.08 * span;

  const tmin = tsMs[0];
  const tmax = tsMs[tsMs.length - 1];
  const tspan = Math.max(1, tmax - tmin);

  function xFromT(t) { return padL + ((t - tmin) / tspan) * plotW; }
  function yFromV(v) { return padT + (1 - ((v - ymin) / (ymax - ymin))) * plotH; }

  // grid + y labels
  ctx.strokeStyle = "rgba(255,255,255,0.09)";
  ctx.fillStyle = "rgba(255,255,255,0.55)";
  const yTicks = 4;
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  ctx.font = Math.round(11 * devicePixelRatio) + "px system-ui";
  for (let i = 0; i <= yTicks; i++) {
    const y = padT + (plotH * i / yTicks);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    const v = ymax - (ymax - ymin) * i / yTicks;
    ctx.fillText(fmt(v, 1), padL - 6*devicePixelRatio, y);
  }

  // x grid + x labels (time)
  const tickCount = (plotW > 900*devicePixelRatio) ? 6 : ((plotW > 620*devicePixelRatio) ? 5 : 4);
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  ctx.font = Math.round(11 * devicePixelRatio) + "px system-ui";
  for (let i = 0; i <= tickCount; i++) {
    const t = tmin + (tspan * i / tickCount);
    const x = xFromT(t);
    ctx.beginPath();
    ctx.moveTo(x, padT);
    ctx.lineTo(x, padT + plotH);
    ctx.stroke();

    const d = new Date(t);
    const lab = d.toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"});
    ctx.fillText(lab, x, padT + plotH + 6*devicePixelRatio);
  }

  // title
  ctx.textAlign = "left";
  ctx.textBaseline = "top";
  ctx.fillStyle = "rgba(255,255,255,0.85)";
  ctx.font = Math.round(12 * devicePixelRatio) + "px system-ui";
  ctx.fillText(yLabel, padL, 2*devicePixelRatio);

  // series
  seriesList.forEach((s, idx) => {
    const vals = s.values || [];
    if (vals.length < 2) return;
    const col = (colors && colors[idx]) ? colors[idx] : "rgba(106,167,255,0.9)";
    ctx.strokeStyle = col;
    ctx.lineWidth = Math.max(1, 1.6 * devicePixelRatio);
    ctx.beginPath();
    let started = false;
    for (let i = 0; i < vals.length && i < tsMs.length; i++) {
      const v = vals[i];
      if (v === null || v === undefined || isNaN(v)) { started = false; continue; }
      const x = xFromT(tsMs[i]);
      const y = yFromV(v);
      if (!started) { ctx.moveTo(x, y); started = true; }
      else { ctx.lineTo(x, y); }
    }
    ctx.stroke();
  });
}

function kv(el, last) {
  el.innerHTML = `
    <b>${t('web.kv.power')}</b><span>${fmt(last.power_total_w, 0)} W</span>
    <b>${t('web.kv.kwh_today')}</b><span>${fmt(last.kwh_today, 3)} kWh</span>
    <b>${t('web.kv.u')}</b><span>${fmt(last.va)} / ${fmt(last.vb)} / ${fmt(last.vc)} V</span>
    <b>${t('web.kv.i')}</b><span>${fmt(last.ia)} / ${fmt(last.ib)} / ${fmt(last.ic)} A</span>
  `;
}

// Stable min/max bucket sampling (anchored to time), to avoid "jiggling" peaks.
function stableSample(arr, target) {
  if (!arr || arr.length <= target) return arr || [];
  const n = arr.length;
  const t0 = parseInt(arr[0].ts, 10);
  const t1 = parseInt(arr[n-1].ts, 10);
  const span = Math.max(1, t1 - t0);
  const buckets = Math.max(10, Math.min(target, 2000));
  const bw = span / buckets;
  const out = [];
  let i = 0;
  for (let b = 0; b < buckets && i < n; b++) {
    const end = (b === buckets-1) ? (t1 + 1) : (t0 + (b+1)*bw);
    let minIdx = -1, maxIdx = -1;
    let minV = Infinity, maxV = -Infinity;
    const startI = i;
    while (i < n && parseInt(arr[i].ts, 10) < end) {
      const v = Number(arr[i].power_total_w);
      if (!isNaN(v)) {
        if (v < minV) { minV = v; minIdx = i; }
        if (v > maxV) { maxV = v; maxIdx = i; }
      }
      i++;
    }
    if (i === startI) continue;
    if (minIdx >= 0 && maxIdx >= 0) {
      if (minIdx === maxIdx) out.push(arr[minIdx]);
      else if (minIdx < maxIdx) { out.push(arr[minIdx]); out.push(arr[maxIdx]); }
      else { out.push(arr[maxIdx]); out.push(arr[minIdx]); }
    } else {
      out.push(arr[startI]);
    }
  }
  // Ensure last point is included.
  if (out.length && out[out.length-1].ts !== arr[n-1].ts) out.push(arr[n-1]);
  out.sort((a,b) => (a.ts||0) - (b.ts||0));
  // Dedupe identical timestamps.
  return out.filter((p, idx) => idx === 0 || p.ts !== out[idx-1].ts);
}

function pickDev(data, dev) {
  const arr0 = data[dev.key] || [];
  const n0 = arr0.length;
  const last = n0 ? arr0[n0-1] : null;

  // Filter to selected time window (minutes) based on latest timestamp.
  let arr = arr0;
  if (last && last.ts) {
    const cutoff = parseInt(last.ts, 10) - (parseInt(windowMin, 10) * 60);
    arr = arr0.filter(x => x && x.ts !== undefined && parseInt(x.ts, 10) >= cutoff);
  }

  // Downsample for drawing performance, but keep it *stable* (time-bucketed)
  // so peaks don't appear/disappear when n changes each refresh.
  const sampled = stableSample(arr, 900);

  return {
    arr: sampled,
    last: last
  };
}

async function tick() {
  try {
    const r = await fetch("/api/state" + qs());
    const data = await r.json();

    document.getElementById("stamp").textContent = new Date().toLocaleString();

    (DEVICES || []).forEach(dev => {
      const ui = UI[dev.key];
      if (!ui) return;
      // Lazy switch state fetch (avoids extra traffic when not needed)
      try {
        const isSwitch = String(dev.kind || "").toLowerCase() === "switch";
        if (isSwitch && ui.swState && (ui.swState.textContent === "–" || ui.swState.textContent === "-")) {
          apiRun('get_switch', {device_key: dev.key}).then(res => {
            if (res && res.ok && ui.swState) {
              ui.swState.textContent = res.on ? t('web.switch.on') : t('web.switch.off');
            }
          }).catch(()=>{});
        }
      } catch (e) {}
      const picked = pickDev(data, dev);
      if (picked.last) kv(ui.kv, picked.last);
      // Always draw so the canvas can show "Warte auf Daten" until filled.
      const ts = picked.arr.map(x=>x.ts*1000);
      drawLineChart(ui.p, ts, [{values: picked.arr.map(x=>x.power_total_w)}], t('web.chart.power'), ["rgba(106,167,255,0.9)"]);
      drawLineChart(ui.v, ts, [
        {values: picked.arr.map(x=>x.va)},
        {values: picked.arr.map(x=>x.vb)},
        {values: picked.arr.map(x=>x.vc)},
      ], t('web.chart.voltage'), ["rgba(106,167,255,0.9)","rgba(255,180,84,0.9)","rgba(136,240,179,0.9)"]);
      drawLineChart(ui.c, ts, [
        {values: picked.arr.map(x=>x.ia)},
        {values: picked.arr.map(x=>x.ib)},
        {values: picked.arr.map(x=>x.ic)},
      ], t('web.chart.current'), ["rgba(106,167,255,0.9)","rgba(255,180,84,0.9)","rgba(136,240,179,0.9)"]);
    });
  } catch (e) {
    // keep silent; next tick will retry
  }
}

tick();
setInterval(tick, REFRESH_MS);
