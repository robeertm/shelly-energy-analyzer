
// keep token across navigation links
try {
  const qs = window.location.search || "";
  if (qs) {
    document.getElementById("nav_live").setAttribute("href", "/" + qs);
    document.getElementById("nav_control").setAttribute("href", "/control" + qs);
  }
} catch (e) {}

function qs() { return ""; }

async function api(path, opts) {
  const u = path + qs();
  const r = await fetch(u, opts || {cache:"no-store"});
  if (!r.ok) {
    let t = "";
    try { t = await r.text(); } catch(e) {}
    throw new Error(`HTTP ${r.status}: ${t || r.statusText}`);
  }
  return r;
}

async function run(action, params) {
  const r = await api("/api/run", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({action, params: params || {} })
  });
  try {
    return await r.json();
  } catch (e) {
    const t = await r.text();
    throw new Error(t || "Antwort ist kein JSON");
  }
}

function esc(s) {
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function renderJob(j) {
  const t = j.started_at ? new Date(j.started_at*1000).toLocaleString() : "";
  const st = j.status || "";
  const a = j.action || "";
  const pct = (j.progress_overall !== undefined && j.progress_overall !== null) ? parseInt(j.progress_overall,10) : 0;
  const err = j.error ? `<div class="jobmeta"><b>Fehler:</b> ${esc(j.error)}</div>` : "";
  let progLines = "";
  const prog = j.progress || {};
  const keys = Object.keys(prog);
  if (keys.length) {
    progLines = keys.map(k => {
      const p = prog[k] || {};
      const pd = parseInt(p.done||0,10);
      const pt = parseInt(p.total||1,10);
      const pp = parseInt(p.percent||0,10);
      const pm = esc(p.message||"");
      return `<div class="jobmeta">${esc(k)}: ${pp}% (${pd}/${pt}) ${pm?"• "+pm:""}</div>`;
    }).join("");
  }
  let files = "";
  const r = j.result || {};
  if (r && r.files && Array.isArray(r.files)) {
    files = `<div class="jobfiles">` + r.files.map(f => {
      const url = (f.url||"") + qs();
      const name = esc(f.name||"file");
      return `<a href="${url}" target="_blank">${name}</a>`;
    }).join("") + `</div>`;
  }
  return `
    <div class="jobcard">
      <div class="jobhead">
        <div class="jobtitle">#${esc(j.id)} • ${esc(a)}</div>
        <div class="jobmeta">${esc(st)} • ${esc(t)}</div>
      </div>
      <div class="progrow">
        <progress max="100" value="${isNaN(pct)?0:pct}"></progress>
        <div class="jobmeta">${isNaN(pct)?0:pct}%</div>
      </div>
      ${progLines}
      ${err}
      ${files}
    </div>
  `;
}

async function refreshJobs() {
  try {
    const r = await api("/api/jobs", {cache:"no-store"});
    const j = await r.json();
    const arr = (j && j.jobs) ? j.jobs : [];
    const el = document.getElementById("jobs");
    if (!el) return;
    if (!arr.length) { el.innerHTML = `<div class="jobmeta">–</div>`; return; }
    el.innerHTML = arr.map(renderJob).join("");
  } catch (e) {}
}

setInterval(refreshJobs, 1500);
refreshJobs();

document.getElementById("btn_sync").addEventListener("click", async ()=>{
  const mode = document.getElementById("sync_mode").value;
  const start = document.getElementById("sync_start").value;
  document.getElementById("sync_log").textContent = "Starte …";
  try {
    const res = await run("sync", {mode, start_date: start});
    document.getElementById("sync_log").textContent = JSON.stringify(res, null, 2);
  } catch (e) {
    document.getElementById("sync_log").textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_plots").addEventListener("click", async ()=>{
  const mode = document.getElementById("plot_mode").value;
  const start = document.getElementById("plot_start").value;
  const end = document.getElementById("plot_end").value;
  const thumbs = document.getElementById("plot_thumbs");
  thumbs.innerHTML = "";
  try {
    const res = await run("plots", {mode, start, end});
    if (res && res.files) {
      res.files.forEach(f => {
        const img = document.createElement("img");
        img.className = "thumb";
        img.src = f.url + qs();
        img.alt = f.name;
        thumbs.appendChild(img);
      });
    } else {
      thumbs.innerHTML = `<div class="jobmeta">${esc(JSON.stringify(res||{}, null, 2))}</div>`;
    }
  } catch (e) {
    thumbs.innerHTML = `<div class="jobmeta">Fehler: ${esc(e && e.message ? e.message : String(e))}</div>`;
  }
});

document.getElementById("btn_summary").addEventListener("click", async ()=>{
  const start = document.getElementById("exp_start").value;
  const end = document.getElementById("exp_end").value;
  const el = document.getElementById("export_log");
  el.innerHTML = "";
  try {
    const res = await run("export_summary", {start, end});
    if (res && res.files) {
      res.files.forEach(f=>{
        const a = document.createElement("a");
        a.href = f.url + qs();
        a.textContent = f.name;
        a.target = "_blank";
        el.appendChild(a);
        el.appendChild(document.createElement("br"));
      });
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_invoices").addEventListener("click", async ()=>{
  const start = document.getElementById("exp_start").value;
  const end = document.getElementById("exp_end").value;
  const period = document.getElementById("inv_period").value;
  const anchor = document.getElementById("inv_anchor").value;
  const el = document.getElementById("export_log");
  el.innerHTML = "";
  try {
    const res = await run("export_invoices", {start, end, period, anchor});
    if (res && res.files) {
      res.files.forEach(f=>{
        const a = document.createElement("a");
        a.href = f.url + qs();
        a.textContent = f.name;
        a.target = "_blank";
        el.appendChild(a);
        el.appendChild(document.createElement("br"));
      });
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});

document.getElementById("btn_bundle").addEventListener("click", async ()=>{
  const el = document.getElementById("export_log");
  el.innerHTML = "";
  try {
    const res = await run("bundle", {hours: 48});
    if (res && res.files) {
      res.files.forEach(f=>{
        const a = document.createElement("a");
        a.href = f.url + qs();
        a.textContent = f.name;
        a.target = "_blank";
        el.appendChild(a);
        el.appendChild(document.createElement("br"));
      });
    } else {
      el.textContent = JSON.stringify(res, null, 2);
    }
  } catch (e) {
    el.textContent = "Fehler: " + (e && e.message ? e.message : String(e));
  }
});
