/* ══════════════════════════════════════════════════════════════════════════
   Aurora skin — enhancement layer
   ──────────────────────────────────────────────────────────────────────────
   Loaded only when the Aurora skin is active.  It never replaces any of the
   dashboard's own rendering; it observes it and adds four things CSS cannot
   express on its own:

     1. the aurora ribbons behind the page,
     2. a live hero above the device cards ("the house, right now"),
     3. --load-hue / --load-lift, so the ribbons follow the actual load,
     4. per-device --dev-tint / --dev-lift for the card glow.

   Every hook is defensive: if the dashboard changes shape, the enhancement
   silently drops out and the page still works.
   ══════════════════════════════════════════════════════════════════════════ */
(function () {
  "use strict";

  if (document.documentElement.getAttribute("data-skin") !== "aurora") return;
  if (window.__auroraSkin) return;
  window.__auroraSkin = true;

  var ROOT = document.documentElement;

  /* Translation via the dashboard's own helper when it exists (the settings
     and setup pages do not have one — the fallback string is then used). */
  function T(key, fallback) {
    try {
      if (typeof window.t === "function") {
        var s = window.t(key, fallback);
        if (s && s !== key) return s;
      }
    } catch (e) {}
    return fallback;
  }

  /* ── 1 · The ribbons ────────────────────────────────────────────────── */

  function mountSky() {
    if (document.getElementById("aurora-sky")) return;
    var sky = document.createElement("div");
    sky.id = "aurora-sky";
    sky.setAttribute("aria-hidden", "true");
    sky.innerHTML = "<i></i><i></i><i></i>";
    document.body.insertBefore(sky, document.body.firstChild);
  }

  /* ── 1b · The circuit ───────────────────────────────────────────────── */

  /* An energy analyzer should not have a generic gradient behind it.  The
     ground here is a circuit board: conductors with 45-degree elbows, junction
     nodes, and charge travelling along them.  The speed and the number of
     pulses come from the live draw, so at three in the morning the background
     is nearly still and when the oven goes on it comes alive.  The colour is
     the same --load-hue the ribbons and the gauge use.

     Cost discipline: 30 fps, device pixel ratio capped at 1.5, paused while the
     tab is hidden, and no pulses at all under prefers-reduced-motion — this
     runs on a phone against a Raspberry Pi. */

  var circuit = null;

  function buildCircuit(w, h) {
    // Mostly-horizontal runs with 45-degree elbows: the shape that reads as
    // wiring rather than as decoration.
    var rows = Math.max(4, Math.min(9, Math.round(h / 130)));
    var lanes = [];
    for (var r = 0; r < rows; r++) {
      var y = (h * (r + 0.5)) / rows + (r % 2 ? -14 : 14);
      var pts = [{ x: -40, y: y }];
      var x = -40;
      var dir = r % 2 ? -1 : 1;
      while (x < w + 40) {
        var run = 60 + ((r * 97 + pts.length * 53) % 130);      // deterministic
        x += run;
        pts.push({ x: x, y: y });
        var elbow = 22 + ((r * 31 + pts.length * 17) % 30);
        x += elbow;
        y += elbow * dir;
        y = Math.max(18, Math.min(h - 18, y));
        pts.push({ x: x, y: y });
        dir = -dir;
      }
      // Cumulative length, so a pulse can be placed by distance travelled.
      var acc = [0], total = 0;
      for (var i = 1; i < pts.length; i++) {
        total += Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y);
        acc.push(total);
      }
      lanes.push({ pts: pts, acc: acc, len: total, phase: (r * 137) % 1000 / 1000 });
    }
    return lanes;
  }

  function pointAt(lane, dist) {
    var d = dist % lane.len;
    if (d < 0) d += lane.len;
    var lo = 0, hi = lane.acc.length - 1;
    while (lo < hi - 1) {
      var mid = (lo + hi) >> 1;
      if (lane.acc[mid] <= d) lo = mid; else hi = mid;
    }
    var seg = lane.acc[hi] - lane.acc[lo] || 1;
    var f = (d - lane.acc[lo]) / seg;
    var a = lane.pts[lo], b = lane.pts[hi];
    return { x: a.x + (b.x - a.x) * f, y: a.y + (b.y - a.y) * f };
  }

  function mountCircuit() {
    if (document.getElementById("aurora-circuit")) return;
    var cv = document.createElement("canvas");
    cv.id = "aurora-circuit";
    cv.setAttribute("aria-hidden", "true");
    document.body.insertBefore(cv, document.body.firstChild);

    var ctx = cv.getContext("2d");
    if (!ctx) return;
    // The colour hook must not touch the background: it is already drawn in the
    // load hue and snapping it would fight itself.
    var raw = Object.getOwnPropertyDescriptor(
      window.CanvasRenderingContext2D.prototype, "strokeStyle");

    var W = 0, H = 0, DPR = 1, lanes = [], t0 = 0, raf = 0;

    function resize() {
      DPR = Math.min(1.5, window.devicePixelRatio || 1);
      W = window.innerWidth;
      H = window.innerHeight;
      cv.width = Math.round(W * DPR);
      cv.height = Math.round(H * DPR);
      cv.style.width = W + "px";
      cv.style.height = H + "px";
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
      lanes = buildCircuit(W, H);
    }

    function hue() {
      var v = parseFloat(getComputedStyle(ROOT).getPropertyValue("--load-hue"));
      return isFinite(v) ? v : 170;
    }
    function lift() {
      var v = parseFloat(getComputedStyle(ROOT).getPropertyValue("--load-lift"));
      return isFinite(v) ? Math.max(0, Math.min(1, v)) : 0;
    }

    var LIGHT = false;
    function readTheme() { LIGHT = ROOT.getAttribute("data-theme") === "light"; }
    readTheme();

    function frame(ts) {
      raf = 0;
      if (document.hidden) return;                 // resumed by visibilitychange
      if (!t0) t0 = ts;
      var t = (ts - t0) / 1000;
      var h = hue(), l = lift();
      // As the draw rises the wires recede and the charge takes over: at peak
      // the eye should follow the current, not the wiring.
      var trace = (LIGHT ? 0.10 : 0.13) * (1 - 0.45 * l);
      var glow  = LIGHT ? 0.44 : 0.62;

      ctx.clearRect(0, 0, W, H);

      // 1 · the conductors themselves — always there, barely visible
      ctx.lineWidth = 1;
      ctx.lineJoin = "round";
      ctx.lineCap = "round";
      for (var i = 0; i < lanes.length; i++) {
        var lane = lanes[i];
        ctx.beginPath();
        ctx.moveTo(lane.pts[0].x, lane.pts[0].y);
        for (var j = 1; j < lane.pts.length; j++) ctx.lineTo(lane.pts[j].x, lane.pts[j].y);
        raw.set.call(ctx, "hsla(" + h.toFixed(0) + ", 40%, " + (LIGHT ? "42%" : "68%") +
                          ", " + trace.toFixed(3) + ")");
        ctx.stroke();
        // junction nodes
        for (var k = 1; k < lane.pts.length - 1; k += 2) {
          ctx.beginPath();
          ctx.arc(lane.pts[k].x, lane.pts[k].y, 1.6, 0, 6.2832);
          ctx.fillStyle = "hsla(" + h.toFixed(0) + ", 45%, " + (LIGHT ? "38%" : "72%") +
                          ", " + (trace * 1.5).toFixed(3) + ")";
          ctx.fill();
        }
      }

      // 2 · the charge — count and speed both follow the live draw
      if (!REDUCED) {
        var perLane = 2 + Math.round(l * 3);
        var speed = 40 + l * 200;                   // px/s
        for (var a = 0; a < lanes.length; a++) {
          var ln = lanes[a];
          for (var b = 0; b < perLane; b++) {
            var d = (t * speed + ln.phase * ln.len + (b * ln.len) / perLane) % ln.len;
            var head = pointAt(ln, d);
            var tail = pointAt(ln, d - 34 - l * 46);
            var alpha = glow * (0.62 + 0.38 * l);
            var g = ctx.createLinearGradient(tail.x, tail.y, head.x, head.y);
            g.addColorStop(0, "hsla(" + h.toFixed(0) + ", 80%, 70%, 0)");
            g.addColorStop(1, "hsla(" + h.toFixed(0) + ", 90%, " + (LIGHT ? "50%" : "78%") +
                              ", " + alpha.toFixed(3) + ")");
            ctx.beginPath();
            ctx.moveTo(tail.x, tail.y);
            ctx.lineTo(head.x, head.y);
            ctx.lineWidth = 2.4;
            raw.set.call(ctx, g);
            ctx.stroke();
            // The head is the spark: a small bright dot with a halo.  Drawn
            // last so nothing paints over it.
            ctx.shadowBlur = 9;
            ctx.shadowColor = "hsla(" + h.toFixed(0) + ", 95%, 70%, " + (alpha * 0.8).toFixed(3) + ")";
            ctx.beginPath();
            ctx.arc(head.x, head.y, 2.6, 0, 6.2832);
            ctx.fillStyle = "hsla(" + h.toFixed(0) + ", 95%, " + (LIGHT ? "46%" : "86%") +
                            ", " + Math.min(1, alpha * 1.5).toFixed(3) + ")";
            ctx.fill();
            ctx.shadowBlur = 0;
          }
        }

        // 3 · mains, along the bottom edge: 50 Hz slowed to something the eye
        //     can follow, amplitude growing with the draw
        var amp = 5 + l * 26;
        var yb = H - 26;
        ctx.beginPath();
        for (var x = 0; x <= W; x += 6) {
          var y = yb + Math.sin((x / 78) + t * 1.7) * amp;
          if (x === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }
        ctx.lineWidth = 1.4;
        raw.set.call(ctx, "hsla(" + h.toFixed(0) + ", 70%, " + (LIGHT ? "45%" : "74%") +
                          ", " + (0.16 + 0.20 * l).toFixed(3) + ")");
        ctx.stroke();
      }

      schedule();
    }

    var last = 0;
    function schedule() {
      if (raf) return;
      raf = requestAnimationFrame(function (ts) {
        // 30 fps is plenty for something this slow, and halves the work.
        if (ts - last < 33) { raf = 0; schedule(); return; }
        last = ts;
        frame(ts);
      });
    }

    var rt = 0;
    window.addEventListener("resize", function () {
      clearTimeout(rt);
      rt = setTimeout(function () { resize(); schedule(); }, 180);
    });
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) schedule();
    });
    try {
      new MutationObserver(readTheme)
        .observe(ROOT, { attributes: true, attributeFilter: ["data-theme"] });
    } catch (e) {}

    resize();
    schedule();
    circuit = cv;
  }

  /* ── 2 · Load → colour ──────────────────────────────────────────────── */

  /* Breakpoints in watts for a whole house, mapped to a hue that decreases
     monotonically (teal → green → yellow → peach → red).  Red is written as
     -12 rather than 348 so a plain interpolation never sweeps the long way
     round the colour wheel. */
  var STOPS = [
    {   w: 0,    hue: 172, key: "aurora.load.idle",    fb: "Idle"    },
    {   w: 300,  hue: 130, key: "aurora.load.low",     fb: "Low"     },
    {   w: 1200, hue: 48,  key: "aurora.load.working", fb: "Working" },
    {   w: 3000, hue: 26,  key: "aurora.load.high",    fb: "High"    },
    {   w: 6000, hue: -12, key: "aurora.load.peak",    fb: "Peak"    }
  ];

  function loadColour(watts) {
    var w = Math.max(0, Number(watts) || 0);
    var i = 0;
    while (i < STOPS.length - 1 && w >= STOPS[i + 1].w) i++;
    var a = STOPS[i];
    var b = STOPS[Math.min(i + 1, STOPS.length - 1)];
    var span = b.w - a.w;
    var f = span > 0 ? Math.min(1, (w - a.w) / span) : 1;
    // The arc is banded, not linear: each step between two stops is an equal
    // quarter of the ring.  A 6 kW linear scale leaves an idle house with a
    // 2 % sliver that reads as a broken gauge, and it hides the whole range a
    // household actually lives in.  Banded, "half full" means "working".
    var band = (i + f) / (STOPS.length - 1);
    return {
      hue: a.hue + (b.hue - a.hue) * f,
      lift: Math.min(1, w / 6000),
      arc: Math.max(0.03, Math.min(1, band)),
      // The label follows the stop the value has actually reached.
      label: T((f > 0.5 ? b : a).key, (f > 0.5 ? b : a).fb),
      stop: (f > 0.5 ? b : a)
    };
  }

  var shownHue = null;
  function paintLoad(watts) {
    var c = loadColour(watts);
    // Ease the hue so a kettle switching on does not snap the whole page.
    shownHue = shownHue === null ? c.hue : shownHue + (c.hue - shownHue) * 0.35;
    ROOT.style.setProperty("--load-hue", shownHue.toFixed(1));
    ROOT.style.setProperty("--load-lift", c.lift.toFixed(3));
    return c;   // c.arc drives the ring, c.lift the ribbons
  }

  /* ── 3 · Numbers that count rather than jump ────────────────────────── */

  var REDUCED = false;
  try {
    REDUCED = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch (e) {}

  function rollTo(el, value, decimals, unitless) {
    var from = Number(el.__auVal);
    var to = Number(value) || 0;
    el.__auVal = to;
    var d = decimals == null ? 0 : decimals;
    function put(v) {
      el.textContent = unitless ? String(Math.round(v)) : v.toFixed(d);
    }
    if (!isFinite(from) || REDUCED || Math.abs(to - from) < Math.pow(10, -d) / 2) {
      put(to);
      return;
    }
    if (el.__auRaf) cancelAnimationFrame(el.__auRaf);
    var t0 = 0;
    var dur = 480;
    function step(ts) {
      if (!t0) t0 = ts;
      var p = Math.min(1, (ts - t0) / dur);
      var e = 1 - Math.pow(1 - p, 3);          // easeOutCubic
      put(from + (to - from) * e);
      if (p < 1) el.__auRaf = requestAnimationFrame(step);
      else el.__auRaf = 0;
    }
    el.__auRaf = requestAnimationFrame(step);
  }

  /* ── 4 · The live hero ──────────────────────────────────────────────── */

  var ARC_R = 58;
  var ARC_LEN = 2 * Math.PI * ARC_R;

  function heroMarkup() {
    return (
      '<div class="au-gauge">' +
        '<svg viewBox="0 0 132 132" aria-hidden="true">' +
          '<defs><linearGradient id="au-arc-grad" x1="0" y1="0" x2="1" y2="1">' +
            '<stop offset="0%"   stop-color="hsl(var(--load-hue) var(--load-sat) 72%)"/>' +
            '<stop offset="100%" stop-color="hsl(calc(var(--load-hue) + 40) var(--load-sat) 62%)"/>' +
          '</linearGradient></defs>' +
          '<circle class="au-track" cx="66" cy="66" r="' + ARC_R + '"/>' +
          '<circle class="au-arc" id="au-arc" cx="66" cy="66" r="' + ARC_R + '" ' +
            'stroke-dasharray="' + ARC_LEN.toFixed(1) + '" ' +
            'stroke-dashoffset="' + ARC_LEN.toFixed(1) + '"/>' +
        '</svg>' +
        '<div class="au-gauge-mid">' +
          '<div class="au-hero-value" id="au-now">0</div>' +
          '<div class="au-hero-unit">W</div>' +
        '</div>' +
      '</div>' +
      '<div class="au-hero-right">' +
        '<div class="au-hero-head">' +
          '<span class="au-hero-title" id="au-hero-title"></span>' +
          '<span class="au-hero-state" id="au-state"></span>' +
        '</div>' +
        '<div class="au-stats" id="au-stats"></div>' +
      '</div>'
    );
  }

  function ensureHero() {
    var grid = document.getElementById("live-grid");
    if (!grid || !grid.parentNode) return null;
    var hero = document.getElementById("au-hero");
    if (hero) return hero;
    hero = document.createElement("div");
    hero.id = "au-hero";
    hero.className = "au-panel";
    hero.innerHTML = heroMarkup();
    grid.parentNode.insertBefore(hero, grid);
    var title = hero.querySelector("#au-hero-title");
    if (title) title.textContent = T("aurora.hero.title", "Right now");
    return hero;
  }

  function stat(cls, value, label) {
    return '<div class="au-stat ' + cls + '">' +
             '<div class="au-stat-value">' + value + '</div>' +
             '<div class="au-stat-label">' + label + '</div>' +
           '</div>';
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /* Devices that represent generation or storage must not be summed into the
     household draw — the dashboard marks them with flow_role. */
  function isDraw(d) {
    var r = String(d && d.flow_role || "");
    return r !== "pv" && r !== "battery";
  }

  function updateHero(data) {
    var hero = ensureHero();
    if (!hero) return;
    var devs = (data && data.devices) || [];
    var draw = devs.filter(isDraw);

    var totalW = 0, kwh = 0, cost = 0, top = null;
    for (var i = 0; i < draw.length; i++) {
      var d = draw[i];
      var w = Number(d.power_w) || 0;
      totalW += w;
      kwh  += Number(d.today_kwh)  || 0;
      cost += Number(d.cost_today) || 0;
      if (!top || w > (Number(top.power_w) || 0)) top = d;
    }

    var c = paintLoad(totalW);

    var now = hero.querySelector("#au-now");
    if (now) {
      // Above 10 kW watts stop being readable — switch the whole gauge to kW.
      var unitEl = hero.querySelector(".au-hero-unit");
      if (totalW >= 10000) {
        rollTo(now, totalW / 1000, 1);
        if (unitEl) unitEl.textContent = "kW";
      } else {
        rollTo(now, totalW, 0, true);
        if (unitEl) unitEl.textContent = "W";
      }
    }

    var arc = hero.querySelector("#au-arc");
    if (arc) {
      arc.setAttribute("stroke-dashoffset", (ARC_LEN * (1 - c.arc)).toFixed(1));
    }

    var state = hero.querySelector("#au-state");
    if (state) state.textContent = c.label;

    var stats = hero.querySelector("#au-stats");
    if (stats) {
      var html = "";
      html += stat("kwh", kwh.toFixed(2) + " kWh", T("aurora.stat.today", "Today"));
      if (cost > 0) {
        html += stat("cost", cost.toFixed(2) + " €", T("aurora.stat.cost", "Cost today"));
      }
      var share = data && data.solar_share_now;
      if (share !== null && share !== undefined && !isNaN(Number(share))) {
        html += stat("solar", Math.round(Number(share) * (Number(share) <= 1 ? 100 : 1)) + " %",
                     T("aurora.stat.solar", "Solar share"));
      }
      if (top && (Number(top.power_w) || 0) > 0) {
        html += stat("top", esc(top.name || top.key),
                     T("aurora.stat.biggest", "Biggest draw"));
      }
      if (stats.__auHtml !== html) { stats.innerHTML = html; stats.__auHtml = html; }
    }

    // The device cards take their glow from the same numbers.
    paintDevices(devs);
  }

  /* ── 5 · Per-device glow ────────────────────────────────────────────── */

  /* Hue per palette slot, matching the six card tints the dashboard assigns
     from the device key, so a device keeps one identity colour everywhere. */
  var DEV_HUES = [217, 122, 36, 291, 187, 340];

  function tintIdx(key) {
    if (typeof window._tintIdxForKey === "function") {
      try { return window._tintIdxForKey(key) % DEV_HUES.length; } catch (e) {}
    }
    var s = String(key == null ? "" : key), h = 2166136261;
    for (var i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); }
    return (h >>> 0) % DEV_HUES.length;
  }

  function paintDevices(devs) {
    for (var i = 0; i < devs.length; i++) {
      var d = devs[i];
      var el = document.getElementById("dc-" + d.key);
      if (!el) continue;
      var hue = DEV_HUES[tintIdx(d.key)];
      var lift = Math.min(1, (Number(d.power_w) || 0) / 3000);
      if (el.__auHue !== hue) {
        el.style.setProperty("--dev-hue", String(hue));
        el.style.setProperty("--dev-tint", "hsl(" + hue + " 62% 60% / .12)");
        el.__auHue = hue;
      }
      // Only write when it actually moved — this runs every live poll.
      var q = Math.round(lift * 20) / 20;
      if (el.__auLift !== q) {
        el.style.setProperty("--dev-lift", String(q));
        el.__auLift = q;
      }
    }
  }

  /* The dashboard paints its tint as an inline background + left border.
     Inline styles beat the stylesheet, so the glass would never show through.
     Replace it with custom properties and let the CSS do the drawing. */
  function takeOverTint() {
    if (typeof window._applyCardTint !== "function") return false;
    if (window._applyCardTint.__auWrapped) return true;
    var wrapped = function (div, key) {
      try {
        var hue = DEV_HUES[tintIdx(key)];
        div.style.setProperty("--dev-hue", String(hue));
        div.style.setProperty("--dev-tint", "hsl(" + hue + " 62% 60% / .12)");
        div.__auHue = hue;
      } catch (e) {}
    };
    wrapped.__auWrapped = true;
    window._applyCardTint = wrapped;
    return true;
  }

  /* ── 5b · Chart colours ─────────────────────────────────────────────── */

  /* The dashboard paints its 41 canvases with 91 different literal hex
     colours — Material greens, Tailwind reds, Flat-UI blues, all mixed.  That
     is what makes the charts read as a different product from the page around
     them.  Rather than a 91-entry lookup table that goes stale the moment a
     new chart is added, the colour is snapped by HUE onto the nearest
     Catppuccin accent.  Greys and near-black/near-white are left alone, so
     text, axes and grid lines (which the dashboard reads from --muted /
     --border already) pass through untouched. */

  var ACCENTS = ["red", "peach", "yellow", "green", "teal", "sky", "blue",
                 "lavender", "mauve", "pink"];
  // Every token in the skin, accents and neutrals alike.  A colour that is
  // already one of ours is passed through: Catppuccin's own subtext greys are
  // saturated enough to be snapped otherwise, which turned chart axis labels
  // blue.
  var TOKENS = ACCENTS.concat(["base", "mantle", "crust", "surface0", "surface1",
                               "surface2", "overlay0", "overlay1", "text",
                               "subtext0", "subtext1", "sapphire", "maroon"]);
  var palette = null;
  var mine = null;

  function readPalette() {
    var cs = getComputedStyle(ROOT);
    var out = [];
    mine = Object.create(null);
    for (var j = 0; j < TOKENS.length; j++) {
      var hx = (cs.getPropertyValue("--ctp-" + TOKENS[j]) || "").trim().toLowerCase();
      if (hx) mine[hx] = 1;
      var rg = hexToRgb(hx);
      if (rg) mine["rgb(" + rg[0] + ", " + rg[1] + ", " + rg[2] + ")"] = 1;
    }
    for (var i = 0; i < ACCENTS.length; i++) {
      var hex = (cs.getPropertyValue("--ctp-" + ACCENTS[i]) || "").trim();
      var rgb = hexToRgb(hex);
      if (rgb) out.push({ hue: rgbToHsl(rgb)[0], css: hex });
    }
    return out.length ? out : null;
  }

  function hexToRgb(h) {
    if (!h) return null;
    h = h.trim();
    var m = /^#([0-9a-f]{3}|[0-9a-f]{4}|[0-9a-f]{6}|[0-9a-f]{8})$/i.exec(h);
    if (!m) return null;
    var v = m[1], a = 1;
    if (v.length === 3 || v.length === 4) {
      if (v.length === 4) a = parseInt(v[3] + v[3], 16) / 255;
      v = v[0] + v[0] + v[1] + v[1] + v[2] + v[2];
    } else if (v.length === 8) {
      // #rrggbbaa — the dashboard builds these by concatenation ("col + '22'")
      a = parseInt(v.slice(6, 8), 16) / 255;
      v = v.slice(0, 6);
    }
    var out = [parseInt(v.slice(0, 2), 16), parseInt(v.slice(2, 4), 16), parseInt(v.slice(4, 6), 16)];
    out.alpha = a;
    return out;
  }

  function parseColour(c) {
    if (typeof c !== "string") return null;
    var hex = hexToRgb(c);
    if (hex) return { rgb: hex, a: hex.alpha == null ? 1 : hex.alpha };
    var t = c.trim();
    var m = /^rgba?\(\s*([\d.]+)[\s,]+([\d.]+)[\s,]+([\d.]+)(?:[\s,/]+([\d.%]+))?\s*\)$/i.exec(t);
    if (m) {
      var a = m[4] === undefined ? 1 :
              (String(m[4]).indexOf("%") >= 0 ? parseFloat(m[4]) / 100 : parseFloat(m[4]));
      return { rgb: [+m[1], +m[2], +m[3]], a: isFinite(a) ? a : 1 };
    }
    // The dashboard also writes hsl() directly (the NILM pattern badges build a
    // tint and its text from one hue).  Without this branch those slipped past
    // the mapper entirely.
    var hm = /^hsla?\(\s*([-\d.]+)(?:deg)?[\s,]+([\d.]+)%[\s,]+([\d.]+)%(?:[\s,/]+([\d.%]+))?\s*\)$/i.exec(t);
    if (hm) {
      var ha = hm[4] === undefined ? 1 :
               (String(hm[4]).indexOf("%") >= 0 ? parseFloat(hm[4]) / 100 : parseFloat(hm[4]));
      return { rgb: hslToRgb(+hm[1], +hm[2] / 100, +hm[3] / 100),
               a: isFinite(ha) ? ha : 1 };
    }
    return null;
  }

  function hslToRgb(h, s, l) {
    h = ((h % 360) + 360) % 360 / 360;
    if (s === 0) { var v = Math.round(l * 255); return [v, v, v]; }
    var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
    var p2 = 2 * l - q;
    function ch(t) {
      if (t < 0) t += 1;
      if (t > 1) t -= 1;
      if (t < 1 / 6) return p2 + (q - p2) * 6 * t;
      if (t < 1 / 2) return q;
      if (t < 2 / 3) return p2 + (q - p2) * (2 / 3 - t) * 6;
      return p2;
    }
    return [Math.round(ch(h + 1 / 3) * 255), Math.round(ch(h) * 255),
            Math.round(ch(h - 1 / 3) * 255)];
  }

  function rgbToHsl(c) {
    var r = c[0] / 255, g = c[1] / 255, b = c[2] / 255;
    var mx = Math.max(r, g, b), mn = Math.min(r, g, b), d = mx - mn;
    var l = (mx + mn) / 2, h = 0, sat = 0;
    if (d) {
      sat = l > 0.5 ? d / (2 - mx - mn) : d / (mx + mn);
      if (mx === r)      h = ((g - b) / d + (g < b ? 6 : 0));
      else if (mx === g) h = ((b - r) / d + 2);
      else               h = ((r - g) / d + 4);
      h *= 60;
    }
    return [h, sat, l];
  }

  /* Snap a colour onto the Catppuccin palette.
     🔴 A colour's LIGHTNESS carries its role, and replacing it destroys pairs.
     The dashboard writes badges as "a pale tint behind dark text of the same
     hue"; mapping both onto one accent made them the same colour and the text
     vanished.  So:
       · mid-tones (L 0.32‥0.72) are identity colours — a chart series, a bar,
         a status dot — and become the accent outright;
       · tints and deep shades keep their own lightness and only have their hue
         moved onto the accent, so a tint stays a tint and dark text stays dark.
     Greys and the extremes are left exactly as they are. */
  function mapColour(c) {
    if (!palette) palette = readPalette();
    if (!palette) return c;
    if (mine && mine[String(c).trim().toLowerCase()]) return c;   // already ours
    var p = parseColour(c);
    if (!p) return c;                                    // gradients, patterns, 'none'
    var hsl = rgbToHsl(p.rgb);
    if (hsl[1] < 0.34 || hsl[2] < 0.18 || hsl[2] > 0.90) return c;
    var best = palette[0], bestD = 999;
    for (var i = 0; i < palette.length; i++) {
      var d = Math.abs(palette[i].hue - hsl[0]);
      if (d > 180) d = 360 - d;
      if (d < bestD) { bestD = d; best = palette[i]; }
    }
    if (hsl[2] >= 0.32 && hsl[2] <= 0.72) {
      if (p.a >= 0.999) return best.css;
      var rgb = hexToRgb(best.css);
      return rgb ? "rgba(" + rgb[0] + "," + rgb[1] + "," + rgb[2] + "," + p.a + ")" : best.css;
    }
    var h = Math.round(best.hue);
    var sat = Math.round(Math.min(1, hsl[1] * 0.9) * 100);
    var lum = Math.round(hsl[2] * 100);
    return p.a >= 0.999
      ? "hsl(" + h + ", " + sat + "%, " + lum + "%)"
      : "hsla(" + h + ", " + sat + "%, " + lum + "%, " + p.a + ")";
  }

  /* The same rule applied to inline style attributes.  The dashboard assembles
     many of them at runtime, so a stylesheet cannot cover them: an attribute
     selector can match "background:#dc2626" but not "that hex followed by an
     optional alpha pair", and a blanket `color:` rule repaints backgrounds.
     🔴 Both halves must agree, so both go through mapColour(). */

  var STYLE_PROPS = ["color", "backgroundColor", "borderColor", "borderLeftColor",
                     "borderTopColor", "borderRightColor", "borderBottomColor",
                     "fill", "stroke", "outlineColor", "caretColor"];

  function relLum(rgb) {
    function f(v) { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); }
    return 0.2126 * f(rgb[0]) + 0.7152 * f(rgb[1]) + 0.0722 * f(rgb[2]);
  }
  function contrast(a, b) {
    var l1 = relLum(a), l2 = relLum(b);
    return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
  }

  function mapInlineStyle(el) {
    var st = el.style;
    if (!st) return;
    for (var i = 0; i < STYLE_PROPS.length; i++) {
      var prop = STYLE_PROPS[i];
      var v = st[prop];
      if (!v) continue;
      var mapped = mapColour(v);
      // Writing an unchanged value would re-trigger the observer forever.
      if (mapped !== v) {
        try { st[prop] = mapped; } catch (e) {}
      }
    }
    /* 🔴 Moving a colour onto the palette can break a pair the dashboard chose
       for contrast: it paints its scheduler pills as white text on raw orange,
       which becomes white on Catppuccin yellow — 1.3:1, unreadable.  So after
       mapping, if an element carries its own solid background, make sure its
       own text still stands on it. */
    var bgv = st.backgroundColor;
    if (!bgv) return;
    var bg = parseColour(bgv);
    if (!bg || bg.a < 0.6) return;
    var fgv = st.color || getComputedStyle(el).color;
    var fg = parseColour(fgv);
    if (!fg) return;
    if (contrast(fg.rgb, bg.rgb) >= 2.6) return;
    var cs = getComputedStyle(ROOT);
    var ink = (cs.getPropertyValue("--ctp-crust") || "#11111b").trim();
    var pale = (cs.getPropertyValue("--ctp-text") || "#cdd6f4").trim();
    var inkRgb = hexToRgb(ink), paleRgb = hexToRgb(pale);
    var pick = (inkRgb && paleRgb &&
                contrast(inkRgb, bg.rgb) >= contrast(paleRgb, bg.rgb)) ? ink : pale;
    try { st.color = pick; } catch (e) {}
  }

  function mapInlineTree(root) {
    if (!root || root.nodeType !== 1) return;
    if (root.hasAttribute && root.hasAttribute("style")) mapInlineStyle(root);
    var list = root.querySelectorAll ? root.querySelectorAll("[style]") : [];
    for (var i = 0; i < list.length; i++) mapInlineStyle(list[i]);
  }

  function hookInlineColours() {
    var host = document.getElementById("panes") || document.body;
    if (!host || host.__auInline) return;
    host.__auInline = true;
    mapInlineTree(host);
    try {
      new MutationObserver(function (recs) {
        for (var i = 0; i < recs.length; i++) {
          var r = recs[i];
          if (r.type === "attributes") { mapInlineStyle(r.target); continue; }
          for (var j = 0; j < r.addedNodes.length; j++) mapInlineTree(r.addedNodes[j]);
        }
      }).observe(host, { childList: true, subtree: true,
                         attributes: true, attributeFilter: ["style"] });
    } catch (e) {}
  }

  function hookCanvasColours() {
    var C = window.CanvasRenderingContext2D;
    if (!C || C.prototype.__auColours) return;
    ["fillStyle", "strokeStyle", "shadowColor"].forEach(function (prop) {
      var d = Object.getOwnPropertyDescriptor(C.prototype, prop);
      if (!d || !d.set || !d.get) return;
      Object.defineProperty(C.prototype, prop, {
        configurable: true,
        enumerable: d.enumerable,
        get: function () { return d.get.call(this); },
        set: function (v) { d.set.call(this, mapColour(v)); }
      });
    });
    var G = window.CanvasGradient;
    if (G && G.prototype.addColorStop) {
      var base = G.prototype.addColorStop;
      G.prototype.addColorStop = function (o, c) { return base.call(this, o, mapColour(c)); };
    }
    C.prototype.__auColours = true;
    // The theme toggle swaps Mocha for Latte — re-read on the next paint.
    try {
      new MutationObserver(function () { palette = null; mine = null; })
        .observe(ROOT, { attributes: true, attributeFilter: ["data-theme"] });
    } catch (e) {}
  }

  /* ── 6 · Hook the dashboard's own live render ───────────────────────── */

  function hookRenderLive() {
    if (typeof window.renderLive !== "function") return false;
    if (window.renderLive.__auWrapped) return true;
    var base = window.renderLive;
    var wrapped = function (data, first) {
      var out;
      try {
        out = base.apply(this, arguments);
      } finally {
        try { updateHero(data); } catch (e) {}
      }
      return out;
    };
    wrapped.__auWrapped = true;
    window.renderLive = wrapped;
    return true;
  }

  /* ── 7 · Keep the active tab visible in the rail ────────────────────── */

  function followNav() {
    var nav = document.getElementById("bottom-nav");
    if (!nav || nav.__auFollow) return;
    nav.__auFollow = true;
    var last = null;
    var scrollToActive = function () {
      var a = nav.querySelector(".nav-btn.active");
      if (!a || a === last) return;
      last = a;
      var want = a.offsetLeft - (nav.clientWidth - a.offsetWidth) / 2;
      try {
        nav.scrollTo({ left: Math.max(0, want), behavior: REDUCED ? "auto" : "smooth" });
      } catch (e) { nav.scrollLeft = Math.max(0, want); }
    };
    /* 🔴 Watching clicks is not enough: the dashboard restores the last tab
       from localStorage on load and the command palette switches panes without
       one, so the rail stayed wherever it was and the active tab sat off-screen.
       Follow the `active` class itself, whoever moved it. */
    try {
      new MutationObserver(scrollToActive).observe(nav, {
        subtree: true, attributes: true, attributeFilter: ["class"]
      });
    } catch (e) {
      nav.addEventListener("click", function () { setTimeout(scrollToActive, 30); });
    }
    setTimeout(scrollToActive, 250);
  }

  /* ── 8 · Start ──────────────────────────────────────────────────────── */

  function boot() {
    mountSky();
    try { mountCircuit(); } catch (e) {}
    hookInlineColours();
    takeOverTint();
    hookRenderLive();
    followNav();
    // If live data already arrived before this file was evaluated, adopt it.
    try {
      if (window._liveLatest) updateHero(window._liveLatest);
    } catch (e) {}
  }

  // Armed before anything can paint — it needs no DOM.
  try { hookCanvasColours(); } catch (e) {}

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }

  /* The dashboard rebuilds cards on device changes; re-take the hooks a few
     times early on in case this file won the race against its own script. */
  var tries = 0;
  var iv = setInterval(function () {
    tries++;
    var done = takeOverTint() && hookRenderLive();
    if (done || tries > 20) clearInterval(iv);
  }, 250);
})();
