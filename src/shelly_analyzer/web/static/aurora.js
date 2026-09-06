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


  /* ── 1b · The board ─────────────────────────────────────────────────── */

  /* An energy analyzer should not have a generic gradient behind it.  The
     ground here is the house's own wiring: bus bars up the margins, branches
     forking inward toward the content, pads and vias where they meet, and
     charge running through all of it.  Speed, colour and how many pulses are
     moving all come from the live draw — at three in the morning it is a slow
     teal trickle, and when the oven goes on it runs fast and red.

     Everything the background does happens on THIS canvas.  That is the
     lesson of the first version, and it was measured, not guessed: the soft
     field used to be three DOM elements with `filter: blur(58px)`,
     `mix-blend-mode` and an animated `scale()`.  A bisection against the live
     dashboard put the whole page at 14 fps with them and 52 fps without —
     while removing the canvas changed nothing at all.  A blur that is
     re-rasterised every frame is the most expensive thing on a page; a
     pre-rendered sprite blitted with drawImage is nearly free.

     Rules this file keeps:
       · Nothing is allocated per frame.  Traces are one Path2D built at
         resize; every glow is a cached sprite.
       · The charge stays in the MARGINS — conductors across a column of text
         are a distraction, not atmosphere.
       · The loop cannot die.  A frame that throws still reschedules, and a
         watchdog restarts it when the browser stops delivering frames at all:
         an occluded window does that WITHOUT firing visibilitychange, which is
         why the background froze and never came back.
       · Phones get less of everything, and `prefers-reduced-motion` gets one
         static frame. */

  var circuit = null;
  var SPR = Object.create(null);     // pulse glow, by hue bucket
  var FIELD = Object.create(null);   // the soft field, by hue bucket
  var GRD = Object.create(null);     // ground wash, by hue bucket

  function rawCtx(cv) {
    var g = cv.getContext ? cv.getContext("2d") : null;
    /* The chart colour hook must not touch the background: it is already drawn
       in the live load hue, and snapping it to the nearest accent is what made
       the first version garish — every soft hsla(26,40%,68%,.13) conductor
       came out as full-strength rgb(254,100,11). */
    if (g) { try { g.__auRaw = true; } catch (e) {} }
    return g;
  }

  function bucket(hue) { return ((Math.round(hue / 6) * 6) % 360 + 360) % 360; }

  function glowSprite(hue, light) {
    var key = hue + ":" + (light ? "l" : "d");
    if (SPR[key]) return SPR[key];
    var R = 24, c = document.createElement("canvas");
    c.width = c.height = R * 2;
    var g = rawCtx(c);
    if (!g) return null;
    var L = light ? 50 : 74;
    var rg = g.createRadialGradient(R, R, 0, R, R, R);
    rg.addColorStop(0.00, "hsla(" + hue + ", 96%, " + (light ? 62 : 92) + "%, 1)");
    rg.addColorStop(0.16, "hsla(" + hue + ", 96%, " + L + "%, 0.78)");
    rg.addColorStop(0.44, "hsla(" + hue + ", 92%, " + L + "%, 0.20)");
    rg.addColorStop(1.00, "hsla(" + hue + ", 92%, " + L + "%, 0)");
    g.fillStyle = rg;
    g.fillRect(0, 0, R * 2, R * 2);
    SPR[key] = c;
    return c;
  }

  function fieldSprite(hue, light) {
    var key = hue + ":" + (light ? "l" : "d");
    if (FIELD[key]) return FIELD[key];
    var S = 256, c = document.createElement("canvas");
    c.width = c.height = S;
    var g = rawCtx(c);
    if (!g) return null;
    var L = light ? 62 : 58, sat = light ? 62 : 68;
    var rg = g.createRadialGradient(S / 2, S / 2, 0, S / 2, S / 2, S / 2);
    rg.addColorStop(0.00, "hsla(" + hue + ", " + sat + "%, " + L + "%, 1)");
    rg.addColorStop(0.42, "hsla(" + hue + ", " + sat + "%, " + L + "%, 0.42)");
    rg.addColorStop(0.72, "hsla(" + hue + ", " + sat + "%, " + L + "%, 0.11)");
    rg.addColorStop(1.00, "hsla(" + hue + ", " + sat + "%, " + L + "%, 0)");
    g.fillStyle = rg;
    g.fillRect(0, 0, S, S);
    FIELD[key] = c;
    return c;
  }

  function groundSprite(hue, light) {
    var key = hue + ":" + (light ? "l" : "d");
    if (GRD[key]) return GRD[key];
    var c = document.createElement("canvas");
    c.width = 8; c.height = 128;
    var g = rawCtx(c);
    if (!g) return null;
    var lg = g.createLinearGradient(0, 0, 0, 128);
    lg.addColorStop(0.0, "hsla(" + hue + ", 74%, " + (light ? 54 : 60) + "%, 0)");
    lg.addColorStop(0.6, "hsla(" + hue + ", 76%, " + (light ? 52 : 58) + "%, 0.34)");
    lg.addColorStop(1.0, "hsla(" + hue + ", 80%, " + (light ? 48 : 56) + "%, 1)");
    g.fillStyle = lg;
    g.fillRect(0, 0, 8, 128);
    GRD[key] = c;
    return c;
  }

  /* The wiring.  Bus bars up each margin; branches fork inward but stop short
     of the reading column, so nothing is ever drawn under a paragraph. */
  function buildBoard(w, h) {
    var pane = 1180;
    try {
      var pv = parseFloat(getComputedStyle(ROOT).getPropertyValue("--pane-max"));
      if (isFinite(pv) && pv > 320) pane = pv;
    } catch (e) {}

    var lanes = [], pads = [], vias = [];
    var narrow = w < 720;
    var mx = narrow ? 8 : Math.max(14, Math.min(40, Math.round(w * 0.021)));
    // How far a branch may reach before it would sit under the text.
    /* Stop short of the reading column rather than reaching into it: a
       conductor behind a paragraph is a distraction, however faint. */
    var reach = narrow ? 58
                       : Math.max(44, Math.min(280, Math.round((w - pane) / 2) - 10));
    var seed = 0;
    function rnd() {                       // deterministic: same board every load
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed / 0x7fffffff;
    }

    function seal(pts, kind) {
      /* Point every lane the same way before measuring it.  The charge walks a
         lane from pts[0] to the last point, so a branch built from the RIGHT
         bus inwards would carry its charge leftwards while everything else ran
         right — two flows meeting in the middle.  Reversing the point list
         leaves the drawn board identical (the stroke is the same polyline) and
         makes the whole picture drift one way: right, and down on the bus. */
      var first = pts[0], last = pts[pts.length - 1];
      var dx = last.x - first.x, dy = last.y - first.y;
      if ((Math.abs(dx) >= Math.abs(dy) ? dx : dy) < 0) pts = pts.slice().reverse();

      var acc = [0], total = 0;
      for (var i = 1; i < pts.length; i++) {
        total += Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y);
        acc.push(total);
      }
      if (total < 8) return null;
      var lane = { pts: pts, acc: acc, len: total, kind: kind,
                   phase: rnd() };
      lanes.push(lane);
      return lane;
    }

    /* A branch: horizontal run, 45-degree elbow, run — and at depth 0 it may
       fork once more.  Two levels is what makes it read as a board rather than
       as a comb. */
    function branch(x0, y0, side, depth, budget) {
      if (budget < 34) return;
      var run1 = budget * (0.38 + rnd() * 0.26);
      var el = 14 + rnd() * 20;
      var down = rnd() < 0.5 ? 1 : -1;
      var x1 = x0 + run1 * side;
      var x2 = x1 + el * side, y2 = y0 + el * down;
      var run2 = (budget - run1 - el) * (0.5 + rnd() * 0.5);
      var x3 = x2 + run2 * side;
      seal([{ x: x0, y: y0 }, { x: x1, y: y0 }, { x: x2, y: y2 }, { x: x3, y: y2 }],
           "branch");
      pads.push({ x: x3, y: y2, r: 2.4 + rnd() });
      if (rnd() < 0.55) vias.push({ x: x1, y: y0, r: 2.6 });
      /* Two forks per level, not one: a single chain of elbows reads as a
         comb, a tree reads as a board. */
      var rest = budget - run1 - el;
      if (depth < 2 && rest > 46) {
        branch(x2, y2, side, depth + 1, rest * 0.78);
        if (rnd() < 0.62) {
          var stub = [{ x: x1, y: y0 },
                      { x: x1 + 15 * side, y: y0 - 15 * down },
                      { x: x1 + (14 + rest * 0.42) * side, y: y0 - 15 * down }];
          seal(stub, "branch");
          pads.push({ x: stub[2].x, y: stub[2].y, r: 2.1 });
        }
      }
    }

    function bus(x0, side) {
      var pts = [{ x: x0, y: -40 }];
      var steps = Math.max(3, Math.min(8, Math.round(h / 155)));
      for (var i = 1; i <= steps; i++) {
        var y = Math.round((h + 80) * i / steps) - 40;
        var jog = (i % 2 ? 11 : -11) * side;
        pts.push({ x: x0, y: y - 30 });
        pts.push({ x: x0 + jog, y: y - 30 + 11 });
        pts.push({ x: x0 + jog, y: y });
        pads.push({ x: x0 + jog, y: y, r: 2.0 });
        if (reach > 54) {
          branch(x0 + jog, y, side, 0, reach);
          if (rnd() < 0.55) branch(x0, y - 30, side, 1, reach * 0.6);
        }
      }
      pts.push({ x: x0, y: h + 40 });
      seal(pts, "bus");
    }

    /* 🔴 A phone used to get ONE conductor along the very bottom edge — which
       is precisely where the floating rail sits, so the background was
       invisible on every phone.  Measured: 1 lane, 5 pads, all of it hidden.
       Phones get the same two bus bars as everything else, pushed into the
       outer margin and with short branches; the cards are glass, so what does
       pass behind them reads as texture rather than as a line through text. */
    bus(mx, 1);
    bus(w - mx, -1);

    /* Trunks: full-width runs with 45-degree elbows that tie the two bus bars
       together.  The first version kept every conductor OUT of the reading
       column — which is defensible, and wrong for this product: Robert wants
       the board to run through the whole picture.  It works because the
       content sits on glass: a trace behind a card is muted by the card's own
       surface, and only the gaps between cards show it at full strength. */
    var rows = Math.max(3, Math.min(narrow ? 6 : 8, Math.round(h / 165)));
    for (var r = 0; r < rows; r++) {
      var y = Math.round((h * (r + 0.5)) / rows) + (r % 2 ? -12 : 12);
      var pts = [{ x: mx, y: y }];
      var x = mx, dir = r % 2 ? -1 : 1, yy = y;
      while (x < w - mx - 20) {
        var run = 70 + ((r * 97 + pts.length * 53) % 150);
        x = Math.min(w - mx, x + run);
        pts.push({ x: x, y: yy });
        if (x >= w - mx - 20) break;
        var el = 18 + ((r * 31 + pts.length * 17) % 26);
        x = Math.min(w - mx, x + el);
        yy = Math.max(16, Math.min(h - 16, yy + el * dir));
        pts.push({ x: x, y: yy });
        if (rnd() < 0.34) pads.push({ x: x, y: yy, r: 1.9 });
        dir = -dir;
      }
      pts.push({ x: w - mx, y: yy });
      seal(pts, "trunk");
    }

    var path = null;
    try {
      path = new Path2D();
      for (var i = 0; i < lanes.length; i++) {
        var pts = lanes[i].pts;
        path.moveTo(pts[0].x, pts[0].y);
        for (var j = 1; j < pts.length; j++) path.lineTo(pts[j].x, pts[j].y);
      }
    } catch (e) { path = null; }

    return { lanes: lanes, pads: pads, vias: vias, path: path, narrow: narrow };
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

  function now() {
    try { return performance.now(); } catch (e) { return +new Date(); }
  }

  /* Three soft masses that drift.  Sine-driven rather than keyframed, so they
     never loop visibly and cost three drawImage calls. */
  var BLOBS = [
    { dh:   0, x: 0.26, y: 0.16, r: 0.62, sx: 0.055, sy: 0.041, ss: 0.031, ph: 0.0, a: 1.00 },
    { dh:  40, x: 0.76, y: 0.09, r: 0.56, sx: 0.043, sy: 0.052, ss: 0.024, ph: 2.3, a: 0.86 },
    { dh: -34, x: 0.50, y: 0.36, r: 0.48, sx: 0.034, sy: 0.029, ss: 0.019, ph: 4.4, a: 0.66 }
  ];

  function mountCircuit() {
    if (document.getElementById("aurora-circuit")) return;
    var cv = document.createElement("canvas");
    cv.id = "aurora-circuit";
    cv.setAttribute("aria-hidden", "true");
    document.body.insertBefore(cv, document.body.firstChild);

    var ctx = rawCtx(cv);
    if (!ctx) return;

    var W = 0, H = 0, DPR = 1, board = { lanes: [], pads: [], vias: [], path: null };
    var t0 = 0, raf = 0, prev = 0, lastFrameAt = 0, faults = 0, alive = true;
    /* Paused BY THE USER, which is not the same as stopped: the watchdog exists
       to revive a loop that died, so it must be told the difference or it would
       restart the animation 1.3 s after every pause. */
    var paused = false;
    try { paused = localStorage.getItem("au-anim") === "off"; } catch (e) {}
    var hueNow = null, liftNow = 0;
    /* Distance travelled, integrated frame by frame.  It must NOT be
       recomputed as t * speed: with the load — and therefore the speed —
       changing under it, every change would teleport the charge by
       t * delta-speed, and that error grows with how long the page has been
       open.  That was the twitch.  Integrating means a change alters the
       PACE and nothing else. */
    var travel = 0;
    var frames = 0;
    var fieldCv = null, fctx = null, fieldAt = -1e9, fieldDirty = true;

    function resize() {
      /* Phones: cap the backing store hard.  A 3x device ratio on a 430px
         screen is 1.7 megapixels of background nobody looks at. */
      var cap = window.innerWidth < 720 ? 1.25 : 1.5;
      DPR = Math.min(cap, window.devicePixelRatio || 1);
      W = window.innerWidth;
      H = window.innerHeight;
      cv.width = Math.round(W * DPR);
      cv.height = Math.round(H * DPR);
      cv.style.width = W + "px";
      cv.style.height = H + "px";
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
      if (!fieldCv) { fieldCv = document.createElement("canvas"); fctx = rawCtx(fieldCv); }
      if (fctx) {
        fieldCv.width = Math.max(2, Math.round(W / 4));
        fieldCv.height = Math.max(2, Math.round(H / 4));
      } else { fieldCv = null; }
      fieldDirty = true;
      board = buildBoard(W, H);
    }

    function cssNum(name, fallback) {
      var v = parseFloat(getComputedStyle(ROOT).getPropertyValue(name));
      return isFinite(v) ? v : fallback;
    }

    var LIGHT = false;
    function readTheme() { LIGHT = ROOT.getAttribute("data-theme") === "light"; }
    readTheme();

    function draw(ts) {
      if (!t0) { t0 = ts; prev = ts; }
      var t = (ts - t0) / 1000;
      var dt = Math.min(0.25, Math.max(0.001, (ts - prev) / 1000));
      prev = ts;

      /* Glide toward the live values instead of jumping to them.  The poller
         updates in steps; the eye should see a slide. */
      var hueT = cssNum("--load-hue", 170);
      var liftT = Math.max(0, Math.min(1, cssNum("--load-lift", 0)));
      if (hueNow === null) { hueNow = hueT; liftNow = liftT; }
      var k = 1 - Math.exp(-dt / 0.9);
      hueNow += (hueT - hueNow) * k;
      liftNow += (liftT - liftNow) * k;
      var hue = hueNow, l = liftNow, bk = bucket(hue);

      ctx.clearRect(0, 0, W, H);
      ctx.globalAlpha = 1;
      ctx.globalCompositeOperation = "source-over";

      // 0 · the field.  It drifts over tens of seconds, so it is composed at a
      //     quarter of the resolution into its own buffer and refreshed a few
      //     times a second; every frame just blits that buffer.  Composing it
      //     directly costs three oversized full-screen blends per frame — the
      //     single most expensive thing the background did.
      if (fieldCv && (ts - fieldAt > 110 || fieldDirty)) {
        fieldAt = ts; fieldDirty = false;
        var fw = fieldCv.width, fh = fieldCv.height, sc = fw / W;
        fctx.clearRect(0, 0, fw, fh);
        var base = (LIGHT ? 0.22 : 0.26) + 0.22 * l;
        for (var bi = 0; bi < BLOBS.length; bi++) {
          var B = BLOBS[bi];
          var f2 = fieldSprite(bucket(hue + B.dh), LIGHT);
          if (!f2) continue;
          var cx = (W * B.x + Math.sin(t * B.sx * 6.283 + B.ph) * W * 0.07) * sc;
          var cy = (H * B.y + Math.cos(t * B.sy * 6.283 + B.ph) * H * 0.06) * sc;
          var rr = W * B.r * (1 + 0.09 * Math.sin(t * B.ss * 6.283 + B.ph)) * sc;
          var rh = rr * 0.80;
          fctx.globalAlpha = Math.min(1, base * B.a);
          fctx.drawImage(f2, cx - rr, cy - rh, rr * 2, rh * 2);
        }
        /* Clear the top band.  The header sits there, and a wash of colour
           behind small text is the difference between atmosphere and haze. */
        fctx.globalAlpha = 1;
        fctx.globalCompositeOperation = "destination-out";
        var vg = fctx.createLinearGradient(0, 0, 0, fh * 0.34);
        vg.addColorStop(0, "rgba(0,0,0,0.92)");
        vg.addColorStop(1, "rgba(0,0,0,0)");
        fctx.fillStyle = vg;
        fctx.fillRect(0, 0, fw, Math.ceil(fh * 0.34));
        fctx.globalCompositeOperation = "source-over";
      }
      if (fieldCv) ctx.drawImage(fieldCv, 0, 0, W, H);

      // 1 · the conductors — one stroke for all of them
      if (board.path) {
        ctx.lineWidth = 1;
        ctx.lineJoin = "round";
        ctx.lineCap = "round";
        ctx.strokeStyle = "hsla(" + hue.toFixed(0) + ", 38%, " +
                          (LIGHT ? "40%" : "74%") + ", " +
                          ((LIGHT ? 0.30 : 0.27) + 0.12 * l).toFixed(3) + ")";
        ctx.stroke(board.path);
      }

      // 2 · pads and vias
      var padA = (LIGHT ? 0.30 : 0.28) + 0.16 * l;
      ctx.fillStyle = "hsla(" + hue.toFixed(0) + ", 48%, " +
                      (LIGHT ? "38%" : "80%") + ", " + padA.toFixed(3) + ")";
      for (var p = 0; p < board.pads.length; p++) {
        var pd = board.pads[p];
        ctx.fillRect(pd.x - pd.r, pd.y - pd.r, pd.r * 2, pd.r * 2);
      }
      if (board.vias.length) {
        ctx.lineWidth = 1;
        ctx.strokeStyle = "hsla(" + hue.toFixed(0) + ", 48%, " +
                          (LIGHT ? "40%" : "78%") + ", " + (padA * 0.8).toFixed(3) + ")";
        for (var v = 0; v < board.vias.length; v++) {
          var vi = board.vias[v];
          ctx.beginPath();
          ctx.arc(vi.x, vi.y, vi.r, 0, 6.2832);
          ctx.stroke();
        }
      }

      if (REDUCED) return;

      // 3 · the charge.  Buses always carry a trickle; branches light up once
      //     the house is actually pulling something.
      var spr = glowSprite(bk, LIGHT);
      if (!spr) return;
      var mob = board.narrow;
      /* Load sets the PACE — a slow drift when the house is quiet, brisk when
         the oven is on.  The old 26..266 px/s was frantic at the top end. */
      var speed = 18 + l * 102;                            // px/s
      travel += speed * dt;
      /* Counts stay fixed.  Deriving them from the load made pulses pop in and
         out every time the rounding boundary was crossed, which read as noise
         rather than as "the house is drawing more". */
      var busN = mob ? 3 : 5;
      var trN = mob ? 2 : 3;
      var brN = 1;
      var head = 0.20 + 0.42 * l;
      var TRAIL = mob ? 6 : 11;
      var R0 = 5.4 + 3.0 * l;

      ctx.globalCompositeOperation = LIGHT ? "source-over" : "lighter";
      for (var i = 0; i < board.lanes.length; i++) {
        var ln = board.lanes[i];
        var n = ln.kind === "bus" ? busN : (ln.kind === "trunk" ? trN : brN);
        if (!n) continue;
        /* Every lane runs the same way along its own path.  Alternating the
           direction by lane index made neighbouring tracks flow against each
           other, which is not how a board carries current. */
        for (var c2 = 0; c2 < n; c2++) {
          var d = (travel + ln.phase * ln.len + (c2 * ln.len) / n) % ln.len;
          for (var q = 0; q < TRAIL; q++) {
            var u = q / TRAIL;
            var pt = pointAt(ln, d - q * (mob ? 6 : 4.6));
            var a = head * (1 - u) * (1 - u) * (1 - u * 0.35);
            var s = R0 * (1 - u * 0.55);
            ctx.globalAlpha = a > 1 ? 1 : (a < 0 ? 0 : a);
            ctx.drawImage(spr, pt.x - s, pt.y - s, s * 2, s * 2);
          }
        }
      }
      ctx.globalCompositeOperation = "source-over";
      ctx.globalAlpha = 1;

      // 4 · the ground: a slow breath along the bottom edge, brighter the more
      //     the house is pulling.  It sits under the nav rail's glass.
      var gh = Math.min(240, H * 0.32);
      var gg = groundSprite(bk, LIGHT);
      if (gg) {
        ctx.globalAlpha = (LIGHT ? 0.12 : 0.15) + 0.24 * l +
                          0.03 * Math.sin(t * 0.62);
        ctx.drawImage(gg, 0, H - gh, W, gh);
        ctx.globalAlpha = 1;
      }
    }

    function frame(ts) {
      raf = 0;
      lastFrameAt = now();
      frames++;
      if (document.hidden) return;              // the watchdog brings it back
      try {
        draw(ts);
      } catch (e) {
        /* A frame that throws must not take the animation with it.  The first
           version left `raf` at 0 with nothing scheduled, so a single
           exception froze the background for the life of the page. */
        if (faults++ === 0 && window.console) console.warn("[aurora] frame:", e);
        if (faults > 90) { stop(); return; }
      }
      if (REDUCED) { stop(); return; }          // one static frame is enough
      schedule();
    }

    function schedule() {
      if (raf || !alive || paused) return;
      raf = requestAnimationFrame(frame);
    }

    function stop() {
      alive = false;
      if (raf) { try { cancelAnimationFrame(raf); } catch (e) {} raf = 0; }
    }

    var rt = 0;
    window.addEventListener("resize", function () {
      clearTimeout(rt);
      rt = setTimeout(function () {
        alive = true;
        try { resize(); } catch (e) {}
        /* A resize still redraws once while paused, so the board is not left
           stretched — it just does not carry on running. */
        if (REDUCED || paused) { raf = 0; requestAnimationFrame(frame); } else schedule();
      }, 200);
    });
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden && !paused) { lastFrameAt = now(); prev = 0; t0 = 0; schedule(); }
    });
    try {
      new MutationObserver(function () {
        readTheme();
        fieldDirty = true;
        if (REDUCED) { raf = 0; requestAnimationFrame(frame); }
      }).observe(ROOT, { attributes: true, attributeFilter: ["data-theme"] });
    } catch (e) {}

    /* 🔴 The watchdog is the fix for the freeze, and it has to be a timer:
       requestAnimationFrame stops being delivered when the window is merely
       covered by another one, and that does NOT fire visibilitychange — so the
       loop had no way back and the background stayed dead until a reload.
       setInterval keeps running in that state. */
    setInterval(function () {
      if (REDUCED || document.hidden || paused) return;
      if (now() - lastFrameAt < 1600) return;
      alive = true;
      if (raf) { try { cancelAnimationFrame(raf); } catch (e) {} raf = 0; }
      lastFrameAt = now();
      schedule();
    }, 1300);

    resize();
    lastFrameAt = now();
    schedule();
    circuit = cv;

    function setPaused(v) {
      paused = !!v;
      try { localStorage.setItem("au-anim", paused ? "off" : "on"); } catch (e) {}
      if (paused) {
        if (raf) { try { cancelAnimationFrame(raf); } catch (e) {} raf = 0; }
      } else {
        alive = true;
        prev = 0;                 // ...but NOT t0: keep the field where it was
        lastFrameAt = now();
        schedule();
      }
      document.documentElement.setAttribute("data-anim", paused ? "off" : "on");
      return paused;
    }
    document.documentElement.setAttribute("data-anim", paused ? "off" : "on");
    if (paused && raf) { try { cancelAnimationFrame(raf); } catch (e) {} raf = 0; }

    // Exposed so a test can assert the loop is actually running.
    window.__auCircuit = {
      frames: function () { return frames; },
      lanes: function () { return board.lanes.length; },
      pads: function () { return board.pads.length; },
      alive: function () { return alive; },
      hue: function () { return hueNow; },
      paused: function () { return paused; },
      setPaused: setPaused,
      toggle: function () { return setPaused(!paused); }
    };
  }

  /* ── 2 · Load → colour ──────────────────────────────────────────────── */

  /* Breakpoints in watts for a whole house, mapped to a hue that decreases
     monotonically (teal → green → yellow → peach → red).  Red is written as
     -12 rather than 348 so a plain interpolation never sweeps the long way
     round the colour wheel. */
  var STOPS = [
    {   w: 0,    hue: 174, key: "aurora.load.idle",    fb: "Idle"    },
    {   w: 250,  hue: 140, key: "aurora.load.low",     fb: "Low"     },
    {   w: 800,  hue: 68,  key: "aurora.load.working", fb: "Working" },
    {   w: 2000, hue: 30,  key: "aurora.load.high",    fb: "High"    },
    {   w: 4500, hue: -12, key: "aurora.load.peak",    fb: "Peak"    }
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
      /* The lift drives the speed of the charge, the brightness of the field
         and the ground glow.  It follows the SAME banded scale as the arc,
         not a straight 0..6000 W line: on a linear scale a house doing 2.4 kW
         sits at 0.40 and the background barely reacts to the difference
         between a quiet evening and the oven being on.  Banded, the range a
         household actually lives in gets the whole span. */
      lift: Math.max(0, Math.min(1, (i + f) / (STOPS.length - 1))),
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
          '<button type="button" class="au-anim-btn" id="au-anim" aria-pressed="false">' +
            '<span class="au-anim-ico" aria-hidden="true"></span>' +
            '<span class="au-anim-lbl"></span>' +
          '</button>' +
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
    /* The hero opens the summary panel, so it has to behave like a control:
       reachable by keyboard and announced with its state. */
    hero.setAttribute("role", "button");
    hero.setAttribute("tabindex", "0");
    hero.setAttribute("aria-controls", "live-insights");
    hero.setAttribute("aria-expanded", "false");
    hero.innerHTML = heroMarkup();
    /* The summary panel belongs UNDER the hero it is opened from, so the
       hero goes in front of it, not in front of the device grid. */
    var below = document.getElementById("live-insights") || grid;
    grid.parentNode.insertBefore(hero, below);
    var title = hero.querySelector("#au-hero-title");
    if (title) title.textContent = T("aurora.hero.title", "Right now");
    wireAnimButton(hero);
    return hero;
  }

  /* Pause/resume the charge. The state is remembered, so a viewer who does not
     want the motion is not asked again on every reload. */
  function wireAnimButton(hero) {
    var btn = hero.querySelector("#au-anim");
    if (!btn || btn.__auWired) return;
    btn.__auWired = 1;
    var sync = function () {
      var c = window.__auCircuit;
      var p = !!(c && c.paused && c.paused());
      var action = p ? T("aurora.anim.resume", "Resume animation")
                     : T("aurora.anim.pause", "Pause animation");
      btn.setAttribute("aria-pressed", p ? "true" : "false");
      btn.setAttribute("aria-label", action);
      btn.title = action;
      var ico = btn.querySelector(".au-anim-ico");
      var lbl = btn.querySelector(".au-anim-lbl");
      if (ico) ico.textContent = p ? "\u25B6" : "\u23F8";
      if (lbl) lbl.textContent = T("aurora.anim.short", "Animation");
    };
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      var c = window.__auCircuit;
      if (c && c.toggle) c.toggle();
      sync();
    });
    sync();
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

  function flowRole(d) { return String(d && d.flow_role || ""); }

  /* What the household is drawing, watts / kWh today / € today, from the device
     tiles alone.  Exposed as window.__auHeroSum because this one sum has now
     been wrong twice, and both times the error was only visible on a real
     installation — it gets executed by tests/test_aurora_hero_sum.py. */
  function heroSum(devs) {
    /* Generation and storage are never a draw.  Neither is the GRID meter, as
       long as the installation HAS generation: it reads the net exchange with
       the public grid (import − export, after PV and battery), which is nothing
       like what the house consumes.  Summing it as if it were an appliance put
       one installation's "right now" at 35.0 kWh / 2.80 € on a day the house
       had actually used 47.7 kWh for 2.04 € — and, worse, the house meter hung
       off the grid meter and was then skipped as a "sub-meter", so the single
       largest consumer fell out of the total entirely.
       Without PV or a battery the grid meter IS the house meter: there it stays
       in, and anything metered behind it is skipped below, exactly as before. */
    var hasGen = devs.some(function (d) {
      var r = flowRole(d);
      return r === "pv" || r === "battery";
    });
    var draw = devs.filter(function (d) {
      var r = flowRole(d);
      if (r === "pv" || r === "battery") return false;
      return !(hasGen && r === "grid");
    });
    /* An installation that meters nothing but its grid connection would be left
       with no tiles at all; there the grid reading is the best measure of the
       draw that exists. */
    if (!draw.length) {
      draw = devs.filter(function (d) { return flowRole(d) === "grid"; });
    }

    /* A sub-meter sits BEHIND another meter, so its load is already inside that
       meter's reading: adding it again counted a 1 960 W water heater twice, and
       "now" read 4 288 W on a house pulling 2 327 W.  Skip a child whose parent
       is one of these tiles — unless the parent is shown net of exactly this
       child (subtract_from_parent_display), in which case the parent no longer
       contains it and it has to be counted after all.
       A house meter fed by the grid connection is NOT such a child: with
       generation present the grid tile is not among these tiles at all, so the
       house is counted — which is the whole point of the rule above. */
    var shown = {};
    for (var s = 0; s < draw.length; s++) shown[String(draw[s].key)] = draw[s];
    function counts(d, tiefe) {
      tiefe = tiefe || 0;
      if (tiefe > 8) return false;               // a cycle in the wiring
      var p = shown[String(d.parent || "")];
      if (!p) return true;                       // no parent among the tiles
      var net = p.net_of_children;
      if (net && net.indexOf(d.key) >= 0) {
        /* The parent's tile no longer shows this child — but the child is still
           physically behind whatever the parent sits behind.  Counting it
           unconditionally added a 1 960 W boiler on top of the 2 327 W grid
           meter that already contained it.  So: count it exactly when the
           parent itself is counted. */
        return counts(p, tiefe + 1);
      }
      return false;                              // already inside the parent
    }

    var out = { totalW: 0, kwh: 0, cost: 0, top: null };
    for (var i = 0; i < draw.length; i++) {
      var d = draw[i];
      if (!counts(d)) continue;
      var w = Number(d.power_w) || 0;
      out.totalW += w;
      out.kwh    += Number(d.today_kwh)  || 0;
      out.cost   += Number(d.cost_today) || 0;
      if (!out.top || w > (Number(out.top.power_w) || 0)) out.top = d;
    }
    return out;
  }
  window.__auHeroSum = heroSum;

  function updateHero(data) {
    var hero = ensureHero();
    if (!hero) return;
    var devs = (data && data.devices) || [];
    var sum = heroSum(devs);
    var totalW = sum.totalW, kwh = sum.kwh, cost = sum.cost, top = sum.top;

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
      if (rgb) {
        var a = rgbToHsl(rgb);
        out.push({ hue: a[0], sat: a[1], css: hex });
      }
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
  /* 🔴 The rule used to SNAP: any colour of middling lightness was replaced
     outright by the nearest of ten accents.  That reads well on a single
     badge and destroys anything continuous.  Measured on the live heatmap:
     the dashboard paints it as a smooth ramp — rgb(85,192,72), (146,187,46),
     (186,183,29), (224,180,12), (236,134,32), (238,87,58) and dozens between
     — and the snap collapsed the whole thing to FOUR colours.  A heatmap whose
     meaning is its gradient had no gradient left, and two series of a chart
     could come out the same colour.

     So the hue is ATTRACTED, not replaced: it moves toward the nearest accent
     by at most 16 degrees, saturation blends part of the way, and lightness is
     never touched.  Accents are 30-90 degrees apart, so a bounded shift cannot
     reorder two colours or merge them — a ramp stays a ramp, and it still
     lands in the palette's family.  Lightness carrying the role is what keeps
     white-on-orange from becoming white-on-yellow. */
  var HUE_PULL = 16;      // degrees, the most a colour may be moved
  var HUE_FRAC = 0.6;     // and it only ever closes this much of the gap
  var SAT_MIX = 0.42;     // how far saturation blends toward the accent

  /* 🔴 The rule MUST be idempotent, and an attraction is not idempotent by
     itself: feeding its own output back in moves the colour again, a little
     less each time.  Measured — rgb(186,183,29) needs SEVEN passes to reach a
     fixed point.  That matters because the inline-style observer writes the
     mapped value back onto the element, which fires the observer, which maps
     it again... ~2200 styled elements times seven round-trips, each with a
     getComputedStyle in the contrast guard, is enough to wedge the main
     thread: with the skin the page stopped answering at all, without it the
     same page was fine.

     So the output is remembered and recognised.  It is emitted as rgb()/rgba()
     — the form the DOM serialises to — so what comes back on the second pass
     is character-for-character what went out. */
  var produced = Object.create(null);
  var memo = Object.create(null);

  function _ckey(c) { return String(c).replace(/\s+/g, "").toLowerCase(); }

  function mapColour(c) {
    if (!palette) palette = readPalette();
    if (!palette) return c;
    if (typeof c !== "string") return c;
    var key = _ckey(c);
    if (produced[key]) return c;                    // our own output, unchanged
    if (mine && mine[String(c).trim().toLowerCase()]) return c;   // already ours
    if (memo[key] !== undefined) return memo[key];
    var p = parseColour(c);
    if (!p) return c;                                    // gradients, patterns, 'none'
    var hsl = rgbToHsl(p.rgb);
    if (hsl[1] < 0.34 || hsl[2] < 0.18 || hsl[2] > 0.90) return c;

    var best = palette[0], bestD = 999, bestSigned = 0;
    for (var i = 0; i < palette.length; i++) {
      var raw = palette[i].hue - hsl[0];
      while (raw > 180) raw -= 360;
      while (raw < -180) raw += 360;
      var d = Math.abs(raw);
      if (d < bestD) { bestD = d; best = palette[i]; bestSigned = raw; }
    }

    /* Close a FRACTION of the gap, then cap it.  Closing it completely is a
       snap in miniature: every colour within the cap lands on the accent
       exactly, and five neighbouring steps of a ramp come out at the same
       hue.  At 60 % two inputs keep 40 % of their separation, and inputs on
       either side of a boundary move apart rather than together. */
    var shift = bestSigned * HUE_FRAC;
    shift = Math.max(-HUE_PULL, Math.min(HUE_PULL, shift));
    var h = ((hsl[0] + shift) % 360 + 360) % 360;
    var sAcc = best.sat == null ? hsl[1] : best.sat;
    var sat = hsl[1] + (sAcc - hsl[1]) * SAT_MIX;
    var rgb = hslToRgb(h, Math.max(0, Math.min(1, sat)), hsl[2]);
    var out = p.a >= 0.999
      ? "rgb(" + rgb[0] + ", " + rgb[1] + ", " + rgb[2] + ")"
      : "rgba(" + rgb[0] + ", " + rgb[1] + ", " + rgb[2] + ", " + p.a + ")";
    produced[_ckey(out)] = 1;
    memo[key] = out;
    return out;
  }

  /* The same rule applied to inline style attributes.  The dashboard assembles
     many of them at runtime, so a stylesheet cannot cover them: an attribute
     selector can match "background:#dc2626" but not "that hex followed by an
     optional alpha pair", and a blanket `color:` rule repaints backgrounds.
     🔴 Both halves must agree, so both go through mapColour(). */

  var STYLE_PROPS = ["color", "backgroundColor", "borderColor", "borderLeftColor",
                     "borderTopColor", "borderRightColor", "borderBottomColor",
                     "fill", "stroke", "outlineColor", "caretColor"];

  /* What is actually behind this element: the first ancestor that paints a
     solid colour, and the root if none does. */
  function backdropOf(el) {
    var n = el;
    while (n) {
      var cs = getComputedStyle(n);
      if (cs.backgroundImage && cs.backgroundImage !== "none") return null;
      var c = parseColour(cs.backgroundColor);
      if (c && c.a > 0.55) return c.rgb;
      n = n.parentElement;
    }
    var root = parseColour(getComputedStyle(ROOT).backgroundColor);
    return root && root.a > 0.55 ? root.rgb : null;
  }

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
    /* 🔴 The guard used to fire only when the element painted its OWN solid
       background.  A colour that is merely written as text sits on whatever
       is behind it — and lightness is deliberately preserved by the mapper, so
       a mid-light amber lands on a white card at 2.0:1.  Measured on the CO2
       tab in the light theme.  Find the real backing colour and, if the pair
       fails, move the LIGHTNESS of the text until it passes; hue and
       saturation are what carry the meaning, so they stay. */
    var bgv = st.backgroundColor;
    if (!bgv || (parseColour(bgv) || { a: 0 }).a < 0.6) {
      var fgOwn = st.color;
      if (!fgOwn) return;
      var f = parseColour(fgOwn);
      if (!f) return;
      var back = backdropOf(el);
      if (!back) return;
      if (contrast(f.rgb, back) >= 3.0) return;
      var hsl = rgbToHsl(f.rgb);
      var up = relLum(back) < 0.45;             // dark ground -> lighten
      for (var k = 1; k <= 14; k++) {
        var L = Math.max(0.06, Math.min(0.96, hsl[2] + (up ? k * 0.05 : -k * 0.05)));
        var cand = hslToRgb(hsl[0], hsl[1], L);
        if (contrast(cand, back) >= 3.0) {
          var out = f.a >= 0.999
            ? "rgb(" + cand[0] + ", " + cand[1] + ", " + cand[2] + ")"
            : "rgba(" + cand[0] + ", " + cand[1] + ", " + cand[2] + ", " + f.a + ")";
          produced[_ckey(out)] = 1;
          try { st.color = out; } catch (e) {}
          return;
        }
      }
      return;
    }
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

  /* 🔴 A context may opt OUT.  The skin's own background canvas paints in the
     live load hue; running that through the snapper turned every soft
     hsla(26,40%,68%,.13) conductor into full-strength rgb(254,100,11) and was
     the reason the background looked garish.  The first attempt at an opt-out
     captured the "raw" setter inside mountCircuit — but this hook is armed
     before boot(), so what it captured WAS the patched setter and the bypass
     never bypassed anything.  A flag on the context is checked at call time
     and cannot be defeated by ordering. */
  var rawGrads = (typeof WeakSet === "function") ? new WeakSet() : null;

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
        set: function (v) { d.set.call(this, this.__auRaw ? v : mapColour(v)); }
      });
    });
    // A gradient remembers the context it came from, so its stops follow the
    // same rule as the fill that will use it.
    ["createLinearGradient", "createRadialGradient", "createConicGradient"]
      .forEach(function (fn) {
        var base = C.prototype[fn];
        if (typeof base !== "function") return;
        C.prototype[fn] = function () {
          var g = base.apply(this, arguments);
          if (this.__auRaw && rawGrads && g) { try { rawGrads.add(g); } catch (e) {} }
          return g;
        };
      });
    var G = window.CanvasGradient;
    if (G && G.prototype.addColorStop) {
      var addStop = G.prototype.addColorStop;
      G.prototype.addColorStop = function (o, c) {
        var raw = rawGrads && rawGrads.has(this);
        return addStop.call(this, o, raw ? c : mapColour(c));
      };
    }
    C.prototype.__auColours = true;
    // The theme toggle swaps Mocha for Latte — re-read on the next paint.
    try {
      new MutationObserver(function () {
        palette = null; mine = null;
        produced = Object.create(null);
        memo = Object.create(null);
      })
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

  /* ── 7 · The rail: reachable, measured, and it follows you ─────────── */

  /* 🔴 24 tabs do not fit any screen, and the first version simply let the
     surplus fall off the end: Control, Calibration and Sync sat at
     x=1282..1473 in a box that ended at 1310, with scrollLeft pinned at 0.
     Three tabs you could not reach at all.  Four things fix that, and all of
     them are needed:
       · the rail may use the whole window (CSS), which alone seats 24 tabs at
         1440px and wider;
       · a mouse wheel over the rail scrolls it sideways — without this a
         desktop mouse has no gesture for a horizontal scroller;
       · the ends fade only where there is actually more, so a full rail has
         crisp edges and a clipped one says so;
       · the buttons tighten when, and only when, the rail overflows. */

  function railSetup() {
    var nav = document.getElementById("bottom-nav");
    if (!nav || nav.__auRail) return;
    nav.__auRail = true;

    /* The loading pill above the rail is positioned from this. */
    function measure() {
      var h = Math.round(nav.getBoundingClientRect().height);
      if (h > 0) ROOT.style.setProperty("--au-rail-h", h + "px");
    }

    function edges() {
      /* 🔴 Tightening the buttons CHANGES scrollWidth, so the overflow has to
         be read again afterwards — the first version decided the end-fades
         from the measurement it took before shrinking them, and left a fade
         hanging off a rail that now fit exactly. */
      nav.toggleAttribute("data-dense", nav.scrollWidth - nav.clientWidth > 0);
      var over = nav.scrollWidth - nav.clientWidth;
      if (over <= 1) { nav.removeAttribute("data-of"); return; }
      var l = nav.scrollLeft > 2, r = nav.scrollLeft < over - 2;
      nav.setAttribute("data-of", l && r ? "both" : (l ? "l" : "r"));
    }

    var last = null;
    function scrollToActive() {
      var a = nav.querySelector(".nav-btn.active");
      if (!a || a === last) return;
      last = a;
      var want = a.offsetLeft - (nav.clientWidth - a.offsetWidth) / 2;
      want = Math.max(0, Math.min(nav.scrollWidth - nav.clientWidth, want));
      try {
        nav.scrollTo({ left: want, behavior: REDUCED ? "auto" : "smooth" });
      } catch (e) { nav.scrollLeft = want; }
    }

    /* A wheel over the rail scrolls it sideways.  Only claim the gesture when
       there is somewhere to go in that direction, so the page still scrolls
       when the rail is already at its end. */
    nav.addEventListener("wheel", function (ev) {
      var over = nav.scrollWidth - nav.clientWidth;
      if (over <= 1) return;
      var d = Math.abs(ev.deltaX) > Math.abs(ev.deltaY) ? ev.deltaX : ev.deltaY;
      if (!d) return;
      var at = nav.scrollLeft;
      if ((d < 0 && at <= 0) || (d > 0 && at >= over)) return;
      ev.preventDefault();
      nav.scrollLeft = at + d;
    }, { passive: false });

    nav.addEventListener("scroll", edges, { passive: true });
    window.addEventListener("resize", function () { measure(); edges(); });

    /* 🔴 Watching clicks is not enough: the dashboard restores the last tab
       from localStorage on load and the command palette switches panes without
       one, so the rail stayed put and the active tab sat off-screen.  Follow
       the `active` class itself, whoever moved it. */
    try {
      new MutationObserver(function () { scrollToActive(); edges(); })
        .observe(nav, { subtree: true, attributes: true, attributeFilter: ["class"] });
    } catch (e) {
      nav.addEventListener("click", function () { setTimeout(scrollToActive, 30); });
    }

    measure(); edges();
    setTimeout(function () { measure(); edges(); scrollToActive(); }, 250);
    setTimeout(function () { measure(); edges(); }, 1200);
  }

  /* ── 7b · Plotly, themed through Plotly ─────────────────────────────── */

  /* The Plots page draws with Plotly, which renders SVG and writes its colours
     as inline style attributes on the paths.  The chart colour hook only sees
     canvas, so the charts came out with the library's defaults — near-white
     grid lines (rgb(238,238,238)) straight across a dark page.  Fighting that
     with `!important` selectors breaks on the next redraw; handing Plotly the
     palette through relayout does not. */

  function themePlots() {
    var P = window.Plotly;
    if (!P || typeof P.relayout !== "function") return false;
    var plots = document.querySelectorAll(".js-plotly-plot");
    if (!plots.length) return false;

    var cs = getComputedStyle(ROOT);
    function tok(name, fb) {
      var v = (cs.getPropertyValue(name) || "").trim();
      return v || fb;
    }
    var ink = tok("--muted", "#9399b2");
    /* Take the hairline straight from the palette.  Building it with
       color-mix and resolving it through a probe element produced a near-black
       grid (rgb(1,1,1)) on the plots page; --border is already exactly this
       colour, needs no round trip, and Plotly splits its alpha into
       stroke-opacity by itself. */
    var grid = tok("--border", tok("--ctp-surface1", "#45475a"));
    var font = getComputedStyle(document.body).fontFamily;

    for (var i = 0; i < plots.length; i++) {
      var el = plots[i];
      var lay = el.layout || {};
      var patch = {
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        "font.color": ink,
        "font.family": font,
        "font.size": 11,
        "legend.bgcolor": "rgba(0,0,0,0)",
        "legend.bordercolor": "rgba(0,0,0,0)",
        "hoverlabel.bgcolor": tok("--card", "#313244"),
        "hoverlabel.bordercolor": tok("--border", "#45475a"),
        "hoverlabel.font.color": tok("--fg", "#cdd6f4"),
        "margin.t": 28, "margin.r": 14, "margin.b": 34, "margin.l": 48
      };
      for (var k in lay) {
        if (!/^[xy]axis\d*$/.test(k)) continue;
        patch[k + ".gridcolor"] = grid;
        patch[k + ".zerolinecolor"] = grid;
        patch[k + ".linecolor"] = grid;
        patch[k + ".tickcolor"] = grid;
      }
      try { P.relayout(el, patch); } catch (e) {}
    }
    return true;
  }

  function hookPlots() {
    /* 🔴 Gate on the PAGE, not on the plots being there yet.  Plotly renders
       after its data arrives, which is long after this file runs — checking
       for a chart at boot meant the theme was never applied on a real load,
       only in a test that happened to look later. */
    var isPlots = /\/plots(\/|$)/.test(location.pathname) ||
                  !!document.querySelector(".plot-group");
    if (!isPlots || window.__auPlots) return;
    window.__auPlots = true;

    /* 🔴 And it must not chase its own tail.  themePlots() calls
       Plotly.relayout, which rewrites the SVG, which fires the observer that
       called it — with a resize event thrown in for good measure.  Two guards:
       a busy flag while our own writes land, and a signature so the work only
       happens when something it depends on has actually changed. */
    var timer = 0, busy = false, sig = "";

    function run() {
      var plots = document.querySelectorAll(".js-plotly-plot");
      var s = plots.length + "|" + (ROOT.getAttribute("data-theme") || "") +
              "|" + window.innerWidth;
      if (s === sig) return;
      sig = s;
      busy = true;
      try {
        themePlots();
        // The skin lays the groups out as a grid; Plotly is responsive but
        // only reacts to a resize event, so tell it.
        try { window.dispatchEvent(new Event("resize")); } catch (e) {}
      } finally {
        setTimeout(function () { busy = false; }, 400);
      }
    }

    function later() {
      if (busy) return;
      clearTimeout(timer);
      timer = setTimeout(run, 140);
    }

    try {
      new MutationObserver(later).observe(document.body,
        { childList: true, subtree: true });
    } catch (e) {}
    try {
      new MutationObserver(function () { sig = ""; later(); }).observe(ROOT,
        { attributes: true, attributeFilter: ["data-theme"] });
    } catch (e) {}
    window.addEventListener("resize", function () { sig = ""; later(); });
    later();
    setTimeout(later, 1200);
    setTimeout(later, 3000);
  }

  /* ── 8 · Start ──────────────────────────────────────────────────────── */

  /* The Plots tab is an iframe.  Painting a second, independent aurora inside
     it stacks two grounds with different geometry: the traces do not line up
     across the boundary and the iframe's opaque background cuts a hard
     horizontal seam where its box ends.  An embedded page therefore keeps the
     skin's typography and card styling but draws no ground of its own — the
     parent's shows through. */
  var EMBEDDED = false;
  try { EMBEDDED = window.self !== window.top; } catch (e) { EMBEDDED = true; }

  function boot() {
    if (EMBEDDED) ROOT.setAttribute("data-au-embedded", "1");
    try { if (!EMBEDDED) mountCircuit(); } catch (e) {}
    hookInlineColours();
    takeOverTint();
    hookRenderLive();
    railSetup();
    hookPlots();
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
