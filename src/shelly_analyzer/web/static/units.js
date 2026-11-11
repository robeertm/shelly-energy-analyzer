/* Unit localisation, one place for every page.
 *
 * The pages build many strings in the browser ("12.3 kWh", "230 V", "24h",
 * "g/kWh", axis labels on canvases and in Plotly).  Instead of threading a
 * helper through each of them, the unit tokens are swapped here, driven by the
 * translation keys web.unit.<token> of the UI language (I18N on the page).
 * A language whose table equals the Latin token (most of them) costs nothing:
 * nothing is patched and no observer runs.
 *
 * Rules, so ordinary words are left alone:
 *   - kWh, kW, MWh, Wh, kWp, kg  anywhere, as whole tokens
 *   - W V A VA VAR Hz g ct h d min s  only straight after a number ("12 W", "24h")
 *     or after a slash ("ct/kWh", "kWh/h"), or as the only text of an element,
 *     or in brackets "(kWh)".
 */
(function () {
  'use strict';
  var TOKENS = ['kWh', 'kWp', 'MWh', 'kW', 'Wh', 'W', 'VAR', 'VA', 'V', 'A', 'Hz', 'kg', 'g', 'ct', 'h', 'd', 'min', 's', 'GB', 'MB', 'KB', 'B'];
  var ANY = ['kWh', 'kWp', 'MWh', 'kW', 'Wh', 'kg'];
  var map = null;

  function build() {
    var src = null;
    try { src = (typeof I18N !== 'undefined') ? I18N : null; } catch (e) { src = null; }
    if (!src) return null;
    var m = {}, diff = false;
    TOKENS.forEach(function (tok) {
      var v = src['web.unit.' + tok.toLowerCase()];
      if (v && v !== tok) { m[tok] = v; diff = true; }
    });
    return diff ? m : {};
  }

  function esc(s) { return s.replace(/[.*+?^${}()|[\]\\\/]/g, '\\$&'); }
  var reAny, reNum, reSlash, reParen, rePer;
  function compile() {
    var any = ANY.map(esc).join('|');
    var all = TOKENS.slice().sort(function (a, b) { return b.length - a.length; }).map(esc).join('|');
    reAny = new RegExp('(^|[^A-Za-z])(' + any + ')(?![A-Za-z])', 'g');
    reNum = new RegExp('([0-9])(\\s?)(' + all + ')(?![A-Za-z0-9])', 'g');
    reSlash = new RegExp('(\\/)(' + all + ')(?![A-Za-z0-9])', 'g');
    rePer = /(^|[^A-Za-z])(g|kg|ct|W|kW|Wh|GB|MB|KB|B)(\/)(kWh|kW|kWp|h|d|s)(?![A-Za-z])/g;
    reParen = new RegExp('(\\()(' + all + ')(\\))', 'g');
  }

  function loc(s, whole) {
    if (typeof s !== 'string' || !s) return s;
    if (map === null) { map = build(); if (map === null) return s; compile(); }
    if (!map || s.length > 4000) return s;
    var r = s;
    if (whole) {
      var tr = r.trim();
      if (map[tr] && (tr.length > 1 || whole === 2 || tr === 'W' || tr === 'V')) return r.replace(tr, map[tr]);
    }
    r = r.replace(rePer, function (m0, a, u, sl, v) { return a + (map[u] || u) + sl + (map[v] || v); });
    r = r.replace(reAny, function (m0, a, u) { return a + (map[u] || u); });
    r = r.replace(reNum, function (m0, d, sp, u) { return d + sp + (map[u] || u); });
    r = r.replace(reSlash, function (m0, a, u) { return a + (map[u] || u); });
    r = r.replace(reParen, function (m0, a, u, c) { return a + (map[u] || u) + c; });
    return r;
  }

  window.seaUnits = { loc: loc };

  function ready() {
    if (map === null) { map = build(); if (map === null) return false; compile(); }
    return Object.keys(map).length > 0;
  }

  // Canvas charts draw their labels with fillText; keep measureText in step.
  function patchCanvas() {
    var P = window.CanvasRenderingContext2D && CanvasRenderingContext2D.prototype;
    if (!P || P.__seaUnits) return;
    P.__seaUnits = true;
    ['fillText', 'strokeText'].forEach(function (n) {
      var o = P[n];
      P[n] = function (txt) { var a = arguments; if (typeof txt === 'string') { a = Array.prototype.slice.call(arguments); a[0] = loc(txt); } return o.apply(this, a); };
    });
    var m = P.measureText;
    P.measureText = function (txt) { return m.call(this, typeof txt === 'string' ? loc(txt) : txt); };
  }

  var SKIP = { SCRIPT: 1, STYLE: 1, TEXTAREA: 1, INPUT: 1, NOSCRIPT: 1 };
  function fixNode(n) {
    if (n.nodeType === 3) {
      var p = n.parentNode;
      if (p && SKIP[p.nodeName]) return;
      var v = n.nodeValue, w = loc(v, p && p.childNodes.length === 1 ? (p.nodeName === 'OPTION' ? 2 : 1) : 0);
      if (w !== v) n.nodeValue = w;
    } else if (n.nodeType === 1 && !SKIP[n.nodeName]) {
      var kids = n.childNodes;
      for (var i = 0; i < kids.length; i++) fixNode(kids[i]);
      ['title', 'placeholder', 'aria-label'].forEach(function (a) {
        var x = n.getAttribute && n.getAttribute(a);
        if (x) { var y = loc(x); if (y !== x) n.setAttribute(a, y); }
      });
    }
  }

  function start() {
    if (!ready()) return;
    patchCanvas();
    var pending = [], queued = false;
    function flush() {
      queued = false;
      var list = pending; pending = [];
      for (var i = 0; i < list.length; i++) { try { fixNode(list[i]); } catch (e) {} }
    }
    var mo = new MutationObserver(function (recs) {
      for (var i = 0; i < recs.length; i++) {
        var r = recs[i];
        if (r.type === 'characterData') pending.push(r.target);
        else for (var j = 0; j < r.addedNodes.length; j++) pending.push(r.addedNodes[j]);
      }
      if (!queued) { queued = true; (window.requestAnimationFrame || setTimeout)(flush); }
    });
    var root = document.documentElement;
    fixNode(root);
    mo.observe(root, { childList: true, subtree: true, characterData: true });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
