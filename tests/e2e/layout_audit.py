"""Layout audit for narrow and wide screens: scroll, clipping, touch targets, inputs, pictures.

``audit_pane(page)`` runs in the page and returns findings for the active pane.  Used by
test_layout_390.py (regression guard at 390 px) and by hand for the full walk-through.
"""
from __future__ import annotations

AUDIT_JS = r"""
(() => {
  const out = {hscroll: null, clipped: [], small_targets: [], small_inputs: [], stamps: []};
  const vw = document.documentElement.clientWidth;
  const de = document.documentElement;
  if (de.scrollWidth > vw + 1) out.hscroll = de.scrollWidth - vw;
  const desc = e => (e.tagName.toLowerCase() + (e.id ? '#' + e.id : '') +
    (e.className && typeof e.className === 'string' ? '.' + e.className.trim().split(/\s+/).slice(0, 2).join('.') : '') +
    ' "' + (e.innerText || e.value || e.getAttribute('aria-label') || e.title || '').trim().slice(0, 30).replace(/\s+/g, ' ') + '"');
  const visible = e => { const r = e.getBoundingClientRect(); const cs = getComputedStyle(e);
    return r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && cs.display !== 'none' && e.offsetParent !== null; };
  const insideScroller = e => { for (let p = e.parentElement; p && p !== document.body; p = p.parentElement) {
      const ox = getComputedStyle(p).overflowX; if ((ox === 'auto' || ox === 'scroll') && p.scrollWidth > p.clientWidth) return true; } return false; };
  const pane = document.querySelector('.pane.active') || document.body;
  for (const e of pane.querySelectorAll('*')) {
    if (!visible(e)) continue;
    const r = e.getBoundingClientRect();
    if (r.right > vw + 2 && !insideScroller(e) && !e.closest('svg,canvas,.js-plotly-plot,.modebar'))
      out.clipped.push(desc(e) + ' right=' + Math.round(r.right));
  }
  const nav = document.body;
  for (const e of document.querySelectorAll('button, a[href], select, input:not([type=hidden]), [role=button], .nav-btn, summary')) {
    if (!visible(e)) continue;
    const r = e.getBoundingClientRect();
    const tag = e.tagName;
    if (tag === 'INPUT' && (e.type === 'checkbox' || e.type === 'radio')) {
      const l = e.closest('label'); const rr = l ? l.getBoundingClientRect() : r;
      if (Math.min(rr.width, rr.height) < 44 && Math.max(rr.height) < 44) out.small_targets.push(desc(e) + ' ' + Math.round(rr.width) + 'x' + Math.round(rr.height));
    } else if (r.height < 44 || r.width < 44) {
      // inline text links inside running text are exempt
      if (tag === 'A' && getComputedStyle(e).display === 'inline') continue;
      out.small_targets.push(desc(e) + ' ' + Math.round(r.width) + 'x' + Math.round(r.height));
    }
    if ((tag === 'INPUT' && !['checkbox', 'radio', 'range', 'button', 'submit', 'color', 'file'].includes(e.type)) || tag === 'SELECT' || tag === 'TEXTAREA') {
      const fs = parseFloat(getComputedStyle(e).fontSize);
      if (fs < 16) out.small_inputs.push(desc(e) + ' ' + fs + 'px');
    }
  }
  for (const img of pane.querySelectorAll('img')) {
    if (!visible(img)) continue;
    const r = img.getBoundingClientRect();
    if (img.naturalWidth && r.width < 60 && r.height < 60 && img.naturalWidth > 200) out.stamps.push(desc(img) + ' ' + Math.round(r.width) + 'x' + Math.round(r.height));
  }
  return out;
})()
"""


def audit_pane(page):
    return page.evaluate(AUDIT_JS)


# Counts requests in flight and records the last DOM change.  Install before the first navigation
# (``page.add_init_script(TRACK_JS)``); ``wait_settled`` reads it.
TRACK_JS = r"""
(() => {
  if (window.__seaTrack) return;
  const t = window.__seaTrack = {inflight: 0, lastMutation: performance.now()};
  const of = window.fetch;
  if (of) window.fetch = function () {
    t.inflight++;
    const done = () => { t.inflight = Math.max(0, t.inflight - 1); t.lastMutation = performance.now(); };
    return of.apply(this, arguments).then(r => { done(); return r; }, e => { done(); throw e; });
  };
  const send = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function () {
    t.inflight++;
    this.addEventListener('loadend', () => { t.inflight = Math.max(0, t.inflight - 1); t.lastMutation = performance.now(); });
    return send.apply(this, arguments);
  };
  const start = () => new MutationObserver(() => { t.lastMutation = performance.now(); })
    .observe(document.documentElement, {subtree: true, childList: true, attributes: true, characterData: true});
  if (document.documentElement) start(); else document.addEventListener('DOMContentLoaded', start);
})()
"""

_SETTLED_JS = """
(quiet) => {
  const t = window.__seaTrack;
  if (!t) return false;
  return t.inflight === 0 && (performance.now() - t.lastMutation) >= quiet;
}
"""


def wait_settled(page, quiet_ms: int = 700, timeout_ms: int = 10000) -> None:
    """Wait until the page is quiet: no fetch/XHR in flight, no DOM change for ``quiet_ms``, fonts ready.

    Replaces fixed sleeps: a busy machine takes longer to fill a pane (device lists, charts), a quiet one
    does not wait for nothing.  Needs ``TRACK_JS`` as init script; without it, falls back to a short sleep.
    ``timeout_ms`` is a cap against a page that never settles (a live ticker), not an expectation.
    """
    try:
        page.evaluate("document.fonts ? document.fonts.ready.then(() => true) : true")
        if page.evaluate("!!window.__seaTrack"):
            page.wait_for_function(_SETTLED_JS, arg=quiet_ms, timeout=timeout_ms, polling=100)
            return
    except Exception:  # navigation in between or a cap hit: audit what is there
        return
    page.wait_for_timeout(quiet_ms)
