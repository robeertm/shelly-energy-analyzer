# End-to-end walkthrough

Opens every page of the web dashboard the way a customer would and reports anything a
customer would notice: console errors, failed requests, `NaN` / `undefined` / raw
translation keys in visible text, the Plots error banner, blank tabs, server tracebacks.

* `installations.py` writes eight kinds of installation (plugs only, meter + plugs, Gen1 3EM,
  single-phase meter, fresh install without data, PV + feed-in, TOU tariff in another time
  zone, tenant) into temp directories. Devices use documentation-only addresses; the server
  gets a dead proxy so nothing leaves the machine.
* `walkthrough.py` drives Chromium (and WebKit when it starts) over every pane, every Plots
  select combination, control, settings and setup, in 9 languages, desktop and phone size,
  light and dark. One extra pass runs installation `a` behind a Home Assistant style ingress
  path prefix.
* `api_tour.py` calls every GET route with parameter variants (unknown / renamed / deleted
  device, DST days, other time zones).
* `test_walkthrough.py` runs the quick mode of both (needs `SEA_E2E=1`).
* `write_load.py` measures what the server writes to disk with eight simulated plugs at a 1 s
  poll; `test_write_load_budget.py` holds it under 0.5 GB a day.
* `perf_bench.py` measures start-up time, resident memory and CPU of the same installation
  at `nice -n 19` on one core (`--history-days 30` for a longer history, `--profile out.txt`
  for py-spy stacks); `test_perf_budget.py` holds them under the limits at the top of the
  file. Limits are CPU time, not wall time, so a busy machine does not turn them red.

```
pip install ".[charts]" pytest playwright && playwright install chromium
python tests/e2e/walkthrough.py --report out.json            # full run, about 25 min
python tests/e2e/walkthrough.py --quick --report out.json    # about 5 min
python tests/e2e/walkthrough.py --langs de,en --installations a,b --browsers chromium
python tests/e2e/api_tour.py --report api.json
SEA_E2E=1 python -m pytest tests/e2e -q
python -m tests.e2e.perf_bench --minutes 5 --history-days 30
```

Chromium is taken from `SEA_CHROMIUM`, else the system `chromium`, else Playwright's own.
Findings carry a severity: high = crash, error banner, 5xx, blank tab; medium = wrong or
misleading number, raw key, `NaN`; low = cosmetic or probably untranslated.
