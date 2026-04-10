# Contributing

Thanks for considering a contribution! Shelly Energy Analyzer is an
opinionated single-developer project, but good bug reports, small pull
requests and translation fixes are very welcome.

## Ways to help (no coding required)

- 🌍 **Translations** — all strings live in a single file: [`src/shelly_analyzer/i18n.py`](src/shelly_analyzer/i18n.py). The project ships with 9 languages; DE / EN / ES are maintained manually, the rest (FR / PT / IT / PL / CS / RU) fall back to English. If you spot an untranslated or awkward string, open a PR adding the key in the language block you care about.
- 📸 **Screenshots** — if you have a bigger / nicer setup (more devices, solar, battery, long-running data) open an issue; a fresh screenshot set is always appreciated.
- 📝 **Documentation** — README improvements, typo fixes, clearer quickstart instructions are all welcome.
- 🧪 **Bug reports** — a concrete reproduction is worth a dozen vague complaints. See the issue template.

## Setting up a dev environment

```bash
git clone https://github.com/robeertm/shelly-energy-analyzer.git
cd shelly-energy-analyzer
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt

# Run
python -m shelly_analyzer

# Or with Demo Mode (no real Shellys needed)
#   → toggle it in Settings → Advanced after first launch
```

Python 3.10+ is required, 3.11 or newer is recommended.

## Project layout

```
src/shelly_analyzer/
  __main__.py              Entry point (Flask app boot + SSL + background services)
  web/
    __init__.py            Flask factory (create_app)
    app_context.py         AppState singleton (config, storage, services)
    background.py          BackgroundServiceManager (live poller, scheduler, ...)
    action_dispatch.py     API action handlers
    blueprints/            Flask blueprints (api_state, api_data, settings, …)
    templates/             Jinja2 templates (dashboard.html, settings.html, …)
    static/                JS + CSS + plotly.min.js
  services/                Stateless integrations (entsoe, spot_price, mqtt, …)
  io/                      Config, SQLite storage, Shelly HTTP client
  i18n.py                  Single-file translations for 9 languages
pyproject.toml             Package metadata (version lives here + __init__.py)
CHANGELOG.md               Every release has an entry
```

## Running what you've changed

There's no test suite yet — `python -m py_compile` on touched files + a
manual smoke test in the browser is the standard bar. If you add a tricky
piece of logic, a small `unittest` script next to the file is welcome but
not required.

```bash
# Syntax check
python -m py_compile src/shelly_analyzer/services/your_file.py

# Smoke test
python -m shelly_analyzer
# → open https://localhost:8765 and verify your change
```

## Versioning

Every change that lands on `main` bumps the patch version:

1. `src/shelly_analyzer/__init__.py` → `__version__`
2. `pyproject.toml` → `version`
3. `CHANGELOG.md` → new entry at the top describing the change

The CI workflow builds and publishes a GitHub Release automatically when
you push a tag matching `v*`:

```bash
git tag v16.13.99
git push origin v16.13.99
```

## Pull request expectations

- **One topic per PR** — separate unrelated changes.
- **Write why, not what** in the commit message. The diff already shows what.
- **Keep the commit history readable** — squash noisy fixup commits before
  opening the PR (or use `Squash and merge` on GitHub).
- **No secrets** in commits or screenshots — double-check for API tokens,
  Telegram bot tokens, SMTP passwords, etc.

## License

By contributing you agree that your contribution will be released under the
same proprietary-but-free-to-use terms as the rest of the project, as
described in [`LICENSE`](LICENSE). You retain copyright on your contribution.
