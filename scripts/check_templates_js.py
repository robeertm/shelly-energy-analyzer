#!/usr/bin/env python3
"""Pre-commit sanity check for HTML template JavaScript.

Why this exists: v16.17.0 shipped a `/settings` page that was completely blank
because a single `\\"` escape inside a template-literal `${T(...)}` interpolation
broke the JS parser. The whole `<script>` block failed to parse, so the entire
settings UI never ran. Users were stuck with a white page, unable to reach the
in-app updater to fix it.

This script parses every `<script>` block in every template in
`src/shelly_analyzer/web/templates/*.html` using esprima and exits non-zero
on the first parse error. Run it before committing any change to a template
or any Python file that renders HTML with embedded JS (i.e. `webdash.py`).

Usage:
    python3 scripts/check_templates_js.py

Exit codes:
    0  All scripts parse cleanly
    1  One or more parse errors (details on stderr)
    2  esprima not installed — install with `pip install esprima`
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    import esprima
except ImportError:
    print("esprima not installed. Run: pip install esprima", file=sys.stderr)
    sys.exit(2)


REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_DIRS = [
    REPO_ROOT / "src" / "shelly_analyzer" / "web" / "templates",
]


def extract_scripts(html: str) -> list[tuple[int, str]]:
    """Return (start_line, script_body) for each non-empty <script> block."""
    results: list[tuple[int, str]] = []
    for m in re.finditer(r"<script[^>]*>(.*?)</script>", html, re.DOTALL):
        body = m.group(1)
        if not body.strip():
            continue
        pre = html[: m.start(1)]
        start_line = pre.count("\n") + 1
        results.append((start_line, body))
    return results


def check_file(path: Path) -> list[str]:
    """Return a list of human-readable error messages (empty = clean)."""
    errors: list[str] = []
    try:
        html = path.read_text(encoding="utf-8")
    except Exception as e:
        return [f"{path}: read failed: {e}"]
    scripts = extract_scripts(html)
    for idx, (start_line, body) in enumerate(scripts):
        try:
            esprima.parseScript(body)
        except Exception as e:
            # esprima.error_handler.Error has .lineNumber / .column / .description
            ln = getattr(e, "lineNumber", None)
            col = getattr(e, "column", None)
            desc = getattr(e, "description", str(e))
            if ln is not None:
                file_line = start_line + ln - 1
                errors.append(
                    f"{path}:{file_line}:{col} [script #{idx + 1}] {desc}"
                )
                lines = body.split("\n")
                lo = max(0, ln - 3)
                hi = min(len(lines), ln + 2)
                for i in range(lo, hi):
                    marker = ">>>" if (i + 1) == ln else "   "
                    snippet = lines[i][:200]
                    errors.append(
                        f"    {marker} {start_line + i:5d} | {snippet}"
                    )
            else:
                errors.append(f"{path}: [script #{idx + 1}] {desc}")
    return errors


def main() -> int:
    any_errors = False
    files_checked = 0
    scripts_checked = 0
    for tdir in TEMPLATE_DIRS:
        if not tdir.exists():
            continue
        for path in sorted(tdir.rglob("*.html")):
            files_checked += 1
            # Also count scripts in this file for the summary
            try:
                scripts_checked += len(extract_scripts(path.read_text(encoding="utf-8")))
            except Exception:
                pass
            errs = check_file(path)
            if errs:
                any_errors = True
                for e in errs:
                    print(e, file=sys.stderr)

    if any_errors:
        print("\n❌ JS template check FAILED. Fix the errors above before committing.", file=sys.stderr)
        print("   A parse error means the <script> block will not run, which leaves", file=sys.stderr)
        print("   the page blank for users.", file=sys.stderr)
        return 1
    print(f"✓ {files_checked} template(s), {scripts_checked} <script> block(s) parse cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
