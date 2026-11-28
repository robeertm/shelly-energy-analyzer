"""The updater's release-ZIP path: version checks, asset choice, and — above all — what an update must NOT touch.

An update replaces the program files of a running installation. If it ever
overwrites or deletes the user's data (measurements, settings, logs, the
virtualenv) the damage is done before anyone can look. Tests here:

* version comparison decides whether the "update available" hint shows at all;
* the asset picker takes the ZIP that matches the platform, never an unrelated file;
* ``updater_helper.main`` copies the new code over the old, but leaves ``data/``,
  ``config.json``, ``logs/``, ``.venv/`` and hidden folders exactly as they were;
* a ZIP with a single top-level folder is unpacked one level down, and a hostile
  ZIP that tries to leave the staging folder does not write outside it;
* a download/GitHub failure is reported as "not reachable", never raised into the page.
"""
import os
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer import updater_helper
from shelly_analyzer.services import updater


# -- versions ------------------------------------------------------------------

@pytest.mark.parametrize("tag,expected", [
    ("17.12.1", (17, 12, 1, 0)), ("v17.12.1", (17, 12, 1, 0)), (" v5.9.1.3 ", (5, 9, 1, 3)),
    ("", None), (None, None), ("latest", None), ("17.12", None), ("17.12.1-rc1", None), ("v1.2.3.4.5", None),
])
def test_parse_version(tag, expected):
    assert updater.parse_version(tag) == expected


def test_is_newer_compares_numbers_not_strings():
    assert updater.is_newer("17.12.0", "17.9.9")          # 12 > 9 numerically (a string compare says no)
    assert updater.is_newer("v17.12.1", "17.12.0")
    assert updater.is_newer("5.9.1.3", "5.9.1.2")
    assert updater.is_newer("5.9.2", "5.9.1.9")
    assert not updater.is_newer("17.12.1", "17.12.1")     # equal is not an update
    assert not updater.is_newer("17.9.9", "17.12.0")      # an older release is not an update
    assert not updater.is_newer("5.9.1", "5.9.1.0")       # 3- and 4-part forms of the same version are equal


def test_unparseable_versions_never_offer_an_update():
    assert not updater.is_newer("nightly", "17.12.0")
    assert not updater.is_newer("17.12.0", "dev")
    assert not updater.is_newer("", "")


# -- asset choice ----------------------------------------------------------------

def _rel(*names):
    return {"assets": [{"name": n, "browser_download_url": f"https://x/{n}"} for n in names]}


def test_asset_matching_the_platform_wins_over_other_zips():
    rel = _rel("app_windows.zip", "app_linux.zip", "app_macos.zip", "notes.txt")
    assert updater._pick_asset(rel, "linux") == ("https://x/app_linux.zip", "app_linux.zip")
    assert updater._pick_asset(rel, "macos")[1] == "app_macos.zip"


def test_asset_falls_back_to_any_zip_but_never_to_a_non_zip():
    assert updater._pick_asset(_rel("source.ZIP"), "linux")[1] == "source.ZIP"
    assert updater._pick_asset(_rel("notes.txt", "app.tar.gz"), "linux") == (None, None)
    assert updater._pick_asset({}, "linux") == (None, None)
    # An asset without a download URL is not usable.
    assert updater._pick_asset({"assets": [{"name": "a_linux.zip", "browser_download_url": ""}]}, "linux") == (None, None)


def test_platform_suffix(monkeypatch):
    for plat, want in (("win32", "windows"), ("darwin", "macos"), ("linux", "linux"), ("freebsd13", "linux")):
        monkeypatch.setattr(sys, "platform", plat)
        assert updater.detect_platform_suffix() == want


# -- GitHub answers ----------------------------------------------------------------

def test_check_latest_release_reports_tag_and_asset(monkeypatch):
    monkeypatch.setattr(updater, "detect_platform_suffix", lambda: "linux")
    monkeypatch.setattr(updater, "_http_get_json",
                        lambda url, timeout_s=3.0: {"tag_name": "v17.12.2", **_rel("a_linux.zip")})
    info = updater.check_latest_release("owner/repo")
    assert info.reachable and info.latest_tag == "v17.12.2" and info.asset_name == "a_linux.zip"


def test_check_latest_release_without_zip_still_names_the_tag(monkeypatch):
    monkeypatch.setattr(updater, "_http_get_json", lambda url, timeout_s=3.0: {"tag_name": "v1.0.0", "assets": []})
    info = updater.check_latest_release("owner/repo")
    assert info.reachable and info.latest_tag == "v1.0.0" and info.asset_url is None


def test_check_latest_release_without_tag_or_network_does_not_raise(monkeypatch):
    monkeypatch.setattr(updater, "_http_get_json", lambda url, timeout_s=3.0: {})
    assert updater.check_latest_release("o/r").reachable is False

    def boom(url, timeout_s=3.0):
        raise OSError("offline")
    monkeypatch.setattr(updater, "_http_get_json", boom)
    info = updater.check_latest_release("o/r")
    assert info.reachable is False and "offline" in info.status and info.latest_tag is None


def test_fetch_releases_skips_untagged_entries_and_honours_the_limit(monkeypatch):
    monkeypatch.setattr(updater, "detect_platform_suffix", lambda: "linux")
    monkeypatch.setattr(updater, "_http_get_json", lambda url, timeout_s=3.0: [
        {"tag_name": "v3.0.0", **_rel("x_linux.zip")}, {"tag_name": ""}, {"tag_name": "v2.0.0"}, {"tag_name": "v1.0.0"}])
    got = updater.fetch_releases("o/r", limit=3)
    assert [r.tag for r in got] == ["v3.0.0", "v2.0.0"]      # 4th is cut by the limit, empty tag is dropped
    assert got[0].asset_name == "x_linux.zip" and got[1].asset_url is None


# -- the file swap -----------------------------------------------------------------

def _make_install(root: Path) -> Path:
    app = root / "app"
    (app / "data").mkdir(parents=True)
    (app / "data" / "energy.db").write_bytes(b"MEASUREMENTS-v1")
    (app / "logs").mkdir()
    (app / "logs" / "app.log").write_text("old log")
    (app / ".venv" / "bin").mkdir(parents=True)
    (app / ".venv" / "bin" / "python").write_text("venv")
    (app / ".git").mkdir()
    (app / ".git" / "HEAD").write_text("ref: refs/heads/main")
    (app / "config.json").write_text('{"user": "settings"}')
    (app / "src" / "pkg").mkdir(parents=True)
    (app / "src" / "pkg" / "old_only.py").write_text("stale module")
    (app / "src" / "pkg" / "mod.py").write_text("VERSION = 1")
    (app / "start.sh").write_text("#!/bin/sh\necho start")
    return app


def _make_staging(root: Path) -> Path:
    st = root / "sea_update_x" / "release"
    (st / "src" / "pkg").mkdir(parents=True)
    (st / "src" / "pkg" / "mod.py").write_text("VERSION = 2")
    (st / "data").mkdir()
    (st / "data" / "energy.db").write_bytes(b"EMPTY-FROM-ZIP")     # a release must never replace the user's data
    (st / "logs").mkdir()
    (st / "logs" / "app.log").write_text("zip log")
    (st / "config.json").write_text('{"user": "defaults"}')
    (st / ".venv").mkdir()
    (st / ".venv" / "marker").write_text("zip venv")
    (st / ".github").mkdir()
    (st / ".github" / "ci.yml").write_text("x")
    (st / ".hidden_file").write_text("h")
    (st / ".DS_Store").write_text("junk")
    (st / "stale.pyc").write_bytes(b"\0")
    (st / "docs").mkdir()
    (st / "docs" / "a.md").write_text("docs")
    (st / "start.sh").write_text("#!/bin/sh\necho NEW start")
    (st / ".gitignore").write_text("*.pyc")
    return st


def _run_main(monkeypatch, app, st, extra=()):
    restarted = []
    monkeypatch.setattr(updater_helper, "_restart_app", lambda restart, app_dir, log: restarted.append(str(restart)))
    monkeypatch.setattr(sys, "argv", ["updater_helper.py", "--app-dir", str(app), "--staging-dir", str(st),
                                      "--restart", "start.sh", "--update-deps", "0", *extra])
    rc = updater_helper.main()
    return rc, restarted


def test_zip_update_replaces_code_and_leaves_user_data_alone(tmp_path, monkeypatch):
    app, st = _make_install(tmp_path), _make_staging(tmp_path)
    rc, restarted = _run_main(monkeypatch, app, st)
    assert rc == 0 and restarted == [str((app / "start.sh").resolve())]
    # New code is in, old files of replaced folders are gone.
    assert (app / "src" / "pkg" / "mod.py").read_text() == "VERSION = 2"
    assert not (app / "src" / "pkg" / "old_only.py").exists()
    assert "NEW start" in (app / "start.sh").read_text()
    assert (app / ".gitignore").read_text() == "*.pyc"
    # The user's data is byte-for-byte untouched.
    assert (app / "data" / "energy.db").read_bytes() == b"MEASUREMENTS-v1"
    assert (app / "config.json").read_text() == '{"user": "settings"}'
    assert (app / "logs" / "app.log").read_text().startswith("old log")
    assert (app / ".venv" / "bin" / "python").read_text() == "venv" and not (app / ".venv" / "marker").exists()
    assert (app / ".git" / "HEAD").exists()
    # Junk and hidden entries from the archive are not copied in.
    for name in (".github", ".hidden_file", ".DS_Store", "stale.pyc", "docs"):
        assert not (app / name).exists(), name
    # The unpacked archive is cleaned up (the sea_update_* parent, not just the inner folder).
    assert not st.parent.exists()


def test_zip_update_without_staging_changes_nothing(tmp_path, monkeypatch):
    app = _make_install(tmp_path)
    monkeypatch.setattr(updater_helper, "_restart_app", lambda *a: pytest.fail("must not restart"))
    monkeypatch.setattr(sys, "argv", ["updater_helper.py", "--app-dir", str(app), "--restart", "start.sh"])
    assert updater_helper.main() == 2
    assert (app / "src" / "pkg" / "mod.py").read_text() == "VERSION = 1"


def test_failed_restart_is_reported_but_the_new_files_stay(tmp_path, monkeypatch):
    app, st = _make_install(tmp_path), _make_staging(tmp_path)

    def broken(restart, app_dir, log):
        raise OSError("cannot exec")
    monkeypatch.setattr(updater_helper, "_restart_app", broken)
    monkeypatch.setattr(sys, "argv", ["updater_helper.py", "--app-dir", str(app), "--staging-dir", str(st),
                                      "--restart", "start.sh", "--update-deps", "0"])
    assert updater_helper.main() == 2
    assert (app / "src" / "pkg" / "mod.py").read_text() == "VERSION = 2"
    assert (app / "data" / "energy.db").read_bytes() == b"MEASUREMENTS-v1"


def test_stale_bytecode_is_cleared(tmp_path, monkeypatch):
    app, st = _make_install(tmp_path), _make_staging(tmp_path)
    cache = app / "src" / "pkg" / "__pycache__"
    cache.mkdir()
    (cache / "mod.cpython-312.pyc").write_bytes(b"old bytecode")
    _run_main(monkeypatch, app, st)
    assert not list(app.rglob("__pycache__"))


def test_wait_for_pid_returns_at_once_for_the_in_place_handoff_and_for_dead_pids():
    import time
    t0 = time.time()
    updater_helper._wait_for_pid(0)
    updater_helper._wait_for_pid(-5)
    assert time.time() - t0 < 0.5
    # A PID that does not exist: os.kill raises -> treated as "already gone".
    t0 = time.time()
    updater_helper._wait_for_pid(2 ** 22 + 12345, timeout_s=5)
    assert time.time() - t0 < 2.0


def test_wait_for_pid_waits_for_a_running_process_then_gives_up_after_the_limit():
    import time
    t0 = time.time()
    updater_helper._wait_for_pid(os.getpid(), timeout_s=0.6)         # we never exit: the limit ends the wait
    assert 0.5 <= time.time() - t0 < 3.0


def test_copy_tree_replaces_an_existing_folder_completely(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    (src / "a").mkdir(parents=True)
    (src / "a" / "new.txt").write_text("n")
    (dst / "a").mkdir(parents=True)
    (dst / "a" / "old.txt").write_text("o")
    updater_helper._copy_tree(src, dst)
    assert (dst / "a" / "new.txt").exists() and not (dst / "a" / "old.txt").exists()


# -- unpacking the download -----------------------------------------------------------

def _zip(path: Path, entries: dict):
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return path


def test_unpack_descends_into_a_single_top_level_folder(tmp_path):
    from shelly_analyzer.web.blueprints import updates
    z = _zip(tmp_path / "r.zip", {"release-1.0/a.txt": "a", "release-1.0/sub/b.txt": "b"})
    st = updates._unzip_to_staging(z)
    try:
        assert st.name == "release-1.0" and (st / "a.txt").read_text() == "a"
    finally:
        import shutil
        shutil.rmtree(st.parent, ignore_errors=True)


def test_unpack_flat_archive_stays_at_the_top(tmp_path):
    from shelly_analyzer.web.blueprints import updates
    z = _zip(tmp_path / "r.zip", {"a.txt": "a", "b.txt": "b"})
    st = updates._unzip_to_staging(z)
    try:
        assert st.name.startswith("sea_update_") and (st / "b.txt").exists()
    finally:
        import shutil
        shutil.rmtree(st, ignore_errors=True)


def test_unpack_does_not_write_outside_the_staging_folder(tmp_path):
    from shelly_analyzer.web.blueprints import updates
    z = _zip(tmp_path / "evil.zip", {"../../escaped.txt": "x", "/abs_escaped.txt": "y", "ok.txt": "z"})
    st = updates._unzip_to_staging(z)
    try:
        assert (st / "ok.txt").exists()
        assert not (tmp_path / "escaped.txt").exists()
        assert not Path("/abs_escaped.txt").exists()
        assert not (st.parent / "escaped.txt").exists()
        assert not (st.parent.parent / "escaped.txt").exists()
    finally:
        import shutil
        shutil.rmtree(st, ignore_errors=True)
