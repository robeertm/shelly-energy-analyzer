"""A git clone of the repository is updated through git (17.10.3).

Before 17.10.3 the in-app updater copied the release ZIP over the install and
skipped ``.git`` (``EXCLUDE_NAMES``). On a ``git clone`` that left git on the
old commit while the files moved on: the app said 17.10.1, ``git status``
showed ~30 modified files on v17.8.3, and a ``git pull`` ran into conflicts.

Now the updater recognises a clone of the configured repository, fetches the
tags, checks that the tag carries the version, saves every local change as a
patch (plus untracked files), switches to the tag, probes the new code and
goes back if the probe fails. ZIP, Docker and add-on installs keep their paths.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from flask import Flask  # noqa: E402

from shelly_analyzer import updater_helper as uh  # noqa: E402
from shelly_analyzer.web.blueprints import updates as updmod  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")

REPO_URL = "https://github.com/owner/repo.git"


def run(cwd, *args):
    return subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True,
                          text=True).stdout.strip()


@pytest.fixture(autouse=True)
def git_env(monkeypatch, tmp_path):
    for k, v in {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
                 "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
                 "GIT_CONFIG_GLOBAL": str(tmp_path / "gitconfig"),
                 "GIT_CONFIG_NOSYSTEM": "1"}.items():
        monkeypatch.setenv(k, v)
    (tmp_path / "gitconfig").write_text("[init]\n\tdefaultBranch = main\n")
    for k in ("SUPERVISOR_TOKEN", "HASSIO_TOKEN", "SEA_CONTAINER", "SEA_APP_DIR"):
        monkeypatch.delenv(k, raising=False)


def _write_release(root: Path, version: str, broken: bool = False, init_version: str = ""):
    pkg = root / "src" / "shelly_analyzer"
    (pkg / "web").mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text(f'__version__ = "{init_version or version}"\n')
    (pkg / "web" / "__init__.py").write_text(
        "raise RuntimeError('broken release')\n" if broken else f"PAGE = 'web {version}'\n")
    (pkg / "core.py").write_text(f"VALUE = '{version}'\n" + "\n".join(f"# line {i}" for i in range(20)) + "\n")
    if version >= "1.1.0":
        (pkg / f"new_in_{version.replace('.', '_')}.py").write_text("X = 1\n")
    (root / ".gitignore").write_text("config.json\ndata/\nlogs/\n.venv/\n__pycache__/\n")


@pytest.fixture
def origin(tmp_path):
    """A bare 'GitHub' with v1.0.0, v1.1.0, v1.2.0, a mistagged v1.2.5 and a
    broken v1.3.0."""
    work = tmp_path / "upstream"
    work.mkdir()
    run(work, "init", "-q")
    for v, kw in (("1.0.0", {}), ("1.1.0", {}), ("1.2.0", {}),
                  ("1.2.5", {"init_version": "1.2.4"}), ("1.3.0", {"broken": True})):
        _write_release(work, v, **kw)
        run(work, "add", "-A")
        run(work, "commit", "-q", "-m", v)
        run(work, "tag", f"v{v}")
    bare = tmp_path / "origin.git"
    run(tmp_path, "clone", "-q", "--bare", str(work), str(bare))
    return bare


@pytest.fixture
def clone(tmp_path, origin):
    """An install made with `git clone` on v1.0.0, remote URL = the public repo
    (fetches go to the local bare repo through url.insteadOf)."""
    app = tmp_path / "app"
    run(tmp_path, "clone", "-q", str(origin), str(app))
    run(app, "remote", "set-url", "origin", REPO_URL)
    run(app, "config", f"url.{origin}.insteadOf", REPO_URL)
    run(app, "checkout", "-q", "main")
    run(app, "reset", "-q", "--hard", "v1.0.0")
    (app / "config.json").write_text('{"secret": "keep me"}')
    (app / "data").mkdir()
    (app / "data" / "energy.sqlite").write_bytes(b"rows")
    return app


def logger():
    lines = []

    def log(msg):
        lines.append(msg)
    log.lines = lines
    return log


def head_tag(app):
    return run(app, "describe", "--tags", "--exact-match", "HEAD")


def tracked_changes(app):
    return run(app, "status", "--porcelain", "--untracked-files=no")


# ── recognising a clone ──────────────────────────────────────────────────
@pytest.mark.parametrize("url, slug", [
    ("https://github.com/Owner/Repo.git", "owner/repo"),
    ("https://github.com/owner/repo", "owner/repo"),
    ("https://x-token@github.com/owner/repo.git/", "owner/repo"),
    ("git@github.com:owner/repo.git", "owner/repo"),
    ("ssh://git@github.com/owner/repo.git", "owner/repo"),
    ("owner/repo", "owner/repo"),
    ("https://gitlab.example/owner/repo.git", ""),
    ("", ""),
])
def test_normalize_repo(url, slug):
    assert uh.normalize_repo(url) == slug


def test_clone_of_the_repo_is_recognised(clone):
    assert uh.git_checkout_remote(clone, "owner/repo") == "origin"
    assert uh.git_checkout_remote(clone, "https://github.com/OWNER/repo") == "origin"


def test_fork_or_plain_folder_keeps_the_zip_path(clone, tmp_path):
    assert uh.git_checkout_remote(clone, "someone-else/repo") is None
    plain = tmp_path / "zip-install"
    plain.mkdir()
    assert uh.git_checkout_remote(plain, "owner/repo") is None
    sub = clone / "src"
    assert uh.git_checkout_remote(sub, "owner/repo") is None, "only the top of the clone"


# ── checking before anything changes ─────────────────────────────────────
def test_prepare_fetches_and_checks_the_tag(clone):
    sha = uh.prepare_git_update(clone, "origin", "v1.2.0")
    assert sha == run(clone, "rev-parse", "v1.2.0^{commit}")
    assert head_tag(clone) == "v1.0.0", "prepare must not touch the working tree"


@pytest.mark.parametrize("tag, why", [
    ("v9.9.9", "not found"),
    ("v1.2.5", "carries version 1.2.4"),
    ("--upload-pack=x", "not a release tag"),
    ("main", "not a release tag"),
])
def test_prepare_refuses(clone, tag, why):
    with pytest.raises(uh.GitUpdateError, match=why):
        uh.prepare_git_update(clone, "origin", tag)
    assert head_tag(clone) == "v1.0.0"


# ── the update itself ────────────────────────────────────────────────────
def test_clean_clone_ends_exactly_on_the_tag(clone):
    log = logger()
    sha = uh.prepare_git_update(clone, "origin", "v1.2.0")
    assert uh.apply_git_update(clone, sha, "v1.2.0", "1.0.0", False, log) is True
    assert head_tag(clone) == "v1.2.0"
    assert tracked_changes(clone) == ""
    assert (clone / "config.json").read_text() == '{"secret": "keep me"}'
    assert (clone / "data" / "energy.sqlite").read_bytes() == b"rows"
    backups = list((clone / "data" / "update-backups").iterdir())
    assert len(backups) == 1
    state = json.loads((backups[0] / "state.json").read_text())
    assert state["diverged"] is False and state["head_version"] == "1.0.0"
    assert (backups[0] / "local-changes.patch").read_bytes() == b""


def test_local_changes_are_saved_never_dropped(clone):
    core = clone / "src" / "shelly_analyzer" / "core.py"
    core.write_text(core.read_text() + "MY_TWEAK = True\n")
    (clone / "my_notes.txt").write_text("mine")
    log = logger()
    sha = uh.prepare_git_update(clone, "origin", "v1.2.0")
    assert uh.apply_git_update(clone, sha, "v1.2.0", "1.0.0", False, log)
    assert "MY_TWEAK" not in core.read_text()
    assert tracked_changes(clone) == ""
    assert (clone / "my_notes.txt").read_text() == "mine", "no git clean"
    backup = next((clone / "data" / "update-backups").iterdir())
    assert b"+MY_TWEAK = True" in (backup / "local-changes.patch").read_bytes()
    with tarfile.open(backup / "untracked.tar.gz") as tar:
        assert "my_notes.txt" in tar.getnames()


def test_drifted_clone_heals_and_keeps_the_real_edits(clone, tmp_path, origin):
    """The state the old updater left behind: git on v1.0.0, files of v1.1.0
    copied over (with .git skipped), plus one edit made by hand."""
    export = tmp_path / "zip-1.1.0"
    run(tmp_path, "clone", "-q", str(origin), str(export))
    run(export, "checkout", "-q", "v1.1.0")
    shutil.rmtree(export / ".git")
    for item in export.iterdir():
        dst = clone / item.name
        if item.is_dir():
            shutil.copytree(item, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dst)
    core = clone / "src" / "shelly_analyzer" / "core.py"
    core.write_text(core.read_text() + "HAND_EDIT = 1\n")
    assert head_tag(clone) == "v1.0.0" and tracked_changes(clone) != ""

    log = logger()
    sha = uh.prepare_git_update(clone, "origin", "v1.2.0")
    assert uh.apply_git_update(clone, sha, "v1.2.0", "1.1.0", False, log)
    assert head_tag(clone) == "v1.2.0"
    assert tracked_changes(clone) == ""
    backup = next((clone / "data" / "update-backups").iterdir())
    state = json.loads((backup / "state.json").read_text())
    assert state["diverged"] is True and state["changed_files"] >= 2
    real = (backup / "local-changes-vs-v1.1.0.patch").read_text()
    assert "+HAND_EDIT = 1" in real
    assert "VALUE = '1.0.0'" not in real, "only the hand edit, not the upstream drift"
    assert any("had drifted" in ln for ln in log.lines), "a log line, no message"


def test_failed_probe_goes_back_with_changes_restored(clone):
    core = clone / "src" / "shelly_analyzer" / "core.py"
    core.write_text(core.read_text() + "MY_TWEAK = True\n")
    (clone / "my_notes.txt").write_text("mine")
    log = logger()
    sha = uh.prepare_git_update(clone, "origin", "v1.3.0")
    assert uh.apply_git_update(clone, sha, "v1.3.0", "1.0.0", False, log) is False
    assert run(clone, "rev-parse", "HEAD") == run(clone, "rev-parse", "v1.0.0")
    assert run(clone, "symbolic-ref", "--short", "HEAD") == "main", "branch restored"
    assert "MY_TWEAK = True" in core.read_text()
    assert (clone / "my_notes.txt").read_text() == "mine"
    assert not (clone / "src" / "shelly_analyzer" / "new_in_1_3_0.py").exists()
    assert any("probe failed" in ln for ln in log.lines)
    assert any("rollback" in ln and "done" in ln for ln in log.lines)


def test_only_the_last_backups_are_kept(clone):
    root = clone / "data" / "update-backups"
    root.mkdir(parents=True)
    for i in range(7):
        (root / f"2000010{i}-000000-0.0.{i}").mkdir()
    uh.write_git_backup(clone, "1.0.0", "v1.2.0", logger())
    assert len(list(root.iterdir())) == uh.BACKUP_KEEP


# ── the route chooses the path ───────────────────────────────────────────
@pytest.fixture
def route(monkeypatch):
    calls = {"fetch_releases": 0, "handoff": []}

    def fetch_releases(repo, limit=10):
        calls["fetch_releases"] += 1
        return []
    monkeypatch.setattr(updmod, "fetch_releases", fetch_releases)
    monkeypatch.setattr(updmod, "_start_handoff",
                        lambda helper, app_dir, args: calls["handoff"].append(args))
    app = Flask(__name__)
    app.extensions["state"] = SimpleNamespace(
        cfg=SimpleNamespace(updates=SimpleNamespace(repo="owner/repo")), _bg=None)
    app.register_blueprint(updmod.bp)
    with app.test_client() as c:
        c.calls = calls
        yield c


def test_install_on_a_clone_goes_through_git(route, clone, monkeypatch):
    """Red before 17.10.3: the route went for the ZIP (fetch_releases) and the
    helper copied it over the clone."""
    monkeypatch.setenv("SEA_APP_DIR", str(clone))
    r = route.post("/api/updates/install", json={"tag": "v1.2.0"})
    d = r.get_json()
    assert r.status_code == 200 and d["ok"] and d["install_kind"] == "git"
    assert route.calls["fetch_releases"] == 0, "no ZIP for a git clone"
    args = route.calls["handoff"][0]
    assert args[args.index("--git-commit") + 1] == run(clone, "rev-parse", "v1.2.0^{commit}")
    assert args[args.index("--git-tag") + 1] == "v1.2.0"
    assert "--staging-dir" not in args
    assert head_tag(clone) == "v1.0.0", "the switch happens in the helper"


def test_install_on_a_clone_reports_a_bad_tag(route, clone, monkeypatch):
    monkeypatch.setenv("SEA_APP_DIR", str(clone))
    r = route.post("/api/updates/install", json={"tag": "v1.2.5"})
    assert r.status_code == 502 and "git update failed" in r.get_json()["error"]
    assert route.calls["handoff"] == [] and route.calls["fetch_releases"] == 0


def test_zip_install_keeps_the_zip_path(route, tmp_path, monkeypatch):
    plain = tmp_path / "zip-install"
    plain.mkdir()
    monkeypatch.setenv("SEA_APP_DIR", str(plain))
    r = route.post("/api/updates/install", json={"tag": "v1.2.0"})
    assert r.status_code == 404  # no asset in the (empty) release list
    assert route.calls["fetch_releases"] == 1 and route.calls["handoff"] == []


# ── the helper's ZIP path is unchanged ───────────────────────────────────
def test_helper_zip_path_still_copies_and_skips_git(tmp_path, monkeypatch):
    app = tmp_path / "app"
    (app / "src").mkdir(parents=True)
    (app / "config.json").write_text("{}")
    staging = tmp_path / "sea_update_x" / "release"
    (staging / "src").mkdir(parents=True)
    (staging / "src" / "new.py").write_text("NEW = 1\n")
    (staging / ".git").mkdir()
    restarted = []
    monkeypatch.setattr(uh, "_restart_app", lambda restart, app_dir, log: restarted.append(app_dir))
    monkeypatch.setattr(sys, "argv", ["updater_helper.py", "--app-dir", str(app),
                                      "--staging-dir", str(staging), "--restart", "start.sh",
                                      "--update-deps", "0"])
    assert uh.main() == 0
    assert (app / "src" / "new.py").exists() and not (app / ".git").exists()
    assert (app / "config.json").read_text() == "{}"
    assert restarted == [app.resolve()]


def test_helper_git_path_restarts_also_after_a_rollback(clone, monkeypatch):
    restarted = []
    monkeypatch.setattr(uh, "_restart_app", lambda restart, app_dir, log: restarted.append(app_dir))
    sha = uh.prepare_git_update(clone, "origin", "v1.3.0")
    monkeypatch.setattr(sys, "argv", ["updater_helper.py", "--app-dir", str(clone),
                                      "--restart", "start.sh", "--update-deps", "0",
                                      "--git-commit", sha, "--git-tag", "v1.3.0",
                                      "--running-version", "1.0.0"])
    assert uh.main() == 3
    assert head_tag(clone) == "v1.0.0"
    assert restarted == [clone.resolve()], "the old version comes back up"
    assert "rolling back" in (clone / "logs" / "updater.log").read_text()


# ── the page tells a git install apart, in every language ────────────────
def test_settings_page_and_languages_know_the_git_path():
    from shelly_analyzer import i18n
    src = (Path(__file__).resolve().parents[1] / "src" / "shelly_analyzer" / "web" /
           "templates" / "settings.html").read_text(encoding="utf-8")
    assert "cfg._git" in src and "settings.updates.git_managed" in src
    assert 'install_kind === "git"' in src and "settings.updates.detail_git" in src
    en = {k: i18n.t("en", k) for k in ("settings.updates.git_managed", "settings.updates.detail_git")}
    for lang in i18n.LANGS:
        for key in en:
            txt = i18n.t(lang, key, v="v1.2.3")
            assert txt and txt != key, (lang, key)
            if lang != "en":
                assert txt != i18n.t("en", key, v="v1.2.3"), ("still English", lang, key)
        assert "v1.2.3" in i18n.t(lang, "settings.updates.detail_git", v="v1.2.3"), lang
