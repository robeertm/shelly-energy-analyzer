from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from typing import List


def _ensure_executable(p: Path) -> None:
    """Ensure a script is executable (macOS/Linux)."""
    try:
        if not p.exists():
            return
        # Add +x for user/group/other
        mode = p.stat().st_mode
        p.chmod(mode | 0o111)
    except Exception:
        pass



def _spawn_detached(cmd: List[str], cwd: Path) -> None:
    """Spawn a process fully detached from the current session (best-effort)."""
    try:
        if os.name == "nt":
            # On Windows, use DETACHED_PROCESS + NEW_PROCESS_GROUP
            subprocess.Popen(
                cmd,
                cwd=str(cwd),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
                close_fds=False,
            )
        else:
            # On macOS/Linux: nohup + new session + no stdio
            subprocess.Popen(
                cmd,
                cwd=str(cwd),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
    except Exception:
        pass

def _clear_quarantine(target: Path) -> None:
    """Best-effort remove Gatekeeper quarantine attributes on macOS."""
    try:
        if sys.platform != "darwin":
            return
        subprocess.run(["xattr", "-dr", "com.apple.quarantine", str(target)], check=False)
    except Exception:
        pass


def _restart_app(restart: Path, app_dir: Path, log) -> None:
    """Launch the updated app, preserving the current PID wherever possible.

    POSIX (Linux / macOS / Docker): ``os.execv`` replaces this helper process
    image in-place with ``python -m shelly_analyzer``. Because PID 1 / the
    service MainPID never dies, systemd (KillMode=mixed + cgroup), launchd
    and Docker all keep the service alive throughout the update — no
    surprise "deactivated successfully" events, no child-process kills.

    Windows: no cgroup-style teardown to worry about. We keep the historical
    detached-spawn path via ``cmd /c start start.bat``. ``os.execv`` on
    Windows is spawn-and-exit semantically, not in-place, so it gives us
    nothing extra there.
    """
    # Ensure exec bits on common start scripts (ZIP extraction may drop +x)
    _ensure_executable(app_dir / "start.command")
    _ensure_executable(app_dir / "start.sh")
    _ensure_executable(restart)
    _clear_quarantine(app_dir)

    if os.name == "nt":
        # Prefer start.bat inside app_dir if restart is missing
        if not restart.exists():
            cand = app_dir / "start.bat"
            if cand.exists():
                restart = cand
        try:
            subprocess.Popen(
                ["cmd", "/c", "start", "", str(restart)],
                cwd=str(app_dir),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
                close_fds=False,
            )
            log(f"[updater] Windows: launched {restart}")
        except Exception as e:
            log(f"[updater] Windows restart failed: {e}")
        # Give the child a moment to grab ownership before we exit
        try:
            time.sleep(0.5)
        except Exception:
            pass
        return

    # POSIX path — execv in-place.
    py = _venv_python(app_dir) or Path(sys.executable)
    new_cmd = [str(py), "-m", "shelly_analyzer"]
    try:
        os.chdir(str(app_dir))
    except Exception:
        pass
    # Not on macOS: closing blindly can hit a guarded fd owned by the system
    # libraries (EXC_GUARD kills the process). Python fds are already
    # non-inheritable (PEP 446), so execv drops them anyway.
    if sys.platform != "darwin":
        try:
            os.closerange(3, 256)
        except Exception:
            pass
    log(f"[updater] POSIX: execv -> {' '.join(new_cmd)} (keeping PID {os.getpid()})")
    try:
        os.execv(str(py), new_cmd)
    except Exception as e:
        # execv failure is rare (e.g. broken venv). Fall back to detached
        # spawn via start.command so the user still ends up with a running
        # app, even if systemd/launchd think the service exited.
        log(f"[updater] execv failed: {e} — falling back to detached spawn")
        if not restart.exists():
            for cand_name in ("start.command", "start.sh"):
                cand = app_dir / cand_name
                if cand.exists():
                    restart = cand
                    break
        suffix = restart.suffix.lower()
        if suffix in (".command", ".sh"):
            _spawn_detached(["/usr/bin/nohup", "/bin/bash", str(restart)], cwd=app_dir)
        else:
            _spawn_detached([str(restart)], cwd=app_dir)
        try:
            time.sleep(0.5)
        except Exception:
            pass


EXCLUDE_NAMES = {
    ".venv",
    "data",
    "logs",
    "config.json",
    "config.example.json",
    ".git",
    ".github",
    ".claude",
    ".vscode",
    "__pycache__",
    "docs",
}


def _wait_for_pid(pid: int, timeout_s: float = 15.0) -> None:
    """Block until the given PID exits or ``timeout_s`` elapses.

    ``pid == 0`` is the sentinel used by the POSIX ``execv`` handoff path in
    ``web/blueprints/updates.py``: there is no parent to wait for because we
    *are* the same process (this helper replaced the app image in-place via
    ``os.execv``). Return immediately in that case.
    """
    if pid <= 0:
        return
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        try:
            if os.name != "nt":
                # signal 0 probes existence on POSIX
                os.kill(pid, 0)
                time.sleep(0.25)
            else:
                # Windows: use OpenProcess + STILL_ACTIVE to detect exit.
                # Falls back to a fixed sleep if ctypes is unavailable.
                try:
                    import ctypes  # noqa: WPS433
                    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                    STILL_ACTIVE = 259
                    k32 = ctypes.windll.kernel32
                    h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
                    if not h:
                        return
                    try:
                        ec = ctypes.c_ulong(0)
                        if not k32.GetExitCodeProcess(h, ctypes.byref(ec)):
                            return
                        if ec.value != STILL_ACTIVE:
                            return
                    finally:
                        k32.CloseHandle(h)
                    time.sleep(0.25)
                except Exception:
                    time.sleep(2.0)
                    return
        except Exception:
            return
    # timeout: proceed anyway


def _safe_rmtree(p: Path) -> None:
    try:
        if p.exists():
            shutil.rmtree(p)
    except Exception:
        pass


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        _safe_rmtree(dst)
    shutil.copytree(src, dst)


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _iter_items(folder: Path) -> List[Path]:
    return [folder / x for x in os.listdir(folder)]


def _venv_python(app_dir: Path) -> Path | None:
    v = app_dir / ".venv"
    if not v.exists():
        return None
    if os.name == "nt":
        py = v / "Scripts" / "python.exe"
    else:
        py = v / "bin" / "python"
    return py if py.exists() else None

# ---------------------------------------------------------------------------
# Git checkouts (since 17.10.3)
#
# An install that is a clone of the public repository is updated through git:
# fetch the tags, check the target, save the old state, switch to the tag.
# Before 17.10.3 the ZIP was copied over such a clone with ``.git`` excluded,
# so git stayed on the old commit while the files moved on — dozens of
# "modified" files and a ``git pull`` that ran into conflicts.
#
# Everything here is stdlib-only: this file runs as a standalone script
# (``python updater_helper.py``) after the app process handed over to it.
# ---------------------------------------------------------------------------

GIT_TIMEOUT_S = 180
BACKUP_DIR_NAME = "update-backups"
BACKUP_KEEP = 5
_TAG_OK = re.compile(r"^v?\d+(\.\d+){2,3}$")
_REPO_IN_URL = re.compile(r"github\.com[:/]+([^/\s]+)/([^/\s]+?)(?:\.git)?/*$", re.IGNORECASE)
_VERSION_IN_INIT = re.compile(r'__version__\s*=\s*["\']([^"\']+)["\']')
_INIT_PATH = "src/shelly_analyzer/__init__.py"


class GitUpdateError(RuntimeError):
    """A git step failed; the message is safe to show (no URLs with tokens)."""


def _git(app_dir: Path, *args: str, check: bool = True,
         timeout: float = GIT_TIMEOUT_S) -> subprocess.CompletedProcess:
    """Run git inside ``app_dir``; bytes in, bytes out.

    ``safe.directory`` keeps a service that runs as another user than the
    owner of the clone from failing on git's ownership check, and
    ``GIT_TERMINAL_PROMPT=0`` keeps a fetch from ever waiting for a password.
    """
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["LC_ALL"] = "C"
    cmd = ["git", "-c", f"safe.directory={app_dir}", "-C", str(app_dir), *args]
    proc = subprocess.run(cmd, capture_output=True, timeout=timeout, env=env,
                          stdin=subprocess.DEVNULL)
    if check and proc.returncode != 0:
        err = proc.stderr.decode("utf-8", "replace").strip().splitlines()
        tail = err[-1] if err else f"exit {proc.returncode}"
        # Never echo a remote URL: it may carry a token.
        tail = re.sub(r"https?://[^\s'\"]+", "<remote>", tail)
        raise GitUpdateError(f"git {args[0]} failed: {tail}")
    return proc


def _out(proc: subprocess.CompletedProcess) -> str:
    return proc.stdout.decode("utf-8", "replace").strip()


def normalize_repo(url_or_slug: str) -> str:
    """``https://github.com/Owner/Repo.git`` / ``git@github.com:owner/repo`` /
    ``owner/repo`` -> ``owner/repo`` (lower case); ``""`` if not GitHub."""
    s = (url_or_slug or "").strip()
    m = _REPO_IN_URL.search(s)
    if m:
        return f"{m.group(1)}/{m.group(2)}".lower()
    if re.fullmatch(r"[\w.-]+/[\w.-]+", s):
        return s[:-4].lower() if s.endswith(".git") else s.lower()
    return ""


def git_checkout_remote(app_dir: Path, repo: str) -> str | None:
    """Name of the remote that points at ``repo`` if ``app_dir`` is a git
    checkout of it, else ``None`` (no ``.git``, no git binary, a fork, or
    git refuses — the ZIP path then stays exactly as before)."""
    try:
        want = normalize_repo(repo)
        if not want or not (Path(app_dir) / ".git").exists():
            return None
        if shutil.which("git") is None:
            return None
        top = _out(_git(app_dir, "rev-parse", "--show-toplevel"))
        if Path(top).resolve() != Path(app_dir).resolve():
            return None  # app_dir is only a sub-folder of some other repo
        remotes = _out(_git(app_dir, "remote")).split()
        for name in sorted(remotes, key=lambda r: r != "origin"):
            # The configured URL, not `remote get-url` (that one expands
            # url.*.insteadOf rewrites, e.g. a local mirror).
            url = _out(_git(app_dir, "config", "--get", f"remote.{name}.url", check=False))
            if normalize_repo(url) == want:
                return name
    except Exception:
        return None
    return None


def _version_at(app_dir: Path, rev: str) -> str:
    proc = _git(app_dir, "show", f"{rev}:{_INIT_PATH}", check=False)
    if proc.returncode != 0:
        return ""
    m = _VERSION_IN_INIT.search(proc.stdout.decode("utf-8", "replace"))
    return m.group(1) if m else ""


def prepare_git_update(app_dir: Path, remote: str, tag: str) -> str:
    """Fetch the tags and check ``tag`` before anything is touched.

    The check: the tag exists on the remote, names a commit, and that commit
    carries exactly this version in ``__init__.py`` — the same promise the
    ZIP path relies on (the release ZIP is built from that tag). Returns the
    commit id. Raises :class:`GitUpdateError`; the working tree is untouched.
    """
    tag = (tag or "").strip()
    if not _TAG_OK.match(tag):
        raise GitUpdateError(f"not a release tag: {tag!r}")
    _git(app_dir, "fetch", "--tags", "--no-recurse-submodules", remote)
    proc = _git(app_dir, "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}", check=False)
    sha = _out(proc)
    if proc.returncode != 0 or not sha:
        raise GitUpdateError(f"tag {tag} not found on the remote")
    found = _version_at(app_dir, sha)
    if found.lstrip("v") != tag.lstrip("v"):
        raise GitUpdateError(f"tag {tag} carries version {found or '?'}")
    return sha


def _untracked_files(app_dir: Path) -> List[str]:
    proc = _git(app_dir, "ls-files", "-z", "--others", "--exclude-standard", check=False)
    return [x for x in proc.stdout.decode("utf-8", "surrogateescape").split("\0") if x]


def write_git_backup(app_dir: Path, running_version: str, target_tag: str, log) -> Path:
    """Save everything a switch to another commit could throw away.

    * ``state.json`` — commit, branch, ``git describe`` and the running version
    * ``local-changes.patch`` — every change against the checked-out commit
      (``git diff --binary HEAD``), the input for the rollback
    * ``local-changes-vs-v<running>.patch`` — only when git and the app had
      drifted apart: the changes against the tag of the version that actually
      runs, i.e. the edits that were really made by hand
    * ``untracked.tar.gz`` — files git does not track and does not ignore

    Ignored files (``config.json``, ``data/``, ``logs/``, ``.venv``) are
    never touched by the switch, so they are not copied.
    """
    stamp = time.strftime("%Y%m%d-%H%M%S")
    root = Path(app_dir) / "data" / BACKUP_DIR_NAME
    dest = root / f"{stamp}-{running_version or 'unknown'}"
    n = 1
    while dest.exists():
        n += 1
        dest = root / f"{stamp}-{running_version or 'unknown'}-{n}"
    dest.mkdir(parents=True)

    head = _out(_git(app_dir, "rev-parse", "HEAD"))
    branch = _out(_git(app_dir, "symbolic-ref", "-q", "--short", "HEAD", check=False))
    describe = _out(_git(app_dir, "describe", "--tags", "--always", "HEAD", check=False))
    head_version = _version_at(app_dir, head)

    patch = _git(app_dir, "diff", "--binary", "HEAD").stdout
    (dest / "local-changes.patch").write_bytes(patch)

    diverged = bool(running_version) and head_version.lstrip("v") != running_version.lstrip("v")
    vs_running = ""
    if diverged:
        for cand in (f"v{running_version.lstrip('v')}", running_version.lstrip("v")):
            ok = _git(app_dir, "rev-parse", "--verify", "--quiet", f"refs/tags/{cand}^{{commit}}", check=False)
            if ok.returncode == 0:
                vs_running = f"local-changes-vs-{cand}.patch"
                (dest / vs_running).write_bytes(_git(app_dir, "diff", "--binary", cand).stdout)
                break

    untracked = _untracked_files(app_dir)
    if untracked:
        with tarfile.open(dest / "untracked.tar.gz", "w:gz") as tar:
            for rel in untracked:
                try:
                    tar.add(str(Path(app_dir) / rel), arcname=rel, recursive=False)
                except Exception as e:
                    log(f"[updater] git backup: could not save {rel}: {e}")

    state = {
        "time": stamp,
        "head": head,
        "branch": branch,
        "describe": describe,
        "head_version": head_version,
        "running_version": running_version,
        "target_tag": target_tag,
        "diverged": diverged,
        "changed_files": len([ln for ln in _out(_git(app_dir, "diff", "--name-only", "HEAD")).splitlines() if ln]),
        "untracked_files": len(untracked),
        "patch_vs_running": vs_running,
    }
    (dest / "state.json").write_text(json.dumps(state, indent=2), encoding="utf-8")
    if diverged:
        # Self-heal of a clone that an older updater left behind: no message,
        # only this line — the switch below brings it back onto the tag.
        log(f"[updater] git: checkout had drifted (git at {describe or head[:12]}, "
            f"app {running_version}, {state['changed_files']} changed files) — "
            f"bringing it onto {target_tag}, differences saved")
    log(f"[updater] git backup written to {dest} ({len(patch)} bytes patch, "
        f"{len(untracked)} untracked files)")
    _prune_backups(root, log)
    return dest


def _prune_backups(root: Path, log) -> None:
    try:
        dirs = sorted([d for d in root.iterdir() if d.is_dir()], key=lambda d: d.name)
        for old in dirs[:-BACKUP_KEEP]:
            _safe_rmtree(old)
            log(f"[updater] git backup pruned: {old.name}")
    except Exception:
        pass


def switch_to_commit(app_dir: Path, sha: str) -> None:
    """Detached checkout of ``sha``; tracked files follow, ignored ones stay.

    No ``git clean``: untracked files are the user's and stay where they are.
    """
    _git(app_dir, "checkout", "--force", "--detach", sha)
    left = _out(_git(app_dir, "status", "--porcelain", "--untracked-files=no"))
    if left:
        raise GitUpdateError("working tree not clean after checkout")


def rollback_git(app_dir: Path, backup: Path, log) -> bool:
    """Back to the commit (and branch) of the backup, local changes re-applied."""
    try:
        state = json.loads((backup / "state.json").read_text(encoding="utf-8"))
        head = state["head"]
        branch = state.get("branch") or ""
        if branch and _out(_git(app_dir, "rev-parse", "--verify", "--quiet", branch, check=False)) == head:
            _git(app_dir, "checkout", "--force", branch)
        else:
            _git(app_dir, "checkout", "--force", "--detach", head)
        patch = backup / "local-changes.patch"
        if patch.exists() and patch.stat().st_size > 0:
            proc = _git(app_dir, "apply", "--binary", "--whitespace=nowarn", str(patch), check=False)
            if proc.returncode != 0:
                log(f"[updater] git rollback: patch did not apply cleanly, it stays in {patch}")
        tar_path = backup / "untracked.tar.gz"
        if tar_path.exists():
            with tarfile.open(tar_path, "r:gz") as tar:
                base = Path(app_dir).resolve()
                for member in tar.getmembers():
                    target = (base / member.name).resolve()
                    if base not in target.parents or not (member.isfile() or member.isdir()):
                        continue
                    tar.extract(member, str(base))
        log(f"[updater] git rollback to {state.get('describe') or head[:12]} done")
        return True
    except Exception as e:
        log(f"[updater] git rollback failed: {e} — backup kept in {backup}")
        return False


def probe_code(app_dir: Path, expected_version: str, log) -> bool:
    """Import the code on disk in a fresh interpreter and compare the version."""
    py = _venv_python(app_dir) or Path(sys.executable)
    code = ("import sys; sys.path.insert(0, 'src'); import shelly_analyzer, "
            "shelly_analyzer.web; print(shelly_analyzer.__version__)")
    try:
        proc = subprocess.run([str(py), "-c", code], cwd=str(app_dir), capture_output=True,
                              text=True, timeout=GIT_TIMEOUT_S, stdin=subprocess.DEVNULL)
    except Exception as e:
        log(f"[updater] probe failed to run: {e}")
        return False
    got = (proc.stdout or "").strip().splitlines()[-1:] or [""]
    if proc.returncode != 0 or got[0].lstrip("v") != expected_version.lstrip("v"):
        err = (proc.stderr or "").strip().splitlines()[-1:] or [""]
        log(f"[updater] probe failed: exit {proc.returncode}, version {got[0]!r}, {err[0][:300]}")
        return False
    log(f"[updater] probe ok: {got[0]}")
    return True


def _install_deps(app_dir: Path, log) -> None:
    req = app_dir / "requirements.txt"
    py = _venv_python(app_dir)
    if not (py and req.exists()):
        return
    log(f"[updater] installing deps via {py}")
    try:
        proc = subprocess.run(
            [str(py), "-m", "pip", "install", "-r", str(req)],
            check=False, capture_output=True, text=True,
        )
        if proc.returncode != 0:
            log(f"[updater] pip install returned {proc.returncode}")
            if proc.stderr:
                log(f"[updater] pip stderr: {proc.stderr.strip()[:500]}")
        else:
            log("[updater] pip install completed")
    except Exception as e:
        log(f"[updater] pip install failed: {e}")


def _clear_pycache(app_dir: Path) -> None:
    for cache_dir in app_dir.rglob("__pycache__"):
        if ".venv" in cache_dir.parts:
            continue
        _safe_rmtree(cache_dir)


def apply_git_update(app_dir: Path, sha: str, tag: str, running_version: str,
                     update_deps: bool, log) -> bool:
    """Backup -> switch -> deps -> probe; on a failed probe back to the old
    state. Returns True when the new tag is in place."""
    try:
        backup = write_git_backup(app_dir, running_version, tag, log)
    except Exception as e:
        log(f"[updater] git backup failed: {e} — nothing changed, staying on {running_version}")
        return False
    try:
        switch_to_commit(app_dir, sha)
        log(f"[updater] git: switched to {tag} ({sha[:12]})")
    except Exception as e:
        log(f"[updater] git switch failed: {e} — rolling back")
        rollback_git(app_dir, backup, log)
        _clear_pycache(app_dir)
        return False
    _clear_pycache(app_dir)
    if update_deps:
        _install_deps(app_dir, log)
    if probe_code(app_dir, tag, log):
        return True
    log("[updater] git: new version failed its probe — rolling back")
    rollback_git(app_dir, backup, log)
    _clear_pycache(app_dir)
    if update_deps:
        _install_deps(app_dir, log)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--app-dir", required=True)
    ap.add_argument("--staging-dir", default="")
    ap.add_argument("--restart", required=True)
    ap.add_argument("--wait-pid", type=int, default=0)
    ap.add_argument("--update-deps", type=int, default=1)
    # Git checkout (17.10.3): switch to this verified commit instead of copying.
    ap.add_argument("--git-commit", default="")
    ap.add_argument("--git-tag", default="")
    ap.add_argument("--running-version", default="")
    args = ap.parse_args()
    app_dir = Path(args.app_dir).resolve()
    staging = Path(args.staging_dir).resolve()

    # Resolve restart path robustly: if relative, interpret relative to app_dir
    restart_arg = Path(args.restart)
    if restart_arg.is_absolute():
        restart = restart_arg.resolve()
    else:
        restart = (app_dir / restart_arg).resolve()

    # Log file for debugging update issues
    log_path = app_dir / "logs" / "updater.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def _log(msg: str) -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} {msg}\n"
        print(line, end="", file=sys.stderr)
        try:
            with open(log_path, "a") as f:
                f.write(line)
        except Exception:
            pass

    _log(f"[updater] waiting for PID {args.wait_pid} to exit")
    _wait_for_pid(int(args.wait_pid or 0))

    if args.git_commit:
        _log(f"[updater] git checkout: {args.running_version} -> {args.git_tag} ({args.git_commit[:12]})")
        ok = apply_git_update(app_dir, args.git_commit, args.git_tag, args.running_version,
                              int(args.update_deps or 0) == 1, _log)
        _log(f"[updater] git update {'done' if ok else 'not applied, old version kept'}")
        try:
            _log(f"[updater] restarting via {restart}")
            _restart_app(restart, app_dir, _log)
            _log("[updater] restart command issued")
        except Exception as e:
            _log(f"[updater] restart failed: {e}")
            return 2
        return 0 if ok else 3

    if not args.staging_dir:
        _log("[updater] no --staging-dir and no --git-commit, nothing to do")
        return 2
    _log(f"[updater] app_dir={app_dir}, staging={staging}")
    _log(f"[updater] config.json exists: {(app_dir / 'config.json').exists()}")

    # Replace app files, preserve user data/config/venv.
    copied = 0
    for item in _iter_items(staging):
        name = item.name
        if name in EXCLUDE_NAMES:
            _log(f"[updater] skip (excluded): {name}")
            continue
        if name.endswith(".pyc") or name == ".DS_Store":
            continue
        if name.startswith(".") and name not in (".gitignore",):
            _log(f"[updater] skip (hidden): {name}")
            continue

        dst = app_dir / name
        try:
            if item.is_dir():
                _copy_tree(item, dst)
            else:
                _copy_file(item, dst)
            copied += 1
        except Exception as e:
            _log(f"[updater] FAILED to copy {name}: {e}")

    _log(f"[updater] copied {copied} items")

    # Clear __pycache__ recursively to avoid stale bytecode
    for cache_dir in app_dir.rglob("__pycache__"):
        _safe_rmtree(cache_dir)
    _log("[updater] cleared __pycache__")

    _log(f"[updater] config.json still exists: {(app_dir / 'config.json').exists()}")

    # Update dependencies if venv exists and requirements.txt exists.
    if int(args.update_deps or 0) == 1:
        req = app_dir / "requirements.txt"
        py = _venv_python(app_dir)
        if py and req.exists():
            _log(f"[updater] installing deps via {py}")
            try:
                proc = subprocess.run(
                    [str(py), "-m", "pip", "install", "-r", str(req)],
                    check=False, capture_output=True, text=True,
                )
                if proc.returncode != 0:
                    _log(f"[updater] pip install returned {proc.returncode}")
                    if proc.stderr:
                        _log(f"[updater] pip stderr: {proc.stderr.strip()[:500]}")
                else:
                    _log("[updater] pip install completed")
            except Exception as e:
                _log(f"[updater] pip install failed: {e}")

    # Clean up staging dir and any leftover /tmp/*.zip from the download.
    try:
        # Ascend one level if the zip had a single top-level folder.
        if staging.parent.name.startswith("sea_update_"):
            _safe_rmtree(staging.parent)
        else:
            _safe_rmtree(staging)
        _log(f"[updater] cleaned staging {staging}")
    except Exception:
        pass

    # Restart app. On POSIX this does os.execv and never returns; on Windows
    # it spawns detached and then we fall through to a clean exit.
    try:
        _log(f"[updater] restarting via {restart}")
        _restart_app(restart, app_dir, _log)
        _log("[updater] restart command issued")
    except Exception as e:
        _log(f"[updater] restart failed: {e}")
        return 2
    return 0



if __name__ == "__main__":
    raise SystemExit(main())