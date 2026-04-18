from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--app-dir", required=True)
    ap.add_argument("--staging-dir", required=True)
    ap.add_argument("--restart", required=True)
    ap.add_argument("--wait-pid", type=int, default=0)
    ap.add_argument("--update-deps", type=int, default=1)
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