"""Static asset routes: plotly.js, widget.js, file downloads."""
from __future__ import annotations

import logging
from pathlib import Path
from flask import Blueprint, Response, current_app, jsonify, request, send_file

bp = Blueprint("static_assets", __name__)
logger = logging.getLogger(__name__)


def _get_state():
    return current_app.extensions["state"]


def _resolve_export_root():
    """Mirror of action_dispatch / app_context export-root logic so the
    file browser sees the same directory exports are written into."""
    state = _get_state()
    out_dir = getattr(state, "out_dir", None) or Path(".")
    configured = str(getattr(state.cfg.ui, "export_directory", "") or "").strip()
    if configured:
        try:
            root = Path(configured).expanduser().resolve()
            if root.exists():
                return root
        except Exception:
            pass
    return (Path(out_dir) / "exports").resolve()


@bp.route("/static/plotly.min.js")
def plotly_js():
    state = _get_state()
    body = state.get_plotly_js()
    if not body:
        return Response(
            b"/* plotly.min.js not available. Install the python package 'plotly' */",
            status=404,
            content_type="application/javascript; charset=utf-8",
        )
    return Response(
        body,
        content_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@bp.route("/widget.js")
def widget_js():
    from flask import request as _req
    state = _get_state()
    profile_id = _req.args.get("profile", "")
    body = state.get_widget_script(profile_id=profile_id).encode("utf-8")
    return Response(
        body,
        content_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


@bp.route("/files/<path:rel_path>")
def serve_file(rel_path: str):
    state = _get_state()
    try:
        data, ctype = state.read_file_bytes(rel_path)
        return Response(
            data,
            content_type=ctype,
            headers={"Cache-Control": "no-store"},
        )
    except FileNotFoundError:
        return Response(status=404)
    except Exception:
        return Response(status=500)


@bp.route("/api/exports/list", methods=["GET"])
def exports_list():
    """Walk the export root and return a flat file list.

    Response: ``{ok, root, files: [{rel_path, name, size, mtime, ext}], total_size}``.
    Used by the file browser in Settings → Tools / Exports so the user can
    inspect/clean the directory over the web — handy when the analyzer is
    remote and there's no shell access.
    """
    try:
        root = _resolve_export_root()
        if not root.exists():
            return jsonify({"ok": True, "root": str(root), "files": [], "total_size": 0, "exists": False})
        files = []
        total = 0
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            try:
                stat = p.stat()
                rel = str(p.relative_to(root))
                files.append({
                    "rel_path": rel,
                    "name": p.name,
                    "size": int(stat.st_size),
                    "mtime": int(stat.st_mtime),
                    "ext": p.suffix.lower().lstrip("."),
                })
                total += int(stat.st_size)
            except OSError:
                continue
        files.sort(key=lambda f: f["mtime"], reverse=True)
        return jsonify({
            "ok": True,
            "root": str(root),
            "files": files,
            "total_size": total,
            "exists": True,
        })
    except Exception as e:
        logger.exception("exports_list failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/exports/delete", methods=["POST"])
def exports_delete():
    """Delete one or more files (relative paths) below the export root.

    Body: ``{"rel_paths": ["web/summary_x.pdf", ...]}``. Each path is
    resolved against the export root; anything that escapes (via ``..`` or
    symlink) is silently skipped. Empty parent directories are NOT removed —
    the root structure (``web/``, ``web/invoices/``, ``web/reports/``)
    stays in place so the next export doesn't have to re-create it.
    """
    try:
        body = request.get_json(silent=True) or {}
        raw_paths = body.get("rel_paths") or []
        if not isinstance(raw_paths, list):
            return jsonify({"ok": False, "error": "rel_paths must be a list"}), 400
        root = _resolve_export_root()
        deleted = []
        errors = []
        freed = 0
        for rp in raw_paths:
            rel = str(rp or "").lstrip("/")
            if not rel:
                continue
            try:
                path = (root / rel).resolve()
                if root not in path.parents and path != root:
                    errors.append({"rel_path": rel, "error": "outside export root"})
                    continue
                if not path.is_file():
                    errors.append({"rel_path": rel, "error": "not a file"})
                    continue
                size = path.stat().st_size
                path.unlink()
                deleted.append(rel)
                freed += size
            except FileNotFoundError:
                errors.append({"rel_path": rel, "error": "not found"})
            except Exception as e:
                errors.append({"rel_path": rel, "error": str(e)})
        return jsonify({
            "ok": True,
            "deleted": deleted,
            "errors": errors,
            "freed_bytes": freed,
        })
    except Exception as e:
        logger.exception("exports_delete failed")
        return jsonify({"ok": False, "error": str(e)}), 500
