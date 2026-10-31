"""The blanket os.closerange(3, 256) before execv must be skipped on macOS
(guarded fds -> EXC_GUARD kills the process)."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1] / "src" / "shelly_analyzer"
FILES = ["updater_helper.py", "__main__.py", "web/blueprints/updates.py"]


def _guarded(tree):
    found = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and "darwin" in ast.dump(node.test) and "NotEq" in ast.dump(node.test):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and getattr(sub.func, "attr", "") == "closerange":
                    found += 1
    return found


def _total(tree):
    return sum(1 for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "closerange")


def test_closerange_skipped_on_darwin():
    for f in FILES:
        tree = ast.parse((ROOT / f).read_text())
        assert _total(tree) >= 1, f
        assert _guarded(tree) == _total(tree), f


if __name__ == "__main__":
    test_closerange_skipped_on_darwin()
    print("ok")
