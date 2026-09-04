"""Unit tests for scripts/archive_deployed_wheel.py.

The script copies each wheel `bundle deploy` published into `.internal/` out to a sibling
`archive/` folder, so a later deploy's PRUNE of `.internal/` cannot destroy the only copy of a
wheel some deployed resource is still pinned to. These tests stub the Databricks CLI so no
workspace is touched.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "archive_deployed_wheel.py"


def _load():
    spec = importlib.util.spec_from_file_location("archive_deployed_wheel", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load()


def _wheel(millis: int) -> str:
    return f"flowx-0.0.{millis}-py3-none-any.whl"


class TestWheelSortKey:
    def test_parses_epoch_millis_patch_version(self):
        assert mod.wheel_sort_key(_wheel(1788286445689)) == 1788286445689

    def test_orders_wheels_oldest_first(self):
        wheels = [_wheel(1788287671576), _wheel(1788286445689)]
        assert sorted(wheels, key=mod.wheel_sort_key) == [
            _wheel(1788286445689),
            _wheel(1788287671576),
        ]

    def test_unparseable_name_sorts_first_and_does_not_raise(self):
        # A hand-placed wheel must never crash the sort; it sorts first so --prune-keep
        # treats it as oldest rather than silently discarding a newer real wheel.
        assert mod.wheel_sort_key("not-a-versioned-wheel.whl") == 0


class TestListDir:
    def test_returns_filenames(self, monkeypatch):
        monkeypatch.setattr(
            mod, "_cli", lambda *a, **k: _Proc(0, f"{_wheel(1)}\n{_wheel(2)}\n")
        )
        assert mod.list_dir(None, "/Volumes/x") == [_wheel(1), _wheel(2)]

    def test_missing_directory_yields_empty_list(self, monkeypatch):
        # `fs ls` on an absent path exits non-zero; that must be an empty archive,
        # not a crash -- the archive folder does not exist before the first run.
        monkeypatch.setattr(mod, "_cli", lambda *a, **k: _Proc(1, "", "no such directory"))
        assert mod.list_dir(None, "/Volumes/x/archive") == []


class _Proc:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


class TestEnsureDir:
    def test_creates_archive_directory(self, monkeypatch):
        """Regression: `fs cp` does NOT create its destination folder, so the first
        run failed with 'no such directory' until an explicit mkdir was added."""
        calls = []

        def fake(profile, *args):
            calls.append(args)
            return _Proc(0)

        monkeypatch.setattr(mod, "_cli", fake)
        ok, err = mod.ensure_dir(None, "/Volumes/x/archive")
        assert ok and err == ""
        assert calls == [("fs", "mkdir", "dbfs:/Volumes/x/archive")]

    def test_reports_failure(self, monkeypatch):
        monkeypatch.setattr(mod, "_cli", lambda *a, **k: _Proc(1, "", "PERMISSION_DENIED"))
        ok, err = mod.ensure_dir(None, "/Volumes/x/archive")
        assert not ok and "PERMISSION_DENIED" in err


class TestCopyFile:
    def test_builds_dbfs_prefixed_cp(self, monkeypatch):
        calls = []

        def fake(profile, *args):
            calls.append(args)
            return _Proc(0)

        monkeypatch.setattr(mod, "_cli", fake)
        ok, _ = mod.copy_file(None, "/a/w.whl", "/b/w.whl")
        assert ok
        assert calls == [("fs", "cp", "dbfs:/a/w.whl", "dbfs:/b/w.whl")]

    def test_surfaces_error_text(self, monkeypatch):
        monkeypatch.setattr(mod, "_cli", lambda *a, **k: _Proc(1, "", "boom"))
        ok, err = mod.copy_file(None, "/a", "/b")
        assert not ok and err == "boom"
