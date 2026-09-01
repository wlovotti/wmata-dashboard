"""Tests for scripts/migrate_all.py's per-sibling argv isolation and exit-signal
propagation (the migrate_all argv-isolation fix, PR #230, and the
migrate_all exit-signal fix, PR #TODO).

``migrate_all.py`` auto-discovers ``migrate_*.py`` siblings and invokes each
module's ``main()``. Sibling ``main()``s call
``argparse.ArgumentParser().parse_args()`` with no explicit argv, which
defaults to reading ``sys.argv[1:]`` — so without isolation, any flag passed
to ``migrate_all.py`` itself (e.g. ``--yes``) leaks into *every* sibling's
own parser, including destructive migrations that happen to share a flag
name.

This file also covers ``_run_sibling`` and ``main()`` normalizing and
propagating a sibling's failure/exit signals (the migrate_all exit-signal
fix, PR #TODO): a sibling's
``sys.exit()`` is ``BaseException``, not ``Exception``, so without explicit
handling it would escape ``main()``'s migration loop entirely and truncate
the whole run; a sibling's non-zero ``main()`` return was previously
discarded outright.

These tests exercise ``migrate_all._run_sibling`` and ``migrate_all.main()``
directly against throwaway fake sibling modules, rather than calling
``migrate_all.main()`` against the real ``scripts_dir`` (which would
discover and execute the *real* ``migrate_*.py`` scripts against the
configured database).
"""

import sys

import pytest

import scripts.migrate_all as migrate_all
from scripts.migrate_all import _run_sibling


def _write_sibling(tmp_path, name, body):
    """Write a throwaway migrate_*.py-shaped module to tmp_path and return its Path."""
    path = tmp_path / name
    path.write_text(body)
    return path


@pytest.fixture(autouse=True)
def _cleanup_fake_sibling_modules():
    """Remove ``_migrate_all__*`` fake-sibling entries this file's tests add to ``sys.modules``.

    ``_run_sibling`` registers each sibling it loads under
    ``sys.modules[module_name]`` (needed for ``importlib`` to execute it).
    For the throwaway fixtures these tests write, nothing else ever clears
    that entry, so repeated test runs leaked fake module objects into
    ``sys.modules`` for the rest of the test process (the migrate_all
    exit-signal fix, PR #TODO).
    """
    before = set(sys.modules)
    yield
    for name in set(sys.modules) - before:
        if name.startswith("_migrate_all__"):
            del sys.modules[name]


@pytest.mark.smoke
def test_run_sibling_isolates_argv_from_migrate_all(tmp_path, monkeypatch):
    """A flag passed to migrate_all.py itself must not reach the sibling's own parser."""
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_one.py",
        "import argparse\n"
        "import sys\n"
        "seen_argv = None\n"
        "parsed_yes = None\n"
        "def main():\n"
        "    global seen_argv, parsed_yes\n"
        "    seen_argv = list(sys.argv)\n"
        "    parser = argparse.ArgumentParser()\n"
        "    parser.add_argument('--yes', action='store_true')\n"
        "    args = parser.parse_args()\n"
        "    parsed_yes = args.yes\n",
    )

    monkeypatch.setattr(sys, "argv", ["migrate_all.py", "--yes"])
    _run_sibling(sibling)

    module = sys.modules["_migrate_all__migrate_fake_one"]
    assert module.seen_argv == [str(sibling)]
    assert module.parsed_yes is False
    # migrate_all's own argv is restored once the sibling returns.
    assert sys.argv == ["migrate_all.py", "--yes"]


@pytest.mark.smoke
def test_run_sibling_does_not_leak_unrecognized_flag_into_sibling_parser(tmp_path, monkeypatch):
    """A sibling whose parser doesn't define migrate_all's flag must not blow up.

    Regression guard for the PR #221 scenario: two siblings define the same
    ``--yes`` flag name; a third might define none of migrate_all's argv at
    all. Isolation must hold regardless of what the sibling's own parser
    declares.
    """
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_two.py",
        "import argparse\n"
        "import sys\n"
        "seen_argv = None\n"
        "def main():\n"
        "    global seen_argv\n"
        "    seen_argv = list(sys.argv)\n"
        "    parser = argparse.ArgumentParser()\n"
        "    parser.parse_args()\n",
    )

    monkeypatch.setattr(sys, "argv", ["migrate_all.py", "--yes", "--agency", "sfmta"])
    _run_sibling(sibling)

    module = sys.modules["_migrate_all__migrate_fake_two"]
    assert module.seen_argv == [str(sibling)]


@pytest.mark.smoke
def test_run_sibling_restores_argv_even_if_sibling_raises(tmp_path, monkeypatch):
    """migrate_all's own argv is restored even when the sibling's main() raises."""
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_boom.py",
        "def main():\n    raise RuntimeError('boom')\n",
    )

    monkeypatch.setattr(sys, "argv", ["migrate_all.py", "--yes"])
    with pytest.raises(RuntimeError, match="boom"):
        _run_sibling(sibling)

    assert sys.argv == ["migrate_all.py", "--yes"]


@pytest.mark.smoke
def test_run_sibling_raises_if_no_main_entry_point(tmp_path, monkeypatch):
    """A sibling missing main() still raises the pre-existing RuntimeError."""
    sibling = _write_sibling(tmp_path, "migrate_fake_no_main.py", "x = 1\n")

    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])
    with pytest.raises(RuntimeError, match="no main"):
        _run_sibling(sibling)


@pytest.mark.smoke
def test_run_sibling_isolates_argv_before_exec_module(tmp_path, monkeypatch):
    """A sibling that reads sys.argv at module scope sees isolated argv too (the migrate_all exit-signal fix, PR #TODO).

    ``_run_sibling`` used to reset ``sys.argv`` only around the
    ``module.main()`` call, after ``spec.loader.exec_module(module)`` had
    already run. A sibling that reads ``sys.argv`` at module scope (rather
    than only inside its own ``main()``) would still see migrate_all's
    original, unisolated argv. The reset must happen before
    ``exec_module``.
    """
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_module_scope_argv.py",
        "import sys\nseen_argv_at_import = list(sys.argv)\ndef main():\n    pass\n",
    )

    monkeypatch.setattr(sys, "argv", ["migrate_all.py", "--yes"])
    _run_sibling(sibling)

    module = sys.modules["_migrate_all__migrate_fake_module_scope_argv"]
    assert module.seen_argv_at_import == [str(sibling)]


@pytest.mark.smoke
def test_run_sibling_treats_sys_exit_zero_or_none_as_success(tmp_path, monkeypatch):
    """A sibling's own sys.exit(0)/sys.exit() early-return is normalized to success (0)."""
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_early_return.py",
        "import sys\ndef main():\n    sys.exit(0)\n",
    )
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    assert _run_sibling(sibling) == 0
    # argv is still restored, same as the non-exit path.
    assert sys.argv == ["migrate_all.py"]


@pytest.mark.smoke
def test_run_sibling_propagates_nonzero_sys_exit_code(tmp_path, monkeypatch):
    """A sibling's sys.exit(N) for N != 0 is surfaced as _run_sibling's return value, not swallowed."""
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_exit_nonzero.py",
        "import sys\ndef main():\n    sys.exit(3)\n",
    )
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    assert _run_sibling(sibling) == 3
    assert sys.argv == ["migrate_all.py"]


@pytest.mark.smoke
def test_run_sibling_normalizes_non_int_sys_exit_code_to_failure(tmp_path, monkeypatch):
    """A sibling's sys.exit('message') is normalized to a non-zero int, not left as a string."""
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_exit_message.py",
        "import sys\ndef main():\n    sys.exit('boom')\n",
    )
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    result = _run_sibling(sibling)
    assert isinstance(result, int)
    assert result != 0


@pytest.mark.smoke
def test_run_sibling_propagates_nonzero_main_return_value(tmp_path, monkeypatch):
    """A sibling's main() returning non-zero (rather than raising) is surfaced, not discarded."""
    sibling = _write_sibling(
        tmp_path,
        "migrate_fake_return_nonzero.py",
        "def main():\n    return 1\n",
    )
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    assert _run_sibling(sibling) == 1


@pytest.mark.smoke
def test_run_sibling_returns_zero_when_main_returns_none(tmp_path, monkeypatch):
    """The common case, a sibling main() with no explicit return, is normalized to 0."""
    sibling = _write_sibling(tmp_path, "migrate_fake_return_none.py", "def main():\n    pass\n")
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    assert _run_sibling(sibling) == 0


def _write_and_patch_scripts_dir(tmp_path, monkeypatch):
    """Point migrate_all.main()'s self-discovery at a throwaway tmp_path directory."""
    fake_self = tmp_path / "migrate_all.py"
    fake_self.write_text("# stand-in for scripts/migrate_all.py's own path\n")
    monkeypatch.setattr(migrate_all, "__file__", str(fake_self))


@pytest.mark.smoke
def test_main_aborts_and_exits_nonzero_on_sibling_sys_exit(tmp_path, monkeypatch, capsys):
    """migrate_all.main() stops running later siblings and exits non-zero on a sibling's sys.exit(N) (the migrate_all exit-signal fix, PR #TODO)."""
    _write_and_patch_scripts_dir(tmp_path, monkeypatch)
    _write_sibling(tmp_path, "migrate_a_fails.py", "import sys\ndef main():\n    sys.exit(2)\n")
    ran_marker = tmp_path / "ran_b"
    _write_sibling(
        tmp_path,
        "migrate_b_should_not_run.py",
        f"def main():\n    open({str(ran_marker)!r}, 'w').close()\n",
    )
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    with pytest.raises(SystemExit) as exc_info:
        migrate_all.main()

    assert exc_info.value.code == 2
    assert not ran_marker.exists()
    assert "All " not in capsys.readouterr().out


@pytest.mark.smoke
def test_main_aborts_and_exits_nonzero_on_sibling_nonzero_return(tmp_path, monkeypatch, capsys):
    """migrate_all.main() exits non-zero, without the success line, when a sibling's main() returns non-zero (the migrate_all exit-signal fix, PR #TODO)."""
    _write_and_patch_scripts_dir(tmp_path, monkeypatch)
    _write_sibling(tmp_path, "migrate_a_fails.py", "def main():\n    return 1\n")
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    with pytest.raises(SystemExit) as exc_info:
        migrate_all.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Finished migrate_a_fails.py" not in out
    assert "All " not in out


@pytest.mark.smoke
def test_main_continues_past_sibling_sys_exit_zero(tmp_path, monkeypatch, capsys):
    """A sibling's sys.exit(0) early-return no longer truncates the whole migrate_all run (the migrate_all exit-signal fix, PR #TODO)."""
    _write_and_patch_scripts_dir(tmp_path, monkeypatch)
    _write_sibling(
        tmp_path, "migrate_a_early_returns.py", "import sys\ndef main():\n    sys.exit(0)\n"
    )
    _write_sibling(tmp_path, "migrate_b_runs_after.py", "def main():\n    pass\n")
    monkeypatch.setattr(sys, "argv", ["migrate_all.py"])

    migrate_all.main()

    out = capsys.readouterr().out
    assert "Finished migrate_a_early_returns.py" in out
    assert "Finished migrate_b_runs_after.py" in out
    assert "All 2 migration(s) completed." in out
