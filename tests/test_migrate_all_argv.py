"""Tests for scripts/migrate_all.py's per-sibling argv isolation (the migrate_all argv-isolation fix, PR #230).

``migrate_all.py`` auto-discovers ``migrate_*.py`` siblings and invokes each
module's ``main()``. Sibling ``main()``s call
``argparse.ArgumentParser().parse_args()`` with no explicit argv, which
defaults to reading ``sys.argv[1:]`` — so without isolation, any flag passed
to ``migrate_all.py`` itself (e.g. ``--yes``) leaks into *every* sibling's
own parser, including destructive migrations that happen to share a flag
name.

These tests exercise ``migrate_all._run_sibling`` directly against
throwaway fake sibling modules, rather than calling ``migrate_all.main()``
(which would discover and execute the *real* ``migrate_*.py`` scripts
against the configured database).
"""

import sys

import pytest

from scripts.migrate_all import _run_sibling


def _write_sibling(tmp_path, name, body):
    """Write a throwaway migrate_*.py-shaped module to tmp_path and return its Path."""
    path = tmp_path / name
    path.write_text(body)
    return path


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
