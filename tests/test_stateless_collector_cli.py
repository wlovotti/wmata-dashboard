"""CLI arg-parsing tests for scripts/stateless_collector.py.

Only tests ``build_arg_parser`` — importing the module (and calling this
helper) must never start the polling loop, so these tests never call
``main`` with a real ``argv`` that would fall through to the ``while
True`` loop.
"""

from pathlib import Path

from scripts.stateless_collector import build_arg_parser


def test_archive_root_overrides_default_to_none():
    """Without the override flags, both archive-root args default to None
    so ``main`` falls back to the config-derived directories.
    """
    parser = build_arg_parser()
    args = parser.parse_args(["--agency", "wmata"])
    assert args.archive_root is None
    assert args.vp_archive_root is None


def test_archive_root_overrides_accept_paths(tmp_path):
    """``--archive-root`` and ``--vp-archive-root`` parse as Path objects,
    independently settable, for laptop/local testing against scratch dirs.
    """
    tu_dir = tmp_path / "tu-scratch"
    vp_dir = tmp_path / "vp-scratch"
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--agency",
            "sfmta",
            "--archive-root",
            str(tu_dir),
            "--vp-archive-root",
            str(vp_dir),
        ]
    )
    assert args.archive_root == tu_dir
    assert isinstance(args.archive_root, Path)
    assert args.vp_archive_root == vp_dir
    assert isinstance(args.vp_archive_root, Path)


def test_agency_still_required():
    """``--agency`` remains required even with the new optional overrides present."""
    parser = build_arg_parser()
    try:
        parser.parse_args(["--archive-root", "/tmp/x"])
    except SystemExit as e:
        assert e.code != 0
    else:
        raise AssertionError("expected SystemExit for missing required --agency")
