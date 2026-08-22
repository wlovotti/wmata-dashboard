"""CLI arg-parsing tests for scripts/stateless_collector.py.

Only tests ``build_arg_parser`` and ``validate_archive_root_pairing`` —
importing the module (and calling these helpers) must never start the
polling loop, so these tests never call ``main`` with a real ``argv``
that would fall through to the ``while True`` loop.
"""

from pathlib import Path

import pytest

from scripts.stateless_collector import build_arg_parser, validate_archive_root_pairing


def test_archive_root_overrides_default_to_none():
    """Without the override flags, both archive-root args default to None
    so ``main`` falls back to the config-derived directories.
    """
    parser = build_arg_parser()
    args = parser.parse_args(["--agency", "wmata"])
    assert args.archive_root is None
    assert args.vp_archive_root is None
    validate_archive_root_pairing(args, parser)  # neither set — must not raise


def test_both_archive_root_overrides_parse_and_validate(tmp_path):
    """``--archive-root`` and ``--vp-archive-root`` parse as Path objects
    and pass validation when both are set together, for laptop/local
    testing against scratch dirs.
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
    validate_archive_root_pairing(args, parser)  # both set — must not raise


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--archive-root", "/tmp/tu-scratch"],
        ["--vp-archive-root", "/tmp/vp-scratch"],
    ],
    ids=["archive-root-only", "vp-archive-root-only"],
)
def test_half_override_is_rejected(extra_args):
    """Passing only one of the two overrides must be rejected — a
    half-override would leave the other real archive tree exposed to the
    upload-and-prune cycle. ``parser.error`` prints usage and exits 2.
    """
    parser = build_arg_parser()
    args = parser.parse_args(["--agency", "wmata", *extra_args])
    with pytest.raises(SystemExit) as exc_info:
        validate_archive_root_pairing(args, parser)
    assert exc_info.value.code == 2


def test_agency_still_required():
    """``--agency`` remains required even with the new optional overrides present."""
    parser = build_arg_parser()
    try:
        parser.parse_args(["--archive-root", "/tmp/x"])
    except SystemExit as e:
        assert e.code != 0
    else:
        raise AssertionError("expected SystemExit for missing required --agency")
