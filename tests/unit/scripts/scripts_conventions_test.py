"""Enforce structural and style conventions for all scripts in src/pudl/scripts/."""

import re
from pathlib import Path

from pudl.workspace.setup import PUDL_ROOT_PATH

_SRC_PUDL = PUDL_ROOT_PATH / "src" / "pudl"
_SCRIPTS_DIR = _SRC_PUDL / "scripts"
_CLICK_ENTRY_POINT = re.compile(r"@click\.(command|group)\s*[\(\n]")
_MAIN_BLOCK = re.compile(
    r'if __name__ == "__main__":.*?sys\.exit\(main\(\)\)', re.DOTALL
)


def _files_with_click_entry_points(root: Path) -> list[Path]:
    return [
        path
        for path in root.rglob("*.py")
        if not path.is_relative_to(_SCRIPTS_DIR)
        if _CLICK_ENTRY_POINT.search(path.read_text())
    ]


def _script_files() -> list[Path]:
    return [
        path
        for path in _SCRIPTS_DIR.rglob("*.py")
        if path.name != "__init__.py"
        if _CLICK_ENTRY_POINT.search(path.read_text())
    ]


def test_no_click_commands_outside_scripts():
    """All Click CLI entry points must be defined in src/pudl/scripts/.

    See src/pudl/scripts/__init__.py for the conventions that govern this subpackage.
    """
    violations = _files_with_click_entry_points(_SRC_PUDL)
    assert not violations, (
        "Click @click.command / @click.group decorators found outside "
        "src/pudl/scripts/. Move the CLI entry point to that subpackage:\n"
        + "\n".join(f"  {v.relative_to(_SRC_PUDL.parent.parent)}" for v in violations)
    )


def test_scripts_use_sys_exit_main():
    """All CLI scripts must use ``if __name__ == "__main__": sys.exit(main())``.

    See src/pudl/scripts/__init__.py for the conventions that govern this subpackage.
    """
    violations = [
        path for path in _script_files() if not _MAIN_BLOCK.search(path.read_text())
    ]
    assert not violations, (
        'CLI scripts missing ``if __name__ == "__main__": sys.exit(main())``:\n'
        + "\n".join(f"  {v.relative_to(_SRC_PUDL.parent.parent)}" for v in violations)
    )
