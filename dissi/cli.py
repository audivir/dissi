"""CLI for dissi."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Annotated, Any

import doctyper
import dotenv

from dissi.main import wrap_program


def eprint(*args: Any) -> None:
    """Prints to stderr."""
    print(*args, file=sys.stderr)  # noqa: T201


def wrap_program_typer(  # noqa: PLR0913, PLR0917
    program: str,
    webhook: Annotated[str, doctyper.Option(envvar="DISCORD_WEBHOOK")],
    args: Annotated[list[str] | None, doctyper.Argument()] = None,
    force_exec: bool = False,
    force_py: bool = False,
    grep: list[str] | None = None,
    title: str | None = None,
    only_log: bool = False,
) -> None:
    """Wraps a Python script or other program in a Discord logging environment.

    Args:
        program: Script or program to wrap.
        webhook: Discord Webhook URL.
        args: Arguments for the wrapped program.
        force_exec: Force the use of subprocess instead of trying to use runpy.
        force_py: Force the use of runpy instead of trying to use subprocess.
        grep: Regex patterns to match stdout and stderr.
        title: Title.
        only_log: Forward only matched stdout/stderr lines.
    """
    if force_exec and force_py:
        eprint("--force-exec not allowed with --force-py")
        raise SystemExit(1)

    if grep and force_py:
        eprint("--force-py is unsupported, when --grep patterns are provided")
        raise SystemExit(1)

    if not grep and (title or only_log):
        eprint("--title and --only-log are ignored without --grep patterns")
        title = None

    orig_program = program
    if os.sep not in program:
        # try to locate program in PATH
        executable = shutil.which(program)
        if executable:
            program = executable

    # try to locate the executable
    if not Path(program).is_file():
        eprint(f"{orig_program} not found")
        raise SystemExit(1)

    wrap_program(webhook, program, args, force_exec, force_py, grep, title, only_log)


def main() -> int:
    """Entrypoint for wrapper."""
    context = {"ignore_unknown_options": True}
    dotenv.load_dotenv(".env")
    app = doctyper.DocTyper()
    app.command(context_settings=context)(wrap_program_typer)
    app()

    return 0  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
