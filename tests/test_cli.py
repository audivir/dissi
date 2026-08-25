"""Tests for dissi.cli."""

from __future__ import annotations

import shutil
import sys
from typing import Any

import pytest

from dissi import cli


def test_wrap_program_typer_rejects_force_exec_with_force_py() -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli.wrap_program_typer(
            program="whatever",
            webhook="https://example.com",
            args=None,
            force_exec=True,
            force_py=True,
        )
    assert exc_info.value.code == 1


def test_wrap_program_typer_rejects_grep_with_force_py() -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli.wrap_program_typer(
            program="whatever",
            webhook="https://example.com",
            args=None,
            force_py=True,
            grep=["pattern"],
        )
    assert exc_info.value.code == 1


def test_wrap_program_typer_rejects_missing_program() -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli.wrap_program_typer(
            program="definitely-not-a-real-program-xyz",
            webhook="https://example.com",
            args=None,
        )
    assert exc_info.value.code == 1


def test_wrap_program_typer_clears_title_without_grep(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[str | None] = []

    def fake_wrap_program(
        unused_webhook: str,
        unused_program: str,
        unused_args: list[str] | None,
        unused_force_exec: bool,
        unused_force_py: bool,
        unused_grep: list[str] | None,
        title: str | None,
        unused_only_log: bool,
    ) -> None:
        calls.append(title)

    monkeypatch.setattr(cli, "wrap_program", fake_wrap_program)

    cli.wrap_program_typer(
        program=sys.executable,
        webhook="https://example.com",
        args=None,
        title="ignored title",
    )

    assert calls == [None]
    assert "ignored" in capsys.readouterr().err


def test_wrap_program_typer_resolves_a_bare_program_name_via_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolved: list[str] = []

    def fake_wrap_program(
        unused_webhook: str,
        program: str,
        unused_args: list[str] | None,
        unused_force_exec: bool,
        unused_force_py: bool,
        unused_grep: list[str] | None,
        unused_title: str | None,
        unused_only_log: bool,
    ) -> None:
        resolved.append(program)

    monkeypatch.setattr(cli, "wrap_program", fake_wrap_program)

    cli.wrap_program_typer(program="true", webhook="https://example.com", args=None)

    assert resolved == [shutil.which("true")]


def test_main_parses_argv_loads_env_and_runs_the_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_wrap_program(  # noqa: PLR0913, PLR0917
        webhook: str,
        program: str,
        args: list[str] | None,
        force_exec: bool,
        force_py: bool,
        grep: list[str] | None,
        title: str | None,
        only_log: bool,
    ) -> None:
        calls.append(
            {
                "webhook": webhook,
                "program": program,
                "args": args,
                "force_exec": force_exec,
                "force_py": force_py,
                "grep": grep,
                "title": title,
                "only_log": only_log,
            }
        )

    monkeypatch.setattr(cli, "wrap_program", fake_wrap_program)
    monkeypatch.setattr(sys, "argv", ["dissi", "--webhook", "https://example.com", sys.executable])

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert exc_info.value.code == 0
    assert calls == [
        {
            "webhook": "https://example.com",
            "program": sys.executable,
            "args": None,
            "force_exec": False,
            "force_py": False,
            "grep": None,
            "title": None,
            "only_log": False,
        }
    ]


def test_main_forwards_program_args_when_given(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str] | None] = []

    def fake_wrap_program(  # noqa: PLR0913, PLR0917
        webhook: str,
        program: str,
        args: list[str] | None,
        force_exec: bool,
        force_py: bool,
        grep: list[str] | None,
        title: str | None,
        only_log: bool,
    ) -> None:
        del webhook, program, force_exec, force_py, grep, title, only_log
        calls.append(args)

    monkeypatch.setattr(cli, "wrap_program", fake_wrap_program)
    monkeypatch.setattr(
        sys, "argv", ["dissi", "--webhook", "https://example.com", sys.executable, "--version"]
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert exc_info.value.code == 0
    assert calls == [["--version"]]
