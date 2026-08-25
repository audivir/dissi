"""Tests for the subprocess-driving helpers in dissi.main."""

from __future__ import annotations

import io
import logging
import sys
from typing import TYPE_CHECKING

from dissi.main import log_buffer, run_grep, run_program

if TYPE_CHECKING:
    import re

    import pytest


def test_run_program_streams_stderr_and_returns_exit_code(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stderr = run_program(
        [sys.executable, "-c", "import sys; print('boom', file=sys.stderr); sys.exit(3)"]
    )

    assert code == 3
    assert stderr == "boom\n"
    assert "boom" in capsys.readouterr().err


def test_run_program_defaults_to_sys_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", [sys.executable, "-c", "import sys; sys.exit(4)"])

    code, stderr = run_program()

    assert code == 4
    assert stderr == ""


def test_log_buffer_returns_full_text_only_for_stderr() -> None:
    logger = logging.getLogger("dissi-test-log-buffer")
    compiled: list[re.Pattern[str]] = []

    stdout_full = log_buffer(
        io.StringIO("first\nsecond"), err=False, compiled=compiled, logger=logger, only_log=False
    )
    stderr_full = log_buffer(
        io.StringIO("first\nsecond"), err=True, compiled=compiled, logger=logger, only_log=False
    )

    assert stdout_full == ""
    assert stderr_full == "first\nsecond"


def test_run_grep_logs_matches_from_stdout_and_stderr(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("dissi-test-run-grep")
    caplog.set_level(logging.INFO, logger=logger.name)
    script = (
        "import sys\nprint('hello stdout')\nprint('hello stderr', file=sys.stderr)\nsys.exit(2)\n"
    )

    code, stderr = run_grep(
        ["hello"], only_log=True, logger=logger, args=[sys.executable, "-c", script]
    )

    assert code == 2
    assert stderr == "hello stderr\n"
    logged_messages = [record.message for record in caplog.records]
    assert "hello stdout" in logged_messages
    assert "hello stderr" in logged_messages


def test_run_grep_defaults_to_sys_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger("dissi-test-run-grep-default-argv")
    monkeypatch.setattr(
        sys, "argv", [sys.executable, "-c", "print('hello'); import sys; sys.exit(1)"]
    )

    code, _ = run_grep(["hello"], only_log=True, logger=logger)

    assert code == 1
