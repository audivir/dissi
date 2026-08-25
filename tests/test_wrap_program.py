"""Integration tests for dissi.main.wrap_program."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any

import pytest
import requests

from dissi import main
from dissi.main import wrap_program

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(autouse=True)
def no_real_signal_handling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main, "setup_signal_handling", lambda unused_handler: None)


@pytest.fixture(autouse=True)
def restore_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", list(sys.argv))


@pytest.fixture(autouse=True)
def clean_dissi_logger() -> Iterator[None]:
    logger = logging.getLogger("dissi")
    yield
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


@pytest.fixture
def posted(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        captured.append(json)
        response = requests.Response()
        response.status_code = 200
        return response

    monkeypatch.setattr(requests, "post", fake_post)
    return captured


def embed_titles(posted: list[dict[str, Any]]) -> list[str]:
    return [entry["embeds"][0]["title"] for entry in posted if "embeds" in entry]


def test_wrap_program_runs_a_script_via_runpy_and_reports_success(
    tmp_path: Path, posted: list[dict[str, Any]]
) -> None:
    script = tmp_path / "ok.py"
    script.write_text("x = 1 + 1\n")

    wrap_program("https://example.com/webhook", str(script), None, False, False, None, None, False)

    titles = embed_titles(posted)
    assert any("started" in title for title in titles)
    assert any("succeeded" in title for title in titles)


def test_wrap_program_reports_a_raised_exception_and_reraises(
    tmp_path: Path, posted: list[dict[str, Any]]
) -> None:
    script = tmp_path / "fails.py"
    script.write_text("raise RuntimeError('boom')\n")

    with pytest.raises(RuntimeError, match="boom"):
        wrap_program(
            "https://example.com/webhook", str(script), None, False, False, None, None, False
        )

    titles = embed_titles(posted)
    assert any("failed" in title for title in titles)


def test_wrap_program_force_exec_reports_the_subprocess_exit_code(
    posted: list[dict[str, Any]],
) -> None:
    wrap_program(
        "https://example.com/webhook",
        sys.executable,
        ["-c", "import sys; sys.exit(9)"],
        True,
        False,
        None,
        None,
        False,
    )

    titles = embed_titles(posted)
    assert any("failed" in title for title in titles)
    descriptions = [entry["embeds"][0]["description"] for entry in posted if "embeds" in entry]
    assert any("Return Code: 9" in description for description in descriptions)


def test_wrap_program_grep_reports_the_subprocess_exit_code(posted: list[dict[str, Any]]) -> None:
    wrap_program(
        "https://example.com/webhook",
        sys.executable,
        ["-c", "print('hello'); import sys; sys.exit(0)"],
        False,
        False,
        ["hello"],
        None,
        True,
    )

    titles = embed_titles(posted)
    assert any("succeeded" in title for title in titles)


def test_wrap_program_falls_back_to_a_subprocess_on_syntax_error(
    tmp_path: Path, posted: list[dict[str, Any]]
) -> None:
    script = tmp_path / "bad.py"
    script.write_text("#!/usr/bin/env python3\ndef broken(:\n")
    script.chmod(0o755)

    wrap_program("https://example.com/webhook", str(script), None, False, False, None, None, False)

    titles = embed_titles(posted)
    assert any("failed" in title for title in titles)


def test_wrap_program_reraises_a_value_error_from_the_script(
    tmp_path: Path, posted: list[dict[str, Any]]
) -> None:
    script = tmp_path / "value_error.py"
    script.write_text("raise ValueError('boom')\n")

    with pytest.raises(ValueError, match="boom"):
        wrap_program(
            "https://example.com/webhook", str(script), None, False, False, None, None, False
        )

    titles = embed_titles(posted)
    assert any("failed" in title for title in titles)


def test_wrap_program_reraises_a_syntax_error_when_force_py_is_set(
    tmp_path: Path, posted: list[dict[str, Any]]
) -> None:
    script = tmp_path / "bad_force_py.py"
    script.write_text("def broken(:\n")

    with pytest.raises(SyntaxError):
        wrap_program(
            "https://example.com/webhook", str(script), None, False, True, None, None, False
        )

    titles = embed_titles(posted)
    assert any("failed" in title for title in titles)
