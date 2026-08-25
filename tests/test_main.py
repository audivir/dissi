"""Tests for dissi.main."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from dissi.main import DiscordWebhookConfig, DiscordWebhookFormatter, cap_cmd, cap_cwd, log_line

if TYPE_CHECKING:
    from collections.abc import Sequence

    from _typeshed import StrPath


@pytest.mark.parametrize(
    ("args", "expected"), [(["echo", "hello world"], "echo 'hello world'"), (["a" * 40], "a" * 20)]
)
def test_cap_cmd_truncates(args: Sequence[StrPath], expected: str) -> None:
    assert cap_cmd(args, max_chars=20) == expected


def test_cap_cwd() -> None:
    assert cap_cwd(Path("/" + "d" * 20), max_chars=5) == "d" * 5


def test_config_rejects_prefixes_of_differing_length() -> None:
    with pytest.raises(ValueError, match="All prefixes must have the same length"):
        DiscordWebhookConfig(prefixes={"INFO": "+", "ERROR": "--"})


def test_config_computes_max_len_from_template_length() -> None:
    config = DiscordWebhookConfig(template="{text}")
    assert config.max_len == 2000


def test_config_max_len_accounts_for_rendered_title_length() -> None:
    config = DiscordWebhookConfig(template="{title}{text}", title="mytitle")
    assert config.max_len == 2000 - len(config.title)


def make_record(msg: str) -> logging.LogRecord:
    return logging.LogRecord("dissi", logging.INFO, __file__, 1, msg, None, None)


@pytest.mark.parametrize(
    ("message", "expected_lines"),
    [
        ("hello", ["+  │hello"]),
        ("first\nmiddle\nlast", ["+  │first", "+  ├middle", "+  └last"]),
    ],
)
def test_format_uses_separators_by_line_position(message: str, expected_lines: list[str]) -> None:
    lines, _ = DiscordWebhookFormatter().format(make_record(message))
    assert lines == expected_lines


def test_format_splits_line_longer_than_max_len() -> None:
    config = DiscordWebhookConfig(template="{text}")
    formatter = DiscordWebhookFormatter(config=config)

    lines, _ = formatter.format(make_record("x" * config.max_len))

    assert len(lines) == 2
    assert lines[1].startswith("+  ↳")


def test_format_reports_the_actual_rendered_length_of_a_split_line() -> None:
    config = DiscordWebhookConfig(template="{text}")
    formatter = DiscordWebhookFormatter(config=config)

    lines, reported_len = formatter.format(make_record("x" * config.max_len))

    assert reported_len == sum(len(line) for line in lines) + len(lines) - 1


@pytest.mark.parametrize(
    ("line", "expected_logged"),
    [
        ("an ERROR occurred", "an ERROR occurred"),
        ("\x1b[31mERROR\x1b[0m", "ERROR"),
    ],
)
def test_log_line_matched_lines_are_written_and_logged_without_ansi(
    line: str,
    expected_logged: str,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger(f"dissi-test-match-{expected_logged}")
    caplog.set_level(logging.INFO, logger=logger.name)
    compiled = [re.compile("ERROR")]

    log_line(line, err=False, compiled=compiled, logger=logger, only_log=True)

    assert line in capsys.readouterr().out
    assert caplog.records[0].message == expected_logged


@pytest.mark.parametrize(("only_log", "expected_stdout"), [(True, ""), (False, "all good\n")])
def test_log_line_only_log_gates_unmatched_lines(
    only_log: bool, expected_stdout: str, capsys: pytest.CaptureFixture[str]
) -> None:
    logger = logging.getLogger(f"dissi-test-unmatched-{only_log}")
    compiled = [re.compile("ERROR")]

    log_line("all good", err=False, compiled=compiled, logger=logger, only_log=only_log)

    assert capsys.readouterr().out == expected_stdout


def test_log_line_ignores_empty_lines(capsys: pytest.CaptureFixture[str]) -> None:
    logger = logging.getLogger("dissi-test-empty-line")
    compiled = [re.compile("ERROR")]

    log_line("", err=False, compiled=compiled, logger=logger, only_log=False)

    assert capsys.readouterr().out == ""
