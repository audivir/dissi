"""Discord Webhook handler and wrapper."""

from __future__ import annotations

import atexit
import concurrent.futures
import http
import logging
import os
import re
import runpy
import shlex
import signal
import socket
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from enum import IntEnum
from pathlib import Path
from threading import Lock, Semaphore, Timer
from typing import IO, TYPE_CHECKING, Any

import requests
from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from logging import Logger, LogRecord, _FormatStyle, _Level
    from types import FrameType

    from _typeshed import StrPath


SIGINT_EXIT_CODE = 130

# regex pattern for common ANSI color/style codes.
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
MAX_RETRIES = 3
SIGNALS_TO_HANDLE = (signal.SIGTERM,)


class Exit(Exception):  # noqa: N818
    """Mimics SystemExit while carrying the captured stderr of the wrapped program."""

    def __init__(self, code: sys._ExitCode, stderr: str) -> None:
        """Initializes the exit with a code and captured stderr."""
        super().__init__()
        self.code = code
        self.stderr = stderr


def cap_cmd(args: Sequence[StrPath] | None = None, max_chars: int = 100) -> str:
    """Joins and shell-quotes args, defaulting to `sys.argv`, truncated to max_chars."""
    if not args:
        args = sys.argv
    return " ".join(shlex.quote(str(a)) for a in args)[:max_chars]


def cap_cwd(cwd: StrPath | None = None, max_chars: int = 100) -> str:
    """Returns cwd, defaulting to the current directory, truncated to its last max_chars."""
    if not cwd:
        cwd = Path().cwd()
    return str(cwd)[-max_chars:]


def setup_signal_handling(handler: DiscordWebhookHandler) -> None:
    """Reports handled signals to Discord before restoring default signal handling."""

    def handle_signal(signo: int, frame: FrameType | None) -> None:
        del frame  # unused
        handler.send()  # flush already-buffered messages, `_send` below would drop them.
        handler.send_embed(
            f"received {signal.Signals(signo).name}",
            f"Command: {cap_cmd()}\nWorking Directory: {cap_cwd()}",
            DiscordColors.RED,
        )
        # propagate the original handling.
        signal.signal(signo, signal.SIG_DFL)
        os.kill(os.getpid(), signo)

    for signo in SIGNALS_TO_HANDLE:
        signal.signal(signo, handle_signal)


def run_program(args: list[str] | None = None) -> tuple[int, str]:
    """Runs args, defaulting to `sys.argv`, as a subprocess, streaming stderr live."""
    if not args:
        args = sys.argv
    with subprocess.Popen(args, stderr=subprocess.PIPE, text=True) as proc:  # noqa: S603
        stderr_lines: list[str] = []
        if proc.stderr:  # pragma: no branch
            for line in proc.stderr:
                sys.stderr.write(line)
                stderr_lines.append(line)
        code = proc.wait()
    stderr = "".join(stderr_lines)
    return code, stderr


def log_line(
    line: str, err: bool, compiled: list[re.Pattern[str]], logger: Logger, only_log: bool
) -> None:
    """Logs line to Discord if it matches compiled and forwards it to stdout/stderr."""
    if not line:
        return
    level = logging.ERROR if err else logging.INFO
    stream = sys.stderr if err else sys.stdout

    if any(c.search(line) for c in compiled):
        logger.log(level, ANSI_PATTERN.sub("", line))
        stream.write(line + "\n")  # always log matched lines.
    elif not only_log:
        stream.write(line + "\n")


def log_buffer(
    stream: IO[str], err: bool, compiled: list[re.Pattern[str]], logger: Logger, only_log: bool
) -> str:
    """Reads stream line by line, forwarding each to `log_line`, and returns its full text."""
    full: list[str] = []
    buffer = ""
    for line in stream:
        if err:
            full.append(line)
        buffer += line
        if "\n" in buffer:
            first, buffer = buffer.split("\n", maxsplit=1)
            log_line(first, err, compiled, logger, only_log)
    log_line(buffer, err, compiled, logger, only_log)

    return "".join(full)


def run_grep(
    grep: list[str], only_log: bool, logger: Logger, args: list[str] | None = None
) -> tuple[int, str]:
    """Runs args, defaulting to `sys.argv`, and logs matching stdout/stderr lines to Discord."""
    if not args:
        args = sys.argv
    compiled = [re.compile(p) for p in grep]

    with subprocess.Popen(  # noqa: S603
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ) as proc:
        if not proc.stdout or not proc.stderr:  # pragma: no cover
            raise RuntimeError("Streams not piped")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            _ = executor.submit(
                log_buffer,
                stream=proc.stdout,
                err=False,
                compiled=compiled,
                logger=logger,
                only_log=only_log,
            )
            f_err = executor.submit(
                log_buffer,
                stream=proc.stderr,
                err=True,
                compiled=compiled,
                logger=logger,
                only_log=only_log,
            )

            stderr = f_err.result()
        code = proc.wait()
    return code, stderr


def flush(self: Logger) -> None:
    """Sends all buffered messages of every `DiscordWebhookHandler` attached to self."""
    for handler in self.handlers:
        if isinstance(handler, DiscordWebhookHandler):
            handler.send()


# adds a flush method to Logger for forced sending of all buffered data.
logging.Logger.flush = flush  # type: ignore[attr-defined]

DISCORD_TIMEOUT = 5
MAX_DISCORD_MESSAGE_LEN = 2000
DIFF_MESSAGE_TEMPLATE = """\
### {hostname}{title}
```diff
{text}
```
"""
DIFF_LEVEL_PREFIXES = {
    "DEBUG": "===",
    "INFO": "+  ",
    "WARNING": "W  ",
    "ERROR": "-  ",
    "CRITICAL": "-!!",
}


class DiscordColors(IntEnum):
    """Stores decimal color codes for Discord embeds."""

    RED = 16711680
    YELLOW = 16776960
    GREEN = 65280
    LIGHT_BLUE = 3447003


class DiscordWebhookConfig:
    """Stores the message template, level prefixes, and computed length limits."""

    def __init__(
        self,
        template: str | None = None,
        prefixes: Mapping[str, str] | None = None,
        title: str | None = None,
    ) -> None:
        """Initializes a Discord Webhook config.

        Raises:
            ValueError: If prefixes has values of differing length.
        """
        self.template = template or DIFF_MESSAGE_TEMPLATE
        self.prefixes = prefixes or DIFF_LEVEL_PREFIXES
        self.title = f": {title}" if title else ""
        template_len = len(
            self.template.format(hostname=socket.gethostname(), title=self.title, text="")
        )
        self.max_len = MAX_DISCORD_MESSAGE_LEN - template_len

        prefix_lens = {len(v) for v in self.prefixes.values()}
        self.prefix_len = prefix_lens.pop()
        if prefix_lens:  # should be empty after removing the only element.
            raise ValueError("All prefixes must have the same length")


class DiscordWebhookFormatter(logging.Formatter):
    """Formats LogRecords into Discord-ready message lines."""

    def __init__(  # noqa: PLR0913
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, Any] | None = None,
        config: DiscordWebhookConfig | None = None,
    ) -> None:
        """Initializes a Discord Webhook formatter."""
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        self.config = config or DiscordWebhookConfig()

    @override
    def format(self, record: LogRecord) -> tuple[list[str], int]:  # type:ignore[override]
        """Formats a LogRecord into Discord-ready message lines.

        Returns:
            Formatted message lines and their combined character length, including separators.
        """
        level_prefix = self.config.prefixes.get(record.levelname, " " * self.config.prefix_len)

        formatted_lines: list[str] = []
        formatted_lines_len = 0

        lines = record.getMessage().split("\n")

        for ix, line in enumerate(lines):
            # a multi-line log message uses different separators per line, e.g.
            # logger.info('1st line\nnext line\nlast line') becomes:
            # +  │1st line
            # +  ├next line
            # +  └last line
            if ix == 0:
                separator = "│"
            elif ix == len(lines) - 1:
                separator = "└"
            else:
                separator = "├"

            split_len = self.config.max_len - self.config.prefix_len - 1  # -1 = separator
            if len(line) > split_len:
                # line too long for a single Discord message, split across multiple messages.
                formatted_lines.append(level_prefix + separator + line[:split_len])
                formatted_lines_len += len(formatted_lines[-1])

                # use a different separator to show that the line was split.
                separator = "↳"
                for segment in [
                    line[x : x + split_len] for x in range(split_len, len(line), split_len)
                ]:
                    formatted_lines.append(level_prefix + separator + segment)
                    formatted_lines_len += len(formatted_lines[-1])
            else:
                formatted_lines.append(level_prefix + separator + line)
                formatted_lines_len += len(formatted_lines[-1])

        # add \n to char count.
        formatted_lines_len += len(formatted_lines) - 1

        return formatted_lines, formatted_lines_len


class DiscordWebhookHandler(logging.Handler):
    """Manages buffering and rate-limited delivery of log records to a Discord webhook."""

    def __init__(
        self,
        webhook_url: str,
        auto_flush: bool = False,
        formatter: DiscordWebhookFormatter | None = None,
        level: _Level = 0,
    ) -> None:
        """Initializes the Discord Webhook handler."""
        super().__init__(level)
        self.webhook_url = webhook_url
        self.formatter: DiscordWebhookFormatter = formatter or DiscordWebhookFormatter()  # type:ignore[mutable-override]
        self.config = self.formatter.config
        # limit concurrent requests to avoid hitting rate limits.
        self._sema = Semaphore(5)
        self._lock = Lock()

        self.auto_flush = auto_flush
        self.last_emit = time.monotonic()
        self.timer_interval = 60 / 30  # 30 messages per minute
        self._start_timer()

        # buffer for storing shorter logs and sending them in larger batches.
        self.buffer: list[tuple[LogRecord, list[str]]] = []
        self.buffer_message_len = 0

        # send all remaining buffered messages before app exit.
        atexit.register(self.send)

    def _start_timer(self) -> None:
        self.timer = Timer(self.timer_interval, self._timer_tick)
        self.timer.daemon = True
        self.timer.start()

    def _timer_tick(self) -> None:
        with self._lock:
            self.send()
        self._start_timer()

    @override
    def close(self) -> None:
        """Flushes buffered messages and cancels the timer before closing."""
        self.timer.cancel()
        with self._lock:
            self.send()
        return super().close()

    def _emit(
        self, record: LogRecord, formatted_lines: list[str], formatted_lines_len: int
    ) -> None:
        if formatted_lines_len >= self.config.max_len:
            # new message can't fit into the buffer, send buffered message and each new line.
            for line in formatted_lines:
                if self.buffer_message_len + len(line) >= self.config.max_len:
                    self.send()

                self.buffer.append((record, [line]))
                self.buffer_message_len += len(line) + 1

            self.send()
        elif self.buffer_message_len + formatted_lines_len >= self.config.max_len:
            # buffered message plus new message is too large, but new message fits into an
            # empty buffer: send buffered message and move new message into buffer.
            self.send()

            self.buffer.append((record, formatted_lines))
            self.buffer_message_len += formatted_lines_len + 1
        else:
            # buffered message plus new message fits into buffer, append it.
            self.buffer.append((record, formatted_lines))
            self.buffer_message_len += formatted_lines_len + 1

        if self.auto_flush:
            self.send()

    @override
    def emit(self, record: LogRecord) -> None:
        """Adds the log record to the buffered messages."""
        if record.exc_info is not None:
            self.send()  # send previous buffered message.
            self.send_error(record)
            return

        try:
            formatted: tuple[list[str], int] = self.format(record)  # type: ignore[assignment]
            formatted_lines, formatted_lines_len = formatted

            with self._lock:
                self.last_emit = time.monotonic()
                self._emit(record, formatted_lines, formatted_lines_len)

        except Exception:  # noqa: BLE001
            self.handleError(record)

    def send_embed(
        self,
        suffix: str,
        description: str | None = None,
        color: int | None = None,
        fields: Sequence[tuple[str, str]] | None = None,
        record: LogRecord | None = None,
    ) -> None:
        """Sends a Discord embed message."""
        embed: dict[str, Any] = {"title": f"{socket.gethostname()[:100]} {suffix}!"}
        if description:
            embed["description"] = description
        if color is not None:
            embed["color"] = color
        if fields:
            embed["fields"] = [{"name": name, "value": value} for name, value in fields]

        payload = {"embeds": [embed]}

        if not record:
            record = logging.LogRecord("Dummy Record", 0, "", 0, None, None, None)

        self._send(record, payload)

    def send_error(self, record: LogRecord) -> None:
        """Sends a record carrying exception info.

        Raises:
            ValueError: If record has no exception info.
        """
        if not record.exc_info:
            raise ValueError(f"No error found in record: {record}")

        exc_type, exc_val, exc_tb = record.exc_info

        code = 1
        suffix = "failed"
        color = DiscordColors.RED

        tb_text = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))

        if isinstance(exc_val, KeyboardInterrupt):
            code = SIGINT_EXIT_CODE
            suffix = "interrupted"
            color = DiscordColors.YELLOW
        elif isinstance(exc_val, (SystemExit, Exit)):
            code = exc_val.code if isinstance(exc_val.code, int) else 0
            if code == 0:
                suffix = "succeeded"
                color = DiscordColors.GREEN
            if code == SIGINT_EXIT_CODE:
                suffix = "interrupted"
                color = DiscordColors.YELLOW
            if isinstance(exc_val, Exit):
                tb_text = exc_val.stderr

        description = f"Command: {cap_cmd()}\nWorking Directory: {cap_cwd()}\nReturn Code: {code}"
        if msg := record.getMessage():
            description += f"\nMessage: {msg[:500]}"
        # limit length.
        fields = [("Traceback", f"```python\n{tb_text[-1000:]}```")] if code and tb_text else None

        self.send_embed(suffix, description, color, fields, record)

    def send(self) -> None:
        """Sends the buffered messages."""
        if self.buffer_message_len == 0:
            # buffer is empty, skip sending.
            return

        # prepare body of message.
        log_message = ""
        for b in self.buffer:
            for line in b[1]:
                log_message += line + "\n"

        record = self.buffer[-1][0]

        # prepare message for sending, excluding the last \n char.
        payload = {
            "content": self.config.template.format(
                hostname=socket.gethostname(), title=self.config.title, text=log_message[:-1]
            )
        }

        self._send(record, payload)

    def _send(self, record: LogRecord, payload: dict[str, Any]) -> None:
        """Sends a payload, retrying on rate limits and recording failures on record."""
        r: requests.Response | None = None
        # retry loop adapted from discord-logger, see README.md.
        for _ in range(MAX_RETRIES):
            with self._sema:
                r = requests.post(self.webhook_url, json=payload, timeout=DISCORD_TIMEOUT)

            if r.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:
                retry_after = int(r.headers.get("Retry-After", 500)) / 100.0
                time.sleep(retry_after)
            elif r.status_code < http.HTTPStatus.BAD_REQUEST:
                self.last_sent = datetime.now(tz=timezone.utc)
                # clear on success.
                self.buffer.clear()
                self.buffer_message_len = 0
                return

        try:
            if r is not None:  # pragma: no branch
                r.raise_for_status()
        except Exception:  # noqa: BLE001
            self.handleError(record)
            # clear on failure.
            self.buffer.clear()
            self.buffer_message_len = 0


def wrap_program(  # noqa: PLR0913, PLR0917
    webhook: str,
    program: str,
    args: list[str] | None,
    force_exec: bool,
    force_py: bool,
    grep: list[str] | None,
    title: str | None,
    only_log: bool,
) -> None:
    """Wraps a Python script or other program in a Discord logging environment."""
    # set up logger.
    logger = logging.getLogger("dissi")
    logger.setLevel(logging.INFO)
    config = DiscordWebhookConfig(title=title)
    formatter = DiscordWebhookFormatter(config=config)
    handler = DiscordWebhookHandler(webhook_url=webhook, formatter=formatter)
    logger.addHandler(handler)

    # set new args.
    sys.argv = [program, *(args or [])]

    # set up signal handling.
    setup_signal_handling(handler)

    # log the start of the program.
    handler.send_embed(
        "started",
        f"Command: {cap_cmd()}\nWorking Directory: {cap_cwd()}",
        DiscordColors.LIGHT_BLUE,
    )

    code = 0
    stderr = ""
    try:
        # try to run as a Python module first, falling back to a normal executable if that fails.
        if grep:
            code, stderr = run_grep(grep, only_log, logger)
        elif force_exec:
            code, stderr = run_program()
        else:
            run_as_py = True
            try:
                runpy.run_path(sys.argv[0], run_name="__main__")
            except (SyntaxError, ValueError) as e:
                if isinstance(e, ValueError) and e.args != (
                    "source code string cannot contain null bytes",
                ):
                    raise
                if force_py:
                    raise
                run_as_py = False
            if not run_as_py:
                code, stderr = run_program()

    except BaseException:
        # log any occurring error.
        logger.exception("")
        raise
    # log successful exits or failed exits from normal executables.
    logger.error("", exc_info=(Exit, Exit(code, stderr), None))
