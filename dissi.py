"""Discord Webhook handler and wrapper."""

from __future__ import annotations

import atexit
import http
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
import traceback
from enum import IntEnum
from pathlib import Path
from threading import Semaphore
from typing import TYPE_CHECKING, Annotated, Any

import dotenv
import requests
import typer

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from logging import Logger, LogRecord, _FormatStyle, _Level

__version__ = "0.1.0"


def _flush(self: Logger) -> None:
    for handler in self.handlers:
        if isinstance(handler, DiscordWebhookHandler):
            handler.send()


# Adding flush method to Logger class
# used for forced sending all buffered data
logging.Logger.flush = _flush

DISCORD_TIMEOUT = 5
MAX_DISCORD_MESSAGE_LEN = 2000
DIFF_MESSAGE_TEMPLATE = """\
### {hostname}
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
    """Decimal color codes for Discord."""

    RED = 16711680
    YELLOW = 16776960
    GREEN = 65280
    LIGHT_BLUE = 3447003


class DiscordWebhookConfig:
    """Config for the template and prefixes."""

    def __init__(
        self, template: str | None = None, prefixes: Mapping[str, str] | None = None
    ) -> None:
        """Initialize a Discord Webhook config."""
        self.template = template or DIFF_MESSAGE_TEMPLATE
        self.prefixes = prefixes or DIFF_LEVEL_PREFIXES
        template_len = len(self.template.format(hostname=socket.gethostname(), text=""))
        self.max_len = MAX_DISCORD_MESSAGE_LEN - template_len

        prefix_lens = {len(v) for v in self.prefixes.values()}
        self.prefix_len = prefix_lens.pop()
        if prefix_lens:  # should be empty after removing the only element
            raise ValueError("All prefixes must have the same length")


class DiscordWebhookFormatter(logging.Formatter):
    """Format LogRecords for Discord."""

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: _FormatStyle = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, Any] | None = None,
        config: DiscordWebhookConfig | None = None,
    ) -> None:
        """Initialize a Discord Webhook formatter."""
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)
        self.config = config or DiscordWebhookConfig()

    def format(self, record: LogRecord) -> tuple[list[str], int]:
        """Format a LogRecord for Discord."""
        level_prefix = self.config.prefixes.get(record.levelname, " " * self.config.prefix_len)

        formatted_lines: list[str] = []
        formatted_lines_len = 0

        lines = record.getMessage().split("\n")

        for ix, line in enumerate(lines):
            # if single log message has multiple lines, show it with using different separators
            # Example:
            # Log message: logger.info('1st line\nnext line\nlast line')
            # Formatted output:
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
                # line is too long to send in single discord message,
                # we need to split it in multiple messages
                formatted_lines.append(level_prefix + separator + line[:split_len])
                formatted_lines_len += split_len

                # use different separator to show, that line was split into multiple messages
                separator = "↳"
                for segment in [
                    line[x : x + split_len] for x in range(split_len, len(line), split_len)
                ]:
                    formatted_lines.append(level_prefix + separator + segment)
                    formatted_lines_len += len(formatted_lines[-1])
            else:
                formatted_lines.append(level_prefix + separator + line)
                formatted_lines_len += len(formatted_lines[-1])

        # add \n to char count
        formatted_lines_len += len(formatted_lines) - 1

        return formatted_lines, formatted_lines_len


class DiscordWebhookHandler(logging.Handler):
    """A handler sending messages to Discord."""

    def __init__(
        self,
        webhook_url: str,
        auto_flush: bool = False,
        formatter: DiscordWebhookFormatter | None = None,
        level: _Level = 0,
    ) -> None:
        """Initialize the Discord Webhook handler."""
        super().__init__(level)
        self.webhook_url = webhook_url
        self.formatter: DiscordWebhookFormatter = formatter or DiscordWebhookFormatter()
        self.config = self.formatter.config
        self._lock = Semaphore(5)

        self.auto_flush = auto_flush

        # buffer for storing shorter logs and sending them in larger batches
        self.buffer: list[tuple[LogRecord, list[str]]] = []
        self.buffer_message_len = 0

        # send all remaining buffered messages before app exit
        atexit.register(self.send)

    def emit(self, record: LogRecord) -> None:
        """Add the log record to the buffered messages."""
        if record.exc_info is not None:
            self.send()  # send previous buffered message
            self.send_error(record)
            return

        try:
            formatted: tuple[list[str], int] = self.format(record)
            formatted_lines, formatted_lines_len = formatted

            if formatted_lines_len >= self.config.max_len:
                # new message is too large, new message can't fit info buffer,
                # send buffered message and also send new message
                for line in formatted_lines:
                    if self.buffer_message_len + len(line) >= self.config.max_len:
                        self.send()

                        self.buffer.append((record, [line]))
                        self.buffer_message_len += len(line) + 1
                    else:
                        self.buffer.append((record, [line]))
                        self.buffer_message_len += len(line) + 1

                self.send()
            elif self.buffer_message_len + formatted_lines_len >= self.config.max_len:
                # buffered message + new message is too large, but new message can fit into buffer,
                # send buffered message and move new message into buffer
                self.send()

                self.buffer.append((record, formatted_lines))
                self.buffer_message_len += formatted_lines_len + 1
            else:
                # buffered message + new message fits info buffer, append it
                self.buffer.append((record, formatted_lines))
                self.buffer_message_len += formatted_lines_len + 1

            if self.auto_flush:
                self.send()
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
        """Send a Discord embed message."""
        embed = {"title": f"{socket.gethostname()[:100]} {suffix}!"}
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
        """Send a record with an exception info."""
        if not record.exc_info:
            raise ValueError(f"No error found in record: {record}")

        exc_type, exc_val, exc_tb = record.exc_info
        code = 1
        suffix = "failed"
        color = DiscordColors.RED
        tb_text = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
        if isinstance(exc_val, KeyboardInterrupt):
            suffix = "interrupted"
            color = DiscordColors.YELLOW
        elif isinstance(exc_val, SystemExit):
            code = exc_val.code
            if not isinstance(code, int):
                code = 0
            if code == 0:
                suffix = "succeed"
                color = DiscordColors.GREEN

        cap_cmd = " ".join(sys.argv)[:100]
        cap_path = str(Path().cwd())[-100:]

        description = f"Command: {cap_cmd}\nWorking Directory: {cap_path}\nReturn Code: {code}"
        if msg := record.getMessage():
            description += f"\nMessage: {msg[:500]}"
        # limit length
        fields = [("Traceback", f"```python\n{tb_text[-1000:]}```")] if code and tb_text else None

        self.send_embed(suffix, description, color, fields, record)

    def send(self) -> None:
        """Send the buffered messages."""
        if self.buffer_message_len == 0:
            # if buffer is empty, skip sending
            return

        # prepare body of message
        log_message = ""
        for b in self.buffer:
            for line in b[1]:
                log_message += line + "\n"

        record = self.buffer[-1][0]

        # prepare message for sending, exclude last \n char
        payload = {
            "content": self.config.template.format(
                hostname=socket.gethostname(), text=log_message[:-1]
            )
        }

        self._send(record, payload)

    def _send(self, record: LogRecord, payload: dict[str, str]) -> None:
        """Send a payload."""
        max_retries = 3

        # few lines of sending code from https://github.com/2press/discord-logger
        for _ in range(max_retries - 1):
            with self._lock:
                r = requests.post(self.webhook_url, json=payload, timeout=DISCORD_TIMEOUT)

            if r.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:
                retry_after = int(r.headers.get("Retry-After", 500)) / 100.0
                time.sleep(retry_after)
                continue
            if r.status_code < http.HTTPStatus.BAD_REQUEST:
                break
            continue

        self.buffer.clear()
        self.buffer_message_len = 0

        try:
            r.raise_for_status()
        except Exception:  # noqa: BLE001
            self.handleError(record)


def wrap_module(
    program: str,
    webhook: Annotated[str, typer.Option(envvar="DISCORD_WEBHOOK")],
    args: Annotated[list[str] | None, typer.Argument()] = None,
) -> None:
    """Wrap a python script in a Discord logging environment.

    Arguments:
        program: Program to wrap.
        webhook: Discord Webhook URL.
        args: Arguments for the wrapped program.
    """
    import runpy

    if os.sep not in program:
        executable = shutil.which(program)
        if executable:
            program = executable

    if not Path(program).is_file():
        print("Program not found!")
        raise typer.Exit(code=1)

    logger = logging.getLogger("dissi")
    handler = DiscordWebhookHandler(webhook_url=webhook)
    logger.addHandler(handler)
    sys.argv = [program, *(args or [])]

    cap_cmd = " ".join(sys.argv)[:100]
    cap_path = str(Path().cwd())[-100:]

    handler.send_embed(
        "started", f"Command: {cap_cmd}\nWorking Directory: {cap_path}", DiscordColors.LIGHT_BLUE
    )
    try:
        try:
            runpy.run_path(sys.argv[0], run_name=__name__)
        except SyntaxError:
            subprocess.Popen(sys.argv).wait()  # noqa: S603
    except BaseException:
        logger.exception("")
        raise
    logger.error("", exc_info=(SystemExit, SystemExit(0), None))


def main() -> None:
    """Main entrypoint."""
    dotenv.load_dotenv()
    app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)
    app.command(context_settings={"ignore_unknown_options": True})(wrap_module)
    app()


if __name__ == "__main__":
    raise SystemExit(main())
