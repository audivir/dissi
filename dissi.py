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
import shutil
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
from typing import IO, TYPE_CHECKING, Annotated, Any

import dotenv
import requests
import typer
from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from logging import Logger, LogRecord, _FormatStyle, _Level
    from types import FrameType

__version__ = "0.1.3"

SIGINT_EXIT_CODE = 130

# The regex pattern for common ANSI color/style codes
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
MAX_RETRIES = 3
SIGNALS_TO_HANDLE = (signal.SIGTERM,)


class Exit(Exception):  # noqa: N818
    """Custom SystemExit."""

    def __init__(self, code: sys._ExitCode, stderr: str) -> None:
        """Initialize the exit."""
        super().__init__()
        self.code = code
        self.stderr = stderr


def _typer_app(func: Callable[..., None], context: dict[str, Any] | None = None) -> None:
    dotenv_path = dotenv.find_dotenv(usecwd=True)
    dotenv.load_dotenv(dotenv_path)
    app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)
    app.command(context_settings=context)(func)
    app()


def _cap_cmd(args: list[str] | None = None, max_chars: int = 100) -> str:
    if not args:
        args = sys.argv
    return " ".join(shlex.quote(a) for a in args)[:max_chars]


def _cap_cwd(cwd: Path | None = None, max_chars: int = 100) -> str:
    if not cwd:
        cwd = Path().cwd()
    return str(cwd)[-max_chars:]


def _setup_signal_handling(handler: DiscordWebhookHandler) -> None:
    def handle_signal(signo: int, frame: FrameType) -> None:
        del frame  # unused
        handler.send_embed(
            f"received {signal.Signals(signo).name}",
            f"Command: {_cap_cmd()}\nWorking Directory: {_cap_cwd()}",
            DiscordColors.RED,
        )
        # propagate the original handling
        signal.signal(signo, signal.SIG_DFL)
        os.kill(os.getpid(), signo)

    for signo in SIGNALS_TO_HANDLE:
        signal.signal(signo, handle_signal)


def _run_program(args: list[str] | None = None) -> tuple[int, str]:
    if not args:
        args = sys.argv
    proc = subprocess.Popen(args, stderr=subprocess.PIPE, text=True)  # noqa: S603
    stderr_lines: list[str] = []
    if proc.stderr:
        for line in proc.stderr:
            sys.stderr.write(line)
            stderr_lines.append(line)
    code = proc.wait()
    stderr = "".join(stderr_lines)
    return code, stderr


def _log_line(
    line: str, err: bool, compiled: list[re.Pattern[str]], logger: Logger, only_log: bool
) -> None:
    if not line:
        return
    level = logging.ERROR if err else logging.INFO
    stream = sys.stderr if err else sys.stdout

    if any(c.search(line) for c in compiled):
        logger.log(level, ANSI_PATTERN.sub("", line))
        stream.write(line + "\n")  # always log matched lines
    elif not only_log:
        stream.write(line + "\n")


def _log_buffer(
    stream: IO[str], err: bool, compiled: list[re.Pattern[str]], logger: Logger, only_log: bool
) -> str:
    full: list[str] = []
    buffer = ""
    for line in stream:
        if err:
            full.append(line)
        buffer += line
        if "\n" in buffer:
            first, buffer = buffer.split("\n", maxsplit=1)
            _log_line(first, err, compiled, logger, only_log)
    _log_line(buffer, err, compiled, logger, only_log)

    return "".join(full)


def _run_grep(
    grep: list[str], only_log: bool, logger: Logger, args: list[str] | None = None
) -> tuple[int, str]:
    """Log matching lines from stdout to Discord."""
    if not args:
        args = sys.argv
    compiled = [re.compile(p) for p in grep]

    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)  # noqa: S603

    if not proc.stdout or not proc.stderr:
        raise RuntimeError("Streams not piped")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        _ = executor.submit(
            _log_buffer,
            stream=proc.stdout,
            err=False,
            compiled=compiled,
            logger=logger,
            only_log=only_log,
        )
        f_err = executor.submit(
            _log_buffer,
            stream=proc.stderr,
            err=True,
            compiled=compiled,
            logger=logger,
            only_log=only_log,
        )

        stderr = f_err.result()
    code = proc.wait()
    return code, stderr


def _flush(self: Logger) -> None:
    for handler in self.handlers:
        if isinstance(handler, DiscordWebhookHandler):
            handler.send()


# Adding flush method to Logger class
# used for forced sending all buffered data
logging.Logger.flush = _flush  # type: ignore[attr-defined]

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
    """Decimal color codes for Discord."""

    RED = 16711680
    YELLOW = 16776960
    GREEN = 65280
    LIGHT_BLUE = 3447003


class DiscordWebhookConfig:
    """Config for the template and prefixes."""

    def __init__(
        self,
        template: str | None = None,
        prefixes: Mapping[str, str] | None = None,
        title: str | None = None,
    ) -> None:
        """Initialize a Discord Webhook config."""
        self.template = template or DIFF_MESSAGE_TEMPLATE
        self.prefixes = prefixes or DIFF_LEVEL_PREFIXES
        self.title = f": {title}" if title else ""
        template_len = len(
            self.template.format(hostname=socket.gethostname(), title=title, text="")
        )
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
        super().__init__(fmt, datefmt, style, validate, defaults=defaults)  # type:ignore[call-arg]
        self.config = config or DiscordWebhookConfig()

    @override
    def format(self, record: LogRecord) -> tuple[list[str], int]:  # type:ignore[override]
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
        self.formatter: DiscordWebhookFormatter = formatter or DiscordWebhookFormatter()  # type:ignore[mutable-override]
        self.config = self.formatter.config
        # Limit concurrent requests to avoid hitting rate limits.
        self._sema = Semaphore(5)
        self._lock = Lock()

        self.auto_flush = auto_flush
        self.last_emit = time.monotonic()
        self.timer_interval = 60 / 30  # 30 messages per minute
        self._start_timer()

        # buffer for storing shorter logs and sending them in larger batches
        self.buffer: list[tuple[LogRecord, list[str]]] = []
        self.buffer_message_len = 0

        # send all remaining buffered messages before app exit
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
        self.timer.cancel()
        with self._lock:
            self.send()
        return super().close()

    def _emit(
        self, record: LogRecord, formatted_lines: list[str], formatted_lines_len: int
    ) -> None:
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

    @override
    def emit(self, record: LogRecord) -> None:
        """Add the log record to the buffered messages."""
        if record.exc_info is not None:
            self.send()  # send previous buffered message
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
        """Send a Discord embed message."""
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
        """Send a record with an exception info."""
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
                suffix = "succeed"
                color = DiscordColors.GREEN
            if code == SIGINT_EXIT_CODE:
                suffix = "interrupted"
                color = DiscordColors.YELLOW
            if isinstance(exc_val, Exit):
                tb_text = exc_val.stderr

        description = f"Command: {_cap_cmd()}\nWorking Directory: {_cap_cwd()}\nReturn Code: {code}"
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
                hostname=socket.gethostname(), title=self.config.title, text=log_message[:-1]
            )
        }

        self._send(record, payload)

    def _send(self, record: LogRecord, payload: dict[str, Any]) -> None:
        """Send a payload."""
        r: requests.Response | None = None
        # few lines of sending code from https://github.com/2press/discord-logger
        for _ in range(MAX_RETRIES):
            with self._sema:
                r = requests.post(self.webhook_url, json=payload, timeout=DISCORD_TIMEOUT)

            if r.status_code == http.HTTPStatus.TOO_MANY_REQUESTS:
                retry_after = int(r.headers.get("Retry-After", 500)) / 100.0
                time.sleep(retry_after)
            elif r.status_code < http.HTTPStatus.BAD_REQUEST:
                self.last_sent = datetime.now(tz=timezone.utc)
                # clear on success
                self.buffer.clear()
                self.buffer_message_len = 0
                return

        try:
            if r:
                r.raise_for_status()
        except Exception:  # noqa: BLE001
            self.handleError(record)
            # clear on failure
            self.buffer.clear()
            self.buffer_message_len = 0


def wrap_program(
    webhook: str,
    program: str,
    args: list[str] | None,
    force_exec: bool,
    force_py: bool,
    grep: list[str] | None,
    title: str | None,
    only_log: bool,
) -> None:
    """Wrap a python script or other program in a Discord logging environment."""
    # set up logger
    logger = logging.getLogger("dissi")
    logger.setLevel(logging.INFO)
    config = DiscordWebhookConfig(title=title)
    formatter = DiscordWebhookFormatter(config=config)
    handler = DiscordWebhookHandler(webhook_url=webhook, formatter=formatter)
    logger.addHandler(handler)

    # set new args
    sys.argv = [program, *(args or [])]

    # set up signal handling
    _setup_signal_handling(handler)

    # log the start of the program
    handler.send_embed(
        "started",
        f"Command: {_cap_cmd()}\nWorking Directory: {_cap_cwd()}",
        DiscordColors.LIGHT_BLUE,
    )

    code = 0
    stderr = ""
    try:
        # if not forced, try to run in as python module first,
        # if it fails, run it as normal executable.
        if grep:
            code, stderr = _run_grep(grep, only_log, logger)
        elif force_exec:
            code, stderr = _run_program()
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
                code, stderr = _run_program()

    except BaseException:
        # log any occuring error
        logger.exception("")
        raise
    # log successful exits or failed exits from normal executables
    logger.error("", exc_info=(Exit, Exit(code, stderr), None))


def wrap_program_typer(
    program: Annotated[str, typer.Argument(help="Script or program to wrap.")],
    webhook: Annotated[str, typer.Option(envvar="DISCORD_WEBHOOK", help="Discord Webhook URL.")],
    args: Annotated[
        list[str] | None, typer.Argument(help="Arguments for the wrapped program.")
    ] = None,
    force_exec: Annotated[
        bool, typer.Option(help="Force the use of subprocess instead of trying to use runpy.")
    ] = False,
    force_py: Annotated[
        bool, typer.Option(help="Force the use of runpy instead of trying to use subprocess.")
    ] = False,
    grep: Annotated[list[str] | None, typer.Option(help="Regex patterns to match stdout.")] = None,
    title: Annotated[str | None, typer.Option(help="Title.")] = None,
    only_log: Annotated[bool, typer.Option(help="Forward only matched stdout lines.")] = False,
) -> None:
    """Wrap a python script or other program in a Discord logging environment."""
    if force_exec and force_py:
        typer.echo("--force-exec not allowed with --force-py", err=True)
        raise typer.Exit(code=1)

    if grep and force_py:
        typer.echo("--force-py is unsupported, when --grep patterns are provided", err=True)
        raise typer.Exit(code=1)

    if not grep and (title or only_log):
        typer.echo("--title and --only-log are ignored without --grep patterns", err=True)
        title = None

    orig_program = program
    if os.sep not in program:
        # try to locate program in PATH
        executable = shutil.which(program)
        if executable:
            program = executable

    # try to locate the executable
    if not Path(program).is_file():
        typer.echo(f"{orig_program} not found", err=True)
        raise typer.Exit(code=1)

    wrap_program(webhook, program, args, force_exec, force_py, grep, title, only_log)


def wrap_program_cli() -> int:
    """Entrypoint for wrapper."""
    _typer_app(wrap_program_typer, context={"ignore_unknown_options": True})
    return 0


if __name__ == "__main__":
    raise SystemExit(wrap_program_cli())
