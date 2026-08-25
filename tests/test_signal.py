"""Tests for dissi.main.setup_signal_handling."""

from __future__ import annotations

import os
import signal
from typing import TYPE_CHECKING, Any

from dissi.main import DiscordWebhookHandler, setup_signal_handling

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest


def test_setup_signal_handling_flushes_notifies_and_repropagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registered: dict[int, Callable[..., None]] = {}
    signal_calls: list[tuple[int, object]] = []
    kill_calls: list[tuple[int, int]] = []

    def fake_signal(signalnum: int, handler_func: Any) -> None:
        signal_calls.append((signalnum, handler_func))
        registered[signalnum] = handler_func

    def fake_kill(pid: int, sig: int) -> None:
        kill_calls.append((pid, sig))

    monkeypatch.setattr(signal, "signal", fake_signal)
    monkeypatch.setattr(os, "kill", fake_kill)

    handler = DiscordWebhookHandler("https://example.com/webhook")
    sent: list[str] = []
    monkeypatch.setattr(handler, "send", lambda: sent.append("send"))
    monkeypatch.setattr(handler, "send_embed", lambda *a, **kw: sent.append("send_embed"))  # noqa: ARG005

    try:
        setup_signal_handling(handler)

        assert signal.SIGTERM in registered
        callback = registered[signal.SIGTERM]

        callback(signal.SIGTERM, None)

        assert sent == ["send", "send_embed"]
        assert (signal.SIGTERM, signal.SIG_DFL) in signal_calls
        assert (os.getpid(), signal.SIGTERM) in kill_calls
    finally:
        handler.close()
