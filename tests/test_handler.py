"""Tests for the Discord-sending behavior of dissi.main.DiscordWebhookHandler."""

from __future__ import annotations

import logging
import time
from typing import Any

import pytest
import requests

from dissi.main import (
    SIGINT_EXIT_CODE,
    DiscordWebhookConfig,
    DiscordWebhookFormatter,
    DiscordWebhookHandler,
    Exit,
)


def make_record_with_exc(exc: BaseException, msg: str = "") -> logging.LogRecord:
    try:
        raise exc
    except type(exc) as caught:
        exc_info = (type(caught), caught, caught.__traceback__)
    return logging.LogRecord("dissi", logging.ERROR, __file__, 1, msg, None, exc_info)


def make_response(status_code: int) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    return response


def test_handler_buffers_emit_until_send(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    logger = logging.getLogger("dissi-test-handler-buffer")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler("https://example.com/webhook")
    logger.addHandler(handler)
    try:
        logger.info("hello")
        assert posted == []

        handler.send()

        assert len(posted) == 1
        assert "hello" in posted[0]["content"]
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_handler_batches_multiple_emits_into_one_post(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    logger = logging.getLogger("dissi-test-handler-batch")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler("https://example.com/webhook")
    logger.addHandler(handler)
    try:
        logger.info("first")
        logger.info("second")

        handler.send()

        assert len(posted) == 1
        assert "first" in posted[0]["content"]
        assert "second" in posted[0]["content"]
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_handler_sends_immediately_when_a_message_exceeds_max_len(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    config = DiscordWebhookConfig(template="{text}")
    logger = logging.getLogger("dissi-test-handler-overflow")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler(
        "https://example.com/webhook", formatter=DiscordWebhookFormatter(config=config)
    )
    logger.addHandler(handler)
    try:
        logger.info("x" * config.max_len)

        assert len(posted) >= 1
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_send_retries_after_rate_limit_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict[str, Any]] = []
    responses = [make_response(429), make_response(200)]

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return responses.pop(0)

    def fake_sleep(seconds: float) -> None:
        del seconds

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(time, "sleep", fake_sleep)

    handler = DiscordWebhookHandler("https://example.com/webhook")
    try:
        handler.send_embed("test")
    finally:
        handler.close()

    assert len(posted) == 2


def test_emit_with_exc_info_flushes_buffer_before_sending_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    logger = logging.getLogger("dissi-test-handler-exc")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler("https://example.com/webhook")
    logger.addHandler(handler)
    try:
        logger.info("buffered before failure")
        assert posted == []

        try:
            raise RuntimeError("boom")  # noqa: TRY301
        except RuntimeError:
            logger.exception("")

        assert len(posted) == 2
        assert "buffered before failure" in posted[0]["content"]
        assert "embeds" in posted[1]
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_flush_sends_buffered_messages_of_every_discord_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    logger = logging.getLogger("dissi-test-flush")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler("https://example.com/webhook")
    stream_handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.addHandler(stream_handler)
    try:
        logger.info("flush me")
        assert posted == []

        logger.flush()  # type: ignore[attr-defined]

        assert len(posted) == 1
        assert "flush me" in posted[0]["content"]
    finally:
        handler.close()
        logger.removeHandler(handler)
        logger.removeHandler(stream_handler)


def test_handler_periodic_timer_flushes_the_buffer(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    logger = logging.getLogger("dissi-test-timer")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler("https://example.com/webhook")
    logger.addHandler(handler)
    try:
        logger.info("waiting for the timer")
        assert posted == []

        time.sleep(handler.timer_interval + 0.5)

        assert len(posted) == 1
        assert "waiting for the timer" in posted[0]["content"]
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_handler_auto_flush_sends_without_explicit_send_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    logger = logging.getLogger("dissi-test-auto-flush")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler("https://example.com/webhook", auto_flush=True)
    logger.addHandler(handler)
    try:
        logger.info("sent right away")

        assert len(posted) == 1
        assert "sent right away" in posted[0]["content"]
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_handler_flushes_buffer_before_a_message_that_no_longer_fits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    config = DiscordWebhookConfig(template="{text}")
    config.max_len = 20

    logger = logging.getLogger("dissi-test-handler-combined-overflow")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler(
        "https://example.com/webhook", formatter=DiscordWebhookFormatter(config=config)
    )
    logger.addHandler(handler)
    try:
        logger.info("1234567890")
        assert posted == []

        logger.info("1234567890")

        assert len(posted) == 1
        assert "1234567890" in posted[0]["content"]
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_handler_batches_and_flushes_many_short_lines_mid_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    config = DiscordWebhookConfig(template="{text}")
    config.max_len = 50

    logger = logging.getLogger("dissi-test-handler-many-short-lines")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler(
        "https://example.com/webhook", formatter=DiscordWebhookFormatter(config=config)
    )
    logger.addHandler(handler)
    try:
        logger.info("\n".join(["12345"] * 12))

        assert len(posted) > 1
        assert all(len(post["content"]) <= 50 for post in posted)
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_send_retries_immediately_on_a_non_rate_limit_error_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []
    responses = [make_response(500), make_response(200)]

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return responses.pop(0)

    monkeypatch.setattr(requests, "post", fake_post)

    handler = DiscordWebhookHandler("https://example.com/webhook")
    try:
        handler.send_embed("test")
    finally:
        handler.close()

    assert len(posted) == 2


def test_emit_reports_formatting_errors_via_handle_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handled: list[logging.LogRecord] = []

    def broken_format(record: logging.LogRecord) -> tuple[list[str], int]:
        del record
        raise ValueError("broken formatter")

    logger = logging.getLogger("dissi-test-emit-format-error")
    logger.setLevel(logging.INFO)
    handler = DiscordWebhookHandler("https://example.com/webhook")
    monkeypatch.setattr(handler, "format", broken_format)
    monkeypatch.setattr(handler, "handleError", handled.append)
    logger.addHandler(handler)
    try:
        logger.info("this will fail to format")

        assert len(handled) == 1
    finally:
        handler.close()
        logger.removeHandler(handler)


def test_send_gives_up_and_reports_error_after_exhausting_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []
    handled: list[logging.LogRecord] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(429)

    def fake_sleep(seconds: float) -> None:
        del seconds

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(time, "sleep", fake_sleep)

    handler = DiscordWebhookHandler("https://example.com/webhook")
    monkeypatch.setattr(handler, "handleError", handled.append)
    try:
        handler.send_embed("test")
    finally:
        handler.close()

    assert len(posted) == 3
    assert len(handled) == 1
    assert handler.buffer_message_len == 0


@pytest.mark.parametrize(
    ("exc", "expected_code", "expected_suffix"),
    [
        (KeyboardInterrupt(), SIGINT_EXIT_CODE, "interrupted"),
        (SystemExit(0), 0, "succeeded"),
        (SystemExit(SIGINT_EXIT_CODE), SIGINT_EXIT_CODE, "interrupted"),
        (SystemExit(7), 7, "failed"),
        (Exit(5, "captured stderr text"), 5, "failed"),
    ],
)
def test_send_error_maps_exception_type_to_status(
    exc: BaseException,
    expected_code: int,
    expected_suffix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    handler = DiscordWebhookHandler("https://example.com/webhook")
    try:
        handler.send_error(make_record_with_exc(exc))
    finally:
        handler.close()

    assert len(posted) == 1
    embed = posted[0]["embeds"][0]
    assert expected_suffix in embed["title"]
    assert f"Return Code: {expected_code}" in embed["description"]
    if isinstance(exc, Exit):
        assert embed["fields"][0]["value"] == "```python\ncaptured stderr text```"


def test_send_error_includes_the_record_message(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: float) -> requests.Response:
        del url, timeout
        posted.append(json)
        return make_response(200)

    monkeypatch.setattr(requests, "post", fake_post)

    record = make_record_with_exc(RuntimeError("boom"), msg="custom failure message")

    handler = DiscordWebhookHandler("https://example.com/webhook")
    try:
        handler.send_error(record)
    finally:
        handler.close()

    description = posted[0]["embeds"][0]["description"]
    assert "Message: custom failure message" in description


def test_send_error_requires_exception_info() -> None:
    handler = DiscordWebhookHandler("https://example.com/webhook")
    try:
        record = logging.LogRecord("dissi", logging.ERROR, __file__, 1, "", None, None)
        with pytest.raises(ValueError, match="No error found in record"):
            handler.send_error(record)
    finally:
        handler.close()
