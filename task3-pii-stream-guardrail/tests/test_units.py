"""Fast unit tests: patterns, the streaming redactor, and the SSE parser. No servers."""

import random
import time

import pytest

from patterns import find_pii, luhn_valid, redact
from redactor import MAX_EMAIL_LEN, MAX_NUMBER_LEN, StreamRedactor
from sse import SSEParser

# ----------------------------------------------------------------------------- patterns
REDACTED_CASES = [
    ("Write to ada.lovelace@example.com today.", "Write to [REDACTED] today."),
    ("Team: support@acme.io, sales@acme.co.uk.", "Team: [REDACTED], [REDACTED]."),
    ("SSN 123-45-6789 on file.", "SSN [REDACTED] on file."),
    ("ITIN 912-70-1234 is a tax ID.", "ITIN [REDACTED] is a tax ID."),
    ("Visa 4111 1111 1111 1111.", "Visa [REDACTED]."),
    ("MC 5555-5555-5555-4444.", "MC [REDACTED]."),
    ("Amex 3782 822463 10005.", "Amex [REDACTED]."),
    ("Glued 4111111111111111 here", "Glued [REDACTED] here"),
    ("Room 4 4111 1111 1111 1111 please", "Room 4 [REDACTED] please"),       # stray number before
    ("Paid 4111 1111 1111 1111 2 times", "Paid [REDACTED] 2 times"),         # stray number after
]

UNTOUCHED = [
    "Not an email: user@localhost or @handle",
    "Impossible SSNs: 000-12-3456, 666-12-3456, 123-00-4567, 123-45-0000",
    "Too long: 123-45-67890 and 1123-45-6789",
    "Order 1234 5678 9012 3456 fails Luhn.",
    "Long 41111111111111110 number",            # a card is never carved out of a longer number
    "Phone 555-123-4567 and year 2026.",
    "The weather in Paris is sunny.",
]


@pytest.mark.parametrize("text,expected", REDACTED_CASES)
def test_pii_is_redacted(text, expected):
    assert redact(text) == expected


@pytest.mark.parametrize("text", UNTOUCHED)
def test_lookalikes_are_left_alone(text):
    assert redact(text) == text


def test_luhn():
    assert luhn_valid("4111 1111 1111 1111") and luhn_valid("3782-822463-10005")
    assert not luhn_valid("1234 5678 9012 3456") and not luhn_valid("4111 1111 1111 111")


def test_card_search_is_linear_on_huge_digit_runs():
    start = time.perf_counter()
    redact("1 " * 20_000)
    assert time.perf_counter() - start < 2.0


# ----------------------------------------------------------------------------- streaming redactor
CORPUS = [
    "Sure. The SSN on file is 123-45-6789, and the backup record shows 987-65-4321.",
    "You can reach Ada at ada.lovelace@example.com or the team at support@acme.io anytime.",
    "The card ending in 1111 is 4111 1111 1111 1111. An older one, 3782-822463-10005, was cancelled.",
    "Customer profile: email jane.doe@example.org, SSN 078-05-1120, card 5555 5555 5555 4444. "
    "Order number 1234 5678 9012 3456 is not a card.",
    "PII at the very end 4111111111111111",
    "ada@example.com is first",
    "Too long: 123-45-67890 and 1123-45-6789 stay.",
    "Room 4 4111 1111 1111 1111 please",
    "Unicode café first, then bob@example.com, then naïve 123-45-6789.",
]


def stream(text: str, sizes) -> tuple[str, StreamRedactor]:
    redactor, out, i = StreamRedactor(), [], 0
    for size in sizes:
        if i >= len(text):
            break
        out.append(redactor.feed(text[i:i + size]))
        i += size
    if i < len(text):
        out.append(redactor.feed(text[i:]))
    out.append(redactor.flush())
    return "".join(out), redactor


@pytest.mark.parametrize("text", CORPUS)
@pytest.mark.parametrize("size", range(1, 31))
def test_streaming_equals_whole_text_redaction(text, size):
    """The core guarantee: however the text is split, the client gets exactly redact(text)."""
    streamed, _ = stream(text, [size] * len(text))
    assert streamed == redact(text)


def test_random_splits_of_realistic_text():
    rng = random.Random(42)
    words = ["the", "card", "is", "4111 1111 1111 1111", "ada.lovelace@example.com", "SSN",
             "123-45-6789", "order", "1234 5678 9012 3456", "on", "2026", "x@y.io", "3782-822463-10005", "."]
    for _ in range(3000):
        text = " ".join(rng.choice(words) for _ in range(rng.randint(1, 30)))
        sizes = [rng.randint(1, 9) for _ in range(len(text))]
        assert stream(text, sizes)[0] == redact(text), text


def test_no_partial_pii_is_ever_released_early():
    text = "SSN 123-45-6789, card 4111 1111 1111 1111, mail ada@example.com."
    redactor, released = StreamRedactor(), ""
    for ch in text:
        released += redactor.feed(ch)
        for secret in ("123-45", "4111 1111", "ada@"):
            assert secret not in released


def test_memory_stays_bounded_on_a_long_stream():
    text = CORPUS[3] * 500                                   # ~70k characters
    _, redactor = stream(text, [5] * len(text))
    assert redactor.max_buffered < 100


def test_buffer_is_capped_for_pathological_tails():
    _, word = stream("a" * 10_000, [7] * 10_000)
    _, digits = stream("1 " * 5_000, [7] * 10_000)
    assert word.max_buffered <= MAX_EMAIL_LEN + 7
    assert digits.max_buffered <= MAX_NUMBER_LEN + 7


def test_redaction_count():
    _, redactor = stream(CORPUS[3], [5] * 200)
    assert redactor.redactions == 3


def test_find_pii_reports_kinds():
    kinds = [f.kind for f in find_pii("a@b.co 123-45-6789 4111111111111111")]
    assert kinds == ["email", "ssn", "card"]


# ----------------------------------------------------------------------------- SSE parser
def test_sse_parser_reassembles_any_split():
    raw = 'data: {"x": "café"}\n\ndata: [DONE]\n\n'.encode()
    for size in range(1, len(raw) + 1):
        parser, out = SSEParser(), []
        for i in range(0, len(raw), size):
            out += parser.feed(raw[i:i + size])
        assert out == ['{"x": "café"}', "[DONE]"]


def test_sse_parser_handles_crlf_comments_and_multiline_data():
    assert SSEParser().feed(b": keep-alive\r\ndata: a\r\ndata: b\r\n\r\n") == ["a\nb"]