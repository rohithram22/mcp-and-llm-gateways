"""PII detection on complete strings: emails, US SSNs, and payment card numbers.

This module only answers "where is the PII in this text?". Streaming is handled
separately (redactor.py); keeping the two apart lets each be tested on its own.
"""

import re
from dataclasses import dataclass

REDACTED = "[REDACTED]"

# local-part@domain.tld  e.g. ada.lovelace@example.com
# The lookbehind stops a match starting in the middle of a longer token.
EMAIL = re.compile(
    r"(?<![\w.%+-])"
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"
)

# 123-45-6789, not part of a longer run of digits/dashes (so 1123-45-6789 or 123-45-67890 don't match).
# Impossible numbers are skipped: area 000 or 666, group 00, serial 0000.
# Area 9xx is deliberately NOT skipped: those are ITINs (taxpayer IDs), just as sensitive.
SSN = re.compile(
    r"(?<![\d-])"
    r"(?!000|666)\d{3}-(?!00)\d{2}-(?!0000)\d{4}"
    r"(?![\d-])"
)

# A run of digit groups separated by single spaces or dashes: "4111 1111 1111 1111", "Room 4 4111 ...".
# Cards are searched INSIDE each run (see card_spans), because a stray number next to a card
# ("Room 4 4111 1111 1111 1111") would otherwise make the whole run fail the Luhn check
# and hide the real card.
DIGIT_RUN = re.compile(r"\d+(?:[ -]\d+)*")
CARD_MIN_DIGITS, CARD_MAX_DIGITS = 13, 19


def luhn_valid(number: str) -> bool:
    """The checksum built into every real card number. Rejects most random digit strings."""
    digits = [int(c) for c in number if c.isdigit()]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:      # double every second digit from the right
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def card_spans(text: str) -> list["Finding"]:
    """Card numbers: 13-19 consecutive digits, made of whole groups, passing Luhn.

    Windows are built from whole groups, so a card is never carved out of the middle of a
    longer unbroken number. Within a run, the longest valid window wins, left to right.
    A window never exceeds 19 digits, so the work per group is bounded even for huge runs.
    """
    spans = []
    for run in DIGIT_RUN.finditer(text):
        groups = [(m.start() + run.start(), m.end() + run.start()) for m in re.finditer(r"\d+", run.group())]
        i = 0
        while i < len(groups):
            # Collect every window starting at group i that is short enough to be a card.
            windows, digits = [], 0
            for j in range(i, len(groups)):
                digits += groups[j][1] - groups[j][0]
                if digits > CARD_MAX_DIGITS:
                    break
                if digits >= CARD_MIN_DIGITS:
                    windows.append(j)
            # Longest first: prefer the full card over a shorter Luhn-valid piece of it.
            for j in reversed(windows):
                start, end = groups[i][0], groups[j][1]
                if luhn_valid(text[start:end]):
                    spans.append(Finding("card", start, end))
                    i = j + 1
                    break
            else:
                i += 1
    return spans


@dataclass(frozen=True)
class Finding:
    kind: str   # "email" | "ssn" | "card"
    start: int
    end: int


def find_pii(text: str) -> list[Finding]:
    """All PII spans in text, sorted, with overlaps merged so each character is redacted at most once."""
    found = [Finding("email", m.start(), m.end()) for m in EMAIL.finditer(text)]
    found += [Finding("ssn", m.start(), m.end()) for m in SSN.finditer(text)]
    found += card_spans(text)
    found.sort(key=lambda f: (f.start, -f.end))

    merged: list[Finding] = []
    for f in found:
        if merged and f.start < merged[-1].end:          # overlaps the previous span
            last = merged[-1]
            merged[-1] = Finding(last.kind, last.start, max(last.end, f.end))
        else:
            merged.append(f)
    return merged


def redact(text: str) -> str:
    """Replace every PII span with [REDACTED]."""
    out, pos = [], 0
    for f in find_pii(text):
        out.append(text[pos:f.start])
        out.append(REDACTED)
        pos = f.end
    out.append(text[pos:])
    return "".join(out)