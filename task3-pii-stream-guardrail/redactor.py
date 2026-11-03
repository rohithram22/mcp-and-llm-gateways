"""Streaming PII redaction: text goes in a few characters at a time, safe text comes out.

The rule: only release text that can no longer become part of a PII match.
Everything else waits in a small buffer until the next characters decide it.
"""

import re

from patterns import REDACTED, find_pii

# The ambiguous tail of the buffer: text at the very end that the NEXT characters
# could still turn into PII.
#   - a trailing "word" of email characters ("noon" -> "noon@example.com")
#   - a trailing run of digits, spaces and dashes ("123-4" -> "123-45-6789")
TAIL_WORD = re.compile(r"[A-Za-z0-9._%+@-]+\Z")
TAIL_DIGITS = re.compile(r"\d[\d -]*\Z")

# Upper bounds on how long a match can be. Of a longer tail, only the last MAX
# characters are held, so the buffer (and memory) can never grow without limit.
MAX_EMAIL_LEN = 254     # RFC 5321 limit for an email address
MAX_NUMBER_LEN = 40     # 19-digit card with separators (37) plus room for the fence


def hold_point(buffer: str) -> int:
    """Index where the ambiguous tail starts. buffer[:i] is final; buffer[i:] must wait.

    Each tail is capped: if a run is longer than any match could be, only its last
    MAX characters are held, because a new match could still be starting in there.
    """
    end = len(buffer)
    hold = end
    word = TAIL_WORD.search(buffer)
    if word:
        hold = min(hold, max(word.start(), end - MAX_EMAIL_LEN))
    digits = TAIL_DIGITS.search(buffer)
    if digits:
        hold = min(hold, max(digits.start(), end - MAX_NUMBER_LEN))
    return hold


class StreamRedactor:
    def __init__(self) -> None:
        self._buffer = ""     # received but not yet released
        self._context = ""    # the last character already released (see _release)
        self.max_buffered = 0  # high-water mark, used to prove memory stays bounded

    def feed(self, text: str) -> str:
        """Add the next piece of text; return whatever is now safe to send (possibly "")."""
        self._buffer += text
        self.max_buffered = max(self.max_buffered, len(self._buffer))
        return self._release(final=False)

    def flush(self) -> str:
        """End of stream: nothing more is coming, so decide and release everything held."""
        return self._release(final=True)

    def _release(self, final: bool) -> str:
        buffer = self._buffer
        cut = len(buffer) if final else hold_point(buffer)
        if cut == 0:
            return ""

        # Scan with one character of already-released text in front. The patterns'
        # left fences (e.g. "not preceded by a digit") need to see that character,
        # otherwise a match could wrongly start in the middle of a longer number.
        offset = len(self._context)
        findings = find_pii(self._context + buffer)

        # Never cut through the middle of a match: if one straddles the cut, hold all of it.
        for f in findings:
            if f.start - offset < cut < f.end - offset:
                cut = max(f.start - offset, 0)

        out, pos = [], 0
        for f in findings:
            start, end = f.start - offset, f.end - offset
            if end > cut:
                continue          # in the held tail: decided later
            start = max(start, 0)
            out.append(buffer[pos:start])
            out.append(REDACTED)
            pos = end
        out.append(buffer[pos:cut])

        if cut > 0:
            self._context = buffer[cut - 1]
        self._buffer = buffer[cut:]
        return "".join(out)