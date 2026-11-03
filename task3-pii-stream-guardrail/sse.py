"""Incremental Server-Sent Events parser.

Network chunks do not respect event boundaries: one chunk may hold half an event,
or three and a half events, or even half of a multi-byte UTF-8 character.
SSEParser reassembles complete events and hands back their `data` payloads.
"""

import codecs


class SSEParser:
    def __init__(self) -> None:
        # An incremental decoder keeps the first bytes of a split UTF-8 character
        # (e.g. "é" = 2 bytes) until the rest arrives, instead of failing.
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._buffer = ""

    def feed(self, chunk: bytes) -> list[str]:
        """Add raw bytes; return the data payload of every event completed by them."""
        self._buffer += self._decoder.decode(chunk)
        self._buffer = self._buffer.replace("\r\n", "\n")  # tolerate CRLF line endings

        payloads = []
        while "\n\n" in self._buffer:  # a blank line ends an event
            raw_event, self._buffer = self._buffer.split("\n\n", 1)
            data_lines = [
                line[5:].removeprefix(" ")
                for line in raw_event.split("\n")
                if line.startswith("data:")       # ignore comments (":...") and other fields
            ]
            if data_lines:
                payloads.append("\n".join(data_lines))
        return payloads


def format_event(data: str) -> bytes:
    return f"data: {data}\n\n".encode()