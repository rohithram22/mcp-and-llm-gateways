"""Watch a completion stream through the gateway, as a user would see it.

Usage:  python task3-pii-stream-guardrail/stream_client.py "what is the ssn?"
Prints the text as it arrives, then TTFT and total time.
"""

import json
import sys
import time

import httpx

from sse import SSEParser

URL = "http://127.0.0.1:8100/v1/chat/completions"
prompt = sys.argv[1] if len(sys.argv) > 1 else "show me the mixed profile"
payload = {"model": "mock-1", "stream": True, "messages": [{"role": "user", "content": prompt}]}

start = time.perf_counter()
first_text_at = None
parser = SSEParser()
with httpx.stream("POST", URL, json=payload, timeout=30) as response:
    for chunk in response.iter_bytes():
        for data in parser.feed(chunk):
            if data == "[DONE]":
                continue
            for choice in json.loads(data)["choices"]:
                text = choice["delta"].get("content")
                if text:
                    first_text_at = first_text_at or time.perf_counter()
                    print(text, end="", flush=True)

total = time.perf_counter() - start
print(f"\n\nTTFT: {(first_text_at - start) * 1000:.0f} ms   total: {total * 1000:.0f} ms")