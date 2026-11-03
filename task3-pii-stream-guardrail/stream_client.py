"""Watch a completion stream through the gateway, as a user would see it.

Usage:  python task3-pii-stream-guardrail/stream_client.py "what is the ssn?"
Prints the text as it arrives, then TTFT and total time. Gateway errors (HTTP errors
before the stream, or an error event during it) are printed instead of crashing.
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
    if response.status_code != 200:
        response.read()
        sys.exit(f"HTTP {response.status_code}: {response.json()['error']['message']}")
    for chunk in response.iter_bytes():
        for data in parser.feed(chunk):
            if data == "[DONE]":
                continue
            event = json.loads(data)
            if "error" in event:                       # the gateway reports mid-stream failures in-band
                print(f"\n[stream error: {event['error']['message']}]")
                continue
            for choice in event["choices"]:
                text = choice["delta"].get("content")
                if text:
                    first_text_at = first_text_at or time.perf_counter()
                    print(text, end="", flush=True)

total = time.perf_counter() - start
ttft = f"{(first_text_at - start) * 1000:.0f} ms" if first_text_at else "n/a"
print(f"\nTTFT: {ttft}   total: {total * 1000:.0f} ms")