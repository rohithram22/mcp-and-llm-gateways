"""Test fixture: the real server, sabotaged with a stray print() inside a tool.

Used by test_stray_print_cannot_corrupt_stdout to prove that even a careless
print() in handler code never reaches the JSON-RPC stream.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import anyio  # noqa: E402

import data  # noqa: E402
import server  # noqa: E402

_original_get_customer = data.get_customer


def noisy_get_customer(customer_id):
    print("DEBUG: looking up", customer_id)  # the classic mistake
    return _original_get_customer(customer_id)


data.get_customer = noisy_get_customer

if __name__ == "__main__":
    anyio.run(server.main)
