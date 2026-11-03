"""Token estimation, used to RESERVE budget before calling a model.

The estimate only has to be a sensible upper-ish bound: after the call, the provider's
reported `usage` replaces it (reconciliation), so small errors are corrected.
"""

import math

CHARS_PER_TOKEN = 4          # rough average for English text across common tokenizers
MESSAGE_OVERHEAD = 4         # role markers and separators added per message by chat formats
DEFAULT_MAX_TOKENS = 512     # used when the client does not say how long the reply may be


class InvalidRequest(ValueError):
    """The request cannot be priced, so it cannot be admitted."""


def estimate_prompt_tokens(messages: list) -> int:
    total = 0
    for message in messages:
        content = message.get("content", "") if isinstance(message, dict) else ""
        total += math.ceil(len(str(content)) / CHARS_PER_TOKEN) + MESSAGE_OVERHEAD
    return total


def reservation_for(payload: dict) -> int:
    """Worst-case cost of a request: estimated prompt + the most the model may generate."""
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise InvalidRequest("'messages' must be a non-empty list")

    max_tokens = payload.get("max_tokens", DEFAULT_MAX_TOKENS)
    if not isinstance(max_tokens, int) or isinstance(max_tokens, bool) or max_tokens <= 0:
        raise InvalidRequest("'max_tokens' must be a positive integer")

    return estimate_prompt_tokens(messages) + max_tokens