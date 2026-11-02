"""Authentication: turn an `Authorization: Bearer <JWT>` header into a verified identity."""

import logging
import os
import time
from dataclasses import dataclass

import jwt

log = logging.getLogger("mcp-gateway.auth")

# The signing secret. In production this comes from a secrets manager, never from code.
JWT_SECRET = os.environ.get("GATEWAY_JWT_SECRET", "dev-only-secret-change-me-0123456789abcdef")
JWT_ALGORITHM = "HS256"
VALID_ROLES = {"admin", "viewer"}


@dataclass(frozen=True)
class Principal:
    """Who is calling, as proven by a verified token."""

    subject: str
    role: str


class AuthError(Exception):
    """Authentication failed. The message is for our logs only; clients get a generic one."""


def issue_token(subject: str, role: str, ttl_seconds: int = 3600, secret: str = JWT_SECRET) -> str:
    """Create a signed token. Used by make_token.py and the tests; the gateway itself only verifies."""
    now = int(time.time())
    claims = {"sub": subject, "role": role, "iat": now, "exp": now + ttl_seconds}
    return jwt.encode(claims, secret, algorithm=JWT_ALGORITHM)


def authenticate(authorization_header: str | None) -> Principal:
    """Verify the bearer token and return the caller. Raises AuthError on any problem (fail closed)."""
    if not authorization_header:
        raise AuthError("missing Authorization header")

    scheme, _, token = authorization_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise AuthError("Authorization header is not 'Bearer <token>'")

    try:
        claims = jwt.decode(
            token.strip(),
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],          # pin the algorithm: blocks "alg: none" and algorithm-swap attacks
            options={"require": ["exp", "sub", "role"]},
        )
    except jwt.ExpiredSignatureError:
        raise AuthError("token expired") from None
    except jwt.InvalidTokenError as exc:
        raise AuthError(f"invalid token: {exc}") from None

    role = claims["role"]
    subject = claims["sub"]
    if role not in VALID_ROLES:
        raise AuthError(f"unknown role {role!r}")
    if not isinstance(subject, str) or not subject:
        raise AuthError("invalid subject")

    return Principal(subject=subject, role=role)