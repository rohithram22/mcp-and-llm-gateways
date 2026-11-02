"""Create a test token.

Usage:  python task2-mcp-gateway/make_token.py <role> <user> [ttl_seconds]
  e.g.  python task2-mcp-gateway/make_token.py admin alice
        python task2-mcp-gateway/make_token.py viewer bob 60
"""

import sys

from auth import issue_token

if len(sys.argv) not in (3, 4):
    sys.exit(__doc__)

role, user = sys.argv[1], sys.argv[2]
ttl = int(sys.argv[3]) if len(sys.argv) == 4 else 3600
print(issue_token(user, role, ttl))