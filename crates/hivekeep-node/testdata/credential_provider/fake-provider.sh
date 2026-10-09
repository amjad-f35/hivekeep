#!/bin/sh
# Records each request beside the link it runs as, then replies with the
# canned response stored there.
set -eu
cat >> "$0.requests"
echo >> "$0.requests"
cat "$0.response"
