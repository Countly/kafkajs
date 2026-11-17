#!/bin/bash
# Prints a file's npm integrity string, sha512-<base64>, computed as the release workflow does.
# Usage: sri.sh <file>
set -eo pipefail

if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  echo "usage: $0 <file>" >&2
  exit 2
fi

# A separate assignment, so a failing digest command fails the script instead of printing "sha512-".
digest="$(openssl dgst -sha512 -binary "$1" | base64 | tr -d '\n')"
echo "sha512-$digest"
