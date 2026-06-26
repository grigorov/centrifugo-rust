#!/usr/bin/env bash
# Build the real Go centrifugo v2.8.6 as a differential behavior oracle.
# Idempotent: clones the tag and builds bin/centrifugo once. `bin/` and `src/`
# are git-ignored.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
SRC="$DIR/src"
BIN="$DIR/bin/centrifugo"
TAG="v2.8.6"

if [ ! -x "$BIN" ]; then
  rm -rf "$SRC"
  git clone --depth 1 --branch "$TAG" https://github.com/centrifugal/centrifugo "$SRC"
  mkdir -p "$DIR/bin"
  ( cd "$SRC" && go build -o "$BIN" . )
fi

"$BIN" version
