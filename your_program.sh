#!/bin/sh
set -e

(
  cd "$(dirname "$0")"
  go build -o ./codecrafters-build-redis-go app/*.go
)

exec ./codecrafters-build-redis-go "$@"
