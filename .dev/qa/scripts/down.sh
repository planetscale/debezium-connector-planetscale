#!/usr/bin/env bash
# Tear down the local QA stack.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QA="$(cd "$HERE/.." && pwd)"
docker compose -f "$QA/docker-compose.yml" down -v --remove-orphans
