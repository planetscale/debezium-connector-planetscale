#!/usr/bin/env bash
#
# Download a *foreign* gRPC stack (including grpc-googleapis, which contributes the
# io.grpc.googleapis.GoogleCloudToProdExperimentalNameResolverProvider service entry)
# to plant on the Connect worker classpath. This emulates the Confluent Cloud managed
# runtime, whose own gRPC collides with the connector's bundled gRPC.
#
# Version is intentionally DIFFERENT from the connector's bundled gRPC (1.56.1) to
# model a genuinely separate runtime copy. Any version carrying the googleapis
# provider reproduces the collision.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="$HERE/../worker/host-grpc"
VER="${GRPC_HOST_VERSION:-1.64.0}"
BASE="https://repo1.maven.org/maven2/io/grpc"

mkdir -p "$DEST"
for art in grpc-api grpc-core grpc-context grpc-googleapis; do
  jar="$art-$VER.jar"
  if [[ -f "$DEST/$jar" ]]; then
    echo "  have $jar"
    continue
  fi
  echo "  downloading $jar"
  curl -fsSL "$BASE/$art/$VER/$jar" -o "$DEST/$jar"
done

echo "host-grpc ($VER) ready:"
ls -1 "$DEST"
