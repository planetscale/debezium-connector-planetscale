# Local QA harness — Confluent Cloud failure reproduction

This harness reproduces — and then proves fixed — the two failures the PlanetScale Debezium
connector hit on **Confluent Cloud Custom Connectors**, without needing a Confluent account.

| | Error | Where it fires |
|---|---|---|
| **1** (fatal) | `java.util.ServiceConfigurationError: io.grpc.NameResolverProvider: io.grpc.googleapis.GoogleCloudToProdExperimentalNameResolverProvider not a subtype` → *"An exception occurred in the change event producer"* | task start, when the Vitess gRPC channel is built |
| **2** (noise) | `java.lang.NoSuchMethodException: io.debezium.embedded.Transformations$1.<init>()` | worker startup, plugin scanning |

## Why a vanilla Connect worker isn't enough

Error 1 is a **classloader collision specific to Confluent's runtime**: the worker ships its own
`io.grpc` (including `grpc-googleapis`) on the parent classloader, while the connector bundles its
own `io.grpc`. Connect loads plugins child-first, so gRPC's `NameResolver` `ServiceLoader` discovers
the host's `googleapis` provider, which extends a *different* `io.grpc.NameResolverProvider` than the
connector's → `not a subtype`.

A plain local Connect worker has no `grpc-googleapis`, so it **won't reproduce Error 1**. We emulate
Confluent by planting a foreign gRPC stack on the worker's `CLASSPATH` (mounted at `/opt/host-grpc`,
deliberately *outside* `plugin.path`). See `docker-compose.yml`.

## Usage

```bash
# prove the bugs reproduce against the CURRENT build (expects BOTH errors):
EXPECT=red   bash scripts/run.sh

# prove the fix (expects NEITHER error; task reaches the relocated gRPC and fails only on
# connectivity/auth to PlanetScale):
EXPECT=green bash scripts/run.sh        # default

# leave the stack up to poke at http://localhost:8083, then:
KEEP=1 EXPECT=green bash scripts/run.sh
bash scripts/down.sh
```

`run.sh` builds the connector if needed, boots Kafka + Connect, plants the foreign gRPC, deploys the
connector, waits for the **task** to settle, and asserts presence/absence of each error from worker
logs + REST status. Exit 0 = expectation met.

Notes:
- `scripts/download-host-grpc.sh` fetches gRPC **1.64.0** (intentionally different from the bundled
  1.56.1, to model a genuinely separate runtime copy) into `worker/host-grpc/` (git-ignored).
- The Connect worker is forced to **JDK SSL** (`noOpenSsl`) to dodge a netty-tcnative `SIGSEGV`
  (`init_have_lse_atomics`) that only happens on **ARM64 / Apple-Silicon** Docker; it's unrelated to
  the bug under test and never occurs on Confluent's amd64 runtime.

## The fix being validated

In `debezium-planetscale/build.gradle.kts`:
- **Drop `debezium-embedded`** (unused standalone engine) → kills Error 2.
- **Relocate `io.grpc` → `com.planetscale.labs.io.grpc`** (excluding `io.grpc.netty.shaded.**` so
  tcnative stays intact) and bundle the gRPC + Vitess stacks into the shaded jar instead of shipping
  them loose → kills Error 1. Drop the redundant non-shaded `grpc-netty`.

After the fix, the GREEN run shows the task talking to PlanetScale via the relocated client:
`com.planetscale.labs.io.grpc.StatusRuntimeException: UNAUTHENTICATED` (expected, placeholder creds).

## Cloud gate

`scripts/confluent-smoke.sh` is the authoritative (slower) check: it uploads the built archive as a
Custom Connector plugin via the `confluent` CLI, creates a connector, polls status, and tears down.
Run it in CI on release tags. See the header of that script for required env vars.

## Known separate finding (not one of the two reported bugs)

The harness also surfaces a **non-fatal** plugin-discovery warning: Debezium `core`'s
`CloudEventsConverter` reflects for `JsonConverter.convertToConnect(Schema, JsonNode)`, which doesn't
exist in Kafka 3.9's `connect-json`. Connect logs *"Failed to discover Converter … CloudEventsConverter"*
and skips it; the connector task still runs. This is a Debezium-vs-Kafka-version mismatch independent
of the gRPC/embedded fixes — relevant only if the CloudEvents output format is used.
