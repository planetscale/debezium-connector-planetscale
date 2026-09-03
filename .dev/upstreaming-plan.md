# Upstreaming Plan: thin tracking fork

**Status:** approved 2026-08-19 (@maxenglander, @mcrauwel). Proposed 2026-07-09.

## Why

This fork currently replaces **9 upstream classes** with modified copies via the
Gradle overlay. Today we support only **3.2.1**, but we want to offer builds for
**all supported 3.x lines** — and every release line we add means re-carrying and
re-verifying those copies against its base. The maintenance cost grows with each
line we take on.

The goal: make this a **thin tracking fork** — track upstream releases with only
the PlanetScale-environment patches (mTLS, auth, packaging), zero copied upstream
source files.

This works. We proved the loop end-to-end with the BIT column fix: upstream
[debezium-connector-vitess#293](https://github.com/debezium/debezium-connector-vitess/pull/293)
merged 2026-07-07, backported to this fork the same day in
[#42](https://github.com/planetscale/debezium-connector-planetscale/pull/42).

And upstream is already converging toward us — three of our fork patches were
independently implemented upstream since 3.2.1:

- Identifier quoting/escaping — [#286](https://github.com/debezium/debezium-connector-vitess/pull/286) (in v3.6.0.Beta2)
- TIMESTAMP `time.precision.mode=connect` — [#287](https://github.com/debezium/debezium-connector-vitess/pull/287) / [#288](https://github.com/debezium/debezium-connector-vitess/pull/288) (in v3.6.0.CR1)
- gRPC header-interceptor modernization (3.3+ line)

Every feature we upstream sooner lands in an earlier release line, which directly
shortens how long we carry overrides.

## Upstream PRs (in order)

Verified 2026-07-09 by diffing fork `main` against upstream `main` + `v3.6.0.Final`:
none of these exist upstream, and no open upstream issue/PR covers them.

| # | PR | Size | What / why upstream wants it | Upstream refs |
|---|----|------|------------------------------|---------------|
| 1 | **Config bugfixes**: `validateInheritEpoch` always returns 0 (validation can never fail) + null-unsafe factory comparison; `vitess.grpc.headers` uses `split(":")` and silently drops header values containing a colon (fix: `split(":", 2)`) | S | Plain bugs; the header fix also makes auth headers fully usable via stock config | [dbz#2479](https://github.com/debezium/dbz/issues/2479), [dbz#2480](https://github.com/debezium/dbz/issues/2480) → [PR #295](https://github.com/debezium/debezium-connector-vitess/pull/295) — **merged 2026-09-01** |
| 2 | **Connection bugfixes**: gRPC channel leak (`compareAndSet(null, ch)` retains only the first channel across restarts; we do `getAndSet` + `shutdownNow()`), NPE-unsafe `close()`, and `Vgtid` string comparison via `==` instead of `.equals` | S | Resource leak + latent correctness bugs, all documented in [buglist.md](buglist.md) | [dbz#2545](https://github.com/debezium/dbz/issues/2545), [dbz#2546](https://github.com/debezium/dbz/issues/2546) → [PR #298](https://github.com/debezium/debezium-connector-vitess/pull/298) — **merged 2026-09-02** |
| 3 | **`vitess.cells`**: set `VStreamFlags.cells` so vtgate serves the stream from tablets in the named cell(s) | S | Locality control for any multi-cell Vitess user. Adaptation: upstream version must be optional + registered in `CONFIG_DEFINITION` (our copy marks it required and never registers it) | [dbz#2547](https://github.com/debezium/dbz/issues/2547) → [PR #300](https://github.com/debezium/debezium-connector-vitess/pull/300) — open, in review |
| 4 | **Zero-date fix**: upstream's `^\d{4}-00-00` regex only catches zero-month; a zero-day date (`2024-01-00`) throws from `Timestamp.valueOf()` and kills the task | S | Crash fix | [dbz#2548](https://github.com/debezium/dbz/issues/2548) → [PR #299](https://github.com/debezium/debezium-connector-vitess/pull/299) — **merged 2026-09-02** |
| 5 | **GEOMETRY support**: map VStream GEOMETRY to `io.debezium.data.geometry.Geometry` instead of dropping the column | M | Parity with the MySQL connector. Needs a design pass first: we currently emit raw bytes with `srid=null`; MySQL's wire format is 4-byte-SRID+WKB and the MySQL connector splits it — upstream review will likely require the same. Validate before opening | — |
| 6 | **Unknown datatypes as bytes**: with `include.unknown.datatypes=true`, upstream declares the schema `bytes()` but delivers a `String` → serialization mismatch at runtime | S | Framed as the schema/value-mismatch bugfix it is (behavior change, so after the goodwill from 1–5) | — |
| 7 | **Generic gRPC TLS / channel-credentials hook** *(strategic)* | M | Upstream hardcodes `usePlaintext()` in the private `newChannel()` — the **only** thing that structurally forces us to copy a class. A generic `vitess.grpc.tls.*` option or pluggable channel-credentials hook is not PlanetScale-specific (any TLS-fronted vtgate needs it). Plan: file the upstream design issue first to socialize it before writing code | — |

Upstream PRs go from the `mcrauwel` fork (the `planetscale/debezium-connector-vitess`
copy is a mirror, not a GitHub fork, so it cannot open cross-repo PRs). Nothing
PlanetScale-branded goes upstream —
specifically the mTLS and username/password auth implementations stay fork-only.

## What stays fork-only (permanent patch set)

- mTLS implementation (`TlsUtils.kt`, `planetscale.tls.*` properties) and the
  always-TLS + Basic-auth channel construction — after PR 7 this becomes a thin
  `ChannelCredentials` wrapper, and after PR 1 Basic auth is expressible as pure
  `vitess.grpc.headers` config
- `PlanetscaleConnector` subclass (no-op `validateConnection` — upstream's
  preflight builds a plaintext channel that can never reach a TLS endpoint; ~10 lines)
- Branding, shadow-jar relocations, confluent-hub/Docker/server packaging
  (build-only, touches no upstream source)

End state: the fork carries **config + a small wrapper module + packaging**, and
tracking a new upstream release is a version bump.

## Housekeeping: tracked, deferred

Because we want to offer builds for **all supported 3.x lines** (today we ship only
3.2.1), override cleanup is per-release-line: each line can only drop the copies its
upstream base already contains (e.g. `VitessMetadata` is deletable on ≥3.6.0.Beta2
lines; the BIT copies only on lines whose base contains #293 — first present in
v3.7.0.Alpha1, in no Final release yet as of 2026-08-20). Doing partial cleanup now
would churn the overlay repeatedly as each PR lands.

**Decision: track the housekeeping work but execute it only once all upstream PRs
above are merged**, then trim each release line to its minimal override set in one
pass (and delete the dead weight outright: the inert `transforms`/`transformer`
ByteBuddy modules, the disabled `BinlogValueConverters` shim, and the
`mysql/Module.java` shading shim it drags in). Likely restructure the overlay with a
per-version override manifest so each line declares exactly what its base lacks.

## Status

- [x] PR 1 — config bugfixes (`validateInheritEpoch`, `grpc.headers` split) — merged 2026-09-01
- [x] PR 2 — connection bugfixes (channel leak, `close()` NPE, `Vgtid` equals) — merged 2026-09-02
- [ ] PR 3 — `vitess.cells` — PR open, in review
- [x] PR 4 — zero-date fix — merged 2026-09-02
- [ ] PR 5 — GEOMETRY support (design pass first)
- [ ] PR 6 — unknown datatypes as bytes
- [ ] Upstream TLS design issue filed (ahead of PR 7)
- [ ] PR 7 — generic gRPC TLS / channel-credentials hook
- [ ] Housekeeping: per-line override trim (blocked on all of the above)

Update this checklist via PR as items land.
