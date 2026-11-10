![](./docs/images/planetscale-debezium-dark.png#gh-dark-mode-only)
![](./docs/images/planetscale-debezium-light.png#gh-light-mode-only)

# Debezium Connector for PlanetScale

[![CI](https://github.com/planetscale/debezium-connector-planetscale/actions/workflows/on.push.yml/badge.svg)](https://github.com/planetscale/debezium-connector-planetscale/actions/workflows/on.push.yml)
![Java 21](https://img.shields.io/badge/Java-21-blue?style=flat&logoColor=white)
![Debezium 3.8.0.Alpha1](https://img.shields.io/badge/Debezium-3.8.0.Alpha1-blue?style=flat&logoColor=white)
[![License](https://img.shields.io/badge/license-Apache--2.0-brightgreen.svg)](https://www.apache.org/licenses/LICENSE-2.0)

This repository contains the [Debezium](https://debezium.io/) connector for PlanetScale. It is based on the [Debezium Vitess connector](https://debezium.io/documentation/reference/stable/connectors/vitess.html) and packages the PlanetScale-specific connector classes, patches, and runtime dependencies for Kafka Connect and Debezium Server.

## Build

Requires Java 21.

```bash
./gradlew build
```

The build produces the following artifacts under `debezium-planetscale/build/`:

| Modality | Artifact | Shading |
| --- | --- | --- |
| Debezium Server | `libs/planetscale-debezium-adapter-<version>.jar` | Partially |
| Debezium Server | `libs/planetscale-debezium-adapter-<version>-all.jar` | Fully |
| Kafka Connect | `connect/dist/planetscale-debezium-connector-planetscale-<version>.zip` | Partially |

> **Debezium Server:** 3.7.0.Final silently ignored external `connector.class` values
> ([debezium/dbz#2801](https://github.com/debezium/dbz/issues/2801)). That is fixed in
> **3.7.1.Final**, so this line works again with Debezium Server (drop the adapter jar into
> `lib/` / use `EXTRA_CONNECTOR`) as well as Kafka Connect / Confluent. Stay on
> [v3.6.3.Final-r1](https://github.com/planetscale/debezium-connector-planetscale/releases/tag/v3.6.3.Final-r1)
> only if you must remain on Server 3.6.x.

## Run

Use the Debezium Server helper in [`./server`](./server).

```bash
cp server/sample-application.properties server/ps.properties
# edit server/ps.properties with your PlanetScale credentials and source settings
./gradlew build
make -C server run
```

## Documentation

https://planetscale.com/docs/vitess/integrations/debezium#debezium-connector-for-planetscale
