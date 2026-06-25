# Debezium Connector for PlanetScale

This package contains the [Debezium](https://debezium.io/) connector for PlanetScale. It is based on the [Debezium Vitess connector](https://debezium.io/documentation/reference/stable/connectors/vitess.html) and packages the PlanetScale-specific connector classes, patches, and runtime dependencies for Kafka Connect and Debezium Server.

## Build

From the repository root:

```bash
./gradlew build
```

The build produces the following artifacts under `debezium-planetscale/build/`:

| Modality | Artifact | Shading |
| --- | --- | --- |
| Debezium Server | `libs/planetscale-debezium-adapter-<version>.jar` | Partially |
| Debezium Server | `libs/planetscale-debezium-adapter-<version>-all.jar` | Fully |
| Kafka Connect | `connect/dist/planetscale-debezium-connector-planetscale-<version>.zip` | Partially |

## Run

For local Debezium Server usage, see `./server` in the source repository.

```bash
cp server/sample-application.properties server/ps.properties
# edit server/ps.properties with your PlanetScale credentials and source settings
./gradlew build
make -C server run
```

## Documentation

https://planetscale.com/docs/vitess/integrations/debezium#debezium-connector-for-planetscale
