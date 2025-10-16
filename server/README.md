## Debezium Server + Planetscale

This directory builds a distribution of [Debezium Server][0] with the
[Planetscale Connector][1] pre-installed. It's easy to build a distribution
and then test it locally.

### How to build

Use the `Makefile` from the root of the project (above the `server`) directory:

**Build**
```bash
make -C server
```

**Run**
```bash
make -C server run DEBUGGER=[yes|no]
```
The debugger defaults to being off (`no`).

### Configuration

Create a `ps.properties` file within the `server` directory before running `make`. This
will ensure Debezium is pre-configured with your properties.

You can find a sample set of properties [here](./sample-application.properties); or below:
```properties
debezium.format.value=json

debezium.sink.type=http
debezium.sink.http.url=http://127.0.0.1:8888/post

debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
debezium.source.schema.history.internal.file.filename=data/schema_history.dat

debezium.source.offset.storage.file.filename=data/offsets.dat
debezium.source.offset.flush.interval.ms=0

debezium.source.database.hostname=aws.connect.psdb.cloud
debezium.source.database.port=443
debezium.source.database.user=(planetscale user)
debezium.source.database.password=(planetscale password)

debezium.source.vitess.keyspace=(database name)

debezium.source.connector.class=com.planetscale.debezium.PlanetscaleConnector
debezium.source.topic.prefix=(topic name)

debezium.tasks.max=1
```

> Replace `vitess.keyspace` with your Planetscale database name, `topic.prefix` with your
> desired topic, and the user/password for Planetscale.

### Debugging

When running with `DEBUGGER=yes`, the `Makefile` injects JVM debugging flags; when
the VM starts, it waits at port `:5005` for the debugging client to connect.

To debug from IntelliJ, perform the following steps:

1) In the top right run profiles dialog, click `Edit Configurations`
2) Add a configuration of type `Remote JVM Debug`
3) It is sufficient to use the default settings
4) Click `Apply`, then `OK`
5) Run `make -C server run DEBUGGER=yes`
6) With the debug run profile selected, click `Debug`
