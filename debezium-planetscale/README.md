## Planetscale for Debezium

This module defines the API surface for the [Debezium][0] connector for [Planetscale][1]. Facade code is held here
which is exposed to downstream users of the connector.

The upstream Vitess adapter is used here, normally, before any relocations or transformations are applied. See the
[architecture doc](../docs/README.md) for more information.

### Usage

The Planetscale connector will show up as a normal Kafka Connect source. You can also construct it directly:
```java
var connector = new com.planetscale.debezium.PlanetscaleConnector();
```

[0]: https://debezium.io/
[1]: https://planetscale.com/
