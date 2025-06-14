1) Target latest release version of Debezium
2) Things like mTLS and pre-fabricated connection parameters are what we are overriding in the adapter.
3) It would be published as a standalone JAR and to Maven; do not add a dependency to the upstream adapter. In effect, the output artifact should always work for the user without an additional dependency on Debezium Vitess. It should appear and act as its own adapter, the PlanetScale Adapter for Debezium.
