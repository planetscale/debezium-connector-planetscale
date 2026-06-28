ARG DEBEZIUM_VERSION=3.2
FROM quay.io/debezium/server:${DEBEZIUM_VERSION}

LABEL org.opencontainers.image.source="https://github.com/planetscale/debezium-connector-planetscale"
LABEL org.opencontainers.image.description="Debezium Server with PlanetScale Connector"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY debezium-planetscale/build/libs/planetscale-debezium-adapter-*.jar /debezium/lib/
# Note: .dockerignore excludes the -all.jar variant
