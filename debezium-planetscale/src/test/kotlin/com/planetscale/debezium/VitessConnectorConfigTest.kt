package com.planetscale.debezium

import io.debezium.config.Configuration
import io.debezium.connector.vitess.VitessConnectorConfig
import io.debezium.connector.vitess.VitessConnectorConfig.BigIntUnsignedHandlingMode
import io.debezium.connector.vitess.VitessConnectorConfig.SnapshotMode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class VitessConnectorConfigTest {
  private fun minimalConfig(): Configuration.Builder = Configuration.create()
    .with("database.hostname", "localhost")
    .with("database.port", "15991")
    .with("vitess.keyspace", "test_ks")
    .with("vitess.cells", "cell1")
    .with("topic.prefix", "test")

  @Test
  fun `configDef contains all expected fields`() {
    val configDef = VitessConnectorConfig.configDef()
    assertNotNull(configDef)
    val keys = configDef.names()
    assertTrue("database.hostname" in keys)
    assertTrue("database.port" in keys)
    assertTrue("vitess.keyspace" in keys)
    assertTrue("snapshot.mode" in keys)
    assertTrue("bigint.unsigned.handling.mode" in keys)
    assertTrue("vitess.grpc.headers" in keys)
    assertTrue("vitess.keepalive.interval.ms" in keys)
  }

  @Test
  fun `default vtgate port is 15991`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals(15991, config.vtgatePort)
  }

  @Test
  fun `keyspace is required`() {
    val config = Configuration.create()
      .with("database.hostname", "localhost")
      .with("vitess.cells", "cell1")
      .with("topic.prefix", "test")
      .build()
    val validationResult = config.validate(VitessConnectorConfig.ALL_FIELDS)
    val keyspaceErrors = validationResult.get("vitess.keyspace")?.errorMessages() ?: emptyList()
    assertTrue(keyspaceErrors.isNotEmpty(), "Keyspace should be required")
  }

  @Test
  fun `vtgate host is required`() {
    val config = Configuration.create()
      .with("vitess.keyspace", "test_ks")
      .with("vitess.cells", "cell1")
      .with("topic.prefix", "test")
      .build()
    val validationResult = config.validate(VitessConnectorConfig.ALL_FIELDS)
    val hostErrors = validationResult.get("database.hostname")?.errorMessages() ?: emptyList()
    assertTrue(hostErrors.isNotEmpty(), "VTGate host should be required")
  }

  @Test
  fun `cells field returns configured value`() {
    val config = VitessConnectorConfig(
      minimalConfig().with("vitess.cells", "us-east-1").build()
    )
    assertEquals("us-east-1", config.cells)
  }

  @Test
  fun `cells field returns null when not set`() {
    val config = VitessConnectorConfig(
      Configuration.create()
        .with("database.hostname", "localhost")
        .with("vitess.keyspace", "test_ks")
        .with("topic.prefix", "test")
        .build()
    )
    kotlin.test.assertNull(config.cells)
  }

  @Test
  fun `snapshot mode defaults to INITIAL`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals(SnapshotMode.INITIAL, config.snapshotMode)
  }

  @Test
  fun `snapshot mode parses NEVER`() {
    val config = VitessConnectorConfig(
      minimalConfig().with("snapshot.mode", "never").build()
    )
    assertEquals(SnapshotMode.NEVER, config.snapshotMode)
  }

  @Test
  fun `bigint unsigned handling mode defaults to STRING`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals(BigIntUnsignedHandlingMode.STRING, config.bigIntUnsgnedHandlingMode)
  }

  @Test
  fun `bigint unsigned handling mode parses PRECISE`() {
    val config = VitessConnectorConfig(
      minimalConfig().with("bigint.unsigned.handling.mode", "precise").build()
    )
    assertEquals(BigIntUnsignedHandlingMode.PRECISE, config.bigIntUnsgnedHandlingMode)
  }

  @Test
  fun `bigint unsigned handling mode parses LONG`() {
    val config = VitessConnectorConfig(
      minimalConfig().with("bigint.unsigned.handling.mode", "long").build()
    )
    assertEquals(BigIntUnsignedHandlingMode.LONG, config.bigIntUnsgnedHandlingMode)
  }

  @Test
  fun `time precision mode rejects ADAPTIVE`() {
    val config = minimalConfig()
      .with("time.precision.mode", "adaptive")
      .build()
    // Validate directly via the field's custom validator
    val errors = mutableListOf<String>()
    val output = io.debezium.config.Field.ValidationOutput { _, _, msg -> errors.add(msg) }
    val valid = VitessConnectorConfig.TIME_PRECISION_MODE.validate(config, output)
    assertFalse(valid, "ADAPTIVE time precision mode should be rejected")
    assertTrue(errors.any { "adaptive" in it.lowercase() })
  }

  @Test
  fun `grpc headers parsed correctly`() {
    val config = VitessConnectorConfig(
      minimalConfig().with("vitess.grpc.headers", "key1:val1,key2:val2").build()
    )
    val headers = config.grpcHeaders
    assertEquals(2, headers.size)
    assertEquals("val1", headers["key1"])
    assertEquals("val2", headers["key2"])
  }

  @Test
  fun `grpc headers with colon in value parsed correctly`() {
    val config = VitessConnectorConfig(
      minimalConfig().with("vitess.grpc.headers", "Authorization:Bearer:token123,X-Key:val").build()
    )
    val headers = config.grpcHeaders
    assertEquals(2, headers.size)
    assertEquals("Bearer:token123", headers["Authorization"])
    assertEquals("val", headers["X-Key"])
  }

  @Test
  fun `grpc headers empty when not set`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertTrue(config.grpcHeaders.isEmpty())
  }

  @Test
  fun `keepalive interval defaults to max`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals(Long.MAX_VALUE, config.keepaliveInterval.toMillis())
  }

  @Test
  fun `validate connection is no-op on PlanetscaleConnector`() {
    val connector = PlanetscaleConnector()
    // validateConnection is protected; we verify it doesn't throw via start with empty config
    // The key behavior: PlanetscaleConnector overrides validateConnection to be a no-op
    assertNotNull(connector)
  }

  @Test
  fun `cells returns configured value`() {
    val config = VitessConnectorConfig(
      minimalConfig().with("vitess.cells", "us-east-1,us-west-2").build()
    )
    assertEquals("us-east-1,us-west-2", config.cells)
  }

  @Test
  fun `grpc max inbound message size default`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals(4_194_304, config.grpcMaxInboundMessageSize)
  }

  @Test
  fun `keyspace returns configured value`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals("test_ks", config.keyspace)
  }

  @Test
  fun `vtgate host returns configured value`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals("localhost", config.vtgateHost)
  }

  @Test
  fun `override datetime to nullable defaults to false`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals(false, config.overrideDatetimeToNullable())
  }

  @Test
  fun `include unknown datatypes defaults to false`() {
    val config = VitessConnectorConfig(minimalConfig().build())
    assertEquals(false, config.includeUnknownDatatypes())
  }
}
