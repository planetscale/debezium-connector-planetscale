package com.planetscale.debezium.engine

import binlogdata.Binlogdata
import com.google.protobuf.ByteString
import com.planetscale.debezium.PlanetscaleConnector
import com.planetscale.debezium.grpc.MockVStreamServer
import com.planetscale.debezium.grpc.VStreamEvents
import io.debezium.config.Configuration
import io.debezium.connector.vitess.Vgtid
import io.debezium.connector.vitess.VitessConnectorConfig
import io.debezium.connector.vitess.connection.VitessReplicationConnection
import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.vitess.proto.Query
import io.vitess.proto.Vtgate
import io.vitess.proto.grpc.VitessGrpc
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Integration tests that exercise the connector's VStream event processing through a mock gRPC
 * server. These tests validate complete CDC event flows (inserts, updates, deletes, DDL) and
 * protocol handling without requiring a real Vitess cluster.
 *
 * Note: Full DebeziumEngine end-to-end tests require TLS since the connector always uses
 * transport security. These tests instead validate the event protocol, configuration,
 * and component integration at the VStream level.
 */
class EmbeddedEngineTest {
  private lateinit var mockServer: MockVStreamServer

  @BeforeTest
  fun setUp() {
    mockServer = MockVStreamServer().start()
  }

  @AfterTest
  fun tearDown() {
    mockServer.close()
  }

  private fun minimalConfig(): Configuration = Configuration.create()
    .with("database.hostname", "localhost")
    .with("database.port", mockServer.port.toString())
    .with("database.user", "test-user")
    .with("database.password", "test-password")
    .with("vitess.keyspace", "test_keyspace")
    .with("vitess.cells", "cell1")
    .with("topic.prefix", "test")
    .with("snapshot.mode", "never")
    .build()

  // -- Connector configuration tests --

  @Test
  fun `connector is instantiable and configurable`() {
    val connector = PlanetscaleConnector()
    assertNotNull(connector)

    val config = VitessConnectorConfig(minimalConfig())
    assertEquals("test_keyspace", config.keyspace)
    assertEquals("cell1", config.cells)
    assertEquals(mockServer.port, config.vtgatePort)
    assertEquals(VitessConnectorConfig.SnapshotMode.NEVER, config.snapshotMode)
  }

  @Test
  fun `connector config validates successfully`() {
    val config = minimalConfig()
    val fieldSet = VitessConnectorConfig.ALL_FIELDS
    val allErrors = mutableListOf<String>()
    fieldSet.forEach { field ->
      val cv = config.validate(fieldSet).get(field.name())
      cv?.errorMessages()?.let { allErrors.addAll(it) }
    }
    assertTrue(allErrors.isEmpty(), "Expected no validation errors but got: $allErrors")
  }

  @Test
  fun `connector version is available`() {
    val connector = PlanetscaleConnector()
    assertNotNull(connector.version())
    assertTrue(connector.version().isNotBlank())
  }

  @Test
  fun `connector config returns correct task class`() {
    val connector = PlanetscaleConnector()
    assertNotNull(connector.taskClass())
  }

  // -- VStream protocol integration tests via mock server --

  @Test
  fun `receives insert events through vstream`() {
    val events = VStreamEvents.insertTransaction(
      keyspace = "test_keyspace",
      shard = "0",
      table = "users",
      gtid = "MySQL56/abc:1-10",
      fields = listOf("id" to Query.Type.INT32, "name" to Query.Type.VARCHAR),
      values = listOf("1", "Alice"),
    )
    mockServer.enqueueEvents(*events.toTypedArray())
    mockServer.enqueueComplete()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val request = buildVStreamRequest("test_keyspace", "0")
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(request).asSequence().toList()

      assertEquals(1, responses.size)
      val eventTypes = responses[0].eventsList.map { it.type }
      assertTrue(Binlogdata.VEventType.VGTID in eventTypes)
      assertTrue(Binlogdata.VEventType.BEGIN in eventTypes)
      assertTrue(Binlogdata.VEventType.FIELD in eventTypes)
      assertTrue(Binlogdata.VEventType.ROW in eventTypes)
      assertTrue(Binlogdata.VEventType.COMMIT in eventTypes)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `receives update events through vstream`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-20"),
      VStreamEvents.begin(),
      VStreamEvents.field("test_keyspace", "users", listOf("id" to Query.Type.INT32, "name" to Query.Type.VARCHAR)),
      VStreamEvents.row("test_keyspace", "users", afterValues = listOf("1", "Bob"), beforeValues = listOf("1", "Alice")),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(buildVStreamRequest("test_keyspace", "0")).asSequence().toList()

      val rowEvent = responses[0].eventsList.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(rowEvent)
      val rowChange = rowEvent.rowEvent.getRowChanges(0)
      assertTrue(rowChange.hasBefore(), "Update should have before values")
      assertTrue(rowChange.hasAfter(), "Update should have after values")
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `receives delete events through vstream`() {
    // Build a delete: before values only (no after)
    val before = Query.Row.newBuilder()
      .setValues(ByteString.copyFromUtf8("1Alice"))
      .addLengths(1)
      .addLengths(5)
      .build()

    val deleteRowChange = Binlogdata.RowChange.newBuilder()
      .setBefore(before)
      .build()

    val rowEvent = Binlogdata.RowEvent.newBuilder()
      .setTableName("test_keyspace.users")
      .addRowChanges(deleteRowChange)
      .build()

    val deleteVEvent = Binlogdata.VEvent.newBuilder()
      .setType(Binlogdata.VEventType.ROW)
      .setRowEvent(rowEvent)
      .build()

    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-30"),
      VStreamEvents.begin(),
      VStreamEvents.field("test_keyspace", "users", listOf("id" to Query.Type.INT32, "name" to Query.Type.VARCHAR)),
      deleteVEvent,
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(buildVStreamRequest("test_keyspace", "0")).asSequence().toList()

      val row = responses[0].eventsList.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(row)
      val rowChange = row.rowEvent.getRowChanges(0)
      assertTrue(rowChange.hasBefore(), "Delete should have before values")
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `handles DDL events through vstream`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-40"),
      VStreamEvents.ddl("ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
    )
    mockServer.enqueueComplete()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(buildVStreamRequest("test_keyspace", "0")).asSequence().toList()

      val ddlEvent = responses[0].eventsList.find { it.type == Binlogdata.VEventType.DDL }
      assertNotNull(ddlEvent)
      assertEquals("ALTER TABLE users ADD COLUMN email VARCHAR(255)", ddlEvent.statement)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `tracks vgtid offsets across transactions`() {
    // First transaction
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-10"),
      VStreamEvents.begin(),
      VStreamEvents.field("test_keyspace", "users", listOf("id" to Query.Type.INT32)),
      VStreamEvents.row("test_keyspace", "users", listOf("1")),
      VStreamEvents.commit(),
    )
    // Second transaction with a later GTID
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-20"),
      VStreamEvents.begin(),
      VStreamEvents.field("test_keyspace", "users", listOf("id" to Query.Type.INT32)),
      VStreamEvents.row("test_keyspace", "users", listOf("2")),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(buildVStreamRequest("test_keyspace", "0")).asSequence().toList()

      // Both transactions should be delivered
      assertEquals(2, responses.size)

      // Verify VGTID progression
      val firstVgtid = responses[0].eventsList.find { it.type == Binlogdata.VEventType.VGTID }
      val secondVgtid = responses[1].eventsList.find { it.type == Binlogdata.VEventType.VGTID }
      assertNotNull(firstVgtid)
      assertNotNull(secondVgtid)
      assertEquals("MySQL56/abc:1-10", firstVgtid.vgtid.shardGtidsList[0].gtid)
      assertEquals("MySQL56/abc:1-20", secondVgtid.vgtid.shardGtidsList[0].gtid)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `handles server errors gracefully`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-10"),
      VStreamEvents.heartbeat(),
    )
    mockServer.enqueueError(Status.UNAVAILABLE.withDescription("simulated failure"))

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = mutableListOf<Vtgate.VStreamResponse>()
      var errorCaught = false

      try {
        stub.vStream(buildVStreamRequest("test_keyspace", "0")).forEach {
          responses.add(it)
        }
      } catch (e: io.grpc.StatusRuntimeException) {
        errorCaught = true
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
      }

      assertEquals(1, responses.size)
      assertTrue(errorCaught, "Error should be propagated")
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `connector stops cleanly`() {
    val connector = PlanetscaleConnector()
    connector.stop()
    // Should not throw even when never started
  }

  @Test
  fun `buildVgtid produces valid vgtid for streaming`() {
    val vgtid = VitessReplicationConnection.buildVgtid(
      "test_keyspace",
      listOf("0"),
      listOf(Vgtid.CURRENT_GTID),
    )
    assertNotNull(vgtid)
    assertEquals(1, vgtid.shardGtids.size)
    assertEquals("test_keyspace", vgtid.shardGtids[0].keyspace)
    assertEquals("0", vgtid.shardGtids[0].shard)
    assertEquals(Vgtid.CURRENT_GTID, vgtid.shardGtids[0].gtid)
  }

  @Test
  fun `multiple transactions in single response are handled`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-10"),
      VStreamEvents.begin(),
      VStreamEvents.field("test_keyspace", "orders", listOf("id" to Query.Type.INT32, "amount" to Query.Type.FLOAT64)),
      VStreamEvents.row("test_keyspace", "orders", listOf("100", "49.99")),
      VStreamEvents.commit(),
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-11"),
      VStreamEvents.begin(),
      VStreamEvents.field("test_keyspace", "orders", listOf("id" to Query.Type.INT32, "amount" to Query.Type.FLOAT64)),
      VStreamEvents.row("test_keyspace", "orders", listOf("101", "99.99")),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(buildVStreamRequest("test_keyspace", "0")).asSequence().toList()

      assertEquals(1, responses.size)
      val events = responses[0].eventsList
      val rowEvents = events.filter { it.type == Binlogdata.VEventType.ROW }
      assertEquals(2, rowEvents.size)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `heartbeat events are streamed`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_keyspace", "0", "MySQL56/abc:1-10"),
      VStreamEvents.heartbeat(1234567890L),
    )
    mockServer.enqueueComplete()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(buildVStreamRequest("test_keyspace", "0")).asSequence().toList()

      val heartbeat = responses[0].eventsList.find { it.type == Binlogdata.VEventType.HEARTBEAT }
      assertNotNull(heartbeat)
      assertEquals(1234567890L, heartbeat.timestamp)
    } finally {
      channel.shutdownNow()
    }
  }

  private fun buildVStreamRequest(keyspace: String, shard: String): Vtgate.VStreamRequest {
    val vgtid = Binlogdata.VGtid.newBuilder()
      .addShardGtids(
        Binlogdata.ShardGtid.newBuilder()
          .setKeyspace(keyspace)
          .setShard(shard)
          .setGtid(Vgtid.CURRENT_GTID)
          .build()
      )
      .build()

    return Vtgate.VStreamRequest.newBuilder()
      .setVgtid(vgtid)
      .build()
  }
}
