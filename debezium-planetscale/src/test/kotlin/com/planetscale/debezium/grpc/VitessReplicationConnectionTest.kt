package com.planetscale.debezium.grpc

import binlogdata.Binlogdata
import io.debezium.config.Configuration
import io.debezium.connector.vitess.Vgtid
import io.debezium.connector.vitess.VitessConnectorConfig
import io.debezium.connector.vitess.connection.VitessReplicationConnection
import io.grpc.Status
import io.vitess.proto.Query
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class VitessReplicationConnectionTest {
  private lateinit var mockServer: MockVStreamServer

  @BeforeTest
  fun setUp() {
    mockServer = MockVStreamServer().start()
  }

  @AfterTest
  fun tearDown() {
    mockServer.close()
  }

  private fun connectorConfig(
    extraConfig: Map<String, String> = emptyMap(),
  ): VitessConnectorConfig {
    val builder = Configuration.create()
      .with("database.hostname", "localhost")
      .with("database.port", mockServer.port.toString())
      .with("database.user", "test-user")
      .with("database.password", "test-password")
      .with("vitess.keyspace", "test_ks")
      .with("vitess.cells", "cell1")
      .with("topic.prefix", "test")
      .with("snapshot.mode", "never")
    extraConfig.forEach { (k, v) -> builder.with(k, v) }
    return VitessConnectorConfig(builder.build())
  }

  @Test
  fun `buildVgtid creates correct vgtid for single shard`() {
    val vgtid = VitessReplicationConnection.buildVgtid(
      "test_ks",
      listOf("-80"),
      listOf("current"),
    )
    assertNotNull(vgtid)
    val shardGtids = vgtid.shardGtids
    assertEquals(1, shardGtids.size)
    assertEquals("test_ks", shardGtids[0].keyspace)
    assertEquals("-80", shardGtids[0].shard)
    assertEquals("current", shardGtids[0].gtid)
  }

  @Test
  fun `buildVgtid creates correct vgtid for multiple shards`() {
    val vgtid = VitessReplicationConnection.buildVgtid(
      "test_ks",
      listOf("-80", "80-"),
      listOf("MySQL56/abc:1-10", "MySQL56/def:1-20"),
    )
    assertNotNull(vgtid)
    assertEquals(2, vgtid.shardGtids.size)
    assertEquals("-80", vgtid.shardGtids[0].shard)
    assertEquals("80-", vgtid.shardGtids[1].shard)
  }

  @Test
  fun `buildVgtid with null shards defaults to current`() {
    val vgtid = VitessReplicationConnection.buildVgtid(
      "test_ks",
      null,
      emptyList(),
    )
    assertNotNull(vgtid)
    assertEquals(1, vgtid.shardGtids.size)
    assertEquals(Vgtid.CURRENT_GTID, vgtid.shardGtids[0].gtid)
  }

  @Test
  fun `buildVgtid with empty shards defaults to current`() {
    val vgtid = VitessReplicationConnection.buildVgtid(
      "test_ks",
      emptyList(),
      emptyList(),
    )
    assertNotNull(vgtid)
    assertEquals(1, vgtid.shardGtids.size)
    assertEquals(Vgtid.CURRENT_GTID, vgtid.shardGtids[0].gtid)
  }

  @Test
  fun `vstream flags include cells and stop_on_reshard`() {
    // Enqueue a complete transaction so the mock server has something to respond with
    val events = VStreamEvents.insertTransaction(
      keyspace = "test_ks",
      shard = "0",
      table = "test_table",
      gtid = "MySQL56/abc:1-10",
      fields = listOf("id" to Query.Type.INT32, "name" to Query.Type.VARCHAR),
      values = listOf("1", "test"),
    )
    events.forEach { mockServer.enqueueEvents(it) }
    mockServer.enqueueComplete()

    val config = connectorConfig(mapOf("vitess.stop_on_reshard" to "true"))

    // Build the VStream request the same way the connector does internally,
    // to verify the flags are set correctly
    val vgtid = Binlogdata.VGtid.newBuilder()
      .addShardGtids(
        Binlogdata.ShardGtid.newBuilder()
          .setKeyspace("test_ks")
          .setShard("0")
          .setGtid(Vgtid.CURRENT_GTID)
          .build()
      )
      .build()

    val flags = io.vitess.proto.Vtgate.VStreamFlags.newBuilder()
      .setStopOnReshard(config.stopOnReshard)
      .setCells(config.cells)
      .build()

    assertTrue(flags.stopOnReshard)
    assertEquals("cell1", flags.cells)
  }

  @Test
  fun `mock server captures vstream requests`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_ks", "0", "current"),
      VStreamEvents.other(),
    )
    mockServer.enqueueComplete()

    // Create a plain-text channel to the mock for direct vstream invocation
    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val vgtid = Binlogdata.VGtid.newBuilder()
        .addShardGtids(
          Binlogdata.ShardGtid.newBuilder()
            .setKeyspace("test_ks")
            .setShard("0")
            .setGtid(Vgtid.CURRENT_GTID)
            .build()
        )
        .build()

      val request = io.vitess.proto.Vtgate.VStreamRequest.newBuilder()
        .setVgtid(vgtid)
        .build()

      val stub = io.vitess.proto.grpc.VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(request)

      // Consume responses
      val allResponses = responses.asSequence().toList()

      assertEquals(1, mockServer.receivedRequests.size)
      val captured = mockServer.receivedRequests[0]
      assertEquals("test_ks", captured.vgtid.shardGtidsList[0].keyspace)
      assertEquals("0", captured.vgtid.shardGtidsList[0].shard)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `mock server streams error`() {
    mockServer.enqueueError(Status.UNAVAILABLE.withDescription("test error"))

    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val vgtid = Binlogdata.VGtid.newBuilder()
        .addShardGtids(
          Binlogdata.ShardGtid.newBuilder()
            .setKeyspace("test_ks")
            .setShard("0")
            .setGtid(Vgtid.CURRENT_GTID)
            .build()
        )
        .build()

      val request = io.vitess.proto.Vtgate.VStreamRequest.newBuilder()
        .setVgtid(vgtid)
        .build()

      val stub = io.vitess.proto.grpc.VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(request)

      var caughtError = false
      try {
        responses.asSequence().toList()
      } catch (e: io.grpc.StatusRuntimeException) {
        caughtError = true
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
        assertTrue(e.status.description?.contains("test error") == true)
      }
      assertTrue(caughtError, "Expected StatusRuntimeException")
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `mock server handles heartbeat events`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_ks", "0", "MySQL56/abc:1-10"),
      VStreamEvents.heartbeat(),
    )
    mockServer.enqueueComplete()

    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val vgtid = Binlogdata.VGtid.newBuilder()
        .addShardGtids(
          Binlogdata.ShardGtid.newBuilder()
            .setKeyspace("test_ks")
            .setShard("0")
            .setGtid(Vgtid.CURRENT_GTID)
            .build()
        )
        .build()

      val request = io.vitess.proto.Vtgate.VStreamRequest.newBuilder()
        .setVgtid(vgtid)
        .build()

      val stub = io.vitess.proto.grpc.VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(request).asSequence().toList()

      assertEquals(1, responses.size)
      val events = responses[0].eventsList
      assertTrue(events.any { it.type == Binlogdata.VEventType.HEARTBEAT })
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `mock server handles DDL events`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("test_ks", "0", "MySQL56/abc:1-10"),
      VStreamEvents.ddl("CREATE TABLE test (id INT)"),
    )
    mockServer.enqueueComplete()

    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .build()

    try {
      val vgtid = Binlogdata.VGtid.newBuilder()
        .addShardGtids(
          Binlogdata.ShardGtid.newBuilder()
            .setKeyspace("test_ks")
            .setShard("0")
            .setGtid(Vgtid.CURRENT_GTID)
            .build()
        )
        .build()

      val request = io.vitess.proto.Vtgate.VStreamRequest.newBuilder()
        .setVgtid(vgtid)
        .build()

      val stub = io.vitess.proto.grpc.VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(request).asSequence().toList()

      assertEquals(1, responses.size)
      val ddlEvent = responses[0].eventsList.find { it.type == Binlogdata.VEventType.DDL }
      assertNotNull(ddlEvent)
      assertEquals("CREATE TABLE test (id INT)", ddlEvent.statement)
    } finally {
      channel.shutdownNow()
    }
  }
}
