package com.planetscale.debezium.grpc

import binlogdata.Binlogdata
import io.debezium.connector.vitess.Vgtid
import io.grpc.ManagedChannelBuilder
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
 * End-to-end geometry replication tests.
 *
 * Verifies that WKB-encoded geometry data (POINT, LINESTRING, POLYGON) streams correctly
 * through the VStream gRPC protocol using binary row encoding.
 */
class GeoReplicationTest {
  private lateinit var mockServer: MockVStreamServer

  @BeforeTest
  fun setUp() {
    mockServer = MockVStreamServer().start()
  }

  @AfterTest
  fun tearDown() {
    mockServer.close()
  }

  private fun newChannel() = ManagedChannelBuilder
    .forAddress("localhost", mockServer.port)
    .usePlaintext()
    .build()

  private fun vstreamRequest(keyspace: String = "geo_ks", shard: String = "0") =
    Vtgate.VStreamRequest.newBuilder()
      .setVgtid(
        Binlogdata.VGtid.newBuilder().addShardGtids(
          Binlogdata.ShardGtid.newBuilder()
            .setKeyspace(keyspace).setShard(shard).setGtid(Vgtid.CURRENT_GTID)
        )
      )
      .build()

  @Test
  fun `POINT geometry streams correctly`() {
    val wkb = VStreamEvents.wkbPoint(100.0, 25.5)

    mockServer.enqueueEvents(
      VStreamEvents.vgtid("geo_ks", "0", "MySQL56/abc:1-10"),
      VStreamEvents.begin(),
      VStreamEvents.field("geo_ks", "locations", listOf("id" to Query.Type.INT32, "geom" to Query.Type.GEOMETRY)),
      VStreamEvents.rowWithBytes("geo_ks", "locations", listOf("1".toByteArray(), wkb)),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = newChannel()
    try {
      val responses = VitessGrpc.newBlockingStub(channel).vStream(vstreamRequest()).asSequence().toList()
      assertEquals(1, responses.size)

      val events = responses[0].eventsList
      val fieldEvent = events.find { it.type == Binlogdata.VEventType.FIELD }
      assertNotNull(fieldEvent)
      assertEquals("GEOMETRY", fieldEvent.fieldEvent.fieldsList[1].type.name)

      val rowEvent = events.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(rowEvent)
      val row = rowEvent.rowEvent.getRowChanges(0).after
      assertTrue(row.values.size() > 0)
      // Verify the row contains 2 columns via lengths
      assertEquals(2, row.lengthsCount)
      // Second column length should be 21 bytes (WKB POINT)
      assertEquals(21L, row.getLengths(1))
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `LINESTRING geometry streams correctly`() {
    val wkb = VStreamEvents.wkbLineString(listOf(0.0 to 0.0, 10.0 to 10.0, 20.0 to 5.0))

    mockServer.enqueueEvents(
      VStreamEvents.vgtid("geo_ks", "0", "MySQL56/abc:1-20"),
      VStreamEvents.begin(),
      VStreamEvents.field("geo_ks", "paths", listOf("id" to Query.Type.INT32, "path" to Query.Type.GEOMETRY)),
      VStreamEvents.rowWithBytes("geo_ks", "paths", listOf("1".toByteArray(), wkb)),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = newChannel()
    try {
      val responses = VitessGrpc.newBlockingStub(channel).vStream(vstreamRequest()).asSequence().toList()
      val rowEvent = responses[0].eventsList.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(rowEvent)
      val row = rowEvent.rowEvent.getRowChanges(0).after
      // WKB LINESTRING: 1 (endian) + 4 (type) + 4 (count) + 3 * 16 (coords) = 57
      assertEquals(57L, row.getLengths(1))
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `POLYGON geometry streams correctly`() {
    val ring = listOf(0.0 to 0.0, 10.0 to 0.0, 10.0 to 10.0, 0.0 to 10.0, 0.0 to 0.0)
    val wkb = VStreamEvents.wkbPolygon(ring)

    mockServer.enqueueEvents(
      VStreamEvents.vgtid("geo_ks", "0", "MySQL56/abc:1-30"),
      VStreamEvents.begin(),
      VStreamEvents.field("geo_ks", "areas", listOf("id" to Query.Type.INT32, "area" to Query.Type.GEOMETRY)),
      VStreamEvents.rowWithBytes("geo_ks", "areas", listOf("1".toByteArray(), wkb)),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = newChannel()
    try {
      val responses = VitessGrpc.newBlockingStub(channel).vStream(vstreamRequest()).asSequence().toList()
      val rowEvent = responses[0].eventsList.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(rowEvent)
      val row = rowEvent.rowEvent.getRowChanges(0).after
      // WKB POLYGON: 1 + 4 + 4 (rings) + 4 (points) + 5 * 16 = 93
      assertEquals(93L, row.getLengths(1))
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `null geometry column handled`() {
    // A row with a -1 length means NULL value in Vitess protocol
    val row = Query.Row.newBuilder()
      .setValues(com.google.protobuf.ByteString.copyFromUtf8("1"))
      .addLengths(1L)   // id = "1"
      .addLengths(-1L)  // geom = NULL
      .build()
    val rowChange = Binlogdata.RowChange.newBuilder().setAfter(row).build()
    val rowEvent = Binlogdata.RowEvent.newBuilder()
      .setTableName("geo_ks.locations")
      .addRowChanges(rowChange)
      .build()
    val rowVEvent = Binlogdata.VEvent.newBuilder()
      .setType(Binlogdata.VEventType.ROW)
      .setRowEvent(rowEvent)
      .build()

    mockServer.enqueueEvents(
      VStreamEvents.vgtid("geo_ks", "0", "MySQL56/abc:1-40"),
      VStreamEvents.begin(),
      VStreamEvents.field("geo_ks", "locations", listOf("id" to Query.Type.INT32, "geom" to Query.Type.GEOMETRY)),
      rowVEvent,
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = newChannel()
    try {
      val responses = VitessGrpc.newBlockingStub(channel).vStream(vstreamRequest()).asSequence().toList()
      assertEquals(1, responses.size)
      val row2 = responses[0].eventsList.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(row2)
      // -1 length indicates NULL
      assertEquals(-1L, row2.rowEvent.getRowChanges(0).after.getLengths(1))
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `geometry with other columns`() {
    val wkb = VStreamEvents.wkbPoint(40.7128, -74.0060) // NYC

    mockServer.enqueueEvents(
      VStreamEvents.vgtid("geo_ks", "0", "MySQL56/abc:1-50"),
      VStreamEvents.begin(),
      VStreamEvents.field("geo_ks", "stores", listOf(
        "id" to Query.Type.INT32,
        "name" to Query.Type.VARCHAR,
        "location" to Query.Type.GEOMETRY,
      )),
      VStreamEvents.rowWithBytes("geo_ks", "stores", listOf(
        "42".toByteArray(),
        "NYC Store".toByteArray(),
        wkb,
      )),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = newChannel()
    try {
      val responses = VitessGrpc.newBlockingStub(channel).vStream(vstreamRequest()).asSequence().toList()
      val rowEvent = responses[0].eventsList.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(rowEvent)
      val r = rowEvent.rowEvent.getRowChanges(0).after
      assertEquals(3, r.lengthsCount)
      assertEquals(2L, r.getLengths(0))   // "42"
      assertEquals(9L, r.getLengths(1))   // "NYC Store"
      assertEquals(21L, r.getLengths(2))  // WKB POINT
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `geometry in update event`() {
    val oldWkb = VStreamEvents.wkbPoint(40.7128, -74.0060)
    val newWkb = VStreamEvents.wkbPoint(34.0522, -118.2437)

    mockServer.enqueueEvents(
      VStreamEvents.vgtid("geo_ks", "0", "MySQL56/abc:1-60"),
      VStreamEvents.begin(),
      VStreamEvents.field("geo_ks", "locations", listOf("id" to Query.Type.INT32, "geom" to Query.Type.GEOMETRY)),
      VStreamEvents.rowWithBytes(
        "geo_ks", "locations",
        afterValues = listOf("1".toByteArray(), newWkb),
        beforeValues = listOf("1".toByteArray(), oldWkb),
      ),
      VStreamEvents.commit(),
    )
    mockServer.enqueueComplete()

    val channel = newChannel()
    try {
      val responses = VitessGrpc.newBlockingStub(channel).vStream(vstreamRequest()).asSequence().toList()
      val rowEvent = responses[0].eventsList.find { it.type == Binlogdata.VEventType.ROW }
      assertNotNull(rowEvent)
      val change = rowEvent.rowEvent.getRowChanges(0)
      assertTrue(change.hasBefore())
      assertTrue(change.hasAfter())
      assertEquals(21L, change.before.getLengths(1))
      assertEquals(21L, change.after.getLengths(1))
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `geometry DDL flows through`() {
    mockServer.enqueueEvents(
      VStreamEvents.vgtid("geo_ks", "0", "MySQL56/abc:1-70"),
      VStreamEvents.ddl("CREATE TABLE geo_ks.locations (id INT PRIMARY KEY, geom GEOMETRY NOT NULL SRID 4326)"),
    )
    mockServer.enqueueComplete()

    val channel = newChannel()
    try {
      val responses = VitessGrpc.newBlockingStub(channel).vStream(vstreamRequest()).asSequence().toList()
      val ddl = responses[0].eventsList.find { it.type == Binlogdata.VEventType.DDL }
      assertNotNull(ddl)
      assertTrue(ddl.statement.contains("GEOMETRY"))
      assertTrue(ddl.statement.contains("SRID 4326"))
    } finally {
      channel.shutdownNow()
    }
  }
}
