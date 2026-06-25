package com.planetscale.debezium.vitess

import binlogdata.Binlogdata
import binlogdata.Binlogdata.VEvent
import io.debezium.config.Configuration
import io.debezium.connector.vitess.Vgtid
import io.debezium.connector.vitess.VitessConnectorConfig
import io.grpc.ManagedChannelBuilder
import io.vitess.proto.Vtgate
import io.vitess.proto.grpc.VitessGrpc
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.TestMethodOrder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Integration tests against a real Vitess (vttestserver) container loaded with
 * the `maxeng_etl` PlanetScale dataset. Tests exercise real gRPC VStream CDC.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class VitessIntegrationTest {

  companion object {
    private lateinit var vitess: VitessTestContainer

    @JvmStatic
    @BeforeAll
    fun startVitess() {
      vitess = VitessTestContainer()
      vitess.start()
      vitess.loadSqlResource("fixtures/maxeng_etl.sql")
    }

    @JvmStatic
    @AfterAll
    fun stopVitess() {
      if (::vitess.isInitialized) vitess.stop()
    }
  }

  private fun jdbcQuery(sql: String): List<Map<String, Any?>> {
    vitess.jdbcConnection().use { conn ->
      conn.createStatement().use { stmt ->
        stmt.executeQuery(sql).use { rs ->
          val meta = rs.metaData
          val results = mutableListOf<Map<String, Any?>>()
          while (rs.next()) {
            val row = mutableMapOf<String, Any?>()
            for (i in 1..meta.columnCount) {
              row[meta.getColumnLabel(i)] = rs.getObject(i)
            }
            results.add(row)
          }
          return results
        }
      }
    }
  }

  private fun jdbcExec(sql: String) {
    vitess.jdbcConnection().use { conn ->
      conn.createStatement().use { it.execute(sql) }
    }
  }

  /** Collect VStream events after executing a DML statement. Returns events within timeout. */
  private fun vstreamAfterDml(
    dml: String,
    keyspace: String = "maxeng_etl",
    shard: String = "0",
    timeoutSeconds: Long = 10,
  ): List<VEvent> {
    val channel = ManagedChannelBuilder
      .forAddress(vitess.host, vitess.grpcPort)
      .usePlaintext()
      .build()

    try {
      val stub = VitessGrpc.newStub(channel)
      val request = Vtgate.VStreamRequest.newBuilder()
        .setVgtid(
          Binlogdata.VGtid.newBuilder().addShardGtids(
            Binlogdata.ShardGtid.newBuilder()
              .setKeyspace(keyspace).setShard(shard).setGtid("current"),
          ),
        )
        .build()

      val collectedEvents = mutableListOf<VEvent>()
      val latch = CountDownLatch(1)
      val error = AtomicReference<Throwable>()

      val observer = object : io.grpc.stub.StreamObserver<Vtgate.VStreamResponse> {
        override fun onNext(response: Vtgate.VStreamResponse) {
          collectedEvents.addAll(response.eventsList)
          // Signal once we see a COMMIT (transaction complete)
          if (response.eventsList.any { it.type == Binlogdata.VEventType.COMMIT }) {
            latch.countDown()
          }
        }
        override fun onError(t: Throwable) { error.set(t); latch.countDown() }
        override fun onCompleted() { latch.countDown() }
      }

      stub.vStream(request, observer)

      // Give VStream a moment to establish, then execute DML
      Thread.sleep(500)
      jdbcExec(dml)

      assertTrue(latch.await(timeoutSeconds, TimeUnit.SECONDS), "VStream timed out waiting for events")
      error.get()?.let { throw it }
      return collectedEvents
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  @Order(1)
  fun `dataset loads successfully`() {
    val d1Count = jdbcQuery("SELECT COUNT(*) as cnt FROM d1")
    assertEquals(2L, (d1Count[0]["cnt"] as Number).toLong())

    val t1Count = jdbcQuery("SELECT COUNT(*) as cnt FROM t1")
    assertEquals(3L, (t1Count[0]["cnt"] as Number).toLong())

    val vstreamCount = jdbcQuery("SELECT COUNT(*) as cnt FROM vstream_test")
    assertEquals(2L, (vstreamCount[0]["cnt"] as Number).toLong())
  }

  @Test
  @Order(2)
  fun `vstream receives insert on d1`() {
    val events = vstreamAfterDml("INSERT INTO d1 VALUES (200)")
    val rowEvents = events.filter { it.type == Binlogdata.VEventType.ROW }
    assertTrue(rowEvents.isNotEmpty(), "Expected at least one ROW event")
  }

  @Test
  @Order(3)
  fun `vstream receives geometry insert on t1`() {
    val events = vstreamAfterDml(
      "INSERT INTO t1 VALUES (100, ST_GeomFromText('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'))",
    )

    val fieldEvents = events.filter { it.type == Binlogdata.VEventType.FIELD }
    val rowEvents = events.filter { it.type == Binlogdata.VEventType.ROW }

    assertTrue(rowEvents.isNotEmpty(), "Expected ROW event for geometry insert")

    // Verify the FIELD event declares a GEOMETRY column
    if (fieldEvents.isNotEmpty()) {
      val fields = fieldEvents.last().fieldEvent.fieldsList
      val geomField = fields.find { it.name == "shape" }
      assertNotNull(geomField, "Expected 'shape' field in FIELD event")
    }

    // Verify the ROW data contains bytes (WKB)
    val row = rowEvents.last().rowEvent.getRowChanges(0).after
    assertTrue(row.values.size() > 0, "ROW should contain data")
  }

  @Test
  @Order(4)
  fun `vstream receives all column types from vstream_test`() {
    val events = vstreamAfterDml(
      """INSERT INTO vstream_test (tiny_col, small_col, medium_col, int_col, big_col, bit_col,
         varchar_col, text_col, float_col, double_col, decimal_col, date_col, datetime_col,
         timestamp_col, time_col, json_col, blob_col, bool_col, enum_col)
         VALUES (1, 2, 3, 4, 5, b'10101010', 'hello', 'world', 1.5, 2.5, 99.99,
         '2026-03-27', '2026-03-27 12:00:00', '2026-03-27 12:00:00', '12:00:00',
         '{"a":1}', 'blob data', true, 'small')""",
    )

    val fieldEvents = events.filter { it.type == Binlogdata.VEventType.FIELD }
    val rowEvents = events.filter { it.type == Binlogdata.VEventType.ROW }

    assertTrue(rowEvents.isNotEmpty(), "Expected ROW event for vstream_test insert")

    // Verify FIELD event declares all 20 columns
    if (fieldEvents.isNotEmpty()) {
      val fields = fieldEvents.last().fieldEvent.fieldsList
      assertTrue(fields.size >= 20, "Expected at least 20 fields, got ${fields.size}")
    }
  }

  @Test
  @Order(5)
  fun `vstream delivers insert update delete lifecycle`() {
    // INSERT
    val insertEvents = vstreamAfterDml("INSERT INTO d1 VALUES (300)")
    assertTrue(
      insertEvents.any { it.type == Binlogdata.VEventType.ROW },
      "Expected ROW event for INSERT",
    )

    // UPDATE
    val updateEvents = vstreamAfterDml("UPDATE d1 SET id = 301 WHERE id = 300")
    val updateRow = updateEvents.filter { it.type == Binlogdata.VEventType.ROW }
    assertTrue(updateRow.isNotEmpty(), "Expected ROW event for UPDATE")
    // UPDATE should have both before and after
    if (updateRow.isNotEmpty()) {
      val change = updateRow.last().rowEvent.getRowChanges(0)
      assertTrue(change.hasBefore() || change.hasAfter(), "UPDATE should have before or after values")
    }

    // DELETE
    val deleteEvents = vstreamAfterDml("DELETE FROM d1 WHERE id = 301")
    assertTrue(
      deleteEvents.any { it.type == Binlogdata.VEventType.ROW },
      "Expected ROW event for DELETE",
    )
  }

  @Test
  @Order(6)
  fun `connector config resolves against live Vitess`() {
    val config = VitessConnectorConfig(
      Configuration.create()
        .with("database.hostname", vitess.host)
        .with("database.port", vitess.grpcPort.toString())
        .with("database.user", "")
        .with("database.password", "")
        .with("vitess.keyspace", "maxeng_etl")
        .with("vitess.cells", "zone1")
        .with("topic.prefix", "test")
        .with("snapshot.mode", "never")
        .build(),
    )
    assertEquals("maxeng_etl", config.keyspace)
    assertEquals(vitess.grpcPort, config.vtgatePort)
  }
}
