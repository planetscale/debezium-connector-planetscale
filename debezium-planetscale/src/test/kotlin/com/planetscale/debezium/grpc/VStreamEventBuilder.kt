package com.planetscale.debezium.grpc

import binlogdata.Binlogdata
import binlogdata.Binlogdata.VEvent
import com.google.protobuf.ByteString
import io.vitess.proto.Query

/**
 * Helper DSL for building realistic VStream event sequences in tests.
 */
object VStreamEvents {
  fun vgtid(keyspace: String, shard: String, gtid: String): VEvent =
    VEvent.newBuilder()
      .setType(Binlogdata.VEventType.VGTID)
      .setVgtid(
        Binlogdata.VGtid.newBuilder()
          .addShardGtids(
            Binlogdata.ShardGtid.newBuilder()
              .setKeyspace(keyspace)
              .setShard(shard)
              .setGtid(gtid)
              .build()
          )
          .build()
      )
      .build()

  fun begin(timestamp: Long = System.currentTimeMillis() / 1000): VEvent =
    VEvent.newBuilder()
      .setType(Binlogdata.VEventType.BEGIN)
      .setTimestamp(timestamp)
      .build()

  fun field(
    keyspace: String,
    table: String,
    fields: List<Pair<String, Query.Type>>,
  ): VEvent {
    val fieldEvent = Binlogdata.FieldEvent.newBuilder()
      .setTableName("$keyspace.$table")

    fields.forEach { (name, type) ->
      fieldEvent.addFields(
        Query.Field.newBuilder()
          .setName(name)
          .setType(type)
          .build()
      )
    }

    return VEvent.newBuilder()
      .setType(Binlogdata.VEventType.FIELD)
      .setFieldEvent(fieldEvent.build())
      .build()
  }

  /**
   * Build a ROW event. Vitess Row proto stores values as a single concatenated ByteString
   * with lengths indicating how to split them.
   */
  fun row(
    keyspace: String,
    table: String,
    afterValues: List<String>,
    beforeValues: List<String>? = null,
  ): VEvent {
    val rowChange = Binlogdata.RowChange.newBuilder()

    rowChange.setAfter(buildRow(afterValues))
    if (beforeValues != null) {
      rowChange.setBefore(buildRow(beforeValues))
    }

    val rowEvent = Binlogdata.RowEvent.newBuilder()
      .setTableName("$keyspace.$table")
      .addRowChanges(rowChange.build())
      .build()

    return VEvent.newBuilder()
      .setType(Binlogdata.VEventType.ROW)
      .setRowEvent(rowEvent)
      .build()
  }

  fun commit(timestamp: Long = System.currentTimeMillis() / 1000): VEvent =
    VEvent.newBuilder()
      .setType(Binlogdata.VEventType.COMMIT)
      .setTimestamp(timestamp)
      .build()

  fun heartbeat(timestamp: Long = System.currentTimeMillis() / 1000): VEvent =
    VEvent.newBuilder()
      .setType(Binlogdata.VEventType.HEARTBEAT)
      .setTimestamp(timestamp)
      .build()

  fun ddl(statement: String, timestamp: Long = System.currentTimeMillis() / 1000): VEvent =
    VEvent.newBuilder()
      .setType(Binlogdata.VEventType.DDL)
      .setStatement(statement)
      .setTimestamp(timestamp)
      .build()

  fun other(timestamp: Long = System.currentTimeMillis() / 1000): VEvent =
    VEvent.newBuilder()
      .setType(Binlogdata.VEventType.OTHER)
      .setTimestamp(timestamp)
      .build()

  /**
   * Build a complete transaction with VGTID + BEGIN + FIELD + ROW + COMMIT.
   */
  fun insertTransaction(
    keyspace: String,
    shard: String,
    table: String,
    gtid: String,
    fields: List<Pair<String, Query.Type>>,
    values: List<String>,
    timestamp: Long = System.currentTimeMillis() / 1000,
  ): List<VEvent> = listOf(
    vgtid(keyspace, shard, gtid),
    begin(timestamp),
    field(keyspace, table, fields),
    row(keyspace, table, values),
    commit(timestamp),
  )

  /**
   * Build a ROW event with raw byte arrays for binary column values (e.g., GEOMETRY WKB).
   * Use this instead of [row] when any column contains non-UTF-8 binary data.
   */
  fun rowWithBytes(
    keyspace: String,
    table: String,
    afterValues: List<ByteArray>,
    beforeValues: List<ByteArray>? = null,
  ): VEvent {
    val rowChange = Binlogdata.RowChange.newBuilder()
    rowChange.setAfter(buildRowFromBytes(afterValues))
    if (beforeValues != null) {
      rowChange.setBefore(buildRowFromBytes(beforeValues))
    }
    val rowEvent = Binlogdata.RowEvent.newBuilder()
      .setTableName("$keyspace.$table")
      .addRowChanges(rowChange.build())
      .build()
    return VEvent.newBuilder()
      .setType(Binlogdata.VEventType.ROW)
      .setRowEvent(rowEvent)
      .build()
  }

  private fun buildRow(values: List<String>): Query.Row {
    val row = Query.Row.newBuilder()
    val concatenated = values.joinToString("")
    row.setValues(ByteString.copyFromUtf8(concatenated))
    values.forEach { row.addLengths(it.length.toLong()) }
    return row.build()
  }

  private fun buildRowFromBytes(values: List<ByteArray>): Query.Row {
    val row = Query.Row.newBuilder()
    val totalSize = values.sumOf { it.size }
    val combined = ByteArray(totalSize)
    var offset = 0
    for (v in values) {
      System.arraycopy(v, 0, combined, offset, v.size)
      offset += v.size
    }
    row.setValues(ByteString.copyFrom(combined))
    values.forEach { row.addLengths(it.size.toLong()) }
    return row.build()
  }

  // -- WKB geometry helpers --

  /** Build a WKB POINT (little-endian). */
  fun wkbPoint(x: Double, y: Double): ByteArray {
    val buf = java.nio.ByteBuffer.allocate(21).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    buf.put(1) // little-endian
    buf.putInt(1) // WKB type: Point
    buf.putDouble(x)
    buf.putDouble(y)
    return buf.array()
  }

  /** Build a WKB LINESTRING (little-endian). */
  fun wkbLineString(points: List<Pair<Double, Double>>): ByteArray {
    val buf = java.nio.ByteBuffer.allocate(9 + 16 * points.size).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    buf.put(1) // little-endian
    buf.putInt(2) // WKB type: LineString
    buf.putInt(points.size)
    for ((x, y) in points) { buf.putDouble(x); buf.putDouble(y) }
    return buf.array()
  }

  /** Build a WKB POLYGON with a single ring (little-endian). */
  fun wkbPolygon(ring: List<Pair<Double, Double>>): ByteArray {
    val buf = java.nio.ByteBuffer.allocate(13 + 16 * ring.size).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    buf.put(1) // little-endian
    buf.putInt(3) // WKB type: Polygon
    buf.putInt(1) // 1 ring
    buf.putInt(ring.size)
    for ((x, y) in ring) { buf.putDouble(x); buf.putDouble(y) }
    return buf.array()
  }
}
