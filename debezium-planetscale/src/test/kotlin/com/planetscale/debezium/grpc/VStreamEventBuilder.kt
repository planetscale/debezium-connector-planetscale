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

  private fun buildRow(values: List<String>): Query.Row {
    val row = Query.Row.newBuilder()
    val concatenated = values.joinToString("")
    row.setValues(ByteString.copyFromUtf8(concatenated))
    values.forEach { row.addLengths(it.length.toLong()) }
    return row.build()
  }
}
