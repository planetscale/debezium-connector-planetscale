package com.planetscale.debezium

import io.debezium.config.Configuration
import io.debezium.connector.vitess.VitessConnectorConfig
import io.debezium.connector.vitess.VitessValueConverter
import io.debezium.relational.Column
import java.sql.Types
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import org.apache.kafka.connect.data.Field

// Covers the BIT column fix ported from upstream debezium/dbz#2191 (PR #293): VStream delivers BIT
// values as raw bytes; BIT(1) must convert to boolean and BIT(N) must pass through as bytes for the
// Kafka Connect BYTES schema. Before the fix the column was silently dropped from change events.
internal class VitessValueConverterBitTest {

  @Test fun convertsBit1ToTrue() {
    val column = bitColumn(length = 1)
    val field = Field(column.name(), 0, converter().schemaBuilder(column).build())
    val converted = converter().converter(column, field).convert(byteArrayOf(1))
    assertEquals(true, converted)
  }

  @Test fun convertsBit1ToFalse() {
    val column = bitColumn(length = 1)
    val field = Field(column.name(), 0, converter().schemaBuilder(column).build())
    val converted = converter().converter(column, field).convert(byteArrayOf(0))
    assertEquals(false, converted)
  }

  @Test fun passesBit8ThroughAsBytes() {
    val column = bitColumn(length = 8)
    val field = Field(column.name(), 0, converter().schemaBuilder(column).build())
    val converted = converter().converter(column, field).convert(byteArrayOf(0xAA.toByte()))
    assertContentEquals(byteArrayOf(0xAA.toByte()), converted as ByteArray)
  }

  @Test fun passesBit64ThroughAsBytes() {
    val bytes = ByteArray(8) { 0xAA.toByte() }
    val column = bitColumn(length = 64)
    val field = Field(column.name(), 0, converter().schemaBuilder(column).build())
    val converted = converter().converter(column, field).convert(bytes)
    assertContentEquals(bytes, converted as ByteArray)
  }

  @Test fun buildsBooleanSchemaForBit1() {
    val schema = converter().schemaBuilder(bitColumn(length = 1)).build()
    assertEquals(org.apache.kafka.connect.data.Schema.Type.BOOLEAN, schema.type())
  }

  @Test fun buildsBytesSchemaForBit8() {
    val schema = converter().schemaBuilder(bitColumn(length = 8)).build()
    assertEquals(org.apache.kafka.connect.data.Schema.Type.BYTES, schema.type())
    assertEquals(io.debezium.data.Bits.LOGICAL_NAME, schema.name())
  }

  private fun bitColumn(length: Int): Column = Column.editor()
    .name("bit_col")
    .type("BIT")
    .jdbcType(Types.BIT)
    .length(length)
    .optional(true)
    .create()

  // Mirrors the construction in VitessDatabaseSchema, with default configuration.
  private fun converter(): VitessValueConverter {
    val config = VitessConnectorConfig(Configuration.create().build())
    return VitessValueConverter(
      config.getDecimalMode(),
      config.getTemporalPrecisionMode(),
      ZoneOffset.UTC,
      config.binaryHandlingMode(),
      config.includeUnknownDatatypes(),
      config.getBigIntUnsgnedHandlingMode(),
      config.overrideDatetimeToNullable(),
      null,
      config.getEventConvertingFailureHandlingMode(),
      config.getServiceRegistry())
  }
}
