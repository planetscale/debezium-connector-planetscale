/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium

import io.debezium.config.Configuration
import io.debezium.connector.vitess.VitessConnectorConfig
import io.debezium.connector.vitess.VitessValueConverter
import io.debezium.relational.Column
import java.sql.Types
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import org.apache.kafka.connect.data.Field

// Covers the parsing helper introduced for `time.precision.mode = connect` support on MySQL TIMESTAMP columns.
// Upstream Vitess pins these to ZonedTimestamp regardless of mode; the fork's patch parses the UTC vstream string
// into a `java.util.Date` so the column serialises as Kafka Connect's Timestamp logical type (epoch millis).
internal class VitessValueConverterTimestampTest {

  @Test fun parsesPlainTimestampAsUtc() {
    val parsed = VitessValueConverter.stringToConnectDate("2026-06-02 08:28:45")
    val expectedMillis = LocalDateTime.of(2026, 6, 2, 8, 28, 45)
      .toInstant(ZoneOffset.UTC).toEpochMilli()
    assertEquals(expectedMillis, parsed!!.time)
  }

  @Test fun parsesFractionalSecondsAndTruncatesToMillis() {
    val parsed = VitessValueConverter.stringToConnectDate("2026-06-02 08:28:45.123456")
    val expectedMillis = LocalDateTime.of(2026, 6, 2, 8, 28, 45, 123_000_000)
      .toInstant(ZoneOffset.UTC).toEpochMilli()
    // java.util.Date is millisecond-precision; sub-millisecond fraction is silently truncated by toEpochMilli.
    assertEquals(expectedMillis, parsed!!.time)
  }

  @Test fun parsesMillisecondFraction() {
    val parsed = VitessValueConverter.stringToConnectDate("2026-06-02 08:28:45.001")
    val expectedMillis = LocalDateTime.of(2026, 6, 2, 8, 28, 45, 1_000_000)
      .toInstant(ZoneOffset.UTC).toEpochMilli()
    assertEquals(expectedMillis, parsed!!.time)
  }

  @Test fun returnsNullForMySqlZeroDateSentinel() {
    assertNull(VitessValueConverter.stringToConnectDate("0000-00-00 00:00:00"))
  }

  // Converter-level zero-date behaviour: the sentinel must follow the same path as a DATETIME zero-date
  // (null for optional columns, epoch fallback for non-optional columns). Delivering nothing instead would
  // route it through JdbcValueConverters#handleUnknownData, which throws for non-optional columns.

  @Test fun convertsZeroDateToNullForOptionalColumn() {
    val column = timestampColumn(optional = true)
    val field = Field(column.name(), 0, org.apache.kafka.connect.data.Timestamp.builder().optional().schema())
    val converted = connectModeConverter().converter(column, field).convert("0000-00-00 00:00:00")
    assertNull(converted)
  }

  @Test fun convertsZeroDateToEpochForNonOptionalColumn() {
    val column = timestampColumn(optional = false)
    val field = Field(column.name(), 0, org.apache.kafka.connect.data.Timestamp.builder().schema())
    val converted = connectModeConverter().converter(column, field).convert("0000-00-00 00:00:00")
    assertEquals(Date(0L), converted)
  }

  @Test fun convertsRegularTimestampThroughConverter() {
    val column = timestampColumn(optional = false)
    val field = Field(column.name(), 0, org.apache.kafka.connect.data.Timestamp.builder().schema())
    val converted = connectModeConverter().converter(column, field).convert("2026-06-02 08:28:45")
    val expectedMillis = LocalDateTime.of(2026, 6, 2, 8, 28, 45).toInstant(ZoneOffset.UTC).toEpochMilli()
    assertEquals(Date(expectedMillis), converted)
  }

  private fun timestampColumn(optional: Boolean): Column = Column.editor()
    .name("ts_col")
    .type("TIMESTAMP")
    .jdbcType(Types.TIMESTAMP_WITH_TIMEZONE)
    .optional(optional)
    .create()

  // Mirrors the construction in VitessDatabaseSchema, with `time.precision.mode = connect`.
  private fun connectModeConverter(): VitessValueConverter {
    val config = VitessConnectorConfig(
      Configuration.create().with(VitessConnectorConfig.TIME_PRECISION_MODE, "connect").build())
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
