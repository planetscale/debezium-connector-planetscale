/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium

import io.debezium.connector.vitess.VitessValueConverter
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

// Covers the parsing helper introduced for `time.precision.mode = connect` support on MySQL TIMESTAMP columns.
// Upstream Vitess pins these to ZonedTimestamp regardless of mode; the fork's patch parses the UTC vstream string
// into a `java.util.Date` so the column serialises as Kafka Connect's Timestamp logical type (epoch millis).
class VitessValueConverterTimestampTest {

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
}
