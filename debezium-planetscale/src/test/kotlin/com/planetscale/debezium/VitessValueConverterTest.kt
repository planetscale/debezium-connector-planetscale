package com.planetscale.debezium

import io.debezium.connector.vitess.VitessValueConverter
import java.math.BigDecimal
import java.time.Duration
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class VitessValueConverterTest {

  // -- stringToDuration tests --

  @Test
  fun `stringToDuration parses positive time`() {
    val duration = VitessValueConverter.stringToDuration("12:30:45")
    assertEquals(Duration.ofHours(12).plusMinutes(30).plusSeconds(45), duration)
  }

  @Test
  fun `stringToDuration parses negative time`() {
    val duration = VitessValueConverter.stringToDuration("-05:15:30")
    assertEquals(Duration.ofHours(-5).minusMinutes(15).minusSeconds(30), duration)
    assertTrue(duration.isNegative)
  }

  @Test
  fun `stringToDuration parses time with microseconds`() {
    val duration = VitessValueConverter.stringToDuration("01:02:03.456789")
    assertEquals(Duration.ofHours(1).plusMinutes(2).plusSeconds(3).plusNanos(456789000), duration)
  }

  @Test
  fun `stringToDuration parses zero time`() {
    val duration = VitessValueConverter.stringToDuration("00:00:00")
    assertEquals(Duration.ZERO, duration)
  }

  @Test
  fun `stringToDuration parses large hours`() {
    val duration = VitessValueConverter.stringToDuration("838:59:59")
    assertEquals(Duration.ofHours(838).plusMinutes(59).plusSeconds(59), duration)
  }

  // -- stringToLocalDate tests --

  @Test
  fun `stringToLocalDate parses valid date`() {
    val date = VitessValueConverter.stringToLocalDate("2024-03-15")
    assertNotNull(date)
    assertEquals(LocalDate.of(2024, 3, 15), date)
  }

  @Test
  fun `stringToLocalDate returns null for zero month`() {
    val date = VitessValueConverter.stringToLocalDate("2024-00-15")
    assertNull(date)
  }

  @Test
  fun `stringToLocalDate returns null for zero day`() {
    val date = VitessValueConverter.stringToLocalDate("2024-03-00")
    assertNull(date)
  }

  @Test
  fun `stringToLocalDate handles zero year`() {
    val date = VitessValueConverter.stringToLocalDate("0000-01-01")
    assertNotNull(date)
    assertEquals(0, date.year)
  }

  // -- stringToTimestamp tests --

  @Test
  fun `stringToTimestamp parses valid datetime`() {
    val ts = VitessValueConverter.stringToTimestamp("2024-03-15 10:30:45")
    assertNotNull(ts)
  }

  @Test
  fun `stringToTimestamp returns null for zero day`() {
    val ts = VitessValueConverter.stringToTimestamp("2024-03-00 10:30:45")
    assertNull(ts)
  }

  @Test
  fun `stringToTimestamp returns null for zero month`() {
    val ts = VitessValueConverter.stringToTimestamp("2024-00-15 10:30:45")
    assertNull(ts)
  }

  // -- convertUnsignedBigint tests (via reflection since the method is protected static) --

  @Test
  fun `convertUnsignedBigint corrects negative values`() {
    // -1 + BIGINT_CORRECTION (18446744073709551616) = 18446744073709551615
    val result = invokeConvertUnsignedBigint(BigDecimal("-1"))
    assertEquals(BigDecimal("18446744073709551615"), result)
  }

  @Test
  fun `convertUnsignedBigint preserves positive values`() {
    val result = invokeConvertUnsignedBigint(BigDecimal("42"))
    assertEquals(BigDecimal("42"), result)
  }

  @Test
  fun `convertUnsignedBigint preserves zero`() {
    val result = invokeConvertUnsignedBigint(BigDecimal.ZERO)
    assertEquals(BigDecimal.ZERO, result)
  }

  @Test
  fun `convertUnsignedBigint corrects large negative`() {
    val negVal = BigDecimal("-9223372036854775808") // Long.MIN_VALUE
    val result = invokeConvertUnsignedBigint(negVal)
    assertTrue(result.compareTo(BigDecimal.ZERO) > 0)
  }

  // -- matches tests (via reflection since the method is protected static) --

  @Test
  fun `matches exact type name`() {
    assertTrue(invokeMatches("JSON", "JSON"))
  }

  @Test
  fun `matches type name with parentheses`() {
    assertTrue(invokeMatches("VARCHAR(255)", "VARCHAR"))
  }

  @Test
  fun `matches returns false for null`() {
    assertFalse(invokeMatches(null, "JSON"))
  }

  @Test
  fun `matches returns false for non-matching type`() {
    assertFalse(invokeMatches("INT32", "JSON"))
  }

  // -- isDateOrDateTime tests --

  @Test
  fun `isDateOrDateTime returns true for DATE`() {
    assertTrue(VitessValueConverter.isDateOrDateTime("DATE"))
  }

  @Test
  fun `isDateOrDateTime returns true for DATETIME`() {
    assertTrue(VitessValueConverter.isDateOrDateTime("DATETIME"))
  }

  @Test
  fun `isDateOrDateTime returns false for TIME`() {
    assertFalse(VitessValueConverter.isDateOrDateTime("TIME"))
  }

  companion object {
    private val convertUnsignedBigintMethod = VitessValueConverter::class.java
      .getDeclaredMethod("convertUnsignedBigint", BigDecimal::class.java)
      .also { it.isAccessible = true }

    private val matchesMethod = VitessValueConverter::class.java
      .getDeclaredMethod("matches", String::class.java, String::class.java)
      .also { it.isAccessible = true }

    private fun invokeConvertUnsignedBigint(value: BigDecimal): BigDecimal =
      convertUnsignedBigintMethod.invoke(null, value) as BigDecimal

    private fun invokeMatches(typeName: String?, match: String): Boolean =
      matchesMethod.invoke(null, typeName, match) as Boolean
  }
}
