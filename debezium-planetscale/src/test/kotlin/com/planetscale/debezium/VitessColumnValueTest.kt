package com.planetscale.debezium

import io.debezium.connector.vitess.VitessType
import io.debezium.connector.vitess.connection.VitessColumnValue
import java.nio.charset.StandardCharsets
import java.sql.Types
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class VitessColumnValueTest {
  private fun columnValue(s: String) = VitessColumnValue(s.toByteArray(StandardCharsets.UTF_8))
  private fun columnValue(bytes: ByteArray?) = VitessColumnValue(bytes)

  @Test
  fun `asString returns UTF-8 string`() {
    val cv = columnValue("hello world")
    assertEquals("hello world", cv.asString())
  }

  @Test
  fun `asInteger parses int from bytes`() {
    val cv = columnValue("42")
    assertEquals(42, cv.asInteger())
  }

  @Test
  fun `asShort parses short from bytes`() {
    val cv = columnValue("123")
    assertEquals(123.toShort(), cv.asShort())
  }

  @Test
  fun `asLong parses long from bytes`() {
    val cv = columnValue("9876543210")
    assertEquals(9876543210L, cv.asLong())
  }

  @Test
  fun `asFloat parses float from bytes`() {
    val cv = columnValue("3.14")
    assertEquals(3.14f, cv.asFloat(), 0.001f)
  }

  @Test
  fun `asDouble parses double from bytes`() {
    val cv = columnValue("2.71828")
    assertEquals(2.71828, cv.asDouble(), 0.00001)
  }

  @Test
  fun `asBytes returns raw bytes`() {
    val bytes = byteArrayOf(1, 2, 3, 4)
    val cv = columnValue(bytes)
    assertContentEquals(bytes, cv.asBytes())
  }

  @Test
  fun `isNull returns true for null value`() {
    val cv = columnValue(null as ByteArray?)
    assertTrue(cv.isNull)
  }

  @Test
  fun `isNull returns false for non-null value`() {
    val cv = columnValue("data")
    assertFalse(cv.isNull)
  }

  @Test
  fun `asGeometry returns Geometry struct`() {
    // Minimal WKB for a POINT: byte order (1) + type (1=point) + x + y
    val wkb = byteArrayOf(
      0x01, // little endian
      0x01, 0x00, 0x00, 0x00, // WKB type: Point
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59.toByte(), 0x40, // x = 100.0
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
    )
    val cv = columnValue(wkb)
    val geom = cv.asGeometry()
    assertNotNull(geom)
  }

  @Test
  fun `asDefault with GEOMETRY type returns geometry`() {
    val wkb = byteArrayOf(
      0x01,
      0x01, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59.toByte(), 0x40,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    )
    val cv = columnValue(wkb)
    val vitessType = VitessType("GEOMETRY", Types.OTHER)
    val result = cv.asDefault(vitessType, false)
    assertNotNull(result)
  }

  @Test
  fun `asDefault with unknown type and include returns bytes`() {
    val bytes = byteArrayOf(0x01, 0x02, 0x03)
    val cv = columnValue(bytes)
    val vitessType = VitessType("UNKNOWN_TYPE", Types.OTHER)
    val result = cv.asDefault(vitessType, true)
    assertNotNull(result)
    assertContentEquals(bytes, result as ByteArray)
  }

  @Test
  fun `asDefault with unknown type and exclude returns null`() {
    val bytes = byteArrayOf(0x01, 0x02, 0x03)
    val cv = columnValue(bytes)
    val vitessType = VitessType("UNKNOWN_TYPE", Types.OTHER)
    val result = cv.asDefault(vitessType, false)
    assertNull(result)
  }

  @Test
  fun `getRawValue returns original bytes`() {
    val bytes = byteArrayOf(10, 20, 30)
    val cv = columnValue(bytes)
    assertContentEquals(bytes, cv.rawValue)
  }
}
