package com.planetscale.debezium.geometry

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Types
import kotlin.test.*

class GeometryValueHandlerTest {

  @Test
  fun `reflection methods extract type information correctly`() {
    // Create mock VitessType-like object for testing reflection
    val mockVitessType = MockVitessType("GEOMETRY", Types.OTHER)
    
    val typeName = GeometryValueHandler::class.java
      .getDeclaredMethod("getTypeName", Any::class.java)
      .apply { isAccessible = true }
      .invoke(GeometryValueHandler, mockVitessType) as String?
    
    val jdbcId = GeometryValueHandler::class.java
      .getDeclaredMethod("getJdbcId", Any::class.java)
      .apply { isAccessible = true }
      .invoke(GeometryValueHandler, mockVitessType) as Int
    
    assertEquals("GEOMETRY", typeName)
    assertEquals(Types.OTHER, jdbcId)
  }

  @Test
  fun `reflection methods handle missing methods gracefully`() {
    val invalidObject = "not a VitessType"
    
    val typeName = GeometryValueHandler::class.java
      .getDeclaredMethod("getTypeName", Any::class.java)
      .apply { isAccessible = true }
      .invoke(GeometryValueHandler, invalidObject) as String?
    
    val jdbcId = GeometryValueHandler::class.java
      .getDeclaredMethod("getJdbcId", Any::class.java)
      .apply { isAccessible = true }
      .invoke(GeometryValueHandler, invalidObject) as Int
    
    assertNull(typeName)
    assertEquals(-1, jdbcId)
  }

  @Test
  fun `isGeometryType detects GEOMETRY types correctly`() {
    val isGeometryMethod = GeometryValueHandler::class.java
      .getDeclaredMethod("isGeometryType", String::class.java, Int::class.javaPrimitiveType)
      .apply { isAccessible = true }
    
    // Valid GEOMETRY types
    assertTrue(isGeometryMethod.invoke(GeometryValueHandler, "GEOMETRY", Types.OTHER) as Boolean)
    assertTrue(isGeometryMethod.invoke(GeometryValueHandler, "POINT", Types.OTHER) as Boolean)
    assertTrue(isGeometryMethod.invoke(GeometryValueHandler, "POLYGON", Types.OTHER) as Boolean)
    assertTrue(isGeometryMethod.invoke(GeometryValueHandler, "LINESTRING", Types.OTHER) as Boolean)
    
    // Invalid cases
    assertFalse(isGeometryMethod.invoke(GeometryValueHandler, "VARCHAR", Types.VARCHAR) as Boolean)
    assertFalse(isGeometryMethod.invoke(GeometryValueHandler, "GEOMETRY", Types.VARCHAR) as Boolean)
    assertFalse(isGeometryMethod.invoke(GeometryValueHandler, "INT", Types.OTHER) as Boolean)
    assertFalse(isGeometryMethod.invoke(GeometryValueHandler, null, Types.OTHER) as Boolean)
  }

  @Test
  fun `getRawValue extracts bytes from mock column value`() {
    val testBytes = byteArrayOf(1, 2, 3, 4, 5)
    val mockColumnValue = MockVitessColumnValue(testBytes)
    
    val rawValue = GeometryValueHandler::class.java
      .getDeclaredMethod("getRawValue", Any::class.java)
      .apply { isAccessible = true }
      .invoke(GeometryValueHandler, mockColumnValue) as ByteArray?
    
    assertContentEquals(testBytes, rawValue)
  }

  @Test
  fun `getRawValue handles objects without getRawValue method`() {
    val invalidObject = "not a VitessColumnValue"
    
    val rawValue = GeometryValueHandler::class.java
      .getDeclaredMethod("getRawValue", Any::class.java)
      .apply { isAccessible = true }
      .invoke(GeometryValueHandler, invalidObject) as ByteArray?
    
    assertNull(rawValue)
  }

  @Test
  fun `handleResolveValue processes GEOMETRY types correctly`() {
    // Mock a POINT geometry: SRID=4326 + WKB point data
    val srid = 4326
    val pointWkb = byteArrayOf(1, 1, 0, 0, 0, 64, 94, -99, 119, -82, 72, 94, -64, 64, 66, -41, 92, -113, 47, 69, 64)
    val geometryBytes = ByteArray(4 + pointWkb.size)
    
    // Pack SRID in little-endian format
    geometryBytes[0] = (srid and 0xFF).toByte()
    geometryBytes[1] = ((srid shr 8) and 0xFF).toByte()
    geometryBytes[2] = ((srid shr 16) and 0xFF).toByte()
    geometryBytes[3] = ((srid shr 24) and 0xFF).toByte()
    pointWkb.copyInto(geometryBytes, 4)
    
    val mockVitessType = MockVitessType("POINT", Types.OTHER)
    val mockColumnValue = MockVitessColumnValue(geometryBytes)
    val mockCallable = MockCallable("original result")
    
    val result = GeometryValueHandler.handleResolveValue(
      mockVitessType,
      mockColumnValue,
      true,
      "ADAPTIVE",
      mockCallable
    )
    
    // Should return converted geometry structure, not original result
    assertTrue(result is Map<*, *>)
    val geometryMap = result as Map<*, *>
    assertEquals(4326, geometryMap["srid"])
    assertContentEquals(pointWkb, geometryMap["wkb"] as ByteArray)
    
    // Original callable should not have been called
    assertFalse(mockCallable.wasCalled)
  }

  @Test
  fun `handleResolveValue delegates to original method for non-GEOMETRY types`() {
    val mockVitessType = MockVitessType("VARCHAR", Types.VARCHAR)
    val mockColumnValue = MockVitessColumnValue(byteArrayOf(1, 2, 3))
    val mockCallable = MockCallable("original result")
    
    val result = GeometryValueHandler.handleResolveValue(
      mockVitessType,
      mockColumnValue,
      true,
      "ADAPTIVE", 
      mockCallable
    )
    
    // Should return original result
    assertEquals("original result", result)
    assertTrue(mockCallable.wasCalled)
  }

  // Mock classes for testing
  private class MockVitessType(private val name: String, private val jdbcId: Int) {
    fun getName(): String = name
    fun getJdbcId(): Int = jdbcId
  }

  private class MockVitessColumnValue(private val rawValue: ByteArray) {
    fun getRawValue(): ByteArray = rawValue
  }

  private class MockCallable(private val result: Any) : java.util.concurrent.Callable<Any> {
    var wasCalled = false
      private set
    
    override fun call(): Any {
      wasCalled = true
      return result
    }
  }
}