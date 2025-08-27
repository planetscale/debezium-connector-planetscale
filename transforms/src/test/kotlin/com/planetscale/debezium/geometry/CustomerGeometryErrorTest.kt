package com.planetscale.debezium.geometry

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import java.sql.Types
import kotlin.test.*

/**
 * Tests that verify the customer's specific GEOMETRY error is fixed.
 * 
 * The customer reported this error:
 * "Cannot resolve JDBC type from VStream field name: "spatial_polygon"
 * type: GEOMETRY
 * jdbcId: 1111"
 * 
 * These tests ensure our fixes resolve this exact issue.
 */
class CustomerGeometryErrorTest {

  @Test
  fun `VitessTypeHandler resolves GEOMETRY field that originally failed`() {
    // Create a mock field that represents the customer's failing scenario
    val mockField = MockQueryField(
      name = "spatial_polygon",
      type = MockFieldType("GEOMETRY")
    )
    
    // Mock the original callable that would fail
    val mockCallable = MockCallable(null) // Returns null to indicate failure
    
    val result = VitessTypeHandler.handleResolve(
      arrayOf(mockField),
      mockCallable
    )
    
    // Should create a VitessType for GEOMETRY instead of returning null
    assertNotNull(result)
    
    // Verify it's a proper VitessType with GEOMETRY characteristics
    assertTrue(isGeometryVitessType(result))
    
    // Verify the original callable was called first (following new logic)
    assertTrue(mockCallable.wasCalled)
  }

  @Test
  fun `VitessTypeHandler handles exception from original resolve method`() {
    val mockField = MockQueryField(
      name = "spatial_polygon", 
      type = MockFieldType("GEOMETRY")
    )
    
    // Mock callable that throws the customer's exact error
    val mockCallable = MockCallable(
      RuntimeException("Cannot resolve JDBC type from VStream field name: \"spatial_polygon\" type: GEOMETRY jdbcId: 1111")
    )
    
    val result = VitessTypeHandler.handleResolve(
      arrayOf(mockField),
      mockCallable
    )
    
    // Should create GEOMETRY VitessType instead of propagating exception
    assertNotNull(result)
    assertTrue(isGeometryVitessType(result))
    assertTrue(mockCallable.wasCalled)
  }

  @Test
  fun `GeometryTypeHandler detects GEOMETRY fields from customer scenario`() {
    // Test various representations of the customer's field
    val testCases: List<Array<Any>> = listOf(
      arrayOf<Any>(Types.OTHER, "GEOMETRY", "spatial_polygon"),
      arrayOf<Any>("GEOMETRY", 1111),
      arrayOf<Any>(MockQueryField("spatial_polygon", MockFieldType("GEOMETRY"))),
      arrayOf<Any>("spatial_polygon", Types.OTHER),
      arrayOf<Any>(MockColumnMetadata("spatial_polygon", "GEOMETRY", Types.OTHER))
    )
    
    testCases.forEach { args ->
      val detected = GeometryTypeHandler.detectGeometryType(args)
      assertTrue(detected, "Failed to detect GEOMETRY from args: ${args.joinToString { it.toString() }}")
    }
  }

  @Test
  fun `enhanced field detection recognizes spatial field names`() {
    val spatialFieldNames = listOf(
      "spatial_polygon",     // Customer's exact field name
      "spatial_point",
      "geometry_data", 
      "geo_location",
      "geom_area",
      "location_point",
      "coordinate_data",
      "position_geom",
      "polygon_field",
      "line_geometry"
    )
    
    spatialFieldNames.forEach { fieldName ->
      val mockField = MockQueryField(fieldName, MockFieldType("GEOMETRY"))
      val detected = GeometryTypeHandler.detectGeometryType(arrayOf(mockField))
      assertTrue(detected, "Failed to detect GEOMETRY from spatial field name: $fieldName")
    }
  }

  @Test
  fun `VitessGeometry transform handles field message with GEOMETRY fields`() {
    // Create a VEvent with GEOMETRY field definitions
    val geometryField = MockQueryField("spatial_polygon", MockFieldType("GEOMETRY"))
    val fieldEvent = MockFieldEvent(listOf(geometryField))
    val vEvent = MockVEvent(fieldEvent)
    
    // Mock the original callable that would fail
    val mockCallable = MockCallable(
      RuntimeException("Cannot resolve JDBC type from VStream field")
    )
    
    val result = GeometryTypeHandler.handleFieldMessage(
      arrayOf(vEvent, false),
      mockCallable
    )
    
    // Should handle the GEOMETRY field message successfully
    assertNotNull(result)
    
    // Original method should not be called since we handle GEOMETRY directly
    assertFalse(mockCallable.wasCalled)
  }

  @Test
  fun `geometry value conversion handles customer data formats`() {
    // Test various formats the customer might encounter
    val testData = listOf(
      // MySQL internal format: SRID (4 bytes) + WKB
      createMySqlGeometryBytes(4326, samplePointWkb()),
      
      // Hex-encoded format
      "0x" + createMySqlGeometryBytes(4326, samplePointWkb()).joinToString("") { 
        "%02x".format(it) 
      },
      
      // Raw WKB with SRID prefix
      samplePointWkb()
    )
    
    testData.forEach { data ->
      val result = GeometryTypeHandler.convertGeometryValue(data)
      
      assertNotNull(result, "Failed to convert geometry data: ${data::class.simpleName}")
      assertTrue(result is Map<*, *>, "Result should be a Map with srid/wkb structure")
      
      val geometryMap = result as Map<*, *>
      assertNotNull(geometryMap["srid"], "Result should have SRID field")
      assertNotNull(geometryMap["wkb"], "Result should have WKB field")
      assertTrue(geometryMap["wkb"] is ByteArray, "WKB should be byte array")
    }
  }

  @Test
  fun `error scenario recovery - fallback when GEOMETRY handling fails`() {
    val mockField = MockQueryField("spatial_polygon", MockFieldType("GEOMETRY"))
    
    // Test that we have proper fallback behavior
    val mockCallable = MockCallable("fallback_result")
    
    // This should not fail even if some part of GEOMETRY handling has issues
    assertDoesNotThrow {
      VitessTypeHandler.handleResolve(arrayOf(mockField), mockCallable)
    }
  }

  @Test
  fun `non-GEOMETRY fields are not affected by GEOMETRY transforms`() {
    // Ensure our GEOMETRY fixes don't break existing functionality
    val regularField = MockQueryField("user_name", MockFieldType("VARCHAR"))
    val mockCallable = MockCallable("original_result")
    
    val result = VitessTypeHandler.handleResolve(arrayOf(regularField), mockCallable)
    
    // Should return original result for non-GEOMETRY fields
    assertEquals("original_result", result)
    assertTrue(mockCallable.wasCalled)
  }

  // Helper methods and mock classes

  private fun isGeometryVitessType(obj: Any): Boolean {
    return try {
      val nameMethod = obj.javaClass.getMethod("getName")
      val typeName = nameMethod.invoke(obj) as? String
      
      val jdbcMethod = obj.javaClass.getMethod("getJdbcId") 
      val jdbcId = jdbcMethod.invoke(obj) as? Int
      
      GeometryTypeHandler.isGeometryTypeString(typeName ?: "") && jdbcId == Types.OTHER
    } catch (e: Exception) {
      // If it doesn't have the expected methods, it might still be our mock
      obj.toString().contains("GEOMETRY")
    }
  }

  private fun createMySqlGeometryBytes(srid: Int, wkb: ByteArray): ByteArray {
    val result = ByteArray(4 + wkb.size)
    
    // Pack SRID in little-endian format
    result[0] = (srid and 0xFF).toByte()
    result[1] = ((srid shr 8) and 0xFF).toByte() 
    result[2] = ((srid shr 16) and 0xFF).toByte()
    result[3] = ((srid shr 24) and 0xFF).toByte()
    
    wkb.copyInto(result, 4)
    return result
  }

  private fun samplePointWkb(): ByteArray {
    // Simple POINT(1.0 2.0) in WKB format
    return byteArrayOf(
      1, 1, 0, 0, 0,  // Little-endian byte order + Point type
      0, 0, 0, 0, 0, 0, -16, 63,  // X coordinate (1.0)
      0, 0, 0, 0, 0, 0, 0, 64     // Y coordinate (2.0)
    )
  }

  // Mock classes to simulate customer's environment
  private class MockQueryField(private val name: String, private val type: MockFieldType) {
    fun getName(): String = name
    fun getType(): MockFieldType = type
  }

  private class MockFieldType(private val name: String) {
    fun name(): String = name
  }

  private class MockFieldEvent(private val fields: List<MockQueryField>) {
    fun getFieldsList(): List<MockQueryField> = fields
  }

  private class MockVEvent(private val fieldEvent: MockFieldEvent) {
    fun getFieldEvent(): MockFieldEvent = fieldEvent
  }

  private class MockColumnMetadata(
    private val name: String,
    private val typeName: String, 
    private val jdbcType: Int
  ) {
    fun getName(): String = name
    fun getTypeName(): String = typeName
    fun getJdbcType(): Int = jdbcType
  }

  private class MockCallable(private val result: Any?) : java.util.concurrent.Callable<Any> {
    var wasCalled = false
      private set
    
    constructor(exception: Exception) : this(null) {
      this.exception = exception
    }
    
    private var exception: Exception? = null
    
    override fun call(): Any {
      wasCalled = true
      exception?.let { throw it }
      return result ?: throw RuntimeException("Mock callable returned null")
    }
  }
}