package com.planetscale.debezium.geometry

import org.apache.kafka.connect.data.Schema
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.sql.Types
import kotlin.test.*

internal class GeometryTypeHandlerTest {

  @Test
  fun `should handle field message call without throwing exception`() {
    // Test that the handler can be invoked without errors
    assertDoesNotThrow {
      val args = arrayOf<Any>("mock_event", true)
      
      // This would normally be called by ByteBuddy, but we test it gracefully handles the call
      val result = GeometryTypeHandler.handleFieldMessage(args) { "original_method_result" }
      assertEquals("original_method_result", result)
    }
  }

  @Test
  fun `should create proper GEOMETRY schema with SRID and WKB fields`() {
    val schema = GeometryTypeHandler.createGeometrySchema()
    
    // Verify schema structure
    assertEquals(Schema.Type.STRUCT, schema.type(), "Schema should be STRUCT type")
    assertEquals("io.debezium.data.geometry.Geometry", schema.name(), "Should use Debezium Geometry schema name")
    assertTrue(schema.isOptional, "GEOMETRY schema should be optional")
    
    // Verify fields
    val fields = schema.fields()
    assertEquals(2, fields.size, "Should have exactly 2 fields: srid and wkb")
    
    val sridField = fields.find { it.name() == "srid" }
    assertNotNull(sridField, "Should have 'srid' field")
    assertEquals(Schema.Type.INT32, sridField.schema().type(), "SRID should be INT32")
    assertTrue(sridField.schema().isOptional, "SRID field should be optional")
    
    val wkbField = fields.find { it.name() == "wkb" }
    assertNotNull(wkbField, "Should have 'wkb' field")
    assertEquals(Schema.Type.BYTES, wkbField.schema().type(), "WKB should be BYTES")
    assertTrue(wkbField.schema().isOptional, "WKB field should be optional")
  }

  @Test
  fun `should detect various geometry type strings`() {
    val geometryTypes = listOf(
      "GEOMETRY", "POINT", "LINESTRING", "POLYGON",
      "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"
    )

    geometryTypes.forEach { geometryType ->
      assertTrue(GeometryTypeHandler.isGeometryTypeString(geometryType), "Should detect $geometryType as geometry type")
    }
  }

  @Test
  fun `should not detect non-geometry types`() {
    val nonGeometryTypes = listOf("VARCHAR", "INTEGER", "DECIMAL", "TIMESTAMP", "BLOB")
    
    nonGeometryTypes.forEach { type ->
      assertFalse(GeometryTypeHandler.isGeometryTypeString(type), "Should not detect $type as geometry type")
    }
  }

  @Test
  fun `should handle case insensitive geometry type detection`() {
    val caseVariations = listOf("geometry", "GEOMETRY", "Geometry", "GeOmEtRy", "point", "POINT", "polygon")
    
    caseVariations.forEach { geometryType ->
      assertTrue(GeometryTypeHandler.isGeometryTypeString(geometryType), "Should detect case variation: $geometryType")
    }
  }

  @Test
  fun `should convert geometry values to proper structure`() {
    // Test with valid MySQL geometry format: SRID (4 bytes little-endian) + WKB data
    val srid = 4326  // WGS84
    val wkbData = byteArrayOf(0x01, 0x01, 0x00, 0x00, 0x00) // Simple POINT WKB
    
    // Create MySQL format: SRID (little-endian) + WKB
    val mysqlGeometry = byteArrayOf(
      (srid and 0xFF).toByte(),
      ((srid shr 8) and 0xFF).toByte(), 
      ((srid shr 16) and 0xFF).toByte(),
      ((srid shr 24) and 0xFF).toByte()
    ) + wkbData
    
    val result = GeometryTypeHandler.convertGeometryValue(mysqlGeometry)
    assertNotNull(result, "Conversion should succeed for valid MySQL geometry")
    
    @Suppress("UNCHECKED_CAST")
    val structValue = result as Map<String, Any>
    
    assertTrue(structValue.containsKey("srid"), "Result should contain SRID")
    assertTrue(structValue.containsKey("wkb"), "Result should contain WKB")
    
    assertEquals(srid, structValue["srid"], "SRID should be correctly parsed")
    assertContentEquals(wkbData, structValue["wkb"] as ByteArray, "WKB should be correctly extracted")
  }
  
  @Test
  fun `should handle hex-encoded geometry strings`() {
    // Test hex string conversion from Vitess
    val hexGeometry = "0xE6100000010100000000000000000000000000000000000000"
    
    val result = GeometryTypeHandler.convertGeometryValue(hexGeometry)
    assertNotNull(result, "Should handle hex-encoded geometry")
    
    @Suppress("UNCHECKED_CAST") 
    val structValue = result as Map<String, Any>
    assertTrue(structValue.containsKey("srid"), "Should extract SRID from hex")
    assertTrue(structValue.containsKey("wkb"), "Should extract WKB from hex")
  }
  
  @Test
  fun `should handle null geometry values`() {
    val result = GeometryTypeHandler.convertGeometryValue(null)
    assertNull(result, "Should return null for null input")
  }
  
  @Test
  fun `should fail fast on invalid geometry data`() {
    // Test with too-short byte array (less than 4 bytes for SRID)
    val invalidGeometry = byteArrayOf(0x01, 0x02)
    
    assertFailsWith<GeometryProcessingException>("Should fail fast on invalid geometry data") {
      GeometryTypeHandler.convertGeometryValue(invalidGeometry)
    }
  }
  
  @Test
  fun `should handle various hex string formats`() {
    val testHexFormats = listOf(
      "0x12345678ABCDEF",  // 0x prefix
      "\\x12345678ABCDEF", // \x prefix  
      "12345678ABCDEF"     // No prefix (fallback to raw bytes)
    )
    
    testHexFormats.forEach { hexString ->
      assertDoesNotThrow("Should handle hex format: $hexString") {
        // This will fail due to invalid geometry structure, but hex parsing should work
        try {
          GeometryTypeHandler.convertGeometryValue(hexString)
        } catch (e: GeometryProcessingException) {
          // Expected - the hex data isn't valid geometry, but hex parsing should work
          assertTrue(e.message?.contains("Geometry data too short") == true)
        }
      }
    }
  }
}
