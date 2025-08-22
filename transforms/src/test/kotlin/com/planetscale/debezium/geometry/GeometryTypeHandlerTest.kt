package com.planetscale.debezium.geometry

import org.apache.kafka.connect.data.Schema
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.sql.Types
import kotlin.test.*

class GeometryTypeHandlerTest {

  @Test
  fun `should handle GEOMETRY type without throwing exception`() {
    // Test that the handler can be invoked without errors
    assertDoesNotThrow {
      val args = arrayOf<Any>(Types.OTHER, "GEOMETRY", "spatial_polygon")
      
      // This would normally be called by ByteBuddy, but we can test the detection logic
      val result = GeometryTypeHandler.handleFieldType(args) { "original_method_result" }
      assertNotNull(result)
    }
  }

  @Test
  fun `should create proper GEOMETRY schema with SRID and WKB fields`() {
    val args = arrayOf<Any>(Types.OTHER, "GEOMETRY", "spatial_polygon")
    
    val result = GeometryTypeHandler.handleFieldType(args) { "fallback" }
    
    // Verify we got a Kafka Connect Schema
    assertTrue(result is Schema, "Result should be a Kafka Connect Schema")
    val schema = result as Schema
    
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
      val args = arrayOf<Any>(Types.OTHER, geometryType, "test_field")
      
      assertDoesNotThrow("Should handle $geometryType type") {
        val result = GeometryTypeHandler.handleFieldType(args) { "fallback" }
        assertNotNull(result)
        assertTrue(result is Schema, "Should return Schema for $geometryType")
      }
    }
  }

  @Test
  fun `should fallback to original method for non-geometry types`() {
    val args = arrayOf<Any>(Types.INTEGER, "INT", "regular_field")
    var originalMethodCalled = false

    val result = GeometryTypeHandler.handleFieldType(args) {
      originalMethodCalled = true
      "original_method_result"
    }

    assertTrue(originalMethodCalled, "Original method should be called for non-geometry types")
    assertEquals("original_method_result", result)
  }

  @Test
  fun `should handle case insensitive geometry type detection`() {
    val caseVariations = listOf("geometry", "GEOMETRY", "Geometry", "GeOmEtRy")
    
    caseVariations.forEach { geometryType ->
      assertDoesNotThrow("Should handle case variation: $geometryType") {
        val args = arrayOf<Any>(Types.OTHER, geometryType, "spatial_field")
        val result = GeometryTypeHandler.handleFieldType(args) { "fallback" }
        assertNotNull(result)
        assertTrue(result is Schema, "Should return Schema for case variation: $geometryType")
      }
    }
  }

  @Test
  fun `should handle polygon type specifically`() {
    // Test the specific case from the customer error
    val args = arrayOf<Any>(Types.OTHER, "POLYGON", "spatial_polygon")
    
    assertDoesNotThrow {
      val result = GeometryTypeHandler.handleFieldType(args) { "fallback" }
      assertNotNull(result)
      assertTrue(result is Schema, "Should return Schema for POLYGON")
      
      val schema = result as Schema
      assertEquals("io.debezium.data.geometry.Geometry", schema.name(), "POLYGON should use Geometry schema")
    }
  }

  @Test
  fun `should convert geometry values to proper structure`() {
    // Test the value conversion functionality
    val testCases = mapOf(
      "POINT(1 2)" to true,
      byteArrayOf(0x01, 0x01) to true,
      null to false
    )
    
    testCases.forEach { (input, shouldSucceed) ->
      if (shouldSucceed) {
        val result = GeometryTypeHandler.convertGeometryValue(input)
        assertNotNull(result, "Conversion should succeed for valid input")
        
        @Suppress("UNCHECKED_CAST")
        val structValue = result as Map<String, Any>
        assertTrue(structValue.containsKey("srid"), "Result should contain SRID")
        assertTrue(structValue.containsKey("wkb"), "Result should contain WKB")
      } else {
        val result = GeometryTypeHandler.convertGeometryValue(input)
        assertNull(result, "Conversion should return null for null input")
      }
    }
  }
}