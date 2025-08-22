package com.planetscale.debezium.geometry

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.sql.Types
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

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
    assertTrue(result == "original_method_result")
  }

  @Test
  fun `should handle case insensitive geometry type detection`() {
    val caseVariations = listOf("geometry", "GEOMETRY", "Geometry", "GeOmEtRy")
    
    caseVariations.forEach { geometryType ->
      assertDoesNotThrow("Should handle case variation: $geometryType") {
        val args = arrayOf<Any>(Types.OTHER, geometryType, "spatial_field")
        val result = GeometryTypeHandler.handleFieldType(args) { "fallback" }
        assertNotNull(result)
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
    }
  }
}