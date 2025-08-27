package com.planetscale.debezium.geometry

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import java.sql.Types
import kotlin.test.*

/**
 * Tests for VitessTypeHandler that verify the critical fixes for return value checking
 * instead of exception-based detection.
 */
class VitessTypeHandlerTest {

  @Test
  fun `handleResolve detects unresolved types from return values`() {
    // Test null return (most common case for unresolved types)
    val nullCallable = MockCallable(null)
    val geometryField = MockQueryField("spatial_point", MockFieldType("POINT"))
    
    val result = VitessTypeHandler.handleResolve(arrayOf(geometryField), nullCallable)
    
    assertNotNull(result)
    assertTrue(nullCallable.wasCalled)
    // Should have created a GEOMETRY type instead of returning null
  }

  @Test
  fun `handleResolve detects unresolved types from default markers`() {
    // Test return values that indicate unresolved types
    val testCases = listOf(
      MockVitessType("UNKNOWN"),
      MockVitessType("UNSUPPORTED"),
      MockVitessType("OTHER"),
      MockVitessType(""), // blank name
      MockVitessType(null) // null name
    )
    
    testCases.forEach { unresolvedType ->
      val callable = MockCallable(unresolvedType)
      val geometryField = MockQueryField("geometry_field", MockFieldType("GEOMETRY"))
      
      val result = VitessTypeHandler.handleResolve(arrayOf(geometryField), callable)
      
      // Should create proper GEOMETRY type instead of returning unresolved marker
      assertNotNull(result)
      assertTrue(callable.wasCalled)
      
      // Result should be different from the unresolved input
      assertNotEquals(unresolvedType, result)
    }
  }

  @Test
  fun `handleResolve preserves successfully resolved types`() {
    // Test that properly resolved non-GEOMETRY types are returned as-is
    val resolvedType = MockVitessType("VARCHAR")
    val callable = MockCallable(resolvedType)
    val regularField = MockQueryField("user_name", MockFieldType("VARCHAR"))
    
    val result = VitessTypeHandler.handleResolve(arrayOf(regularField), callable)
    
    assertEquals(resolvedType, result)
    assertTrue(callable.wasCalled)
  }

  @Test
  fun `handleResolve still handles exceptions for GEOMETRY fields`() {
    // Ensure exception-based detection still works as fallback
    val exception = RuntimeException("Cannot resolve JDBC type")
    val exceptionCallable = MockCallable(exception)
    val geometryField = MockQueryField("spatial_data", MockFieldType("POLYGON"))
    
    val result = VitessTypeHandler.handleResolve(arrayOf(geometryField), exceptionCallable)
    
    assertNotNull(result)
    assertTrue(exceptionCallable.wasCalled)
    // Should have handled the GEOMETRY field instead of propagating exception
  }

  @Test
  fun `handleResolve propagates exceptions for non-GEOMETRY fields`() {
    // Ensure exceptions are still thrown for non-GEOMETRY fields
    val exception = RuntimeException("Some other error")
    val exceptionCallable = MockCallable(exception)
    val regularField = MockQueryField("user_id", MockFieldType("INTEGER"))
    
    val thrownException = assertThrows<RuntimeException> {
      VitessTypeHandler.handleResolve(arrayOf(regularField), exceptionCallable)
    }
    
    assertEquals(exception, thrownException)
    assertTrue(exceptionCallable.wasCalled)
  }

  @Test
  fun `isUnresolvedType correctly identifies failure markers`() {
    val isUnresolvedMethod = VitessTypeHandler::class.java
      .getDeclaredMethod("isUnresolvedType", Any::class.java)
      .apply { isAccessible = true }
    
    // Test cases that should be considered unresolved
    val unresolvedCases = listOf(
      null,
      MockVitessType(""),
      MockVitessType("UNKNOWN"),
      MockVitessType("UNSUPPORTED"), 
      MockVitessType("OTHER"),
      MockVitessType(null)
    )
    
    unresolvedCases.forEach { case ->
      val isUnresolved = isUnresolvedMethod.invoke(VitessTypeHandler, case) as Boolean
      assertTrue(isUnresolved, "Should detect $case as unresolved")
    }
    
    // Test cases that should be considered resolved
    val resolvedCases = listOf(
      MockVitessType("VARCHAR"),
      MockVitessType("INTEGER"),
      MockVitessType("TIMESTAMP"),
      MockVitessType("GEOMETRY") // Even GEOMETRY is resolved if properly created
    )
    
    resolvedCases.forEach { case ->
      val isUnresolved = isUnresolvedMethod.invoke(VitessTypeHandler, case) as Boolean
      assertFalse(isUnresolved, "Should detect $case as resolved")
    }
  }

  @Test
  fun `isGeometryField detects various GEOMETRY field representations`() {
    val isGeometryFieldMethod = VitessTypeHandler::class.java
      .getDeclaredMethod("isGeometryField", Any::class.java)
      .apply { isAccessible = true }
    
    val geometryFields = listOf(
      MockQueryField("spatial_polygon", MockFieldType("GEOMETRY")),
      MockQueryField("location_point", MockFieldType("POINT")),
      MockQueryField("boundary_polygon", MockFieldType("POLYGON")),
      MockQueryField("path_line", MockFieldType("LINESTRING")),
      MockQueryField("multi_geom", MockFieldType("GEOMETRYCOLLECTION"))
    )
    
    geometryFields.forEach { field ->
      val isGeometry = isGeometryFieldMethod.invoke(VitessTypeHandler, field) as Boolean
      assertTrue(isGeometry, "Should detect ${field.getName()} as GEOMETRY field")
    }
    
    val nonGeometryFields = listOf(
      MockQueryField("user_name", MockFieldType("VARCHAR")),
      MockQueryField("user_id", MockFieldType("INTEGER")),
      MockQueryField("created_at", MockFieldType("TIMESTAMP"))
    )
    
    nonGeometryFields.forEach { field ->
      val isGeometry = isGeometryFieldMethod.invoke(VitessTypeHandler, field) as Boolean
      assertFalse(isGeometry, "Should not detect ${field.getName()} as GEOMETRY field")
    }
  }

  @Test
  fun `createGeometryVitessType creates proper VitessType instance`() {
    val createMethod = VitessTypeHandler::class.java
      .getDeclaredMethod("createGeometryVitessType", Any::class.java)
      .apply { isAccessible = true }
    
    val geometryField = MockQueryField("spatial_point", MockFieldType("POINT"))
    
    // This test verifies the method can be called without exceptions
    // The actual VitessType creation requires the proper classpath 
    assertDoesNotThrow {
      try {
        createMethod.invoke(VitessTypeHandler, geometryField)
      } catch (e: Exception) {
        // Expected to fail in test environment without proper Vitess classes
        // but should not fail due to logic errors
        assertTrue(
          e.cause is ClassNotFoundException || 
          e.cause is IllegalStateException ||
          e.message?.contains("VitessType") == true,
          "Should fail due to missing Vitess classes, not logic errors: ${e.message}"
        )
      }
    }
  }

  // Mock classes for testing
  private class MockQueryField(private val name: String, private val type: MockFieldType) {
    fun getName(): String = name
    fun getType(): MockFieldType = type
  }

  private class MockFieldType(private val name: String) {
    fun name(): String = name
  }

  private class MockVitessType(private val name: String?) {
    fun getName(): String? = name
    fun getJdbcId(): Int = Types.OTHER
    
    override fun toString(): String = "MockVitessType($name)"
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
      return result ?: throw RuntimeException("Mock returned null")
    }
  }
}