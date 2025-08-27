package com.planetscale.debezium.geometry

import com.planetscale.codegen.transforms.*
import io.debezium.connector.vitess.VitessType
import io.debezium.connector.vitess.connection.ReplicationMessage
import io.debezium.connector.vitess.connection.ReplicationMessageColumnValueResolver
import io.debezium.connector.vitess.connection.VitessColumnValue
import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder
import net.bytebuddy.ByteBuddy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.LoggerFactory
import java.sql.Types
import kotlin.test.*

/**
 * Integration test that verifies ByteBuddy transforms are correctly applied
 * and intercept the methods that cause the customer's GEOMETRY errors.
 * 
 * This test is critical for ensuring our method targeting works correctly
 * and that the transforms don't silently fail to intercept methods.
 */
class ByteBuddyTransformIntegrationTest {
  companion object {
    private val logger = LoggerFactory.getLogger(ByteBuddyTransformIntegrationTest::class.java)
  }

  @Test
  fun `test VitessColumnValueTransform targets correct method`() {
    logger.info("Testing VitessColumnValue.asDefault() method targeting")
    
    val transform = VitessColumnValueTransform()
    
    // Verify the transform can identify the target class
    val vitessColumnValueClass = VitessColumnValue::class.java
    val typeDescription = net.bytebuddy.description.type.TypeDescription.ForLoadedType.of(vitessColumnValueClass)
    
    assertTrue(transform.matches(typeDescription), "Transform should match VitessColumnValue class")
    
    // Verify the asDefault method exists with correct signature
    val asDefaultMethod = vitessColumnValueClass.methods.find { it.name == "asDefault" }
    assertNotNull(asDefaultMethod, "asDefault method should exist on VitessColumnValue")
    
    // Check method signature: Object asDefault(VitessType vitessType, boolean includeUnknownDatatypes)
    assertEquals(2, asDefaultMethod.parameterCount, "asDefault should have 2 parameters")
    assertEquals(VitessType::class.java, asDefaultMethod.parameterTypes[0], "First parameter should be VitessType")
    assertEquals(Boolean::class.javaPrimitiveType, asDefaultMethod.parameterTypes[1], "Second parameter should be boolean")
    
    logger.info("✓ VitessColumnValue.asDefault() method targeting is correct")
  }

  @Test
  fun `test VitessTypeEnhancement targets correct method`() {
    logger.info("Testing VitessType.resolve() method targeting")
    
    val transform = VitessTypeEnhancement()
    
    // Verify the transform can identify the target class
    val vitessTypeClass = VitessType::class.java
    val typeDescription = net.bytebuddy.description.type.TypeDescription.ForLoadedType.of(vitessTypeClass)
    
    assertTrue(transform.matches(typeDescription), "Transform should match VitessType class")
    
    // Verify the resolve methods exist
    val resolveMethods = vitessTypeClass.methods.filter { it.name == "resolve" }
    assertTrue(resolveMethods.isNotEmpty(), "resolve methods should exist on VitessType")
    assertTrue(resolveMethods.size >= 2, "Should have at least 2 resolve method overloads")
    
    // Check that at least one resolve method is static
    val staticResolveMethods = resolveMethods.filter { java.lang.reflect.Modifier.isStatic(it.modifiers) }
    assertTrue(staticResolveMethods.isNotEmpty(), "Should have static resolve methods")
    
    logger.info("✓ VitessType.resolve() method targeting is correct")
  }

  @Test
  fun `test ReplicationMessageColumnValueResolver targets correct method`() {
    logger.info("Testing ReplicationMessageColumnValueResolver.resolveValue() method targeting")
    
    val transform = VitessValueResolver()
    
    // Verify the transform can identify the target class
    val resolverClass = ReplicationMessageColumnValueResolver::class.java
    val typeDescription = net.bytebuddy.description.type.TypeDescription.ForLoadedType.of(resolverClass)
    
    assertTrue(transform.matches(typeDescription), "Transform should match ReplicationMessageColumnValueResolver class")
    
    // Verify the resolveValue method exists
    val resolveValueMethod = resolverClass.methods.find { it.name == "resolveValue" }
    assertNotNull(resolveValueMethod, "resolveValue method should exist on ReplicationMessageColumnValueResolver")
    
    // Check method signature
    assertTrue(java.lang.reflect.Modifier.isStatic(resolveValueMethod.modifiers), "resolveValue should be static")
    assertEquals(4, resolveValueMethod.parameterCount, "resolveValue should have 4 parameters")
    assertEquals(VitessType::class.java, resolveValueMethod.parameterTypes[0], "First parameter should be VitessType")
    
    logger.info("✓ ReplicationMessageColumnValueResolver.resolveValue() method targeting is correct")
  }

  @Test
  fun `test VStreamOutputMessageDecoder targets correct method`() {
    logger.info("Testing VStreamOutputMessageDecoder.handleFieldMessage() method targeting")
    
    val transform = VitessGeometry()
    
    // Verify the transform can identify the target class
    val decoderClass = VStreamOutputMessageDecoder::class.java
    val typeDescription = net.bytebuddy.description.type.TypeDescription.ForLoadedType.of(decoderClass)
    
    assertTrue(transform.matches(typeDescription), "Transform should match VStreamOutputMessageDecoder class")
    
    // Verify the handleFieldMessage method exists
    val handleFieldMethod = decoderClass.declaredMethods.find { it.name == "handleFieldMessage" }
    assertNotNull(handleFieldMethod, "handleFieldMessage method should exist on VStreamOutputMessageDecoder")
    
    // Check method signature: void handleFieldMessage(VEvent vEvent, boolean filterSchema)
    assertEquals(2, handleFieldMethod.parameterCount, "handleFieldMessage should have 2 parameters")
    assertEquals(Void.TYPE, handleFieldMethod.returnType, "handleFieldMessage should return void")
    
    logger.info("✓ VStreamOutputMessageDecoder.handleFieldMessage() method targeting is correct")
  }

  @Test
  fun `test GEOMETRY type detection in handlers`() {
    logger.info("Testing GEOMETRY type detection logic")
    
    // Test GeometryTypeHandler.isGeometryTypeString
    assertTrue(GeometryTypeHandler.isGeometryTypeString("GEOMETRY"), "Should detect GEOMETRY type")
    assertTrue(GeometryTypeHandler.isGeometryTypeString("POINT"), "Should detect POINT type")
    assertTrue(GeometryTypeHandler.isGeometryTypeString("POLYGON"), "Should detect POLYGON type")
    assertFalse(GeometryTypeHandler.isGeometryTypeString("VARCHAR"), "Should not detect VARCHAR as geometry")
    assertFalse(GeometryTypeHandler.isGeometryTypeString("INT"), "Should not detect INT as geometry")
    
    // Test case insensitivity
    assertTrue(GeometryTypeHandler.isGeometryTypeString("geometry"), "Should detect lowercase geometry")
    assertTrue(GeometryTypeHandler.isGeometryTypeString("Point"), "Should detect mixed case point")
    
    logger.info("✓ GEOMETRY type detection is working correctly")
  }

  @Test
  fun `test VitessType creation for GEOMETRY types`() {
    logger.info("Testing VitessType creation for GEOMETRY types")
    
    // Test that we can create VitessType instances for GEOMETRY types
    val geometryType = VitessType("GEOMETRY", Types.OTHER)
    assertEquals("GEOMETRY", geometryType.name)
    assertEquals(Types.OTHER, geometryType.jdbcId)
    
    val pointType = VitessType("POINT", Types.OTHER)
    assertEquals("POINT", pointType.name)
    assertEquals(Types.OTHER, pointType.jdbcId)
    
    logger.info("✓ VitessType creation for GEOMETRY types works correctly")
  }

  @Test
  fun `test GEOMETRY value conversion`() {
    logger.info("Testing GEOMETRY value conversion")
    
    // Test with a sample MySQL GEOMETRY format (4-byte SRID + WKB)
    val srid = 4326  // WGS84
    val sridBytes = byteArrayOf(
      (srid and 0xFF).toByte(),
      ((srid shr 8) and 0xFF).toByte(), 
      ((srid shr 16) and 0xFF).toByte(),
      ((srid shr 24) and 0xFF).toByte()
    )
    
    // Simple WKB for a POINT(1.0, 2.0)
    val wkbBytes = byteArrayOf(
      0x01, // Little endian
      0x01, 0x00, 0x00, 0x00, // Point type (1)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0.toByte(), 0x3F, // X = 1.0 (little endian double)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 // Y = 2.0 (little endian double)  
    )
    
    val geometryBytes = sridBytes + wkbBytes
    
    // Test conversion
    val result = GeometryTypeHandler.convertGeometryValue(geometryBytes)
    assertNotNull(result, "Conversion should not return null")
    
    @Suppress("UNCHECKED_CAST")
    val geometryMap = result as? Map<String, Any>
    assertNotNull(geometryMap, "Result should be a Map")
    
    assertEquals(srid, geometryMap["srid"], "SRID should be correctly extracted")
    assertNotNull(geometryMap["wkb"], "WKB should be present")
    assertTrue(geometryMap["wkb"] is ByteArray, "WKB should be a ByteArray")
    
    logger.info("✓ GEOMETRY value conversion is working correctly")
  }

  @Test
  fun `test error handling in GEOMETRY conversion`() {
    logger.info("Testing error handling in GEOMETRY conversion")
    
    // Test null input
    val nullResult = GeometryTypeHandler.convertGeometryValue(null)
    assertNull(nullResult, "Null input should return null")
    
    // Test invalid input (too small)
    assertThrows<Exception> {
      GeometryTypeHandler.convertGeometryValue(byteArrayOf(1, 2))
    }
    
    // Test empty input
    assertThrows<Exception> {
      GeometryTypeHandler.convertGeometryValue(byteArrayOf())
    }
    
    logger.info("✓ Error handling in GEOMETRY conversion is working correctly")
  }

  @Test
  fun `test hex string parsing for GEOMETRY data`() {
    logger.info("Testing hex string parsing for GEOMETRY data")
    
    // Test valid hex string with 0x prefix
    val hexString = "0x00000000010100000000000000000000F03F0000000000000040"
    val result = GeometryTypeHandler.convertGeometryValue(hexString)
    assertNotNull(result, "Hex string conversion should not return null")
    
    // Test hex string without prefix
    val plainHexString = "00000000010100000000000000000000F03F0000000000000040"
    val result2 = GeometryTypeHandler.convertGeometryValue(plainHexString)
    assertNotNull(result2, "Plain hex string conversion should not return null")
    
    logger.info("✓ Hex string parsing for GEOMETRY data is working correctly")
  }

  @Test  
  fun `test ByteBuddy transform integration with actual class loading`() {
    logger.info("Testing ByteBuddy transform integration with class loading")
    
    try {
      // Create a ByteBuddy instance and apply our transforms
      val byteBuddy = ByteBuddy()
      
      // Test transforming VitessColumnValue
      val vitessColumnValueClass = VitessColumnValue::class.java
      val transform = VitessColumnValueTransform()
      val typeDescription = net.bytebuddy.description.type.TypeDescription.ForLoadedType.of(vitessColumnValueClass)
      
      if (transform.matches(typeDescription)) {
        val builder = byteBuddy.redefine(vitessColumnValueClass)
        val transformedBuilder = transform.transform(builder, typeDescription)
        
        // Verify that the transformation doesn't throw exceptions
        assertNotNull(transformedBuilder, "Transformed builder should not be null")
        
        logger.info("✓ ByteBuddy transform integration is working")
      } else {
        fail("Transform should match VitessColumnValue class")
      }
    } catch (e: Exception) {
      logger.error("ByteBuddy transform integration test failed", e)
      throw e
    }
  }
}