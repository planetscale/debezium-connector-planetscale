package com.planetscale.debezium.geometry

import net.bytebuddy.implementation.bind.annotation.*
import org.slf4j.LoggerFactory
import java.sql.Types
import java.util.concurrent.Callable

/**
 * ByteBuddy handler for intercepting value resolution in the Vitess connector
 * to properly convert GEOMETRY field values from raw VStream bytes to Debezium's
 * standard geometry structure.
 * 
 * This handler works in conjunction with GeometryTypeHandler to provide complete
 * GEOMETRY support - GeometryTypeHandler handles schema resolution, while this
 * handler processes the actual field values during message processing.
 */
object GeometryValueHandler {
  private val logger = LoggerFactory.getLogger(GeometryValueHandler::class.java)
  
  // Memory safety constants  
  private const val MAX_VALUE_SIZE_BYTES = 64 * 1024 * 1024 // 64 MB limit for individual values

  /**
   * Intercepts calls to ReplicationMessageColumnValueResolver.resolveValue()
   * and handles GEOMETRY types specially by converting raw bytes to proper
   * Debezium geometry structures.
   * 
   * Method signature: 
   * static Object resolveValue(VitessType, ReplicationMessage$ColumnValue<byte[]>, boolean, TemporalPrecisionMode)
   */
  @JvmStatic
  @RuntimeType
  fun handleResolveValue(
    @Argument(0) vitessType: Any,           // VitessType instance
    @Argument(1) columnValue: Any,          // ReplicationMessage$ColumnValue<byte[]> instance  
    @Argument(2) includeUnknownDatatypes: Boolean,
    @Argument(3) temporalPrecisionMode: Any, // TemporalPrecisionMode instance
    @SuperCall callable: Callable<Any>     // Original resolveValue method
  ): Any {
    return try {
      // Extract type information using reflection
      val typeName = getTypeName(vitessType)
      val jdbcId = getJdbcId(vitessType)
      
      // Check if this is a GEOMETRY type
      if (isGeometryType(typeName, jdbcId)) {
        logger.debug("Intercepting GEOMETRY value conversion for type: {}", typeName)
        
        // Extract raw bytes from VitessColumnValue
        val rawBytes = getRawValue(columnValue)
        
        // MEMORY SAFETY: Validate raw bytes size before processing
        if (rawBytes != null) {
          validateValueSize(rawBytes)
        }
        
        // Use existing GeometryTypeHandler conversion logic with fallback
        val convertedValue = try {
          GeometryTypeHandler.convertGeometryValue(rawBytes)
        } catch (conversionError: Exception) {
          logger.warn("GEOMETRY value conversion failed, falling back to raw bytes", conversionError)
          
          // FALLBACK: Return raw bytes if conversion fails
          // This ensures the message can still be processed even if GEOMETRY conversion fails
          rawBytes
        }
        
        logger.debug("Successfully converted GEOMETRY value: srid={}, wkb_length={}", 
          (convertedValue as? Map<*, *>)?.get("srid"),
          ((convertedValue as? Map<*, *>)?.get("wkb") as? ByteArray)?.size ?: 
           (convertedValue as? ByteArray)?.size)
        
        return convertedValue ?: callable.call()
      }
      
      // For non-GEOMETRY types, call original method
      callable.call()
      
    } catch (e: Exception) {
      logger.error("Error in GEOMETRY value conversion, falling back to default resolution", e)
      // Fallback to original method to prevent breaking other data types
      callable.call()
    }
  }

  /**
   * Determines if this VitessType represents a GEOMETRY field.
   */
  private fun isGeometryType(typeName: String?, jdbcId: Int): Boolean {
    return (jdbcId == Types.OTHER && 
           typeName?.let { GeometryTypeHandler.isGeometryTypeString(it.uppercase()) } == true)
  }

  /**
   * Extracts the type name from a VitessType instance using reflection.
   */
  private fun getTypeName(vitessType: Any): String? {
    return try {
      validateReflectionInput(vitessType, "VitessType")
      val method = validateAndGetMethod(vitessType::class.java, "getName", emptyArray())
      method.invoke(vitessType) as? String
    } catch (e: Exception) {
      logger.debug("Could not extract type name from VitessType", e)
      null
    }
  }

  /**
   * Extracts the JDBC type ID from a VitessType instance using reflection.
   */
  private fun getJdbcId(vitessType: Any): Int {
    return try {
      validateReflectionInput(vitessType, "VitessType")
      val method = validateAndGetMethod(vitessType::class.java, "getJdbcId", emptyArray())
      method.invoke(vitessType) as? Int ?: -1
    } catch (e: Exception) {
      logger.debug("Could not extract JDBC ID from VitessType", e)
      -1
    }
  }

  /**
   * Extracts raw bytes from a VitessColumnValue instance using reflection.
   */
  private fun getRawValue(columnValue: Any): ByteArray? {
    return try {
      validateReflectionInput(columnValue, "VitessColumnValue")
      val method = validateAndGetMethod(columnValue::class.java, "getRawValue", emptyArray())
      method.invoke(columnValue) as? ByteArray
    } catch (e: Exception) {
      logger.debug("Could not extract raw value from VitessColumnValue", e)
      null
    }
  }

  /**
   * Makes the isGeometryTypeString method from GeometryTypeHandler accessible
   * for use in this value handler.
   */
  private fun isGeometryTypeString(typeString: String): Boolean {
    return typeString in setOf(
      "GEOMETRY", "POINT", "LINESTRING", "POLYGON", 
      "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"
    )
  }

  /**
   * Validates input for reflection operations to prevent security issues and null pointer exceptions.
   */
  private fun validateReflectionInput(obj: Any, expectedType: String) {
    require(obj::class.java.name.isNotBlank()) { "Object class name cannot be blank" }
    logger.debug("Validating reflection input for expected type: {} (actual: {})", expectedType, obj::class.java.simpleName)
  }

  /**
   * Safely retrieves a method with parameter validation.
   */
  private fun validateAndGetMethod(clazz: Class<*>, methodName: String, parameterTypes: Array<Class<*>>): java.lang.reflect.Method {
    require(methodName.isNotBlank()) { "Method name cannot be blank" }
    require(clazz.name.isNotBlank()) { "Class name cannot be blank" }
    
    return try {
      val method = clazz.getMethod(methodName, *parameterTypes)
      logger.debug("Successfully validated method: {}.{}", clazz.simpleName, methodName)
      method
    } catch (e: NoSuchMethodException) {
      val availableMethods = clazz.methods.filter { it.name == methodName }.map { method ->
        val params = method.parameterTypes.joinToString(", ") { it.simpleName }
        "$methodName($params)"
      }
      val error = "Method validation failed: Method '$methodName' not found on class '${clazz.name}'. Available methods with same name: ${availableMethods.joinToString(", ")}"
      logger.error(error)
      throw NoSuchMethodException(error)
    } catch (e: SecurityException) {
      val error = "Security validation failed: Access denied to method '$methodName' on class '${clazz.name}'"
      logger.error(error)
      throw SecurityException(error)
    }
  }

  /**
   * Validates that value size is within acceptable limits to prevent memory exhaustion.
   */
  private fun validateValueSize(bytes: ByteArray) {
    if (bytes.size > MAX_VALUE_SIZE_BYTES) {
      val error = "GEOMETRY value too large: ${bytes.size} bytes (max $MAX_VALUE_SIZE_BYTES bytes)"
      logger.error(error)
      throw IllegalArgumentException(error)
    }
    
    if (bytes.size > 10 * 1024 * 1024) { // Warn for values > 10MB
      logger.warn("Large GEOMETRY value detected: {} MB", bytes.size / (1024 * 1024))
    }
  }
}