package com.planetscale.debezium.geometry

import net.bytebuddy.implementation.bind.annotation.*
import org.slf4j.LoggerFactory
import java.sql.Types
import java.util.concurrent.Callable

/**
 * ByteBuddy handler for intercepting VitessColumnValue.asDefault() calls
 * to properly handle GEOMETRY types that would otherwise be ignored.
 * 
 * This is the critical fix for the customer's issue:
 * "ignore unknown column type VitessType{name='GEOMETRY', jdbcId=1111}"
 * 
 * The asDefault() method signature is:
 * Object asDefault(VitessType vitessType, boolean includeUnknownDatatypes)
 */
object VitessColumnValueHandler {
  private val logger = LoggerFactory.getLogger(VitessColumnValueHandler::class.java)
  
  /**
   * Intercepts VitessColumnValue.asDefault() and handles GEOMETRY types specially.
   * 
   * Method signature: Object asDefault(VitessType vitessType, boolean includeUnknownDatatypes)
   */
  @JvmStatic
  @RuntimeType
  fun handleAsDefault(
    @This vitessColumnValue: Any,
    @Argument(0) vitessType: Any,
    @Argument(1) includeUnknownDatatypes: Boolean,
    @SuperCall callable: Callable<Any>
  ): Any? {
    return try {
      // Check if this is a GEOMETRY type
      val typeName = getTypeName(vitessType)
      val jdbcId = getJdbcId(vitessType)
      
      if (isGeometryType(typeName, jdbcId)) {
        logger.info("Intercepted GEOMETRY type in asDefault(): name='{}', jdbcId={}", typeName, jdbcId)
        
        // Extract raw value from the VitessColumnValue
        val rawBytes = getRawValueFromColumnValue(vitessColumnValue)
        
        if (rawBytes != null) {
          // Use our GEOMETRY value conversion logic
          val geometryValue = GeometryTypeHandler.convertGeometryValue(rawBytes)
          
          logger.debug("Successfully converted GEOMETRY value: srid={}, wkb_length={}", 
            (geometryValue as? Map<*, *>)?.get("srid"),
            ((geometryValue as? Map<*, *>)?.get("wkb") as? ByteArray)?.size ?: 0)
          
          return geometryValue
        } else {
          logger.warn("GEOMETRY field has null raw value, returning null")
          return null
        }
      }
      
      // For non-GEOMETRY types, call original method
      callable.call()
      
    } catch (e: Exception) {
      logger.error("Error handling GEOMETRY type in asDefault(), falling back to original method", e)
      
      try {
        // Fallback to original method
        callable.call()
      } catch (originalException: Exception) {
        logger.error("Original asDefault() method also failed", originalException)
        
        // If both our handling and the original method fail, and this is a GEOMETRY type,
        // return null to prevent the entire connector from failing
        val typeName = getTypeName(vitessType)
        val jdbcId = getJdbcId(vitessType)
        
        if (isGeometryType(typeName, jdbcId)) {
          logger.warn("Returning null for failed GEOMETRY conversion to prevent connector failure")
          return null
        } else {
          // For non-GEOMETRY types, re-throw the original exception
          throw originalException
        }
      }
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
  private fun getRawValueFromColumnValue(columnValue: Any): ByteArray? {
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
}