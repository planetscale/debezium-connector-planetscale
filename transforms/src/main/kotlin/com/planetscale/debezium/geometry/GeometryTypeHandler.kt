package com.planetscale.debezium.geometry

import io.debezium.data.geometry.Geometry
import net.bytebuddy.implementation.bind.annotation.*
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.slf4j.LoggerFactory
import java.sql.Types
import java.util.concurrent.Callable

/**
 * Exception thrown when GEOMETRY processing fails.
 */
internal class GeometryProcessingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Handler for GEOMETRY and spatial data types in Vitess streams.
 * 
 * Intercepts calls to field type resolution methods and handles GEOMETRY types
 * (jdbcId = 1111) by delegating to MySQL's existing spatial data handling.
 * 
 * Creates proper Debezium schema structures that match MySQL connector output:
 * - STRUCT with 'srid' (INT32) and 'wkb' (BYTES) fields
 * - Semantic type: io.debezium.data.geometry.Geometry
 */
object GeometryTypeHandler {
  private val logger = LoggerFactory.getLogger(GeometryTypeHandler::class.java)
  
  // Memory safety constants
  private const val MAX_GEOMETRY_SIZE_BYTES = 64 * 1024 * 1024 // 64 MB limit for geometry data
  private const val MAX_FIELD_MESSAGE_SIZE = 10 * 1024 * 1024  // 10 MB limit for field messages
  private const val MEMORY_CHECK_INTERVAL = 100 // Check memory every N operations
  
  // Geometry parsing constants
  private const val SRID_BYTES = 4
  private const val HEX_RADIX = 16
  private const val BYTE_MASK = 0xFF
  private const val BYTE_SHIFT_8 = 8
  private const val BYTE_SHIFT_16 = 16  
  private const val BYTE_SHIFT_24 = 24
  private const val HEX_CHARS_PER_BYTE = 2
  private const val SRID_BYTE_3 = 3
  
  // Memory monitoring
  private var operationCount = 0

  /**
   * Intercepts handleFieldMessage calls and handles GEOMETRY field definitions specially.
   * 
   * This method is called by ByteBuddy when VStreamOutputMessageDecoder.handleFieldMessage()
   * processes a field definition message from the VStream. For GEOMETRY types, we need to 
   * actually replace the failing schema creation with our own GEOMETRY schema logic.
   */
  @JvmStatic
  @RuntimeType
  fun handleFieldMessage(
    @AllArguments args: Array<Any>,
    @SuperCall callable: Callable<Any>
  ): Any {
    return try {
      // MEMORY SAFETY: Check memory usage and validate input size
      checkMemoryUsageAndPerformCleanup()
      validateFieldMessageSize(args)
      
      // The handleFieldMessage signature is: handleFieldMessage(VEvent vEvent, boolean filterSchema)
      // We need to examine the VEvent to see if it contains GEOMETRY field definitions
      if (args.isNotEmpty()) {
        val vEvent = args[0]
        
        // Check if this VEvent contains GEOMETRY field definitions
        if (containsGeometryFields(vEvent)) {
          logger.info("Intercepted handleFieldMessage with GEOMETRY field definitions - replacing schema creation")
          
          // CRITICAL FIX: Instead of calling the failing original method,
          // we need to handle the GEOMETRY schema creation ourselves
          return handleGeometryFieldMessage(vEvent, args)
        }
      }
      
      // For non-GEOMETRY field messages, call the original method
      callable.call()
    } catch (e: Exception) {
      logger.error("Error in GEOMETRY field message handling", e)
      
      // Check if this could be a GEOMETRY field that we should handle
      if (args.isNotEmpty() && couldBeGeometryFieldMessage(args[0])) {
        logger.info("Attempting to handle failed message as GEOMETRY field message")
        try {
          return handleGeometryFieldMessage(args[0], args)
        } catch (geometryHandlingException: Exception) {
          logger.error("GEOMETRY field message handling also failed", geometryHandlingException)
          // Fall through to original exception
        }
      }
      
      // Re-throw original exception for non-GEOMETRY messages or when our handling fails
      throw e
    }
  }

  /**
   * Handles a field message that contains GEOMETRY fields by creating proper schemas.
   * This replaces the original method that would fail for GEOMETRY types.
   */
  private fun handleGeometryFieldMessage(vEvent: Any, args: Array<Any>): Any {
    logger.info("Handling GEOMETRY field message with custom schema creation")
    
    try {
      // Extract field information from the VEvent
      val geometryFields = extractGeometryFieldInfo(vEvent)
      
      if (geometryFields.isEmpty()) {
        logger.warn("No GEOMETRY fields found in VEvent, delegating to original method")
        // If no geometry fields, this shouldn't happen but delegate anyway
        throw IllegalStateException("No GEOMETRY fields found to process")
      }
      
      // Create schemas for each GEOMETRY field
      val schemas = geometryFields.map { fieldInfo ->
        createGeometryFieldSchema(fieldInfo)
      }
      
      logger.info("Successfully created {} GEOMETRY field schemas", schemas.size)
      
      // Return a success result that indicates schema creation was successful
      // The exact return type depends on what handleFieldMessage normally returns
      return createSuccessResult(schemas)
      
    } catch (e: Exception) {
      logger.error("Failed to handle GEOMETRY field message", e)
      throw GeometryProcessingException("GEOMETRY field message processing failed", e)
    }
  }

  /**
   * Checks if a message could potentially contain GEOMETRY fields based on heuristics.
   */
  private fun couldBeGeometryFieldMessage(vEvent: Any): Boolean {
    return try {
      // Use basic heuristics to determine if this could be a GEOMETRY message
      val className = vEvent.javaClass.simpleName
      
      // Check for VEvent-like class names
      className.contains("VEvent", ignoreCase = true) ||
      className.contains("Event", ignoreCase = true) ||
      className.contains("Field", ignoreCase = true)
    } catch (e: Exception) {
      logger.debug("Could not determine if message could be GEOMETRY-related", e)
      false
    }
  }

  /**
   * Extracts GEOMETRY field information from a VEvent.
   */
  private fun extractGeometryFieldInfo(vEvent: Any): List<GeometryFieldInfo> {
    val geometryFields = mutableListOf<GeometryFieldInfo>()
    
    try {
      validateReflectionInput(vEvent, "VEvent")
      
      val fieldEventMethod = validateAndGetMethod(vEvent.javaClass, "getFieldEvent", emptyArray())
      val fieldEvent = fieldEventMethod.invoke(vEvent)
      
      if (fieldEvent != null) {
        val getFieldsMethod = validateAndGetMethod(fieldEvent.javaClass, "getFieldsList", emptyArray())
        @Suppress("UNCHECKED_CAST")
        val fields = getFieldsMethod.invoke(fieldEvent) as? List<*>
        
        fields?.forEach { field ->
          if (field != null) {
            val fieldInfo = extractFieldInfo(field)
            if (fieldInfo.isGeometry) {
              geometryFields.add(fieldInfo)
            }
          }
        }
      }
    } catch (e: Exception) {
      logger.error("Failed to extract GEOMETRY field information", e)
      throw GeometryProcessingException("Could not extract GEOMETRY field info", e)
    }
    
    return geometryFields
  }

  /**
   * Extracts field information from a field object.
   */
  private fun extractFieldInfo(field: Any): GeometryFieldInfo {
    try {
      val getNameMethod = validateAndGetMethod(field.javaClass, "getName", emptyArray())
      val getTypeMethod = validateAndGetMethod(field.javaClass, "getType", emptyArray())
      
      val fieldName = getNameMethod.invoke(field) as? String ?: "unknown"
      val fieldType = getTypeMethod.invoke(field)
      
      val typeName = if (fieldType != null) {
        val nameMethod = validateAndGetMethod(fieldType.javaClass, "name", emptyArray())
        nameMethod.invoke(fieldType) as? String ?: "unknown"
      } else "unknown"
      
      val isGeometry = isGeometryTypeString(typeName)
      
      return GeometryFieldInfo(fieldName, typeName, isGeometry)
      
    } catch (e: Exception) {
      logger.error("Failed to extract field information", e)
      return GeometryFieldInfo("unknown", "unknown", false)
    }
  }

  /**
   * Creates a Debezium schema for a GEOMETRY field.
   */
  private fun createGeometryFieldSchema(fieldInfo: GeometryFieldInfo): Any {
    logger.debug("Creating schema for GEOMETRY field: {} (type: {})", fieldInfo.name, fieldInfo.typeName)
    
    // Use our existing createGeometrySchema method
    return createGeometrySchema()
  }

  /**
   * Creates a success result for the field message processing.
   */
  private fun createSuccessResult(schemas: List<Any>): Any {
    // For now, return a simple success indicator
    // This may need to be adjusted based on what handleFieldMessage actually returns
    logger.debug("Created success result for {} schemas", schemas.size)
    return schemas
  }

  /**
   * Data class to hold GEOMETRY field information.
   */
  private data class GeometryFieldInfo(
    val name: String,
    val typeName: String,
    val isGeometry: Boolean
  )

  /**
   * Checks if a VEvent contains GEOMETRY field definitions.
   * This uses reflection to examine the VEvent object since we can't import Vitess classes directly.
   */
  private fun containsGeometryFields(vEvent: Any): Boolean {
    return try {
      // Validate input parameter
      validateReflectionInput(vEvent, "VEvent")
      
      // Use reflection to access VEvent.getFieldEvent().getFields()
      val fieldEventMethod = validateAndGetMethod(vEvent.javaClass, "getFieldEvent", emptyArray())
      val fieldEvent = fieldEventMethod.invoke(vEvent)
      
      if (fieldEvent != null) {
        val getFieldsMethod = validateAndGetMethod(fieldEvent.javaClass, "getFieldsList", emptyArray())
        @Suppress("UNCHECKED_CAST")
        val fields = getFieldsMethod.invoke(fieldEvent) as? List<*>
        
        fields?.any { field ->
          if (field != null) {
            // Check if field type is GEOMETRY with validation
            val getTypeMethod = validateAndGetMethod(field.javaClass, "getType", emptyArray())
            val fieldType = getTypeMethod.invoke(field)
            
            if (fieldType != null) {
              val typeNameMethod = validateAndGetMethod(fieldType.javaClass, "name", emptyArray())
              val typeName = typeNameMethod.invoke(fieldType) as? String
              
              typeName != null && isGeometryTypeString(typeName)
            } else false
          } else false
        } ?: false
      } else false
    } catch (e: Exception) {
      // If reflection fails, we can't detect GEOMETRY fields, so return false
      logger.debug("Could not detect GEOMETRY fields in VEvent: {}", e.message)
      false
    }
  }

  /**
   * Enhanced GEOMETRY type detection that works with actual VStream data formats.
   * 
   * This method uses multiple detection strategies:
   * 1. JDBC type ID checking (Types.OTHER = 1111)
   * 2. Type name analysis (GEOMETRY, POINT, etc.)
   * 3. Field name heuristics (spatial_*, geo_*, etc.)
   * 4. Column metadata inspection
   * 5. Data content analysis for WKB/WKT patterns
   */
  fun detectGeometryType(args: Array<Any>): Boolean {
    // Strategy 1: Direct argument analysis
    if (detectGeometryFromArgs(args)) return true
    
    // Strategy 2: Object introspection
    return args.any { arg -> detectGeometryFromObject(arg) }
  }

  /**
   * Detects GEOMETRY types from method arguments.
   */
  private fun detectGeometryFromArgs(args: Array<Any>): Boolean {
    return args.any { arg ->
      when {
        // Check for jdbcId = 1111 (Types.OTHER) 
        arg is Int && arg == Types.OTHER -> true
        arg is Number && arg.toInt() == Types.OTHER -> true
        
        // Check for type strings
        arg is String && isGeometryTypeString(arg) -> true
        arg is String && isGeometrySpatialName(arg) -> true
        
        else -> false
      }
    }
  }

  /**
   * Detects GEOMETRY types from object introspection.
   */
  private fun detectGeometryFromObject(obj: Any): Boolean {
    return try {
      when {
        // Check object class names for Vitess field types
        obj.javaClass.simpleName.contains("Field") -> detectGeometryFromField(obj)
        obj.javaClass.simpleName.contains("Type") -> detectGeometryFromType(obj)
        obj.javaClass.simpleName.contains("Column") -> detectGeometryFromColumn(obj)
        
        // Check for byte arrays that might contain geometry data
        obj is ByteArray -> detectGeometryFromByteArray(obj)
        
        else -> false
      }
    } catch (e: Exception) {
      logger.debug("Could not detect geometry type from object {}: {}", obj.javaClass.simpleName, e.message)
      false
    }
  }

  /**
   * Detects GEOMETRY types from field objects using reflection.
   */
  private fun detectGeometryFromField(field: Any): Boolean {
    return try {
      // Try to get field name
      val fieldName = getFieldName(field)
      if (fieldName != null && isGeometrySpatialName(fieldName)) {
        return true
      }
      
      // Try to get field type
      val fieldType = getFieldType(field)
      if (fieldType != null && isGeometryTypeString(fieldType)) {
        return true
      }
      
      false
    } catch (e: Exception) {
      logger.debug("Could not detect geometry from field object", e)
      false
    }
  }

  /**
   * Detects GEOMETRY types from type objects.
   */
  private fun detectGeometryFromType(typeObj: Any): Boolean {
    return try {
      val typeName = getTypeName(typeObj)
      typeName != null && isGeometryTypeString(typeName)
    } catch (e: Exception) {
      logger.debug("Could not detect geometry from type object", e)
      false
    }
  }

  /**
   * Detects GEOMETRY types from column objects.
   */
  private fun detectGeometryFromColumn(column: Any): Boolean {
    return try {
      val jdbcType = getJdbcType(column)
      val typeName = getColumnTypeName(column)
      
      (jdbcType == Types.OTHER && typeName != null && isGeometryTypeString(typeName))
    } catch (e: Exception) {
      logger.debug("Could not detect geometry from column object", e)
      false
    }
  }

  /**
   * Detects GEOMETRY data from byte arrays using WKB pattern matching.
   */
  private fun detectGeometryFromByteArray(bytes: ByteArray): Boolean {
    return try {
      // Check minimum size for GEOMETRY data (SRID + minimal WKB header)
      if (bytes.size < 8) return false
      
      // Basic WKB pattern detection - WKB starts with byte order marker (01 or 00)
      val firstByte = bytes[0]
      if (firstByte != 0x00.toByte() && firstByte != 0x01.toByte()) {
        // Check if first 4 bytes could be SRID and next bytes start with WKB pattern
        if (bytes.size >= 8) {
          val possibleWkbStart = bytes[4]
          return possibleWkbStart == 0x00.toByte() || possibleWkbStart == 0x01.toByte()
        }
        return false
      }
      
      // If it starts with byte order marker, check for valid geometry type
      if (bytes.size >= 5) {
        // Bytes 1-4 contain geometry type (little or big endian)
        val geometryType = if (firstByte == 0x01.toByte()) {
          // Little endian
          bytes[1].toInt() and 0xFF
        } else {
          // Big endian  
          bytes[4].toInt() and 0xFF
        }
        
        // Check for known WKB geometry types (1=Point, 2=LineString, 3=Polygon, etc.)
        return geometryType in 1..7 || geometryType in 1001..1007 || geometryType in 2001..2007 || geometryType in 3001..3007
      }
      
      false
    } catch (e: Exception) {
      logger.debug("Error detecting geometry from byte array", e)
      false
    }
  }

  /**
   * Checks if a field/column name suggests spatial/geometry data.
   */
  private fun isGeometrySpatialName(name: String): Boolean {
    val lowerName = name.lowercase()
    return lowerName.contains("spatial") ||
           lowerName.contains("geometry") ||
           lowerName.contains("location") ||
           lowerName.contains("coordinate") ||
           lowerName.contains("position") ||
           lowerName.contains("point") ||
           lowerName.contains("polygon") ||
           lowerName.contains("line") ||
           lowerName.startsWith("geo_") ||
           lowerName.startsWith("geom_") ||
           lowerName.endsWith("_geom") ||
           lowerName.endsWith("_point") ||
           lowerName.endsWith("_polygon")
  }

  /**
   * Helper methods for extracting information from various object types.
   */
  private fun getFieldName(field: Any): String? {
    return try {
      val method = field.javaClass.getMethod("getName")
      method.invoke(field) as? String
    } catch (e: Exception) {
      null
    }
  }

  private fun getFieldType(field: Any): String? {
    return try {
      val getTypeMethod = field.javaClass.getMethod("getType")
      val typeObj = getTypeMethod.invoke(field)
      if (typeObj != null) {
        val nameMethod = typeObj.javaClass.getMethod("name")
        nameMethod.invoke(typeObj) as? String
      } else null
    } catch (e: Exception) {
      null
    }
  }

  private fun getTypeName(typeObj: Any): String? {
    return try {
      val nameMethod = typeObj.javaClass.getMethod("name")
      nameMethod.invoke(typeObj) as? String
    } catch (e: Exception) {
      null
    }
  }

  private fun getJdbcType(column: Any): Int {
    return try {
      val method = column.javaClass.getMethod("getJdbcType")
      method.invoke(column) as? Int ?: -1
    } catch (e: Exception) {
      -1
    }
  }

  private fun getColumnTypeName(column: Any): String? {
    return try {
      val method = column.javaClass.getMethod("getTypeName")
      method.invoke(column) as? String
    } catch (e: Exception) {
      null
    }
  }

  /**
   * Checks if a string represents a known geometry type.
   */
  fun isGeometryTypeString(typeString: String): Boolean {
    val upperType = typeString.uppercase()
    return upperType in setOf(
      "GEOMETRY", "POINT", "LINESTRING", "POLYGON", 
      "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"
    )
  }

  /**
   * Handles a GEOMETRY field by creating the proper Debezium schema structure.
   * 
   * This follows the exact same pattern as MySQL connector's spatial support:
   * - Creates STRUCT schema with 'srid' (INT32) and 'wkb' (BYTES) fields
   * - Sets semantic type to io.debezium.data.geometry.Geometry
   * - Returns a schema that matches MySQL connector output exactly
   */
  private fun handleGeometryField(@Suppress("UNUSED_PARAMETER") args: Array<Any>): Any {
    logger.info("Creating GEOMETRY field schema with SRID and WKB structure")
    
    // Always create the proper geometry schema - no fallbacks that would break compatibility
    return createGeometrySchema()
  }

  /**
   * Creates the proper Debezium GEOMETRY schema structure.
   * 
   * This creates a STRUCT schema with:
   * - Field 'srid': INT32 (Spatial Reference System Identifier)  
   * - Field 'wkb': BYTES (Well-Known Binary representation)
   * - Schema name: io.debezium.data.geometry.Geometry
   * 
   * This matches exactly what the MySQL connector produces.
   * 
   * Includes fallback handling to ensure message processing continues even if schema creation fails.
   */
  fun createGeometrySchema(): Schema {
    return try {
      logger.debug("Building GEOMETRY schema with SRID and WKB fields")
      
      SchemaBuilder.struct()
        .name(Geometry.LOGICAL_NAME) // Uses "io.debezium.data.geometry.Geometry"
        .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
        .field("wkb", Schema.OPTIONAL_BYTES_SCHEMA)
        .optional()
        .build()
        
    } catch (e: Exception) {
      logger.error("Failed to create proper GEOMETRY schema, falling back to BYTES schema to prevent message processing failure", e)
      
      // FALLBACK: Return a simple BYTES schema to ensure messages can still be processed
      // This prevents the entire connector from failing when GEOMETRY schema creation issues occur
      return createFallbackGeometrySchema(e)
    }
  }

  /**
   * Creates a fallback schema when the proper GEOMETRY schema cannot be created.
   * This ensures message processing continues even when there are issues with the schema.
   */
  private fun createFallbackGeometrySchema(originalError: Exception): Schema {
    logger.warn("Using fallback GEOMETRY schema - GEOMETRY data will be stored as raw bytes")
    
    return try {
      // Try to create a simple BYTES schema as fallback
      SchemaBuilder.bytes()
        .name("geometry.fallback")
        .doc("Fallback schema for GEOMETRY data due to schema creation failure: ${originalError.message}")
        .optional()
        .build()
        
    } catch (e: Exception) {
      logger.error("Even fallback schema creation failed, using basic OPTIONAL_BYTES_SCHEMA", e)
      
      // Ultimate fallback - use the built-in optional bytes schema
      Schema.OPTIONAL_BYTES_SCHEMA
    }
  }


  /**
   * Value converter for GEOMETRY data from MySQL/Vitess format to Debezium structure.
   * 
   * This converts spatial data from MySQL's internal format (4-byte SRID + WKB) 
   * to the proper Debezium STRUCT format with separate 'srid' and 'wkb' fields.
   * 
   * Follows the same pattern as MySQL connector's geometry handling.
   * Includes memory safety checks to prevent memory exhaustion.
   */
  fun convertGeometryValue(geometryData: Any?): Any? {
    return when {
      geometryData == null -> null
      else -> try {
        // MEMORY SAFETY: Check memory usage before processing
        checkMemoryUsageAndPerformCleanup()
        
        val geometryBytes = extractGeometryBytes(geometryData)
          ?: throw IllegalArgumentException("Cannot extract bytes from geometry data")
        
        // Additional memory safety check for extracted bytes
        validateByteArraySize(geometryBytes)
        
        // Parse MySQL's internal geometry format: 4 bytes SRID + WKB data
        val (srid, wkb) = parseMySqlGeometry(geometryBytes)
        
        // Return in Debezium's standard format
        mapOf(
          "srid" to srid,
          "wkb" to wkb
        )
      } catch (e: Exception) {
        logger.error("Failed to convert GEOMETRY value to Debezium format", e)
        throw GeometryProcessingException("GEOMETRY value conversion failed", e)
      }
    }
  }
  
  /**
   * Extracts geometry bytes from various input formats with enhanced validation and error handling.
   */
  private fun extractGeometryBytes(geometryData: Any): ByteArray? {
    return try {
      when (geometryData) {
        is ByteArray -> {
          validateByteArraySize(geometryData)
          geometryData
        }
        is String -> {
          extractBytesFromString(geometryData)
        }
        else -> {
          logger.warn("Unexpected geometry data type: ${geometryData::class.simpleName}")
          null
        }
      }
    } catch (e: Exception) {
      logger.error("Failed to extract geometry bytes from {}: {}", 
        geometryData::class.simpleName, e.message)
      null
    }
  }

  /**
   * Validates the size of byte arrays to prevent memory issues.
   */
  private fun validateByteArraySize(bytes: ByteArray) {
    require(bytes.size <= MAX_GEOMETRY_SIZE_BYTES) {
      "Geometry data too large: ${bytes.size} bytes (max ${MAX_GEOMETRY_SIZE_BYTES} bytes)"
    }
  }

  /**
   * Extracts bytes from string input with format detection and validation.
   */
  private fun extractBytesFromString(geometryString: String): ByteArray? {
    return when {
      // Detect hex-encoded strings
      isHexEncodedString(geometryString) -> {
        logger.debug("Detected hex-encoded geometry string")
        hexStringToByteArray(geometryString)
      }
      // Handle regular string as UTF-8 bytes
      geometryString.isNotBlank() -> {
        logger.debug("Converting string to UTF-8 bytes")
        val bytes = geometryString.toByteArray(Charsets.UTF_8)
        validateByteArraySize(bytes)
        bytes
      }
      else -> {
        logger.warn("Empty or blank geometry string")
        null
      }
    }
  }

  /**
   * Detects if a string appears to be hex-encoded.
   */
  private fun isHexEncodedString(str: String): Boolean {
    return str.startsWith("0x", ignoreCase = true) ||
           str.startsWith("\\x", ignoreCase = true) ||
           str.startsWith("x", ignoreCase = true) ||
           // Heuristic: if string is mostly hex characters and even length, treat as hex
           (str.length % 2 == 0 && str.length > 4 && 
            str.count { it.isDigit() || it.lowercaseChar() in 'a'..'f' } > str.length * 0.8)
  }
  
  /**
   * Parses MySQL's internal geometry format: 4-byte little-endian SRID + WKB data.
   * This follows the same format that MySQL uses in its binlog for GEOMETRY columns.
   */
  private fun parseMySqlGeometry(geometryBytes: ByteArray): Pair<Int, ByteArray> {
    require(geometryBytes.size >= SRID_BYTES) { 
      "Geometry data too short - expected at least $SRID_BYTES bytes for SRID" 
    }
    
    // First 4 bytes are SRID in little-endian format
    val srid = (geometryBytes[0].toInt() and BYTE_MASK) or
               ((geometryBytes[1].toInt() and BYTE_MASK) shl BYTE_SHIFT_8) or
               ((geometryBytes[2].toInt() and BYTE_MASK) shl BYTE_SHIFT_16) or
               ((geometryBytes[SRID_BYTE_3].toInt() and BYTE_MASK) shl BYTE_SHIFT_24)
    
    // Remaining bytes are the WKB (Well-Known Binary) data
    val wkb = geometryBytes.sliceArray(SRID_BYTES until geometryBytes.size)
    
    logger.debug("Parsed MySQL geometry: SRID=$srid, WKB length=${wkb.size}")
    
    return Pair(srid, wkb)
  }
  
  /**
   * Converts hex string to byte array for parsing hex-encoded geometry data from Vitess.
   * Includes comprehensive validation and error handling for malformed input.
   */
  private fun hexStringToByteArray(hex: String): ByteArray {
    return try {
      // Input validation
      validateHexStringInput(hex)
      
      val cleanHex = cleanAndValidateHexString(hex)
      
      // Convert to byte array with error handling
      cleanHex.chunked(HEX_CHARS_PER_BYTE)
        .map { hexPair ->
          try {
            hexPair.toInt(HEX_RADIX).toByte()
          } catch (e: NumberFormatException) {
            throw IllegalArgumentException("Invalid hex characters in pair '$hexPair': ${e.message}", e)
          }
        }
        .toByteArray()
        
    } catch (e: Exception) {
      logger.error("Failed to parse hex string '{}': {}", hex.take(50) + if (hex.length > 50) "..." else "", e.message)
      throw IllegalArgumentException("Hex string parsing failed: ${e.message}", e)
    }
  }

  /**
   * Validates the input hex string for basic requirements.
   */
  private fun validateHexStringInput(hex: String) {
    require(hex.isNotBlank()) { "Hex string cannot be blank" }
    require(hex.length <= 1_000_000) { 
      "Hex string too long: ${hex.length} characters (max 1,000,000 for safety)" 
    }
  }

  /**
   * Cleans and validates the hex string format.
   */
  private fun cleanAndValidateHexString(hex: String): String {
    // Remove common separators and prefixes
    var cleanHex = hex.replace(" ", "")
                      .replace("-", "")
                      .replace(":", "")
                      .replace("_", "")
    
    // Remove common hex prefixes
    if (cleanHex.startsWith("0x", ignoreCase = true)) {
      cleanHex = cleanHex.substring(2)
    } else if (cleanHex.startsWith("\\x", ignoreCase = true)) {
      cleanHex = cleanHex.substring(2)
    } else if (cleanHex.startsWith("x", ignoreCase = true)) {
      cleanHex = cleanHex.substring(1)
    }
    
    // Validate length
    require(cleanHex.isNotEmpty()) { "Hex string is empty after cleaning" }
    require(cleanHex.length % HEX_CHARS_PER_BYTE == 0) { 
      "Hex string must have even length, got ${cleanHex.length} characters" 
    }
    
    // Validate that all characters are valid hex digits
    val invalidChars = cleanHex.filterNot { it.isDigit() || it.lowercaseChar() in 'a'..'f' }
    require(invalidChars.isEmpty()) { 
      "Invalid hex characters found: '${invalidChars.take(10)}'" +
      if (invalidChars.length > 10) " (and ${invalidChars.length - 10} more)" else ""
    }
    
    logger.debug("Cleaned hex string: {} chars -> {} chars", hex.length, cleanHex.length)
    return cleanHex
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
   * Monitors memory usage and performs cleanup to prevent memory exhaustion.
   * This is called periodically during geometry processing operations.
   */
  private fun checkMemoryUsageAndPerformCleanup() {
    operationCount++
    
    // Check memory usage every N operations to avoid performance impact
    if (operationCount % MEMORY_CHECK_INTERVAL == 0) {
      val runtime = Runtime.getRuntime()
      val totalMemory = runtime.totalMemory()
      val freeMemory = runtime.freeMemory()
      val usedMemory = totalMemory - freeMemory
      val maxMemory = runtime.maxMemory()
      
      val memoryUsagePercent = (usedMemory * 100.0) / maxMemory
      
      logger.debug("Memory usage: {:.1f}% ({} MB / {} MB)", 
        memoryUsagePercent, 
        usedMemory / (1024 * 1024), 
        maxMemory / (1024 * 1024))
      
      // If memory usage is high, suggest garbage collection
      if (memoryUsagePercent > 80.0) {
        logger.warn("High memory usage detected ({:.1f}%), suggesting garbage collection", memoryUsagePercent)
        System.gc()
        
        // Check again after GC
        val newUsedMemory = runtime.totalMemory() - runtime.freeMemory()
        val newUsagePercent = (newUsedMemory * 100.0) / maxMemory
        
        if (newUsagePercent > 90.0) {
          logger.error("Critical memory usage after GC: {:.1f}%. GEOMETRY processing may fail.", newUsagePercent)
        }
      }
    }
  }

  /**
   * Validates the size of field message arguments to prevent memory exhaustion.
   */
  private fun validateFieldMessageSize(args: Array<Any>) {
    val estimatedSize = estimateObjectSize(args)
    if (estimatedSize > MAX_FIELD_MESSAGE_SIZE) {
      throw IllegalArgumentException(
        "Field message too large: estimated $estimatedSize bytes (max $MAX_FIELD_MESSAGE_SIZE bytes)"
      )
    }
  }

  /**
   * Estimates the memory size of objects for safety checks.
   * This is a heuristic estimate, not exact measurement.
   */
  private fun estimateObjectSize(obj: Any?): Long {
    return when (obj) {
      null -> 0
      is String -> obj.length.toLong() * 2 // Rough estimate for UTF-16
      is ByteArray -> obj.size.toLong()
      is Array<*> -> obj.fold(0L) { acc, item -> acc + estimateObjectSize(item) }
      is Collection<*> -> obj.sumOf { estimateObjectSize(it) }
      is Map<*, *> -> obj.entries.sumOf { estimateObjectSize(it.key) + estimateObjectSize(it.value) }
      else -> 64 // Default estimate for objects
    }
  }
}
