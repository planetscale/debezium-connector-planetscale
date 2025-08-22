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
  
  private const val SRID_BYTES = 4
  private const val HEX_RADIX = 16
  private const val BYTE_MASK = 0xFF
  private const val BYTE_SHIFT_8 = 8
  private const val BYTE_SHIFT_16 = 16  
  private const val BYTE_SHIFT_24 = 24
  private const val HEX_CHARS_PER_BYTE = 2
  private const val SRID_BYTE_3 = 3

  /**
   * Intercepts field type resolution and handles GEOMETRY types specially.
   * 
   * This method is called by ByteBuddy when the original Debezium Vitess connector
   * encounters a field type that it needs to resolve. For GEOMETRY types 
   * (jdbcId = Types.OTHER = 1111), we handle them using MySQL's spatial support.
   * For all other types, we delegate to the original method.
   */
  @JvmStatic
  @RuntimeType
  fun handleFieldType(
    @AllArguments args: Array<Any>,
    @SuperCall callable: Callable<Any>
  ): Any {
    return try {
      // Check if this is a GEOMETRY type by examining the arguments
      val isGeometryType = detectGeometryType(args)
      
      if (isGeometryType) {
        logger.info("Intercepted GEOMETRY field type resolution - creating proper Debezium schema")
        handleGeometryField(args)
      } else {
        // For non-GEOMETRY types, call the original method
        callable.call()
      }
    } catch (e: Exception) {
      logger.error("Error in GEOMETRY type handling - field resolution will fail", e)
      throw GeometryProcessingException("Failed to handle GEOMETRY field type resolution", e)
    }
  }

  /**
   * Detects if the current field being processed is a GEOMETRY type.
   * 
   * The error message shows:
   * "Cannot resolve JDBC type from VStream field name: "spatial_polygon"
   * type: GEOMETRY
   * jdbcId: 1111"
   * 
   * So we look for jdbcId == 1111 (Types.OTHER) and type == "GEOMETRY"
   */
  private fun detectGeometryType(args: Array<Any>): Boolean {
    return args.any { arg ->
      when {
        // Check for jdbcId = 1111 (Types.OTHER)
        arg is Int && arg == Types.OTHER -> true
        arg is Number && arg.toInt() == Types.OTHER -> true
        
        // Check for type string containing "GEOMETRY"
        arg is String && arg.uppercase().contains("GEOMETRY") -> true
        
        // Check for type string containing spatial geometry types
        arg is String && isGeometryTypeString(arg) -> true
        
        else -> false
      }
    }
  }

  /**
   * Checks if a string represents a known geometry type.
   */
  private fun isGeometryTypeString(typeString: String): Boolean {
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
   */
  private fun createGeometrySchema(): Schema {
    logger.debug("Building GEOMETRY schema with SRID and WKB fields")
    
    return SchemaBuilder.struct()
      .name(Geometry.LOGICAL_NAME) // Uses "io.debezium.data.geometry.Geometry"
      .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
      .field("wkb", Schema.OPTIONAL_BYTES_SCHEMA)
      .optional()
      .build()
  }


  /**
   * Value converter for GEOMETRY data from MySQL/Vitess format to Debezium structure.
   * 
   * This converts spatial data from MySQL's internal format (4-byte SRID + WKB) 
   * to the proper Debezium STRUCT format with separate 'srid' and 'wkb' fields.
   * 
   * Follows the same pattern as MySQL connector's geometry handling.
   */
  fun convertGeometryValue(geometryData: Any?): Any? {
    return when {
      geometryData == null -> null
      else -> try {
        val geometryBytes = extractGeometryBytes(geometryData)
          ?: throw IllegalArgumentException("Cannot extract bytes from geometry data")
        
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
   * Extracts geometry bytes from various input formats.
   */
  private fun extractGeometryBytes(geometryData: Any): ByteArray? {
    return when (geometryData) {
      is ByteArray -> geometryData
      // Handle hex-encoded geometry strings from Vitess
      is String -> {
        if (geometryData.startsWith("0x") || geometryData.startsWith("\\x")) {
          hexStringToByteArray(geometryData.removePrefix("0x").removePrefix("\\x"))
        } else {
          geometryData.toByteArray()
        }
      }
      else -> {
        logger.warn("Unexpected geometry data type: ${geometryData::class.simpleName}")
        null
      }
    }
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
   */
  private fun hexStringToByteArray(hex: String): ByteArray {
    val cleanHex = hex.replace(" ", "").replace("-", "")
    require(cleanHex.length % HEX_CHARS_PER_BYTE == 0) { "Hex string must have even length" }
    
    return cleanHex.chunked(HEX_CHARS_PER_BYTE)
      .map { it.toInt(HEX_RADIX).toByte() }
      .toByteArray()
  }
}
