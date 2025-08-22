package com.planetscale.debezium.geometry

import io.debezium.data.geometry.Geometry
import net.bytebuddy.implementation.bind.annotation.*
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.slf4j.LoggerFactory
import java.sql.Types
import java.util.concurrent.Callable

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
        logger.debug("Handling GEOMETRY type field - creating proper schema")
        handleGeometryField(args)
      } else {
        // For non-GEOMETRY types, call the original method
        callable.call()
      }
    } catch (e: Exception) {
      logger.warn("Error handling field type, delegating to original method", e)
      callable.call()
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
  private fun handleGeometryField(args: Array<Any>): Any {
    logger.info("Creating GEOMETRY field schema with SRID and WKB structure")
    
    return try {
      createGeometrySchema()
    } catch (e: Exception) {
      logger.error("Failed to create GEOMETRY schema", e)
      // Return a basic schema as fallback
      createFallbackSchema()
    }
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
   * Creates a fallback schema if the main geometry schema creation fails.
   */
  private fun createFallbackSchema(): Schema {
    logger.warn("Using fallback BYTES schema for GEOMETRY field")
    return Schema.OPTIONAL_BYTES_SCHEMA
  }

  /**
   * Value converter for GEOMETRY data.
   * 
   * This would convert spatial data from MySQL format to the Debezium structure.
   * For now, this is a placeholder that would need to integrate with
   * MySQL's existing BinlogGeometry class.
   */
  fun convertGeometryValue(geometryData: Any?): Any? {
    if (geometryData == null) return null
    
    return try {
      // TODO: Integrate with io.debezium.connector.binlog.BinlogGeometry
      // to properly parse and convert spatial data to SRID + WKB structure
      
      // Placeholder structure matching MySQL connector output
      mapOf(
        "srid" to 0, // Default SRID
        "wkb" to when (geometryData) {
          is ByteArray -> geometryData
          is String -> geometryData.toByteArray()
          else -> geometryData.toString().toByteArray()
        }
      )
    } catch (e: Exception) {
      logger.warn("Failed to convert GEOMETRY value", e)
      null
    }
  }
}