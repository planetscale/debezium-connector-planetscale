package com.planetscale.debezium.geometry

import net.bytebuddy.implementation.bind.annotation.*
import org.slf4j.LoggerFactory
import java.sql.Types
import java.util.concurrent.Callable

/**
 * Handler for enhanced VitessType.resolve() method that adds GEOMETRY support.
 * 
 * This intercepts calls to VitessType.resolve() and handles GEOMETRY types that are
 * not supported by the original Vitess connector. When a GEOMETRY field is encountered,
 * we create a proper VitessType with the correct JDBC mapping.
 * 
 * This resolves the "Cannot resolve JDBC type from VStream field" error for GEOMETRY types.
 */
object VitessTypeHandler {
  private val logger = LoggerFactory.getLogger(VitessTypeHandler::class.java)

  /**
   * Intercepts VitessType.resolve() calls and handles GEOMETRY types specially.
   * 
   * The resolve method signatures are:
   * - static VitessType resolve(Query.Field field)
   * - static VitessType resolve(Query.Field field, boolean includeEnumAndSetMeta)
   */
  @JvmStatic
  @RuntimeType
  fun handleResolve(
    @AllArguments args: Array<Any>,
    @SuperCall callable: Callable<Any>
  ): Any {
    return try {
      // Call the original resolve method first
      val result = callable.call()
      
      // Check if the original method failed to resolve (returned null or indicates failure)
      if (isUnresolvedType(result) && args.isNotEmpty()) {
        val field = args[0] // This should be a Query.Field object
        
        if (isGeometryField(field)) {
          logger.info("Original VitessType.resolve() failed for GEOMETRY field, creating custom VitessType")
          return createGeometryVitessType(field)
        }
      }
      
      // Return original result for successful resolution or non-GEOMETRY types
      result
    } catch (e: Exception) {
      // If the original method throws an exception, also check for GEOMETRY
      if (args.isNotEmpty()) {
        val field = args[0] // This should be a Query.Field object
        
        if (isGeometryField(field)) {
          logger.info("VitessType.resolve() threw exception for GEOMETRY field, creating custom VitessType")
          return createGeometryVitessType(field)
        }
      }
      
      // Re-throw the original exception for non-GEOMETRY types
      throw e
    }
  }

  /**
   * Checks if the result from VitessType.resolve() indicates an unresolved type.
   * This handles cases where the method returns null, default values, or special markers
   * indicating that the type could not be resolved.
   */
  private fun isUnresolvedType(result: Any?): Boolean {
    return when {
      // Null result indicates failure to resolve
      result == null -> true
      
      // If result has a getName() method, check if it indicates unknown/unresolved type
      else -> try {
        val nameMethod = result.javaClass.getMethod("getName")
        val typeName = nameMethod.invoke(result) as? String
        
        // Check for markers that indicate unresolved types
        typeName.isNullOrBlank() || 
        typeName.equals("UNKNOWN", ignoreCase = true) ||
        typeName.equals("UNSUPPORTED", ignoreCase = true) ||
        typeName.equals("OTHER", ignoreCase = true)
      } catch (e: Exception) {
        // If we can't determine the type name, assume it's resolved
        logger.debug("Could not determine if type is resolved: {}", e.message)
        false
      }
    }
  }

  /**
   * Checks if a Query.Field represents a GEOMETRY type using reflection.
   */
  private fun isGeometryField(field: Any): Boolean {
    return try {
      // Validate input and use reflection to access Query.Field methods
      validateReflectionInput(field, "Query.Field")
      
      val getTypeMethod = validateAndGetMethod(field.javaClass, "getType", emptyArray())
      val fieldType = getTypeMethod.invoke(field)
      
      if (fieldType != null) {
        val nameMethod = validateAndGetMethod(fieldType.javaClass, "name", emptyArray())
        val typeName = nameMethod.invoke(fieldType) as? String
        
        // Check if the type name indicates a geometry type
        typeName != null && GeometryTypeHandler.isGeometryTypeString(typeName)
      } else {
        logger.debug("Field type is null, not a GEOMETRY type")
        false
      }
    } catch (e: Exception) {
      logger.debug("Could not determine if field is GEOMETRY type: {}", e.message)
      false
    }
  }

  /**
   * Creates a VitessType for GEOMETRY fields using reflection.
   * 
   * This constructs a VitessType("GEOMETRY", Types.OTHER) which properly maps
   * GEOMETRY fields to JDBC Types.OTHER (1111).
   */
  private fun createGeometryVitessType(field: Any): Any {
    return try {
      // Validate input and get the actual type name from the field
      validateReflectionInput(field, "Query.Field")
      
      val getTypeMethod = validateAndGetMethod(field.javaClass, "getType", emptyArray()) 
      val fieldType = getTypeMethod.invoke(field) 
        ?: throw IllegalArgumentException("Field getType() returned null")
      
      val nameMethod = validateAndGetMethod(fieldType.javaClass, "name", emptyArray())
      val typeName = nameMethod.invoke(fieldType) as? String 
        ?: throw IllegalArgumentException("Field type name() returned null")
      
      logger.debug("Creating VitessType for GEOMETRY field with type: {}", typeName)
      
      // SECURITY FIX: Validate ClassLoader before loading classes
      val classLoader = field.javaClass.classLoader
      validateClassLoaderSecurity(classLoader)
      
      // Find the VitessType constructor: VitessType(String name, int jdbcId)
      // We need to find the class from the field's classloader since we can't import it directly
      val vitessTypeClass = loadVitessTypeClassSecurely(classLoader)
      val constructor = vitessTypeClass.getConstructor(String::class.java, Int::class.javaPrimitiveType)
      
      // Create VitessType("GEOMETRY", Types.OTHER) or specific type like "POINT"
      constructor.newInstance(typeName, Types.OTHER)
    } catch (e: Exception) {
      logger.error("Failed to create GEOMETRY VitessType, this will likely cause field resolution to fail", e)
      throw IllegalStateException("Cannot create VitessType for GEOMETRY field", e)
    }
  }

  /**
   * Validates that the provided ClassLoader is safe to use for loading classes.
   * This prevents security vulnerabilities from arbitrary class loading.
   */
  private fun validateClassLoaderSecurity(classLoader: ClassLoader) {
    when {
      // Allow system classloader
      classLoader == ClassLoader.getSystemClassLoader() -> return
      
      // Allow parent delegation to system classloader
      classLoader.parent == ClassLoader.getSystemClassLoader() -> return
      
      // Allow loading from our own classloader hierarchy
      classLoader == VitessTypeHandler::class.java.classLoader -> return
      
      // Allow classloaders that have our classloader in their hierarchy
      isClassLoaderInHierarchy(classLoader, VitessTypeHandler::class.java.classLoader) -> return
      
      else -> {
        val error = "Security validation failed: Unsafe ClassLoader detected: ${classLoader::class.java.name}"
        logger.error(error)
        throw SecurityException(error)
      }
    }
  }

  /**
   * Checks if the target classloader is in the hierarchy of the source classloader.
   */
  private fun isClassLoaderInHierarchy(source: ClassLoader?, target: ClassLoader?): Boolean {
    var current = source
    while (current != null) {
      if (current == target) return true
      current = current.parent
    }
    return false
  }

  /**
   * Securely loads the VitessType class with additional validation.
   */
  private fun loadVitessTypeClassSecurely(classLoader: ClassLoader): Class<*> {
    val expectedClassName = "io.debezium.connector.vitess.VitessType"
    
    try {
      val loadedClass = classLoader.loadClass(expectedClassName)
      
      // Validate the loaded class is actually what we expect
      if (loadedClass.name != expectedClassName) {
        throw SecurityException("Class name mismatch: expected $expectedClassName, got ${loadedClass.name}")
      }
      
      // Validate the class has the expected constructor
      val constructor = loadedClass.getConstructor(String::class.java, Int::class.javaPrimitiveType)
      if (constructor == null) {
        throw SecurityException("VitessType class does not have expected constructor")
      }
      
      logger.debug("Successfully validated and loaded VitessType class")
      return loadedClass
      
    } catch (e: ClassNotFoundException) {
      throw IllegalStateException("VitessType class not found - this indicates a classpath issue", e)
    } catch (e: NoSuchMethodException) {
      throw IllegalStateException("VitessType class does not have expected constructor", e)
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