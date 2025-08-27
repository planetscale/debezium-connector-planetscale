/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.codegen.transforms

import net.bytebuddy.build.Plugin
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.ClassFileLocator
import net.bytebuddy.dynamic.DynamicType
import org.slf4j.LoggerFactory

abstract class AbstractTransform : Plugin {
  companion object {
    private val logger = LoggerFactory.getLogger(AbstractTransform::class.java)
  }

  override fun matches(target: TypeDescription): Boolean {
    return true // by default
  }

  override fun close() {
    // nothing at this time
  }

  override fun apply(
    builder: DynamicType.Builder<*>,
    typeDescription: TypeDescription,
    classFileLocator: ClassFileLocator,
  ): DynamicType.Builder<*> = transform(
    builder,
    typeDescription
  ).also {
    it.make()
  }

  open fun transform(builder: DynamicType.Builder<*>): DynamicType.Builder<*> {
    return builder // by default, no transformation
  }

  /**
   * Transform method that includes type description for validation.
   * This is called by the apply method and should be overridden by subclasses
   * that need to validate target methods.
   */
  open fun transform(builder: DynamicType.Builder<*>, typeDescription: TypeDescription): DynamicType.Builder<*> {
    return transform(builder) // delegate to single-parameter version by default
  }

  /**
   * Validates that the target method exists on the specified type.
   * This prevents silent failures when ByteBuddy transforms target methods that don't exist.
   * 
   * @param typeDescription The type to check for the method
   * @param methodName The name of the method to validate
   * @param expectedParameterTypes Optional array of expected parameter types for stricter validation
   * @throws IllegalStateException if the method doesn't exist or signature doesn't match
   */
  protected fun validateTargetMethod(
    typeDescription: TypeDescription, 
    methodName: String,
    expectedParameterTypes: Array<Class<*>>? = null
  ) {
    val methods = typeDescription.declaredMethods.filter { it.name == methodName }
    
    if (methods.isEmpty()) {
      val error = "Transform validation failed: Method '$methodName' does not exist on type '${typeDescription.name}'"
      logger.error(error)
      throw IllegalStateException(error)
    }
    
    // If parameter types are specified, validate the signature
    expectedParameterTypes?.let { paramTypes ->
      val matchingMethod = methods.find { method ->
        method.parameters.size == paramTypes.size &&
        method.parameters.zip(paramTypes).all { (param, expectedType) ->
          param.type.asErasure().name == expectedType.name
        }
      }
      
      if (matchingMethod == null) {
        val availableSignatures = methods.map { method ->
          val params = method.parameters.joinToString(", ") { it.type.asErasure().simpleName }
          "$methodName($params)"
        }
        val expectedSignature = "$methodName(${paramTypes.joinToString(", ") { it.simpleName }})"
        
        val error = "Transform validation failed: Method signature '$expectedSignature' not found on type '${typeDescription.name}'. " +
                   "Available signatures: ${availableSignatures.joinToString(", ")}"
        logger.error(error)
        throw IllegalStateException(error)
      }
      
      logger.debug("Validated method signature: {} on type {}", "$methodName(${paramTypes.joinToString(", ") { it.simpleName }})", typeDescription.name)
    }
    
    logger.debug("Validated target method '{}' exists on type '{}'", methodName, typeDescription.name)
  }
}
