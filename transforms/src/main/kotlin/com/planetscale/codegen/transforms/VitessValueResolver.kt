package com.planetscale.codegen.transforms

import com.planetscale.debezium.geometry.GeometryValueHandler
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

/**
 * ByteBuddy transform that intercepts value resolution in the Vitess connector
 * to properly convert GEOMETRY field values from raw VStream bytes to Debezium's
 * standard geometry structure.
 * 
 * This complements the VitessGeometry transform (which handles schema resolution)
 * by ensuring GEOMETRY field values are properly converted during message processing.
 */
class VitessValueResolver : AbstractTransform() {
  override fun matches(target: TypeDescription): Boolean = 
    target.simpleName == "ReplicationMessageColumnValueResolver"

  override fun transform(builder: Builder<*>, typeDescription: TypeDescription): Builder<*> = builder.apply {
    // Validate that the target method exists before applying the transformation
    validateTargetMethod(typeDescription, "resolveValue")
    
    method(ElementMatchers.named("resolveValue"))
      .intercept(MethodDelegation.to(GeometryValueHandler::class.java))
  }
}