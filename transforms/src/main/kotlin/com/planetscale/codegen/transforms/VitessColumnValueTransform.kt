package com.planetscale.codegen.transforms

import com.planetscale.debezium.geometry.VitessColumnValueHandler
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

/**
 * ByteBuddy transform that intercepts VitessColumnValue.asDefault() method
 * to handle GEOMETRY types that would otherwise be ignored with 
 * "ignore unknown column type VitessType{name='GEOMETRY', jdbcId=1111}".
 * 
 * This is the core fix for the customer's GEOMETRY support issue.
 * The asDefault() method is where the actual "ignore unknown column type" 
 * message is logged, so we need to intercept it and provide proper
 * GEOMETRY value conversion.
 */
class VitessColumnValueTransform : AbstractTransform() {
  override fun matches(target: TypeDescription): Boolean = 
    target.simpleName == "VitessColumnValue"

  override fun transform(builder: Builder<*>, typeDescription: TypeDescription): Builder<*> = builder.apply {
    // Validate that the target method exists before applying the transformation
    validateTargetMethod(typeDescription, "asDefault")
    
    method(ElementMatchers.named("asDefault"))
      .intercept(MethodDelegation.to(VitessColumnValueHandler::class.java))
  }
}