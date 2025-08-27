package com.planetscale.codegen.transforms

import com.planetscale.debezium.geometry.VitessTypeHandler
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

/**
 * ByteBuddy transform that enhances VitessType.resolve() method to properly handle
 * GEOMETRY types that are not included in the original Vitess connector.
 * 
 * This addresses the core issue where VitessType.resolve() fails for GEOMETRY fields
 * by intercepting the resolve methods and handling GEOMETRY types specially.
 * 
 * The error "Cannot resolve JDBC type from VStream field" occurs in VitessType.resolve()
 * when it encounters a field type that's not in its internal mapping. This transform
 * ensures GEOMETRY types are properly resolved.
 */
class VitessTypeEnhancement : AbstractTransform() {
  override fun matches(target: TypeDescription): Boolean = 
    // Target the VitessType class that contains the resolve methods
    target.simpleName == "VitessType"

  override fun transform(builder: Builder<*>, typeDescription: TypeDescription): Builder<*> = builder.apply {
    // Validate that the target method exists before applying the transformation
    validateTargetMethod(typeDescription, "resolve")
    
    // Intercept both static resolve methods:
    // - resolve(Query.Field field)  
    // - resolve(Query.Field field, boolean includeEnumAndSetMeta)
    method(ElementMatchers.named("resolve"))
      .intercept(MethodDelegation.to(VitessTypeHandler::class.java))
  }
}