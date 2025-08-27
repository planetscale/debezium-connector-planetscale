package com.planetscale.codegen.transforms

import com.planetscale.debezium.geometry.GeometryTypeHandler
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

/**
 * ByteBuddy transform that adds GEOMETRY/spatial data type support to the Vitess connector
 * by intercepting the exact method that handles VStream field message processing.
 * 
 * This targets the handleFieldMessage method in VStreamOutputMessageDecoder that is responsible
 * for creating table schemas from field definitions. When it encounters GEOMETRY types 
 * with jdbcId=1111 (Types.OTHER), our handler will create the proper Debezium schema structure.
 */
class VitessGeometry : AbstractTransform() {
  override fun matches(target: TypeDescription): Boolean = 
    // Target the specific decoder that handles VStream field type resolution
    target.simpleName == "VStreamOutputMessageDecoder"

  override fun transform(builder: Builder<*>, typeDescription: TypeDescription): Builder<*> = builder.apply {
    // Validate that the target method exists before applying the transformation
    validateTargetMethod(typeDescription, "handleFieldMessage")
    
    method(ElementMatchers.named("handleFieldMessage"))
      .intercept(MethodDelegation.to(GeometryTypeHandler::class.java))
  }
}
