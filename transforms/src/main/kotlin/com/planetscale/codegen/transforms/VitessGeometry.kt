package com.planetscale.codegen.transforms

import com.planetscale.debezium.geometry.GeometryTypeHandler
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

/**
 * ByteBuddy transform that adds GEOMETRY/spatial data type support to the Vitess connector
 * by intercepting field type resolution and delegating GEOMETRY types to MySQL's spatial handling.
 */
class VitessGeometry : AbstractTransform() {
  override fun matches(target: TypeDescription): Boolean = 
    target.simpleName == "VStreamOutputMessageDecoder" ||
    target.simpleName.contains("VitessType") ||
    target.simpleName.contains("FieldType")

  override fun transform(builder: Builder<*>): Builder<*> = builder
    // Intercept methods that handle JDBC type resolution for VStream fields
    .method(ElementMatchers.nameContains("decode"))
    .intercept(MethodDelegation.to(GeometryTypeHandler::class.java))
}