package com.planetscale.codegen.transforms

import com.planetscale.debezium.geometry.GeometryTypeHandler
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

/**
 * ByteBuddy transform that adds GEOMETRY/spatial data type support to the Vitess connector
 * by intercepting specific field type resolution methods that convert VitessType to JDBC types.
 * 
 * This targets the exact method that throws "Cannot resolve JDBC type from VStream field"
 * errors when encountering GEOMETRY types with jdbcId=1111 (Types.OTHER).
 */
internal class VitessGeometry : AbstractTransform() {
  override fun matches(target: TypeDescription): Boolean = 
    // Target the specific decoder that handles VStream field type resolution
    target.simpleName == "VStreamOutputMessageDecoder" ||
    // Also target any classes that handle VitessType to JDBC type conversion
    target.simpleName.contains("VitessType") ||
    target.simpleName.contains("ColumnDefinition")

  override fun transform(builder: Builder<*>): Builder<*> = builder.apply {
    method(ElementMatchers.nameContains("decode"))
      .intercept(MethodDelegation.to(GeometryTypeHandler::class.java))
  }
}
