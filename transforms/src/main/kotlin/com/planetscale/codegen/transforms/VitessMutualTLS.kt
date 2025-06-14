/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.codegen.transforms

import net.bytebuddy.build.Plugin
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.ClassFileLocator
import net.bytebuddy.dynamic.DynamicType
import net.bytebuddy.implementation.FixedValue
import net.bytebuddy.matcher.ElementMatchers

internal class VitessMutualTLS : Plugin {
  override fun matches(target: TypeDescription?): Boolean {
    return false  // don't yet apply
  }

  override fun close() {
    // nothing at this time
  }

  override fun apply(
    builder: DynamicType.Builder<*>,
    typeDescription: TypeDescription,
    classFileLocator: ClassFileLocator
  ): DynamicType.Builder<*> = builder
    .method(ElementMatchers.named("toString"))
    .intercept(FixedValue.value("Hello World!"))
}
