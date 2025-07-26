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

@Suppress("unused")
class VitessPluginHooks : Plugin {
  private val plugins: List<Plugin> = listOf(
    VitessHello(),
    VitessManagedChannel(),
  )

  override fun matches(target: TypeDescription): Boolean {
    return plugins.any { it.matches(target) }
  }

  override fun close() {
    plugins.forEach { it.close() }
  }

  override fun apply(
    builder: DynamicType.Builder<*>,
    typeDescription: TypeDescription,
    classFileLocator: ClassFileLocator,
  ): DynamicType.Builder<*> = builder.apply {
    plugins.forEach { plugin ->
      plugin.apply(this, typeDescription, classFileLocator)
    }
  }
}
