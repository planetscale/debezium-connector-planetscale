/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.codegen.transforms

import net.bytebuddy.build.Plugin
import net.bytebuddy.description.type.TypeDescription

abstract class AbstractTransform : Plugin {
  override fun matches(target: TypeDescription): Boolean {
    return true  // by default
  }

  override fun close() {
    // nothing at this time
  }
}
