/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.codegen.transforms

import com.planetscale.debezium.hello.DebeziumVitessHello
import net.bytebuddy.description.type.TypeDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatchers

class VitessHello : AbstractTransform() {
  override fun matches(target: TypeDescription): Boolean = target.simpleName == "VitessConnector"

  override fun transform(builder: Builder<*>): Builder<*> = builder
    // intercept the method `start`
    .method(ElementMatchers.named("start"))
    // delegate it to `DebeziumVitessHello.start`
    .intercept(MethodDelegation.to(DebeziumVitessHello::class.java))
}
