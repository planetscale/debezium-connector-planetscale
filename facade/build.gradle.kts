/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("UnstableApiUsage", "unused")

import com.planetscale.codegen.transforms.VitessPluginHooks
import net.bytebuddy.build.gradle.ByteBuddyTask
import net.bytebuddy.build.gradle.Adjustment
import net.bytebuddy.build.gradle.Adjustment.ErrorHandler

plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.bytebuddy)
  alias(libs.plugins.planetscale.debezium)
  alias(libs.plugins.planetscale.debezium.build)
}

group = "com.planetscale.labs"

byteBuddy {
  transformation {
    plugin = VitessPluginHooks::class.java
  }
  adjustment = Adjustment.SELF
  adjustmentErrorHandler = ErrorHandler.IGNORE
}

val transformVitess by tasks.registering(ByteBuddyTask::class) {
  group = "build"
  description = "Transform classes for use with Vitess plugin"
  source = layout.buildDirectory.dir("classes/kotlin/main")
  target = layout.buildDirectory.dir("classes/kotlin/main")
  classPath.from(configurations.compileClasspath)

//  transformation {
//    plugin = VitessPluginHooks::class.java
//  }
}
