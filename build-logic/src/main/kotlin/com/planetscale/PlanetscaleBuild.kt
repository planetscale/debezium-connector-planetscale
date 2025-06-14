/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale

import org.gradle.accessors.dm.LibrariesForDebezium
import org.gradle.api.Project
import org.gradle.kotlin.dsl.the

/**
 * Static build constants.
 */
object PlanetscaleBuild {
  /** Package group to use for all Maven artifacts; should match the main package prefix. */
  const val PACKAGE_GROUP: String = "com.planetscale.labs"

  /** @return Pinned version of Debezium. */
  @JvmStatic fun Project.debeziumVersion(): String {
    return the<LibrariesForDebezium>().versions.debezium.get()
  }
}
