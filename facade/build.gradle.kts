/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("UnstableApiUsage", "unused", "VulnerableLibrariesLocal")

plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.bytebuddy)
  alias(libs.plugins.planetscale.debezium)
  alias(libs.plugins.planetscale.debezium.build)
}

group = "com.planetscale.labs"

kotlin {
  explicitApi()
}

dependencies {
  implementation(debezium.core)
  implementation(debezium.embedded)
  implementation(debezium.connectors.vitess)
}
