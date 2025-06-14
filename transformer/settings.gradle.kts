/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("DSL_SCOPE_VIOLATION", "UnstableApiUsage")

pluginManagement {
  includeBuild("../build-logic")
  includeBuild("../transforms")

  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
}

plugins {
  includeBuild("../build-logic")
  id("org.gradle.toolchains.foojay-resolver-convention") version ("0.10.0")
}

dependencyResolutionManagement {
  repositories {
    mavenCentral()
    google()
  }
  versionCatalogs {
    create("libs") {
      from(files("../gradle/libs.versions.toml"))
    }
    create("debezium") {
      from(files("../gradle/debezium.versions.toml"))
    }
  }
}
