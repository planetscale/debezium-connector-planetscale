/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("UnstableApiUsage")

pluginManagement {
  includeBuild("../build-logic")
  includeBuild("../transformer")
  includeBuild("../transforms")

  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
}

plugins {
  id("com.autonomousapps.build-health") version ("2.10.1")
  id("org.jetbrains.kotlin.jvm") version "2.1.20" apply false
  id("com.gradle.enterprise") version ("3.16.2")
  id("com.gradle.common-custom-user-data-gradle-plugin") version ("2.1")
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

rootProject.name = "debezium-planetscale"

enableFeaturePreview("STABLE_CONFIGURATION_CACHE")
enableFeaturePreview("GROOVY_COMPILATION_AVOIDANCE")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
