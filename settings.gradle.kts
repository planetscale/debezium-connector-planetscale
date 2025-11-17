/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("UnstableApiUsage")

pluginManagement {
  includeBuild("build-logic")
  includeBuild("transforms")
  includeBuild("transformer")

  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
}

plugins {
  id("com.autonomousapps.build-health") version ("2.10.1")
  id("org.jetbrains.kotlin.jvm") version "2.2.21" apply false
  id("com.gradle.enterprise") version ("3.16.2")
  id("com.gradle.common-custom-user-data-gradle-plugin") version ("2.1")
  id("org.gradle.toolchains.foojay-resolver-convention") version ("1.0.0")
}

gradleEnterprise {
  buildScan {
    termsOfServiceUrl = "https://gradle.com/terms-of-service"
    termsOfServiceAgree = "yes"
  }
}

dependencyResolutionManagement {
  repositories {
    mavenCentral()
    google()
  }
  versionCatalogs {
    create("libs")
    create("debezium") {
      from(files("gradle/debezium.versions.toml"))
    }
  }
}

rootProject.name = "debezium-connector-planetscale"

includeBuild("debezium-planetscale")

enableFeaturePreview("STABLE_CONFIGURATION_CACHE")
enableFeaturePreview("GROOVY_COMPILATION_AVOIDANCE")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

// Use `latest` for the latest version, or any other tag, branch, or commit SHA on this project.
val elidePluginVersion: String by settings
apply(from = "https://gradle.elide.dev/$elidePluginVersion/elide.gradle.kts")
