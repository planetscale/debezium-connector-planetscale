/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */

plugins {
  alias(libs.plugins.shadow)
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spdx)
  alias(libs.plugins.planetscale.debezium)
  alias(libs.plugins.planetscale.debezium.build)
  `java-library`
}

group = "com.planetscale.labs"

val planetscaleAdapter: Configuration by configurations.creating {
  isCanBeConsumed = true
}

dependencies {
  api(debezium.core)
  api(debezium.embedded)
  implementation(kotlin("stdlib"))

  planetscaleAdapter(libs.planetscale.debezium.facade)

  testImplementation(libs.kotlin.test.junit5)
  testImplementation(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.junit.platform.launcher)
}

java {
  toolchain {
    languageVersion = JavaLanguageVersion.of(24)
  }
}

shadow {

}

tasks.test {
  useJUnitPlatform()
}
