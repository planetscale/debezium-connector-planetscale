/*
* Copyright (c) 2025 James S. Clark
*
* This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
* permission from the copyright holder, depicted above. All rights reserved.
*/
@file:Suppress("UnstableApiUsage", "unused")

plugins {
  java
  `jvm-test-suite`
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.powerAssert)
  alias(libs.plugins.planetscale.debezium.build)
}

val vitessAdapter: Configuration by configurations.creating {
  isCanBeResolved = true
  isCanBeConsumed = true
}

configurations.compileClasspath.configure {
  extendsFrom(vitessAdapter)
}

dependencies {
  api(libs.bundles.vitess.client)
  api(libs.bundles.bytebuddy)

  api(debezium.core)
  api(debezium.embedded)
  api(debezium.connectors.vitess)

  vitessAdapter(debezium.connectors.vitess)

  testImplementation(kotlin("test"))
}

testing {
  suites {
    val test by getting(JvmTestSuite::class) {
      useJUnitJupiter()
    }
  }
}
