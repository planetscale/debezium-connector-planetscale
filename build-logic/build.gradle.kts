/*
* Copyright (c) 2025 James S. Clark
*
* This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
* permission from the copyright holder, depicted above. All rights reserved.
*/
@file:Suppress("UnstableApiUsage")

import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
  `maven-publish`
  distribution
  signing
  idea
  java
  `jvm-toolchains`
  `java-gradle-plugin`
  `kotlin-dsl`
}

group = "com.planetscale.labs"

val javaTarget = 21
val javaVersion = JavaVersion.toVersion(javaTarget)
val kotlinJvmTarget = JvmTarget.fromTarget(javaTarget.toString())

java {
  sourceCompatibility = javaVersion
  targetCompatibility = javaVersion
}

kotlin {
  compilerOptions {
    jvmTarget = kotlinJvmTarget
  }
}

dependencies {
  implementation(gradleApi())
  implementation(libs.plugin.bytebuddy)
  implementation(libs.plugin.kotlin.jvm)
  implementation(libs.plugin.kotlin.atomicfu)
  implementation(libs.plugin.kotlin.powerAssert)
  implementation(libs.plugin.kotlinx.atomicfu)
  implementation(libs.plugin.kotlinx.kover)
  implementation(libs.plugin.spotless)
  implementation(libs.plugin.testlogger)
  implementation(files(libs::class.java.protectionDomain.codeSource.location))
  implementation(files(debezium::class.java.protectionDomain.codeSource.location))
}

gradlePlugin {
  plugins {
    create("internalBuild") {
      id = "com.planetscale.debezium.conventions"
      implementationClass = "com.planetscale.conventions.PlanetscaleConventionsPlugin"
    }
  }
}
