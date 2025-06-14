/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("VulnerableLibrariesLocal", "unused")

import com.planetscale.codegen.transforms.VitessHello
import net.bytebuddy.build.gradle.Adjustment
import net.bytebuddy.build.gradle.Adjustment.ErrorHandler
import net.bytebuddy.build.gradle.ByteBuddyTask
import net.bytebuddy.build.gradle.Discovery

plugins {
  alias(libs.plugins.shadow)
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spdx)
  alias(libs.plugins.bytebuddy)
  alias(libs.plugins.planetscale.debezium)
  alias(libs.plugins.planetscale.debezium.build)
  application
  signing
  `java-library`
  `maven-publish`
}

group = "com.planetscale.labs"
version = debezium.versions.debezium.get()
val packagePrefix = group as String
val vitessPackage = "io.debezium.connector.vitess"

val planetscaleAdapter: Configuration by configurations.creating
val vitessAdapter: Configuration by configurations.creating

fun DependencyHandlerScope.planetscale(dep: Provider<MinimalExternalModuleDependency>) {
  implementation(dep) { isTransitive = false }
  planetscaleAdapter(dep) { isTransitive = false }
}

application {
  mainClass = "com.planetscale.debezium.PlanetscaleDebezium"
}

kotlin {
  explicitApi()
}

byteBuddy {
  discovery = Discovery.UNIQUE
  adjustment = Adjustment.FULL
  adjustmentErrorHandler = ErrorHandler.FAIL
}

val debeziumClasses by tasks.registering(Copy::class) {
  group = "build"
  description = "Copy Debezium classes to build directory"
  from(zipTree(vitessAdapter.files.single { it.name.startsWith("debezium-connector-vitess") && it.name.endsWith(".jar") }))
  into(layout.buildDirectory.dir("debezium/classes"))
  include("**/*.class")
}

val transformVitess by tasks.registering(ByteBuddyTask::class) {
  group = "build"
  description = "Transform classes for use with Vitess plugin"
  source = layout.buildDirectory.dir("debezium/classes")
  target = layout.buildDirectory.dir("classes/kotlin-transformed/main")
  classPath.from(configurations.compileClasspath)
  dependsOn(tasks.compileKotlin, debeziumClasses)
  transformation { plugin = VitessHello::class.java }
}

dependencies {
  api(debezium.core)
  api(debezium.embedded)
  api(libs.grpc.auth)
  runtimeOnly(libs.kafka.connect.api)
  api(libs.vitess.grpc.client) {
    exclude(group = "com.google.code.findbugs", module = "jsr305")
    exclude(group = "org.codehaus.mojo", module = "animal-sniffer-annotations")
    exclude(group = "com.google.errorprone", module = "error_prone_annotations")
    exclude(group = "com.google.j2objc", module = "j2objc-annotations")
    exclude(group = "io.opentracing.contrib", module = "opentracing-grpc")
    exclude(group = "org.apache.logging.log4j", module = "log4j-api")
  }

  planetscale(libs.planetscale.debezium.transforms)
  shadow(kotlin("stdlib"))
  vitessAdapter(debezium.connectors.vitess)
  compileOnly(debezium.connectors.vitess)

  testImplementation(libs.kotlin.test.junit5)
  testImplementation(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.junit.platform.launcher)
}

signing {
  useGpgCmd()
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      from(components["shadow"])

      pom {
        description = "Debezium Adapter for Planetscale"
      }
    }
  }
  repositories {
    maven("file://${rootProject.layout.buildDirectory.dir("m2").get().asFile.absolutePath}")
  }
}

tasks {
  jar {
    from(transformVitess)
    dependsOn(transformVitess)
  }

  compileKotlin {
    dependsOn(debeziumClasses)
    finalizedBy(transformVitess)
  }

  named("run", JavaExec::class) {
    dependsOn(shadowJar)

    classpath = files(
      configurations.compileClasspath,
      configurations.runtimeClasspath,
      shadowJar.get().outputs.files.single(),
    )
  }

  shadowJar {
    archiveClassifier = ""
    archiveBaseName = "planetscale-debezium-adapter"
    includeEmptyDirs = false
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true

    relocate(vitessPackage, "${packagePrefix}.${vitessPackage}")
    from(jar)
    mergeServiceFiles()

    // only package our own transitive classes; this includes symbols which are needed for transform-injected hooks.
    dependencyFilter.include {
      it.moduleGroup == "com.planetscale.labs"
    }
    exclude(
      // don't include bytebuddy classes; we only use them at build time.
      "net/bytebuddy/**",
      // don't include build-time transform code.
      "com/planetscale/codegen/**",
      // don't include specifications from the original vitess connector.
      "META-INF/maven/io.debezium/debezium-connector-vitess/",
      // don't include kotlin metadata.
      "META-INF/*.kotlin_module",
      // don't include metadata about the vitess adapter.
      "META-INF/maven/**",
    )
    manifest {
      attributes(
        "Implementation-Title" to "Planetscale Debezium Adapter",
        "Implementation-Version" to project.version,
      )
    }
  }

  test {
    useJUnitPlatform()
  }

  build {
    dependsOn(shadowJar, publish)
  }
}
