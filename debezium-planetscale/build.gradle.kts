/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("VulnerableLibrariesLocal")

plugins {
  alias(libs.plugins.shadow)
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spdx)
  alias(libs.plugins.planetscale.debezium.build)
  signing
  `java-library`
  `maven-publish`
}

group = "com.planetscale.labs"
val packagePrefix = group as String
val vitessPackage = "io.debezium.connector.vitess"

val planetscaleAdapter: Configuration by configurations.creating
val vitessAdapter: Configuration by configurations.creating

dependencies {
  api(debezium.core)
  api(debezium.embedded)
  api(libs.grpc.auth)
  api(libs.vitess.grpc.client) {
    exclude(group = "com.google.code.findbugs", module = "jsr305")
    exclude(group = "org.codehaus.mojo", module = "animal-sniffer-annotations")
    exclude(group = "com.google.errorprone", module = "error_prone_annotations")
    exclude(group = "com.google.j2objc", module = "j2objc-annotations")
    exclude(group = "io.opentracing.contrib", module = "opentracing-grpc")
    exclude(group = "org.apache.logging.log4j", module = "log4j-api")
  }

  planetscaleAdapter(libs.planetscale.debezium.facade)
  vitessAdapter(debezium.connectors.vitess)

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
  shadowJar {
    archiveClassifier = ""
    archiveBaseName = "planetscale-debezium-adapter"
    configurations = listOf(vitessAdapter)
    relocate(vitessPackage, "${packagePrefix}.${vitessPackage}")
    from(jar)

    dependencyFilter.include {
      debezium.connectors.vitess.get().let { vitessAdapter ->
        // only force-shadow the vitess adapter.
        it.moduleGroup == vitessAdapter.group && it.moduleName == vitessAdapter.name
      }
    }

    exclude(
      // don't include specifications from the original vitess connector.
      "META-INF/maven/io.debezium/debezium-connector-vitess/"
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
