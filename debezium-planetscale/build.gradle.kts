import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

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
  signing
  `java-library`
  `maven-publish`
}

group = "com.planetscale.labs"
val packagePrefix = group as String
val vitessPackage = "io.debezium.connector.vitess"

val planetscaleAdapter: Configuration by configurations.creating {
  isCanBeConsumed = true
}
val vitessAdapter: Configuration by configurations.creating {
  isCanBeConsumed = true
}

dependencies {
  api(debezium.core)
  api(debezium.embedded)
  implementation(kotlin("stdlib"))

  planetscaleAdapter(libs.planetscale.debezium.facade)
  vitessAdapter(debezium.connectors.vitess)

  testImplementation(libs.kotlin.test.junit5)
  testImplementation(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.junit.platform.launcher)
}

java {
  toolchain {
    languageVersion = JavaLanguageVersion.of(24)
  }
}

signing {
  useGpgCmd()
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      from(components["shadow"])
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
    configurations = listOf()
    relocate(vitessPackage, "${packagePrefix}.${vitessPackage}")
    from(jar)

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
    dependsOn(
      shadowJar,
      publish,
    )
  }
}
