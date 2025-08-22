/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("VulnerableLibrariesLocal", "unused")

import com.planetscale.PlanetscaleBuild
import com.planetscale.codegen.transforms.VitessHello
import com.planetscale.codegen.transforms.VitessManagedChannel
import com.planetscale.codegen.transforms.VitessGeometry
import dev.sigstore.sign.tasks.SigstoreSignFilesTask
import net.bytebuddy.build.gradle.Adjustment
import net.bytebuddy.build.gradle.Adjustment.ErrorHandler
import net.bytebuddy.build.gradle.ByteBuddyTask
import net.bytebuddy.build.gradle.Discovery

plugins {
  application
  signing
  `java-library`
  `maven-publish`
  `jvm-test-suite`
  alias(libs.plugins.shadow)
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.spdx)
  alias(libs.plugins.sigstore)
  alias(libs.plugins.bytebuddy)
  alias(libs.plugins.kotlinx.abicheck)
  alias(libs.plugins.planetscale.debezium)
  alias(libs.plugins.planetscale.debezium.build)
}

val packagePrefix = PlanetscaleBuild.PACKAGE_GROUP
val vitessPackage = "io.debezium.connector.vitess"
val mysqlPackage = "io.debezium.connector.mysql"

val enableSigning = findProperty("planetscale.release") == "true"
val enableSigstore = findProperty("planetscale.sigstore") == "true"
val planetscaleAdapter: Configuration by configurations.creating
val debeziumConnectors: Configuration by configurations.creating

val kafkaConnect: Configuration by configurations.creating {
  isCanBeResolved = true
  extendsFrom(configurations.runtimeClasspath.get(), configurations.compileClasspath.get())
}

listOf(planetscaleAdapter, debeziumConnectors).forEach {
  it.resolutionStrategy.activateDependencyLocking()
}

fun DependencyHandlerScope.planetscale(dep: Provider<MinimalExternalModuleDependency>) {
  implementation(dep) { isTransitive = false }
  planetscaleAdapter(dep) { isTransitive = false }
}

fun DependencyHandlerScope.connector(dep: Provider<MinimalExternalModuleDependency>) {
  compileOnly(dep)
  debeziumConnectors(dep)
}

application {
  mainClass = "com.planetscale.debezium.PlanetscaleDebezium"
}

kotlin {
  explicitApi()
}

spdxSbom {
  targets {
    create("release") {
      configurations = listOf(
        "compileClasspath",
        debeziumConnectors.name,
      )
    }
  }
}

byteBuddy {
  discovery = Discovery.UNIQUE
  adjustment = Adjustment.FULL
  adjustmentErrorHandler = ErrorHandler.FAIL
}

dependencies {
  // debezium dependencies from upstream vitess adapter.
  api(debezium.core)
  api(debezium.embedded)
  runtimeOnly(libs.kafka.connect.api)

  api(libs.vitess.grpc.client) {
    // these exclusions come from the `pom.xml` for the vitess connector.
    exclude(group = "com.google.code.findbugs", module = "jsr305")
    exclude(group = "org.codehaus.mojo", module = "animal-sniffer-annotations")
    exclude(group = "com.google.errorprone", module = "error_prone_annotations")
    exclude(group = "com.google.j2objc", module = "j2objc-annotations")
    exclude(group = "io.opentracing.contrib", module = "opentracing-grpc")
    exclude(group = "org.apache.logging.log4j", module = "log4j-api")
  }

  // extra dependencies needed by the planetscale connector.
  api(libs.grpc.auth)
  api(libs.grpc.netty.shaded)
  kafkaConnect(libs.netty.transport.epoll)
  kafkaConnect(libs.grpc.netty.shaded)

  // kotlin and kotlin extensions.
  api(kotlin("stdlib"))
  implementation(libs.bundles.kotlinx)

  // internal configurations (packaged classes, transforms which are included within the final JAR).
  planetscale(libs.planetscale.debezium.transforms)
  connector(debezium.connectors.vitess)
  connector(debezium.connectors.mysql)

  // test dependencies.
  testImplementation(platform(libs.testcontainers.bom))
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.kotlin.test.junit5)
  testImplementation(libs.kotlinx.coroutines.test)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(debezium.connectors.vitess)
  testRuntimeOnly(libs.junit.platform.launcher)
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

signing {
  useGpgCmd()
  isRequired = enableSigning
  sign(publishing.publications["maven"])
  sign(configurations.runtimeElements.get())
}

val debeziumClasses by tasks.registering(Copy::class) {
  group = "build"
  description = "Copy Debezium classes to build directory"
  debeziumConnectors.files.filter { it.name.startsWith("debezium-connector-") && it.name.endsWith(".jar") }.forEach {
    from(zipTree(it))
  }
  into(layout.buildDirectory.dir("debezium/classes"))
  include("**/*.class")
  exclude("**/VitessReplicationConnection*")  // fix: private `newChannel` override
  finalizedBy(debeziumClassesPatched)
}

val debeziumClassesPatched by tasks.registering(Copy::class) {
  group = "build"
  description = "Copy patched Debezium classes to build directory"
  from(layout.buildDirectory.dir("classes/java/main"))
  into(layout.buildDirectory.dir("debezium/classes"))
  include("**/*.class")
  dependsOn(tasks.compileJava)
}

val transformVitess by tasks.registering(ByteBuddyTask::class) {
  group = "build"
  description = "Transform classes for use with Vitess plugin"
  source = layout.buildDirectory.dir("debezium/classes")
  target = layout.buildDirectory.dir("classes/kotlin-transformed/main")
  classPath.from(debeziumClasses, configurations.compileClasspath, configurations.runtimeClasspath)
  dependsOn(tasks.compileKotlin, debeziumClasses, debeziumClassesPatched)
  transformation { plugin = VitessHello::class.java }
  transformation { plugin = VitessManagedChannel::class.java }
  transformation { plugin = VitessGeometry::class.java }
}

val connectRoot = layout.buildDirectory.dir("connect")
val connectOut = layout.buildDirectory.dir("connect/pkg")
val connectDistRoot = layout.buildDirectory.dir("connect/dist")

// `doc/` directory includes `README.md` and `LICENSE.txt`
val assembleConnectDoc by tasks.registering(Copy::class) {
  from(layout.projectDirectory.dir("src/main/config")) {
    include("README.md", "LICENSE.txt")
  }
  into(connectOut.get().dir("doc"))
}

// `lib/` directory includes the transformed vitess connector and all dependencies
val assembleConnectLib by tasks.registering(Copy::class) {
  from(kafkaConnect) {
    exclude("debezium-connector-vitess-*.jar")  // packaged with final planetscale connector
    exclude("netty-transport-native-unix-common*.jar")  // causes UDS compat issues
  }
  from(tasks.shadowJar)
  into(connectOut.get().dir("lib"))
}

// connect root directory includes the transformed vitess connector and all dependencies
val assembleConnectLayout by tasks.registering(Copy::class) {
  from(layout.projectDirectory.dir("src/main/config")) {
    include("manifest.json")
  }
  into(connectOut.get())

  dependsOn(
    assembleConnectDoc,
    assembleConnectLib,
  )
}

val assembleConnectDist by tasks.registering(Copy::class) {
  from(connectOut.get())
  into(connectDistRoot.get().dir("packages/planetscale-debezium-connector-planetscale-${version}"))

  dependsOn(
    assembleConnectDoc,
    assembleConnectLib,
    assembleConnectLayout,
  )
}

val assembleConnectZip by tasks.registering(Zip::class) {
  group = "build"
  description = "Assemble the connector distribution ZIP for Kafka Connect"
  archiveFileName.set("planetscale-debezium-connector-planetscale-${version}.zip")
  destinationDirectory.set(connectDistRoot.get())
  from(connectDistRoot.get().dir("packages/planetscale-debezium-connector-planetscale-${version}"))
  dependsOn(assembleConnectDist)
}

val connectDist by tasks.registering {
  group = "build"
  description = "Assemble the connector distribution for Kafka Connect"

  dependsOn(
    assembleConnectDoc,
    assembleConnectLib,
    assembleConnectLayout,
    assembleConnectDist,
    assembleConnectZip,
  )
}

tasks {
  jar {
    from(transformVitess)
    dependsOn(transformVitess)
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
  }

  // Only enable signing when `planetscale.release` is set to true.
  if (!enableSigning) {
    withType<Sign>().configureEach {
      enabled = false
    }
  }
  if (!enableSigning || !enableSigstore) {
    withType<SigstoreSignFilesTask>().configureEach {
      enabled = false
    }
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

    // `io.debezium.connector.vitess` → `com.planetscale.labs.io.debezium.connector.vitess`.
    relocate(vitessPackage, "$packagePrefix.$vitessPackage")

    // `io.debezium.connector.mysql` → `com.planetscale.labs.io.debezium.connector.mysql`.
    relocate(mysqlPackage, "$packagePrefix.$mysqlPackage")

    // include local classes for the adapter surface.
    from(jar)

    // merge and rewrite service files accounting for relocations.
    mergeServiceFiles()

    // only package our own transitive classes; this includes symbols which are needed for transform-injected hooks.
    dependencyFilter.include {
      it.moduleGroup == PlanetscaleBuild.PACKAGE_GROUP
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
      // many tools scan these attributes, so they are good to set.
      attributes(
        "Implementation-Title" to "Planetscale Debezium Adapter",
        "Implementation-Version" to project.version,
      )
    }
  }

  test {
    useJUnitPlatform()
  }

  publish {
    dependsOn(shadowJar, spdxSbom)
  }
  build {
    dependsOn(shadowJar, spdxSbom, publish, connectDist)
  }
}
