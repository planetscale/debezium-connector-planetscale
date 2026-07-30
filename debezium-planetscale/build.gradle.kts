/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("VulnerableLibrariesLocal", "unused")

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.planetscale.PlanetscaleBuild
import dev.sigstore.sign.tasks.SigstoreSignFilesTask
import net.bytebuddy.build.gradle.Adjustment
import net.bytebuddy.build.gradle.Adjustment.ErrorHandler
import net.bytebuddy.build.gradle.ByteBuddyTask
import net.bytebuddy.build.gradle.Discovery
import org.apache.tools.ant.filters.ReplaceTokens
import java.time.LocalDate

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

// Values injected into the Connect manifest.json at package time so they never go stale:
//   version      -> the full project version (e.g. 3.2.1.Final-r2), matching the artifact
//   release_date -> the build date (ISO yyyy-MM-dd), overridable with -PreleaseDate=YYYY-MM-DD
val manifestReleaseDate: String = providers.gradleProperty("releaseDate").orNull ?: LocalDate.now().toString()

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
  // NB: debezium-embedded is intentionally NOT a dependency. It is the standalone engine
  // (unused by this Kafka Connect plugin) and, when packaged, Connect's plugin scanner tries
  // to instantiate its anonymous `io.debezium.embedded.Transformations$1` (a Transformation
  // with no no-arg ctor) → NoSuchMethodException during plugin discovery.
  //
  // kafka-connect-api must be on the (main + test) compile classpath. It was previously supplied
  // transitively by debezium-embedded's `api`; with embedded removed we declare it directly.
  // `implementation` keeps it on the runtime classpath too — matching the prior packaging — though
  // under plugin isolation org.apache.kafka.* is always loaded from the worker's parent classloader,
  // so the bundled copy is inert.
  implementation(libs.kafka.connect.api)

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
  testImplementation(libs.testcontainers.core)
  testImplementation(libs.kotlin.test.junit5)
  testRuntimeOnly(libs.mysql.connector.j)
  testImplementation(libs.kotlinx.coroutines.test)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.grpc.netty.shaded)
  testImplementation(libs.grpc.stub)
  testImplementation(debezium.connectors.vitess)
  testImplementation(debezium.connectors.mysql)
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
    val ghRepo = System.getenv("GITHUB_REPOSITORY")
    if (ghRepo != null && enableSigning) {
      maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/$ghRepo")
        credentials {
          username = System.getenv("GITHUB_ACTOR") ?: ""
          password = System.getenv("GITHUB_TOKEN") ?: ""
        }
      }
    }
  }
}

val enableGpgSigning = enableSigning && (findProperty("signing.gnupg.keyName") != null || System.getenv("GPG_KEY_ID") != null)

signing {
  useGpgCmd()
  isRequired = enableGpgSigning
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
  exclude("**/VitessColumnValue*") // fix: geom and custom types
  exclude("**/VitessReplicationConnection*") // fix: private `newChannel` override
  exclude("**/VitessValueConverter*") // fix: custom type support (geo)
  exclude("**/VitessDatabaseSchema*") // fix: custom type support (geo)
  exclude("**/VitessConnectorConfig*") // fix: overrides for cell hint, etc
  // fix: BIT columns silently dropped (upstream debezium/dbz#2191, merged for 3.7); drop these
  // two excludes + fork copies once we build against a release that contains the fix.
  exclude("**/VitessType*") // fix: BIT -> Types.BIT mapping with column width
  exclude("**/connection/ReplicationMessageColumnValueResolver*") // fix: Types.BIT -> asBytes()
  exclude("**/VitessMetadata*") // fix: backtick-quote keyspace identifiers (e.g. hyphenated names)
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
    exclude("debezium-connector-vitess-*.jar") // packaged with final planetscale connector
    exclude("netty-transport-native-unix-common*.jar") // causes UDS compat issues
    // The gRPC + Vitess client stacks are bundled & relocated inside the shaded adapter jar
    // (io.grpc → com.planetscale.labs.io.grpc). Shipping the loose, un-relocated copies would
    // re-introduce the host-runtime classloader collision that breaks gRPC name resolution.
    exclude("grpc-*.jar")
    exclude("vitess-client-*.jar", "vitess-grpc-client-*.jar")
  }
  from(tasks.shadowJar)
  into(connectOut.get().dir("lib"))
}

// connect root directory includes the transformed vitess connector and all dependencies
val assembleConnectLayout by tasks.registering(Copy::class) {
  from(layout.projectDirectory.dir("src/main/config")) {
    include("manifest.json")
    // inject @version@ / @release_date@ so the manifest always matches the built artifact
    filter(
      mapOf("tokens" to mapOf("version" to project.version.toString(), "release_date" to manifestReleaseDate)),
      ReplaceTokens::class.java,
    )
  }
  into(connectOut.get())

  dependsOn(
    assembleConnectDoc,
    assembleConnectLib,
  )
}

val assembleConnectDist by tasks.registering(Copy::class) {
  from(connectOut.get())
  into(connectDistRoot.get().dir("packages/planetscale-debezium-connector-planetscale-$version"))

  dependsOn(
    assembleConnectDoc,
    assembleConnectLib,
    assembleConnectLayout,
  )
}

val assembleConnectZip by tasks.registering(Zip::class) {
  group = "build"
  description = "Assemble the connector distribution ZIP for Kafka Connect"
  archiveFileName.set("planetscale-debezium-connector-planetscale-$version.zip")
  destinationDirectory.set(connectDistRoot.get())
  from(connectDistRoot.get().dir("packages/planetscale-debezium-connector-planetscale-$version"))
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

  // Only enable GPG signing when release mode AND a GPG key is configured.
  if (!enableGpgSigning) {
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

  fun ShadowJar.configureShadowedJar(classifier: String = "") {
    archiveBaseName = "planetscale-debezium-adapter"
    includeEmptyDirs = false
    archiveClassifier = classifier

    // `io.debezium.connector.vitess` → `com.planetscale.labs.io.debezium.connector.vitess`.
    relocate(vitessPackage, "$packagePrefix.$vitessPackage")

    // `io.debezium.connector.mysql` → `com.planetscale.labs.io.debezium.connector.mysql`.
    relocate(mysqlPackage, "$packagePrefix.$mysqlPackage")

    // Isolate the gRPC *core* stack from the host runtime. Confluent Cloud's worker ships
    // its own io.grpc (incl. grpc-googleapis) on the parent classloader; our bundled io.grpc
    // is a second, child-first copy, so gRPC's NameResolver ServiceLoader sees the host's
    // googleapis provider extending a *different* io.grpc.NameResolverProvider → it fails with
    // "not a subtype" and the connector cannot build a channel. Relocating io.grpc into our
    // namespace makes our ServiceLoader look for `<pkg>.io.grpc.NameResolverProvider`, which the
    // host's service files never reference, so the collision disappears.
    //
    // Relocate the ENTIRE gRPC tree, INCLUDING grpc-netty-shaded's own `io.grpc.netty.shaded.**`,
    // plus the Vitess client + its top-level protobuf packages. All of these ALSO ship in the stock
    // debezium-server dist, so leaving them un-relocated makes our (rewritten) copies collide by
    // class name with the dist's — the fat jar then cannot be dropped in without removing those jars.
    // Relocating the whole set makes the fat jar fully self-contained and a true drop-in with nothing
    // stripped. Relocation only renames classes, so the jar does not grow. io.debezium core is
    // intentionally NOT relocated — it is the identical version the dist ships (no conflict).
    //
    // Trade-off: re-shading grpc-netty-shaded breaks its bundled native binding (the netty
    // epoll/tcnative .so names no longer match the relocated package), so netty falls back to the
    // NIO transport + JDK SSL provider. That is functionally fine for the VTGate TLS connection
    // (validated end-to-end), at a minor performance cost vs. native epoll/OpenSSL.
    relocate("io.grpc", "$packagePrefix.io.grpc")
    relocate("io.vitess", "$packagePrefix.io.vitess")
    listOf(
      "binlogdata", "binlogservice", "logutil", "mysqlctl", "queryservice", "replicationdata",
      "tableacl", "tabletmanagerdata", "tabletmanagerservice", "throttlerdata", "throttlerservice",
      "vschema", "vtadmin", "vtctldata", "vtctlservice", "vttest", "vttime",
    ).forEach { relocate(it, "$packagePrefix.$it") }

    // include local classes for the adapter surface.
    from(jar)

    // merge and rewrite service files accounting for relocations.
    mergeServiceFiles()

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

  shadowJar {
    configureShadowedJar()

    // Bundle our own transitive classes (transform-injected hooks), PLUS the gRPC and Vitess
    // client stacks so their `io.grpc` references are rewritten by the relocation above and the
    // connector ships a single, self-contained, relocated gRPC. grpc-netty (non-shaded) is
    // excluded — the connector uses grpc-netty-shaded — so we don't drag in a UdsNameResolver
    // provider that needs the (intentionally omitted) netty unix-common native lib.
    dependencyFilter.include {
      it.moduleGroup == PlanetscaleBuild.PACKAGE_GROUP ||
        it.moduleGroup == "io.vitess" ||
        (it.moduleGroup == "io.grpc" && it.moduleName != "grpc-netty")
    }
  }

  val fullyShadowed by registering(ShadowJar::class) {
    configureShadowedJar(classifier = "all")
  }

  named("test", Test::class) {
    outputs.cacheIf { false }
    outputs.upToDateWhen { false }
  }

  named("run", JavaExec::class) {
    dependsOn(shadowJar)

    classpath = files(
      configurations.compileClasspath,
      configurations.runtimeClasspath,
      shadowJar.get().outputs.files.single(),
    )
  }

  test {
    useJUnitPlatform()
  }

  publish {
    dependsOn(shadowJar, fullyShadowed, spdxSbom)
  }
  build {
    dependsOn(shadowJar, fullyShadowed, spdxSbom, publish, connectDist)
  }
}
