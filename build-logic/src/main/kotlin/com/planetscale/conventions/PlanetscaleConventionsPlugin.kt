/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("UnstableApiUsage")

package com.planetscale.conventions

import com.adarshr.gradle.testlogger.TestLoggerExtension
import com.adarshr.gradle.testlogger.theme.ThemeType
import com.diffplug.gradle.spotless.BaseKotlinExtension
import com.diffplug.gradle.spotless.SpotlessExtension
import com.planetscale.PlanetscaleBuild
import com.planetscale.PlanetscaleBuild.debeziumVersion
import kotlinx.atomicfu.plugin.gradle.AtomicFUPluginExtension
import kotlinx.kover.gradle.plugin.dsl.KoverProjectExtension
import org.gradle.accessors.dm.LibrariesForDebezium
import org.gradle.accessors.dm.LibrariesForLibs
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.dsl.LockMode
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.jvm.tasks.Jar
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JvmVendorSpec
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.exclude
import org.gradle.kotlin.dsl.findByType
import org.gradle.kotlin.dsl.repositories
import org.gradle.kotlin.dsl.the
import org.gradle.kotlin.dsl.withType
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmExtension
import java.nio.file.Path
import kotlin.io.path.name
import kotlin.io.path.readText

// Runtime JVM target.
private const val JVM_TARGET = "17"
private const val JVM_TOOLCHAIN = "24"
private const val JVM_TOOLCHAIN_VENDOR = "Oracle"

// Minimum line coverage percentage to enforce.
private const val MINIMUM_COVERAGE = 1

// Kotlin features.
private const val ENABLE_ATOMICFU = true

// Dependencies which don't share a version with main Debezium.
private val unalignedDeps = sortedSetOf(
  "mysql-binlog-connector-java",
)

// Configurations which should be locked and verified.
private val meaningfulConfigurations = listOfNotNull(
  "compileClasspath",
  "runtimeClasspath",
)

// Plugins applied to all builds.
private val stockPlugins = listOf(
  "com.adarshr.test-logger",
  "com.diffplug.spotless",
  "kotlinx-atomicfu",
  "org.jetbrains.kotlin.jvm",
  "org.jetbrains.kotlin.plugin.atomicfu",
)

// Projects for which coverage is ignored.
private val coverageIgnoredProjects = sortedSetOf(
  "transformer",
)

// Projects which are not in the true-root.
private val leafProjects = sortedSetOf(
  "debezium-planetscale",
  "transformer",
  "transforms",
)

// Rules to disable from ktlint.
private val disabledLinterRules = listOf(
  "standard" to "annotation",
  "standard" to "no-wildcard-imports",
  "standard" to "no-trailing-comma",
  "standard" to "multiline-expression-wrapping",
  "standard" to "discouraged-comment-location",
)

@Suppress("unused") // used at build time
class PlanetscaleConventionsPlugin : Plugin<Project> {
  companion object {
    // Debezium version property to set.
    private const val DEBEZIUM_VERSION_PROPERTY = "debeziumVersion"
  }

  // Cached access to the active project.
  private lateinit var project: Project

  // Access to version catalogs.
  private val libs by lazy { project.the<LibrariesForLibs>() }
  private val debezium by lazy { project.the<LibrariesForDebezium>() }

  // Version of Debezium to build upon, and our own version.
  val debeziumVersion: String by lazy {
    project.findProperty(DEBEZIUM_VERSION_PROPERTY) as? String ?: project.debeziumVersion()
  }

  // Pinned gRPC version to use.
  val grpcVersion: String by lazy {
    libs.versions.grpc.get()
  }

  // Pinned Netty version to use.
  val nettyVersion: String by lazy {
    libs.versions.netty.get()
  }

  // Pinned TCNative version to use.
  val tcnativeVersion: String by lazy {
    libs.versions.tcnative.get()
  }

  private fun renderProjectVersion(debezium: String): String = buildString {
    // `3.2.1.Final`
    append(debezium)
    // `3.2.1.Final-`
    append('-')
    // `r1`
    val connectorVersion = project.trueProjectRoot().resolve(".version").readText().lines().first {
      !it.startsWith("#") && !it.isBlank()
    }
    append(connectorVersion)
  }

  override fun apply(target: Project) {
    project = target

    // configure stock plugins
    stockPlugins.forEach { pluginId -> project.pluginManager.apply(pluginId) }

    // enable coverage if not special-cased
    if (project.name !in coverageIgnoredProjects) {
      project.pluginManager.apply("org.jetbrains.kotlinx.kover")
    }

    // use consistent project coordinates and versioning
    project.group = PlanetscaleBuild.PACKAGE_GROUP
    project.version = renderProjectVersion(debeziumVersion)

    // project repositories
    project.repositories {
      mavenCentral()
    }

    // configure extensions
    project.the<TestLoggerExtension>().apply { configureTestLogger() }
    project.the<SpotlessExtension>().apply { configureSpotless(project, libs) }
    project.extensions.findByType<KoverProjectExtension>()?.apply { configureKover(project) }

    // disable kover verification
    project.tasks.findByName("koverVerify")?.apply {
      enabled = false
      onlyIf { false }
    }

    // configure kotlin and java
    if (project.pluginManager.hasPlugin("java")) {
      project.the<JavaPluginExtension>().apply {
        sourceCompatibility = JavaVersion.toVersion(JVM_TARGET)
        targetCompatibility = JavaVersion.toVersion(JVM_TARGET)

        toolchain {
          vendor.set(JvmVendorSpec.ORACLE)
          languageVersion.set(JavaLanguageVersion.of(JVM_TOOLCHAIN))
          nativeImageCapable.set(true)
        }
      }
    }
    if (project.pluginManager.hasPlugin("org.jetbrains.kotlin.jvm")) {
      project.the<KotlinJvmExtension>().apply {
        compilerOptions {
          jvmTarget.set(JvmTarget.fromTarget(JVM_TARGET))

          jvmToolchain {
            vendor.set(JvmVendorSpec.of(JVM_TOOLCHAIN_VENDOR))
            languageVersion.set(JavaLanguageVersion.of(JVM_TOOLCHAIN))
          }
        }
      }
    }

    // tune jar tasks
    project.afterEvaluate {
      // atomicfu
      if (ENABLE_ATOMICFU) project.pluginManager.withPlugin("kotlinx-atomicfu") {
        project.extensions.findByType<AtomicFUPluginExtension>()?.apply { configureAtomicFu(libs) }
      }

      tasks.withType(Jar::class.java).configureEach {
        isReproducibleFileOrder = true
        isPreserveFileTimestamps = false
      }
    }

    project.tasks.register("resolveAndLockAll") {
      notCompatibleWithConfigurationCache("filters configurations at execution time")

      doFirst {
        require(project.gradle.startParameter.isWriteDependencyLocks) {
          "$path must be run from the command line with the `--write-locks` flag"
        }
      }
      doLast {
        project.configurations.filter {
          // Add any custom filtering on the configurations to be resolved
          it.isCanBeResolved
        }.forEach { it.resolve() }
      }
    }

    // lock compile and runtime configurations
    project.afterEvaluate {
      if (System.getenv("CI") == "true" && project.findProperty("planetscale.release") != "true") {
        // don't lock configurations in non-release builds, as this can cause issues with
        // the dependency upgrade cycle.
        return@afterEvaluate
      }
      meaningfulConfigurations.forEach {
        project
          .configurations
          .findByName(it)
          ?.resolutionStrategy {
            // activate use of lock-files
            activateDependencyLocking()

            dependencyLocking {
              lockMode.set(LockMode.LENIENT)
            }
          }
      }
    }

    // use a consistent version of debezium throughout
    target.configurations.all {
      // not supported at `1.56.x`, remove when supported in upstream Debezium version
      exclude(group = "io.grpc", module = "grpc-inprocess")

      resolutionStrategy.eachDependency {
        if (requested.group == "io.debezium" && requested.name !in unalignedDeps) {
          useVersion(debeziumVersion)
          because("Pinned upstream version of Debezium")
        }
        if (requested.group == "io.grpc") {
          useVersion(grpcVersion)
          because("Pinned for compatibility to Debezium's effective version")
        }
        if (requested.group == "io.netty") {
          if ("tcnative" in requested.module.name) {
            useVersion(tcnativeVersion)
          } else {
            useVersion(nettyVersion)
          }
          because("Pinned for compatibility to Debezium's effective version")
        }
      }
    }
  }
}

private fun Project.trueProjectRoot(): Path {
  val root = rootProject.layout.projectDirectory.asFile.toPath()
  return if (root.name in leafProjects) root.parent else root
}

private fun AtomicFUPluginExtension.configureAtomicFu(libs: LibrariesForLibs) {
  dependenciesVersion = libs.versions.kotlinx.atomicfu.get()
  transformJvm = true
  jvmVariant = "VH"
}

private fun KoverProjectExtension.configureKover(project: Project) {
  currentProject {
    instrumentation { excludedClasses.add("io.debezium*") }
  }
  reports {
    verify {
      rule { minBound(MINIMUM_COVERAGE) }
    }
  }
  project.tasks.named("check").configure {
    dependsOn("koverXmlReport")
    dependsOn("koverHtmlReport")
    dependsOn("koverBinaryReport")
  }
}

private fun SpotlessExtension.configureSpotless(project: Project, libs: LibrariesForLibs) {
  isEnforceCheck = false

  fun BaseKotlinExtension.KtlintConfig.baselines() {
    val root = project.trueProjectRoot()
    setEditorConfigPath(root.resolve(".editorconfig").toFile())

    // disable rules which are disabled
    editorConfigOverride(
      disabledLinterRules.associate { (ruleSet, rule) ->
        "ktlint_${ruleSet}_$rule" to "disabled"
      }
    )
  }

  kotlin {
    ktlint(libs.versions.ktlint.get()).apply { baselines() }
  }
  kotlinGradle {
    ktlint(libs.versions.ktlint.get()).apply { baselines() }
  }
}

private fun TestLoggerExtension.configureTestLogger() {
  theme = ThemeType.MOCHA_PARALLEL
  showPassed = true
  showSkipped = true
  showFailed = true
  showStandardStreams = false
}
