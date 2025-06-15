/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.conventions

import com.planetscale.PlanetscaleBuild
import com.planetscale.PlanetscaleBuild.debeziumVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.dsl.LockMode
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.jvm.tasks.Jar
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JvmVendorSpec
import org.gradle.kotlin.dsl.findByType
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmExtension

// Dependencies which don't share a version with main Debezium.
private val unalignedDeps = sortedSetOf(
  "mysql-binlog-connector-java",
)

// Configurations which should be locked and verified.
private val meaningfulConfigurations = listOfNotNull(
  "runtimeClasspath",
  "compileClasspath",
)

@Suppress("unused") // used at build time
class PlanetscaleConventionsPlugin : Plugin<Project> {
  companion object {
    // Debezium version property to set.
    private const val DEBEZIUM_VERSION_PROPERTY = "debeziumVersion"

    // Java toolchain configuration.
    @JvmStatic private val toolchainVersion = JavaLanguageVersion.of(21)
    @JvmStatic private val toolchainVendor = JvmVendorSpec.AZUL
  }

  // Cached access to the active project.
  private lateinit var project: Project

  // Version of Debezium to build upon, and our own version.
  val debeziumVersion: String by lazy {
    project.findProperty(DEBEZIUM_VERSION_PROPERTY) as? String ?: project.debeziumVersion()
  }

  override fun apply(target: Project) {
    project = target

    // use consistent project coordinates and versioning
    project.group = PlanetscaleBuild.PACKAGE_GROUP
    project.version = debeziumVersion

    // tune jar tasks
    project.afterEvaluate {
      tasks.withType(Jar::class.java).configureEach {
        isReproducibleFileOrder = true
        isPreserveFileTimestamps = false
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
      resolutionStrategy.eachDependency {
        if (requested.group == "io.debezium" && requested.name !in unalignedDeps) {
          useVersion(debeziumVersion)
          because("Pinned upstream version of Debezium")
        }
      }
    }

    // use a consistent java toolchain
    target.extensions.findByType<JavaPluginExtension>()?.apply {
      toolchain {
        languageVersion.set(toolchainVersion)
        vendor.set(toolchainVendor)
      }
    }

    // use consistent kotlin toolchain configuration
    target.extensions.findByType<KotlinJvmExtension>()?.apply {
      jvmToolchain {
        languageVersion.set(toolchainVersion)
        vendor.set(toolchainVendor)
      }
      compilerOptions {
        jvmTarget.set(JvmTarget.fromTarget(toolchainVersion.asInt().toString()))
      }
    }
  }
}
