/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.conventions

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JvmVendorSpec
import org.gradle.kotlin.dsl.the
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmExtension

@Suppress("unused") // used at build time
class PlanetscaleConventionsPlugin : Plugin<Project> {
  // Java toolchain configuration.
  private val toolchainVersion = JavaLanguageVersion.of(23)
  private val toolchainVendor = JvmVendorSpec.AZUL

  override fun apply(target: Project) {
    // use a consistent java toolchain
    target.the<JavaPluginExtension>().apply {
      toolchain {
        languageVersion.set(toolchainVersion)
        vendor.set(toolchainVendor)
      }
    }

    // use consistent kotlin toolchain configuration
    target.the<KotlinJvmExtension>().apply {
      jvmToolchain {
        languageVersion.set(toolchainVersion)
        vendor.set(toolchainVendor)
      }
      compilerOptions {
        freeCompilerArgs.set(listOf("-no-stdlib"))
        jvmTarget.set(JvmTarget.fromTarget(toolchainVersion.asInt().toString()))
      }
    }
  }
}
