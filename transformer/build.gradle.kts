/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
plugins {
  alias(libs.plugins.planetscale.debezium.build)
  `java-gradle-plugin`
}

dependencies {
  implementation(gradleApi())
  implementation(libs.planetscale.debezium.transforms)
}

val pluginId = "com.planetscale.debezium"
val pluginClass = "com.planetscale.codegen.PlanetscaleCodegenPlugin"

gradlePlugin {
  plugins {
    create(pluginId) {
      id = pluginId
      implementationClass = pluginClass
      description = "Wires codegen for the Planetscale Debezium connector"
      displayName = "Planetscale Debezium Codegen Plugin"
    }
  }
}
