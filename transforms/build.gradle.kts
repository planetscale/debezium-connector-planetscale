/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
plugins {
  alias(libs.plugins.kotlin.jvm)
}

group = "com.planetscale.labs"

dependencies {
  api(libs.bundles.vitess.client)
  api(libs.bundles.bytebuddy)
  api(debezium.core)
  api(debezium.embedded)
}
