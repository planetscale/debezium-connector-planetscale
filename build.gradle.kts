/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("unused")

// Multi-module project coordination - no more composite build dependencies

subprojects {
  // Apply common configurations to all subprojects
  group = "com.planetscale.debezium"
  
  repositories {
    mavenCentral()
    google()
  }
  
  // Unified dependency resolution - locking configured per module as needed
}

// Root project tasks that coordinate subproject activities
tasks {
  val clean by registering {
    description = "Clean all subprojects"
    dependsOn(subprojects.map { it.tasks.named("clean") })
  }
  
  val assemble by registering {
    description = "Assemble all subprojects"
    dependsOn(subprojects.map { it.tasks.named("assemble") })
  }
  
  val build by registering {
    description = "Build all subprojects"
    dependsOn(subprojects.map { it.tasks.named("build") })
  }
  
  val test by registering {
    description = "Test all subprojects"
    dependsOn(subprojects.map { it.tasks.named("test") })
  }
  
  val check by registering {
    description = "Check all subprojects"
    dependsOn(subprojects.map { it.tasks.named("check") })
  }
  
  val detekt by registering {
    description = "Run detekt on all subprojects"
    dependsOn(subprojects.mapNotNull { project ->
      project.tasks.findByName("detekt")
    })
  }
  
  val spotlessCheck by registering {
    description = "Check code formatting on all subprojects"
    dependsOn(subprojects.mapNotNull { project ->
      project.tasks.findByName("spotlessCheck")
    })
  }
  
  val spotlessApply by registering {
    description = "Apply code formatting on all subprojects"
    dependsOn(subprojects.mapNotNull { project ->
      project.tasks.findByName("spotlessApply")
    })
  }
  
  val publish by registering {
    description = "Publish main connector artifacts"
    dependsOn(":debezium-planetscale:publish")
  }
  
  val resolveAndLockAll by registering {
    description = "Resolve and lock all dependencies for all subprojects"
    dependsOn(subprojects.map { project ->
      project.tasks.named("dependencies")
    })
    doLast {
      // Generate unified lock files
      subprojects.forEach { project ->
        project.dependencyLocking.lockAllConfigurations()
      }
    }
  }
}