/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
@file:Suppress("unused")

val allProjects = listOf(
  "debezium-planetscale",
  "transformer",
  "transforms",
)

fun Task.doTaskForAllProjects(name: String? = this.name) {
  dependsOn(
    allProjects.map {
      gradle.includedBuild(it).task(":$name")
    }
  )
}

tasks {
  val clean by registering { doTaskForAllProjects() }
  val build by registering { doTaskForAllProjects() }
  val test by registering { doTaskForAllProjects() }
  val check by registering { doTaskForAllProjects() }
  val publish by registering { dependsOn(gradle.includedBuild("debezium-planetscale").task(":publish")) }
}
