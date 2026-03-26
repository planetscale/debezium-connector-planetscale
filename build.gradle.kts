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
  val apiCheck by registering { doTaskForAllProjects() }
  val apiDump by registering { doTaskForAllProjects() }
  val assemble by registering { doTaskForAllProjects() }
  val build by registering { doTaskForAllProjects() }
  val check by registering { doTaskForAllProjects() }
  val clean by registering { doTaskForAllProjects() }
  val publish by registering { dependsOn(gradle.includedBuild("debezium-planetscale").task(":publish")) }
  val resolveAndLockAll by registering { doTaskForAllProjects() }
  val spotlessApply by registering { doTaskForAllProjects() }
  val spotlessCheck by registering { doTaskForAllProjects() }
  val spotlessKotlinApply by registering { doTaskForAllProjects() }
  val spotlessKotlinCheck by registering { doTaskForAllProjects() }
  val spotlessKotlinGradleApply by registering { doTaskForAllProjects() }
  val spotlessKotlinGradleCheck by registering { doTaskForAllProjects() }
  val test by registering { doTaskForAllProjects() }
}
