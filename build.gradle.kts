val allProjects = listOf(
  "debezium-planetscale",
  "facade",
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
}
