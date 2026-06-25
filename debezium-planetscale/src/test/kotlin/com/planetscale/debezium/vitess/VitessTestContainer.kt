package com.planetscale.debezium.vitess

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration

/**
 * Testcontainers wrapper for `vitess/vttestserver:mysql80`.
 *
 * Provides a single-shard Vitess cluster with both MySQL (for data loading)
 * and gRPC (for VStream) access.
 */
class VitessTestContainer(
  private val keyspace: String = DEFAULT_KEYSPACE,
) : GenericContainer<VitessTestContainer>(
  DockerImageName.parse(IMAGE),
) {
  val mysqlPort: Int get() = getMappedPort(MYSQL_PORT)
  val grpcPort: Int get() = getMappedPort(GRPC_PORT)
  val jdbcUrl: String get() = "jdbc:mysql://${host}:${mysqlPort}/${keyspace}"

  init {
    withEnv("PORT", BASE_PORT.toString())
    withEnv("KEYSPACES", keyspace)
    withEnv("NUM_SHARDS", "1")
    withEnv("MYSQL_MAX_CONNECTIONS", "100")
    withEnv("MYSQL_BIND_HOST", "0.0.0.0")
    withExposedPorts(MYSQL_PORT, GRPC_PORT)
    waitingFor(
      Wait.forLogMessage(".*Local cluster started.*", 1)
        .withStartupTimeout(Duration.ofSeconds(120)),
    )
  }

  /** Open a JDBC connection to the Vitess MySQL port. */
  fun jdbcConnection(): Connection =
    DriverManager.getConnection(jdbcUrl, "root", "")

  /**
   * Load a SQL file from the classpath into the Vitess instance.
   * Parses line-by-line, accumulating multi-line statements terminated by `;`.
   */
  fun loadSqlResource(resourcePath: String) {
    val lines = javaClass.classLoader.getResourceAsStream(resourcePath)
      ?.bufferedReader()?.readLines()
      ?: error("Resource not found: $resourcePath")

    jdbcConnection().use { conn ->
      conn.createStatement().use { stmt ->
        val buf = StringBuilder()
        for (line in lines) {
          val trimmed = line.trim()
          if (trimmed.isEmpty() || trimmed.startsWith("--")) continue
          buf.append(line)
          if (trimmed.endsWith(";")) {
            val sql = buf.toString().trim().removeSuffix(";").trim()
            buf.clear()
            if (sql.isNotBlank()) {
              stmt.execute(sql)
            }
          } else {
            buf.append('\n')
          }
        }
      }
    }
  }

  companion object {
    const val IMAGE = "vitess/vttestserver:mysql80"
    const val DEFAULT_KEYSPACE = "maxeng_etl"
    const val BASE_PORT = 33574
    const val GRPC_PORT = 33575  // BASE + 1
    const val MYSQL_PORT = 33577 // BASE + 3
  }
}
