/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium

import kotlinx.coroutines.test.runTest
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import kotlin.test.BeforeTest
import org.testcontainers.utility.DockerImageName.parse as containerImage

// Integrates testing with Testcontainers and a running instance of Vitess Lite. Starts and stops Vitess with each test
// case on a given class; use `VitessSharedIntegrationTest` to share the Vitess instance across multiple cases.
abstract class VitessIntegrationTest {
  private companion object {
    private const val VITESS_LITE_DIGEST = "sha256:0d97735a1ccc297138aac90cf3f8919c3028b4392ec7d3bf0ca6163ecc02cd9d"
    private const val VITESS_LITE = "vitess/lite:mysql84@$VITESS_LITE_DIGEST"
    private val vitessPorts = arrayOf(
      3306, // mysql
      15001, // vtgate http
      15999, // vtgate grpc
    )
  }

  @Container private val container = GenericContainer(containerImage(VITESS_LITE))
    .withExposedPorts(*vitessPorts)

  protected lateinit var adapter: PlanetscaleConnector

  // Access the connector within a test.
  protected fun connector(): PlanetscaleConnector = adapter

  // Access the Vitess container within a test.
  protected fun container(): GenericContainer<*> = container

  @BeforeTest fun setUp() {
    adapter = PlanetscaleConnector()
  }

  protected inline fun <reified T> withVitess(block: PlanetscaleConnector.() -> T): T {
    adapter.start(buildMap { configureConnector() })
    return block.invoke(adapter)
  }

  protected fun <T> testVitess(block: suspend PlanetscaleConnector.() -> T) = runTest {
    adapter.start(buildMap { configureConnector() })
    block.invoke(adapter)
  }

  protected fun MutableMap<String, String?>.defaultConfiguration() {
    // default configuration
  }

  open fun MutableMap<String, String?>.configureConnector() {
    defaultConfiguration()
  }
}
