/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium

import java.util.ServiceLoader
import kotlin.test.*

class KafkaSpiTest {
  @Test fun `spi - connector responds to CloudEventsProvider`() {
    val impls = ServiceLoader.load(io.debezium.converters.spi.CloudEventsProvider::class.java).toList()
    assertNotNull(impls)
    assertTrue(impls.isNotEmpty())
  }

  @Test fun `spi - connector responds to SourceConnector`() {
    val impls = ServiceLoader.load(org.apache.kafka.connect.source.SourceConnector::class.java).toList()
    assertNotNull(impls)
    assertTrue(impls.isNotEmpty())
  }
}
