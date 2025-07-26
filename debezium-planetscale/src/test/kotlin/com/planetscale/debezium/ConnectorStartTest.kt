/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium

import kotlin.test.*

class ConnectorStartTest : VitessIntegrationTest() {
  @Test fun adapterIsConstructable() {
    assertNotNull(container())
    assertNotNull(connector())
  }
}
