/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium.channel

import kotlin.test.*

class PlanetscaleAuthTest {
  @Test fun buildAuthHeader() {
    val passSample = "password"
    val passChars = passSample.toCharArray()
    val header = assertNotNull(PlanetscaleAuth.authorizationHeader("user", passChars))
    assertTrue(header.startsWith("Basic "))
    assertFalse(passSample in header)
    for (i in passChars.indices) {
      assertEquals(0.toChar(), passChars[i])
    }
  }
}
