/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium

import io.debezium.config.Configuration
import io.debezium.connector.vitess.VitessConnector
import org.apache.kafka.common.config.ConfigValue

public class PlanetscaleConnector : VitessConnector() {
  override fun validateConnection(configValues: Map<String?, ConfigValue?>?, config: Configuration?) {
    // super.validateConnection(configValues, config)
    // nerfed for use by planetscale
  }
}
