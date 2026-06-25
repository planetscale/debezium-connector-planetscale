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
    // no-op: the upstream adapter attempts to connect to vitess to pre-flight the connection config, but at this stage,
    // we don't have config fully loaded, and/or grpc interceptors, so this call is guaranteed to fail for hosted grpc
    // endpoints like planetscale.
    //
    // instead, the connection config itself is validated, and then the connection simply succeeds or fails.

    // super.validateConnection(configValues, config)
  }
}
