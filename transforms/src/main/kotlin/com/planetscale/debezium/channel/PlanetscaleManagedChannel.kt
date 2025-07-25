/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium.channel

import com.planetscale.debezium.PlanetscaleConstants
import io.debezium.connector.vitess.VitessConnectorConfig
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit

@Suppress("unused") internal object PlanetscaleManagedChannel {
  @JvmStatic private lateinit var config: VitessConnectorConfig

  @JvmStatic private fun managedChannelBuilder() = ManagedChannelBuilder.forAddress(
    PlanetscaleConstants.HOST,
    PlanetscaleConstants.PORT,
  )

  @JvmStatic fun newChannel(host: String, port: Int, maxMessageSize: Int): ManagedChannel =
    managedChannelBuilder()
      .useTransportSecurity()
      .maxInboundMessageSize(maxMessageSize)
      .keepAliveTime(config.keepaliveInterval.toMillis(), TimeUnit.MILLISECONDS)
      .build()
}
