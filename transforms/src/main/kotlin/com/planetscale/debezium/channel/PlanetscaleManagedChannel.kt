/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium.channel

import com.planetscale.debezium.PlanetscaleConstants
import io.debezium.annotation.VisibleForTesting
import io.debezium.connector.vitess.VitessConnectorConfig
import io.grpc.*
import net.bytebuddy.implementation.bind.annotation.FieldValue
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.TimeUnit

// Constants used for channel initialization and configuration.
private const val CONFIG_FIELD = "config"
private const val AUTHORIZATION_HEADER = "authorization"
private const val BASIC_AUTH = "Basic"

// Intercepts gRPC channel setup in order to add authorization headers and otherwise configure for use with Planetscale.
@Suppress("unused") internal object PlanetscaleManagedChannel : ClientInterceptor {
  private val authorizationHeader = Metadata.Key.of(AUTHORIZATION_HEADER, Metadata.ASCII_STRING_MARSHALLER)
  private val logger by lazy { LoggerFactory.getLogger(PlanetscaleManagedChannel::class.java) }

  @JvmStatic private lateinit var config: VitessConnectorConfig

  @JvmStatic private fun managedChannelBuilder(host: String?, port: Int?, config: VitessConnectorConfig) =
    ManagedChannelBuilder.forAddress(
      host ?: PlanetscaleConstants.HOST,
      port ?: PlanetscaleConstants.PORT,
    ).also {
      // mount configuration for adapter (we use it later for authorization)
      this.config = config
    }

  @JvmStatic fun newChannel(
    @FieldValue(CONFIG_FIELD) config: VitessConnectorConfig,
    host: String?,
    port: Int?,
    maxMessageSize: Int,
  ): ManagedChannel = managedChannelBuilder(host, port, config)
    .useTransportSecurity()
    .maxInboundMessageSize(maxMessageSize)
    .intercept(this)
    .keepAliveTime(config.keepaliveInterval.toMillis(), TimeUnit.MILLISECONDS)
    .build()

  override fun <ReqT : Any?, RespT : Any?> interceptCall(
    method: MethodDescriptor<ReqT?, RespT?>,
    callOptions: CallOptions,
    next: Channel,
  ): ClientCall<ReqT?, RespT?> = object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
    next.newCall(method, callOptions),
  ) {
    override fun start(responseListener: Listener<RespT>, headers: Metadata) {
      val user = config.vtgateUsername
      val passAvailable = config.vtgatePassword?.isNotEmpty() == true

      if (user.isNullOrBlank() || !passAvailable) {
        logger.warn("No credentials resolvable for Vitess adapter; Planetscale connection may fail")
        super.start(responseListener, headers)
      } else {
        PlanetscaleAuth.authorizationHeader(user, config.vtgatePassword.toCharArray()).let { header ->
          headers.put(authorizationHeader, header)
          super.start(responseListener, headers)
        }
      }
    }
  }
}

// Implements authorization functions related to Planetscale.
@VisibleForTesting internal object PlanetscaleAuth {
  /**
   * Build an HTTP Basic `Authorization` header value from the given [user] and [password].
   *
   * **Note:** The [password] provided is zeroed out immediately after use to prevent leakage. This is a mutating
   * operation which destroys the contents of the array.
   *
   * @param user Username to use.
   * @param password Password to use, as a mutable character array.
   * @return String header value that should be used, **including** the `Basic ` prefix.
   */
  @JvmStatic fun authorizationHeader(user: String, password: CharArray): String = buildString {
    append(user)
    append(':')
    append(password)
  }.let { preimage ->
    buildString {
      append(BASIC_AUTH)
      append(' ')
      append(Base64.getEncoder().encodeToString(preimage.toByteArray(StandardCharsets.UTF_8)))
    }
  }.also {
    // force-zero password immediately after use
    password.fill('\u0000')
  }
}
