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
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.util.*
import java.util.concurrent.TimeUnit
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory

private const val CONFIG_FIELD = "config"
private const val AUTHORIZATION_HEADER = "authorization"
private const val BASIC_AUTH = "Basic"

// Intercepts gRPC channel setup in order to add authorization headers and otherwise configure for use with Planetscale.
@Suppress("unused") internal object PlanetscaleManagedChannel : ClientInterceptor {
  private val authorizationHeader = Metadata.Key.of(AUTHORIZATION_HEADER, Metadata.ASCII_STRING_MARSHALLER)
  private val logger by lazy { LoggerFactory.getLogger(PlanetscaleManagedChannel::class.java) }

  // mTLS config keys (mirrors TlsUtils constants from debezium-planetscale module)
  private const val TLS_CREDENTIAL_FILE = "planetscale.tls.certificate.file"
  private const val TLS_CREDENTIAL_BASE64 = "planetscale.tls.certificate.b64"
  private const val TLS_CREDENTIAL_PASSWORD = "planetscale.tls.certificate.password"
  private const val TLS_TRUST_FILE = "planetscale.tls.truststore.file"

  @JvmStatic private lateinit var config: VitessConnectorConfig

  @JvmStatic fun newChannel(
    @FieldValue(CONFIG_FIELD) callerConfig: VitessConnectorConfig,
    host: String?,
    port: Int?,
    maxMessageSize: Int,
  ): ManagedChannel {
    this.config = callerConfig
    val resolvedHost = host ?: PlanetscaleConstants.HOST
    val resolvedPort = port ?: PlanetscaleConstants.PORT
    val rawConfig = callerConfig.config

    val channelCredentials = if (rawConfig.hasKey(TLS_CREDENTIAL_FILE) || rawConfig.hasKey(TLS_CREDENTIAL_BASE64)) {
      loadTlsCredentials(rawConfig)
    } else {
      null
    }

    val builder = if (channelCredentials != null) {
      Grpc.newChannelBuilderForAddress(resolvedHost, resolvedPort, channelCredentials)
    } else {
      ManagedChannelBuilder.forAddress(resolvedHost, resolvedPort).useTransportSecurity()
    }
    return builder
      .maxInboundMessageSize(maxMessageSize)
      .intercept(this)
      .keepAliveTime(config.keepaliveInterval.toMillis(), TimeUnit.MILLISECONDS)
      .build()
  }

  /** Load mTLS channel credentials from config (mirrors TlsUtils.tlsCredential logic). */
  @JvmStatic private fun loadTlsCredentials(config: io.debezium.config.Configuration): ChannelCredentials? {
    val certFile = config.getString(TLS_CREDENTIAL_FILE)
    val certB64 = config.getString(TLS_CREDENTIAL_BASE64)
    val password = config.getString(TLS_CREDENTIAL_PASSWORD)
    val trustFile = config.getString(TLS_TRUST_FILE)

    val tlsBuilder = TlsChannelCredentials.newBuilder()

    // Load custom trust store if provided
    if (!trustFile.isNullOrBlank() && (certB64 != null || certFile != null)) {
      val path = Path.of(trustFile)
      val ext = path.toString().substringAfterLast('.').lowercase()
      val storeType = if (ext == "jks") "JKS" else "PKCS12"
      val ks = KeyStore.getInstance(storeType)
      Files.newInputStream(path).buffered().use { ks.load(it, null) }
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
      tmf.init(ks)
      @Suppress("SpreadOperator")
      tlsBuilder.trustManager(*tmf.trustManagers)
    }

    // Load client certificate
    val pass = password?.toCharArray()
    return try {
      when {
        certB64 != null -> {
          val bytes = Base64.getDecoder().decode(certB64)
          val ks = KeyStore.getInstance("PKCS12")
          bytes.inputStream().use { ks.load(it, pass) }
          val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
          kmf.init(ks, pass)
          @Suppress("SpreadOperator")
          tlsBuilder.keyManager(*kmf.keyManagers)
          tlsBuilder.build()
        }
        certFile != null -> {
          val path = Path.of(certFile)
          val ext = path.toString().substringAfterLast('.').lowercase()
          val storeType = if (ext == "jks") "JKS" else "PKCS12"
          val ks = KeyStore.getInstance(storeType)
          Files.newInputStream(path).buffered().use { ks.load(it, pass) }
          val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
          kmf.init(ks, pass)
          @Suppress("SpreadOperator")
          tlsBuilder.keyManager(*kmf.keyManagers)
          tlsBuilder.build()
        }
        else -> null
      }
    } finally {
      pass?.fill('\u0000')
    }
  }

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
