/*
 * Copyright (c) 2025 James S. Clark
 *
 * This is private source code.  You may not use, copy, or distribute this file under any circumstances without written
 * permission from the copyright holder, depicted above. All rights reserved.
 */
package com.planetscale.debezium.mtls

import io.debezium.config.Configuration
import io.grpc.ChannelCredentials
import io.grpc.TlsChannelCredentials
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.util.Base64
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory
import kotlin.io.path.extension
import kotlin.io.path.inputStream

private const val JKS_EXTENSION = "jks"
private const val P12_EXTENSION = "p12"
private const val PFX_EXTENSION = "pfx"
private const val KEY_TYPE_JKS = "JKS"
private const val KEY_TYPE_P12 = "PKCS12"

// Retrieve a configuration string from a Debezium config, or return `null` if it is not present.
private fun Configuration.getStringSafe(name: String): String? = when (hasKey(name)) {
  true -> getString(name) as String
  else -> null
}

// Check a file before use, emitting a message about what kind of file it is and what happened if it fails checks.
private inline fun <R> withValidFile(path: Path, role: String, block: (Path) -> R): R {
  require(Files.exists(path)) { "$role must exist at path: '$path'" }
  require(Files.isRegularFile(path)) { "$role must be a file: '$path'" }
  require(Files.isReadable(path)) { "$role must be readable: '$path'" }
  return block.invoke(path)
}

// Utilities used internally by the Planetscale Connector for mTLS/TLS support.
internal object TlsUtils {
  const val TLS_CREDENTIAL_FILE: String = "planetscale.tls.certificate.file"
  const val TLS_CREDENTIAL_BASE64: String = "planetscale.tls.certificate.b64"
  const val TLS_CREDENTIAL_PASSWORD: String = "planetscale.tls.certificate.password"
  const val TLS_TRUST_FILE: String = "planetscale.tls.truststore.file"

  // Enumerates supported keystore types.
  internal enum class KeystoreType {
    JKS,
    P12,
    ;

    val code: String get() = when (this) {
      JKS -> KEY_TYPE_JKS
      P12 -> KEY_TYPE_P12
    }

    companion object {
      @JvmStatic fun fromPath(path: Path): KeystoreType = when (path.extension.trim().lowercase()) {
        JKS_EXTENSION -> JKS
        P12_EXTENSION, PFX_EXTENSION -> P12
        else -> error("Unsupported keystore type: ${path.extension} (at path: '$path')")
      }
    }
  }

  // Interface for password holders.
  internal sealed interface Password {
    val isPresent: Boolean
    fun consume(): CharArray
    fun consumeSafe(): CharArray? = when (isPresent) {
      true -> consume()
      else -> null
    }
  }

  // Sentinel object used when no password is available or provided.
  internal object NoPassword : Password {
    override val isPresent: Boolean get() = false
    override fun consume() = error("No password present")
  }

  // Mutable password holder which can be zeroed after consumption.
  private class PasswordHolder private constructor(
    private val password: CharArray,
  ) : Password {
    override val isPresent: Boolean get() = true

    override fun consume(): CharArray = password.copyOf().also {
      password.fill('\u0000')
    }

    companion object {
      @JvmStatic fun fromString(value: String): PasswordHolder = PasswordHolder(value.toCharArray())
    }
  }

  // Finalize the TLS credential builder.
  @Suppress("SpreadOperator")
  private fun TlsChannelCredentials.Builder.finalizeKeyManager(ks: KeyStore, pass: CharArray?): ChannelCredentials {
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
    kmf.init(ks, pass)
    keyManager(*kmf.keyManagers)
    return build()
  }

  // Obtain credentials (a certificate and private key) from a JKS (Java Key Store) file.
  private fun TlsChannelCredentials.Builder.getCredentialsFromJks(
    stream: InputStream,
    password: Password,
  ): ChannelCredentials = KeyStore.getInstance(KEY_TYPE_JKS).let { store ->
    finalizeKeyManager(
      store,
      password.consumeSafe().also { pass ->
        store.load(stream, pass)
      },
    )
  }

  // Obtain credentials (a certificate and private key) from a P12 (PKCS#12) file.
  private fun TlsChannelCredentials.Builder.getCredentialsFromP12(
    stream: InputStream,
    password: Password,
  ): ChannelCredentials = KeyStore.getInstance(KEY_TYPE_P12).let { store ->
    finalizeKeyManager(
      store,
      password.consumeSafe().also { pass ->
        store.load(stream, pass)
      },
    )
  }

  // Load trust store information.
  @Suppress("SpreadOperator")
  private fun TlsChannelCredentials.Builder.loadTrustStore(path: Path) = withValidFile(path, "TLS trust store") {
    path.inputStream().buffered().use { buf ->
      val type = KeystoreType.fromPath(path)
      val keystore = KeyStore.getInstance(type.code)
      keystore.load(buf, null)
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
      tmf.init(keystore)
      trustManager(*tmf.trustManagers)
      Unit
    }
  }

  // Create a password holder for the provided value.
  internal fun passwordOf(value: String?): Password = when (value == null || value.isEmpty()) {
    true -> NoPassword
    else -> PasswordHolder.fromString(value)
  }

  // Obtain credentials from the provided file stream.
  internal fun credentialsFromFile(
    builder: TlsChannelCredentials.Builder,
    type: KeystoreType,
    stream: InputStream,
    password: Password = NoPassword,
  ): ChannelCredentials = when (type) {
    KeystoreType.JKS -> builder.getCredentialsFromJks(stream, password)
    KeystoreType.P12 -> builder.getCredentialsFromP12(stream, password)
  }

  // Obtain credentials from the provided file path.
  internal fun credentialsFromFile(
    builder: TlsChannelCredentials.Builder,
    file: Path,
    password: Password = NoPassword,
  ): ChannelCredentials = withValidFile(file, "TLS credentials") {
    file.inputStream().buffered().use { buf ->
      credentialsFromFile(builder, KeystoreType.fromPath(file), buf, password)
    }
  }

  // Create a TLS channel credential from the provided configuration, throwing if none is present.
  fun tlsCredential(config: Configuration): ChannelCredentials? = with(TlsChannelCredentials.newBuilder()) {
    val certFile = config.getStringSafe(TLS_CREDENTIAL_FILE)
    val certB64 = config.getStringSafe(TLS_CREDENTIAL_BASE64)
    val tlsPassword = config.getStringSafe(TLS_CREDENTIAL_PASSWORD)

    // load custom trust store, if provided
    val trustStore = config.getStringSafe(TLS_TRUST_FILE)
    if (!trustStore.isNullOrBlank()) {
      loadTrustStore(Path.of(trustStore))
    }

    when {
      // with a base64-encoded key file (always p12), decode and load it; b64 wins if both are set
      certB64 != null -> Base64.getDecoder().decode(certB64).inputStream().use { stream ->
        getCredentialsFromP12(stream, passwordOf(tlsPassword))
      }

      // otherwise, if a cert file is present, load based on its extension
      certFile != null -> credentialsFromFile(this, Path.of(certFile), passwordOf(tlsPassword))

      // with no inputs, yield `null` without error
      else -> null
    }
  }

  // Create a TLS channel credential from the provided configuration, or return `null` if none is present.
  @Suppress("PrintStackTrace", "TooGenericExceptionCaught")
  fun tlsCredentialSafe(config: Configuration): ChannelCredentials? = try {
    tlsCredential(config)
  } catch (err: RuntimeException) {
    err.printStackTrace()
    null
  }
}
