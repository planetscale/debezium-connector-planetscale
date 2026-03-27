package com.planetscale.debezium.mtls

import io.grpc.ServerCredentials
import io.grpc.TlsServerCredentials
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory

/**
 * Generates a self-signed CA and signed server/client certificates for mTLS testing.
 * All artifacts are P12 keystores created via `keytool` (bundled with the JDK).
 */
object TestCertificateAuthority {
  private const val PASSWORD = "changeit"
  private const val KEY_ALG = "RSA"
  private const val KEY_SIZE = "2048"
  private const val STORE_TYPE = "PKCS12"
  private const val VALIDITY = "1" // 1 day — enough for tests

  data class CertBundle(
    val dir: Path,
    val caKeystore: Path,
    val serverKeystore: Path,
    val clientKeystore: Path,
    val trustStore: Path,
    val password: String = PASSWORD,
  ) {
    /** Build gRPC [ServerCredentials] that require client auth, using the server keystore and CA trust store. */
    fun buildServerCredentials(): ServerCredentials {
      val serverKs = KeyStore.getInstance(STORE_TYPE)
      serverKs.load(Files.newInputStream(serverKeystore), password.toCharArray())
      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
      kmf.init(serverKs, password.toCharArray())

      val trustStoreType = if (trustStore.toString().endsWith(".jks")) "JKS" else STORE_TYPE
      val trustKs = KeyStore.getInstance(trustStoreType)
      trustKs.load(Files.newInputStream(trustStore), password.toCharArray())
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
      tmf.init(trustKs)

      @Suppress("SpreadOperator")
      return TlsServerCredentials.newBuilder()
        .keyManager(*kmf.keyManagers)
        .trustManager(*tmf.trustManagers)
        .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
        .build()
    }
  }

  /** Generate all mTLS artifacts in a new temporary directory. */
  fun generate(): CertBundle {
    val dir = Files.createTempDirectory("mtls-test-")
    val caP12 = dir.resolve("ca.p12")
    val caPem = dir.resolve("ca.pem")
    val serverP12 = dir.resolve("server.p12")
    val serverCsr = dir.resolve("server.csr")
    val serverSignedPem = dir.resolve("server-signed.pem")
    val clientP12 = dir.resolve("client.p12")
    val clientCsr = dir.resolve("client.csr")
    val clientSignedPem = dir.resolve("client-signed.pem")
    val trustStoreJks = dir.resolve("truststore.jks")

    // 1. Generate CA key pair with self-signed cert
    keytool(
      "-genkeypair", "-alias", "ca",
      "-keystore", caP12.str(), "-storetype", STORE_TYPE,
      "-storepass", PASSWORD, "-keypass", PASSWORD,
      "-keyalg", KEY_ALG, "-keysize", KEY_SIZE,
      "-dname", "CN=Test CA,O=Test",
      "-ext", "bc:c",
      "-validity", VALIDITY,
    )

    // 2. Export CA cert to PEM
    keytool(
      "-exportcert", "-alias", "ca",
      "-keystore", caP12.str(), "-storepass", PASSWORD,
      "-rfc", "-file", caPem.str(),
    )

    // 3. Generate server key pair
    keytool(
      "-genkeypair", "-alias", "server",
      "-keystore", serverP12.str(), "-storetype", STORE_TYPE,
      "-storepass", PASSWORD, "-keypass", PASSWORD,
      "-keyalg", KEY_ALG, "-keysize", KEY_SIZE,
      "-dname", "CN=localhost,O=Test",
      "-validity", VALIDITY,
    )

    // 4. Create server CSR
    keytool(
      "-certreq", "-alias", "server",
      "-keystore", serverP12.str(), "-storepass", PASSWORD,
      "-file", serverCsr.str(),
    )

    // 5. Sign server CSR with CA (add SAN for localhost)
    keytool(
      "-gencert", "-alias", "ca",
      "-keystore", caP12.str(), "-storepass", PASSWORD,
      "-infile", serverCsr.str(), "-outfile", serverSignedPem.str(),
      "-ext", "san=dns:localhost,ip:127.0.0.1",
      "-rfc", "-validity", VALIDITY,
    )

    // 6. Import CA cert into server keystore, then the signed server cert
    keytool(
      "-importcert", "-alias", "ca",
      "-keystore", serverP12.str(), "-storepass", PASSWORD,
      "-file", caPem.str(), "-noprompt",
    )
    keytool(
      "-importcert", "-alias", "server",
      "-keystore", serverP12.str(), "-storepass", PASSWORD,
      "-file", serverSignedPem.str(),
    )

    // 7. Generate client key pair
    keytool(
      "-genkeypair", "-alias", "client",
      "-keystore", clientP12.str(), "-storetype", STORE_TYPE,
      "-storepass", PASSWORD, "-keypass", PASSWORD,
      "-keyalg", KEY_ALG, "-keysize", KEY_SIZE,
      "-dname", "CN=Test Client,O=Test",
      "-validity", VALIDITY,
    )

    // 8. Create client CSR
    keytool(
      "-certreq", "-alias", "client",
      "-keystore", clientP12.str(), "-storepass", PASSWORD,
      "-file", clientCsr.str(),
    )

    // 9. Sign client CSR with CA
    keytool(
      "-gencert", "-alias", "ca",
      "-keystore", caP12.str(), "-storepass", PASSWORD,
      "-infile", clientCsr.str(), "-outfile", clientSignedPem.str(),
      "-rfc", "-validity", VALIDITY,
    )

    // 10. Import CA cert into client keystore, then the signed client cert
    keytool(
      "-importcert", "-alias", "ca",
      "-keystore", clientP12.str(), "-storepass", PASSWORD,
      "-file", caPem.str(), "-noprompt",
    )
    keytool(
      "-importcert", "-alias", "client",
      "-keystore", clientP12.str(), "-storepass", PASSWORD,
      "-file", clientSignedPem.str(),
    )

    // 11. Create trust store containing only the CA cert (JKS format so TlsUtils.loadTrustStore
    // can load it with a null password — PKCS12 requires a password to read cert entries)
    keytool(
      "-importcert", "-alias", "ca",
      "-keystore", trustStoreJks.str(), "-storetype", "JKS",
      "-storepass", PASSWORD,
      "-file", caPem.str(), "-noprompt",
    )

    return CertBundle(
      dir = dir,
      caKeystore = caP12,
      serverKeystore = serverP12,
      clientKeystore = clientP12,
      trustStore = trustStoreJks,
    )
  }

  private fun Path.str(): String = toAbsolutePath().toString()

  private val keytoolPath: String by lazy {
    val javaHome = System.getProperty("java.home")
    val keytool = java.nio.file.Path.of(javaHome, "bin", "keytool")
    if (java.nio.file.Files.isExecutable(keytool)) keytool.toString() else "keytool"
  }

  private fun keytool(vararg args: String) {
    val process = ProcessBuilder(keytoolPath, *args)
      .redirectErrorStream(true)
      .start()
    val output = process.inputStream.bufferedReader().readText()
    val exitCode = process.waitFor()
    check(exitCode == 0) { "keytool failed (exit $exitCode): $output\nargs: ${args.joinToString(" ")}" }
  }
}
