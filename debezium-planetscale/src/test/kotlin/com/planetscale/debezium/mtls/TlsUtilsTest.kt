package com.planetscale.debezium.mtls

import com.planetscale.debezium.mtls.TlsUtils.KeystoreType
import com.planetscale.debezium.mtls.TlsUtils.NoPassword
import io.debezium.config.Configuration
import java.io.File
import java.nio.file.Path
import kotlin.io.path.absolutePathString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TlsUtilsTest {
  // -- Password tests --

  @Test
  fun `passwordOf with null returns NoPassword`() {
    val password = TlsUtils.passwordOf(null)
    assertFalse(password.isPresent)
    assertTrue(password is NoPassword)
  }

  @Test
  fun `passwordOf with empty string returns NoPassword`() {
    val password = TlsUtils.passwordOf("")
    assertFalse(password.isPresent)
  }

  @Test
  fun `passwordOf with value returns present Password`() {
    val password = TlsUtils.passwordOf("secret")
    assertTrue(password.isPresent)
  }

  @Test
  fun `password consume returns chars and marks consumed`() {
    val password = TlsUtils.passwordOf("hello")
    assertTrue(password.isPresent)
    val chars = password.consume()
    assertEquals("hello", String(chars))
    assertFalse(password.isPresent)
  }

  @Test
  fun `password consume zeroes original`() {
    val password = TlsUtils.passwordOf("test")
    password.consume()
    assertFalse(password.isPresent)
  }

  @Test
  fun `double consume returns null via consumeSafe`() {
    val password = TlsUtils.passwordOf("test")
    password.consume()
    assertNull(password.consumeSafe())
  }

  @Test
  fun `NoPassword consume throws`() {
    assertFailsWith<IllegalStateException> {
      NoPassword.consume()
    }
  }

  // -- KeystoreType tests --

  @Test
  fun `KeystoreType fromPath jks`() {
    assertEquals(KeystoreType.JKS, KeystoreType.fromPath(Path.of("test.jks")))
  }

  @Test
  fun `KeystoreType fromPath p12`() {
    assertEquals(KeystoreType.P12, KeystoreType.fromPath(Path.of("test.p12")))
  }

  @Test
  fun `KeystoreType fromPath pfx`() {
    assertEquals(KeystoreType.P12, KeystoreType.fromPath(Path.of("test.pfx")))
  }

  @Test
  fun `KeystoreType fromPath unknown throws`() {
    assertFailsWith<IllegalStateException> {
      KeystoreType.fromPath(Path.of("test.pem"))
    }
  }

  @Test
  fun `KeystoreType code returns correct values`() {
    assertEquals("JKS", KeystoreType.JKS.code)
    assertEquals("PKCS12", KeystoreType.P12.code)
  }

  // -- tlsCredential tests --

  @Test
  fun `tlsCredential with no config returns null`() {
    val config = Configuration.empty()
    val credential = TlsUtils.tlsCredential(config)
    assertNull(credential)
  }

  @Test
  fun `tlsCredential with JKS file loads credential`() {
    val jksFile = createTestKeystore("JKS", "jks")
    try {
      val config = Configuration.create()
        .with(TlsUtils.TLS_CREDENTIAL_FILE, jksFile.absolutePathString())
        .with(TlsUtils.TLS_CREDENTIAL_PASSWORD, KEYSTORE_PASSWORD)
        .build()
      val credential = TlsUtils.tlsCredential(config)
      assertNotNull(credential)
    } finally {
      jksFile.toFile().delete()
    }
  }

  @Test
  fun `tlsCredential with P12 file loads credential`() {
    val p12File = createTestKeystore("PKCS12", "p12")
    try {
      val config = Configuration.create()
        .with(TlsUtils.TLS_CREDENTIAL_FILE, p12File.absolutePathString())
        .with(TlsUtils.TLS_CREDENTIAL_PASSWORD, KEYSTORE_PASSWORD)
        .build()
      val credential = TlsUtils.tlsCredential(config)
      assertNotNull(credential)
    } finally {
      p12File.toFile().delete()
    }
  }

  @Test
  fun `tlsCredential with base64 P12 loads credential`() {
    val p12File = createTestKeystore("PKCS12", "p12")
    try {
      val bytes = p12File.toFile().readBytes()
      val b64 = java.util.Base64.getEncoder().encodeToString(bytes)
      val config = Configuration.create()
        .with(TlsUtils.TLS_CREDENTIAL_BASE64, b64)
        .with(TlsUtils.TLS_CREDENTIAL_PASSWORD, KEYSTORE_PASSWORD)
        .build()
      val credential = TlsUtils.tlsCredential(config)
      assertNotNull(credential)
    } finally {
      p12File.toFile().delete()
    }
  }

  @Test
  fun `tlsCredential with invalid file throws`() {
    val tempFile = File.createTempFile("invalid", ".p12")
    tempFile.writeText("not-a-keystore")
    try {
      val config = Configuration.create()
        .with(TlsUtils.TLS_CREDENTIAL_FILE, tempFile.absolutePath)
        .build()
      assertFailsWith<Exception> {
        TlsUtils.tlsCredential(config)
      }
    } finally {
      tempFile.delete()
    }
  }

  @Test
  fun `tlsCredentialSafe with invalid file returns null`() {
    val tempFile = File.createTempFile("invalid", ".p12")
    tempFile.writeText("not-a-keystore")
    try {
      val config = Configuration.create()
        .with(TlsUtils.TLS_CREDENTIAL_FILE, tempFile.absolutePath)
        .build()
      val credential = TlsUtils.tlsCredentialSafe(config)
      assertNull(credential)
    } finally {
      tempFile.delete()
    }
  }

  @Test
  fun `tlsCredential with nonexistent file throws`() {
    val config = Configuration.create()
      .with(TlsUtils.TLS_CREDENTIAL_FILE, "/nonexistent/path/cert.p12")
      .build()
    assertFailsWith<IllegalArgumentException> {
      TlsUtils.tlsCredential(config)
    }
  }

  @Test
  fun `tlsCredential with custom truststore loads both`() {
    val p12File = createTestKeystore("PKCS12", "p12")
    val trustFile = createTestKeystore("PKCS12", "p12")
    try {
      val config = Configuration.create()
        .with(TlsUtils.TLS_CREDENTIAL_FILE, p12File.absolutePathString())
        .with(TlsUtils.TLS_CREDENTIAL_PASSWORD, KEYSTORE_PASSWORD)
        .with(TlsUtils.TLS_TRUST_FILE, trustFile.absolutePathString())
        .build()
      val credential = TlsUtils.tlsCredential(config)
      assertNotNull(credential)
    } finally {
      p12File.toFile().delete()
      trustFile.toFile().delete()
    }
  }

  companion object {
    private const val KEYSTORE_PASSWORD = "testpass"

    private val keytoolPath: String by lazy {
      val javaHome = System.getProperty("java.home")
      val keytool = java.nio.file.Path.of(javaHome, "bin", "keytool")
      if (java.nio.file.Files.isExecutable(keytool)) keytool.toString() else "keytool"
    }

    /**
     * Generate a self-signed keystore using keytool (ships with JDK).
     */
    private fun createTestKeystore(type: String, extension: String): Path {
      val file = File.createTempFile("test-keystore", ".$extension")
      file.delete() // keytool wants to create the file itself

      val storeType = if (type == "JKS") "JKS" else "PKCS12"

      val process = ProcessBuilder(
        keytoolPath,
        "-genkeypair",
        "-keystore", file.absolutePath,
        "-storetype", storeType,
        "-storepass", KEYSTORE_PASSWORD,
        "-keypass", KEYSTORE_PASSWORD,
        "-alias", "test",
        "-keyalg", "RSA",
        "-keysize", "2048",
        "-dname", "CN=Test,O=Test,L=Test,C=US",
        "-validity", "365",
      ).redirectErrorStream(true).start()

      val exitCode = process.waitFor()
      require(exitCode == 0) {
        "keytool failed with exit code $exitCode: ${process.inputStream.bufferedReader().readText()}"
      }
      return file.toPath()
    }
  }
}
