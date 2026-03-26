package com.planetscale.debezium.mtls

import binlogdata.Binlogdata
import com.planetscale.debezium.grpc.MockVStreamServer
import com.planetscale.debezium.grpc.VStreamEvents
import io.debezium.config.Configuration
import io.debezium.connector.vitess.Vgtid
import io.grpc.Metadata
import io.grpc.StatusRuntimeException
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.MetadataUtils
import io.vitess.proto.Query
import io.vitess.proto.Vtgate
import io.vitess.proto.grpc.VitessGrpc
import java.nio.file.Files
import java.util.Base64
import kotlin.io.path.readBytes
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * End-to-end mTLS integration tests.
 *
 * Generates a CA + server/client certificates at test time, stands up a TLS-enabled
 * mock gRPC server requiring client auth (mTLS), and exercises the full credential
 * loading path through [TlsUtils.tlsCredential] → gRPC channel → mTLS handshake.
 */
class MtlsIntegrationTest {
  private lateinit var certs: TestCertificateAuthority.CertBundle
  private lateinit var mockServer: MockVStreamServer

  @BeforeTest
  fun setUp() {
    certs = TestCertificateAuthority.generate()
    mockServer = MockVStreamServer(certs.buildServerCredentials()).start()
  }

  @AfterTest
  fun tearDown() {
    mockServer.close()
    // Clean up temp cert directory
    Files.walk(certs.dir).sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
  }

  /** Build a Debezium [Configuration] with mTLS properties pointing to the test certs. */
  private fun mtlsConfig(
    certFile: String? = certs.clientKeystore.toAbsolutePath().toString(),
    certB64: String? = null,
    trustFile: String? = certs.trustStore.toAbsolutePath().toString(),
    password: String? = certs.password,
  ): Configuration {
    val builder = Configuration.create()
      .with("database.hostname", "localhost")
      .with("database.port", mockServer.port.toString())
      .with("database.user", "test-user")
      .with("database.password", "test-password")
      .with("vitess.keyspace", "test_ks")
      .with("vitess.cells", "cell1")
      .with("topic.prefix", "test")
      .with("snapshot.mode", "never")
    if (certFile != null) builder.with(TlsUtils.TLS_CREDENTIAL_FILE, certFile)
    if (certB64 != null) builder.with(TlsUtils.TLS_CREDENTIAL_BASE64, certB64)
    if (trustFile != null) builder.with(TlsUtils.TLS_TRUST_FILE, trustFile)
    if (password != null) builder.with(TlsUtils.TLS_CREDENTIAL_PASSWORD, password)
    return builder.build()
  }

  /** Create a gRPC channel using the same code path as [VitessReplicationConnection.newChannel]. */
  private fun createChannel(config: Configuration): io.grpc.ManagedChannel {
    val credentials = TlsUtils.tlsCredential(config)
      ?: error("Expected TLS credentials from config")

    val authHeader = run {
      val user = config.getString("database.user")
      val pass = config.getString("database.password")
      "Basic " + Base64.getEncoder().encodeToString("$user:$pass".toByteArray())
    }
    val headers = Metadata()
    headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), authHeader)

    return NettyChannelBuilder
      .forAddress("localhost", mockServer.port, credentials)
      .intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
      .build()
  }

  // -- Tests --

  @Test
  fun `mTLS with P12 file succeeds`() {
    val config = mtlsConfig()
    val channel = createChannel(config)
    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val response = stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())
      assertNotNull(response)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `mTLS with base64 P12 succeeds`() {
    val b64 = Base64.getEncoder().encodeToString(certs.clientKeystore.readBytes())
    val config = mtlsConfig(certFile = null, certB64 = b64)
    val channel = createChannel(config)
    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      val response = stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())
      assertNotNull(response)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `mTLS vstream receives events`() {
    val events = VStreamEvents.insertTransaction(
      keyspace = "test_ks",
      shard = "0",
      table = "users",
      gtid = "MySQL56/abc:1-10",
      fields = listOf("id" to Query.Type.INT32, "name" to Query.Type.VARCHAR),
      values = listOf("1", "Alice"),
    )
    mockServer.enqueueEvents(*events.toTypedArray())
    mockServer.enqueueComplete()

    val config = mtlsConfig()
    val channel = createChannel(config)
    try {
      val request = Vtgate.VStreamRequest.newBuilder()
        .setVgtid(
          Binlogdata.VGtid.newBuilder().addShardGtids(
            Binlogdata.ShardGtid.newBuilder()
              .setKeyspace("test_ks").setShard("0").setGtid(Vgtid.CURRENT_GTID)
          )
        )
        .build()

      val stub = VitessGrpc.newBlockingStub(channel)
      val responses = stub.vStream(request).asSequence().toList()

      assertEquals(1, responses.size)
      val eventTypes = responses[0].eventsList.map { it.type }
      assertTrue(Binlogdata.VEventType.VGTID in eventTypes)
      assertTrue(Binlogdata.VEventType.ROW in eventTypes)
      assertTrue(Binlogdata.VEventType.COMMIT in eventTypes)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `mTLS carries auth header in metadata`() {
    val config = mtlsConfig()
    val channel = createChannel(config)
    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())

      assertTrue(mockServer.receivedMetadata.isNotEmpty())
      val authValue = mockServer.receivedMetadata.first()
        .get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER))
      assertNotNull(authValue)
      assertTrue(authValue.startsWith("Basic "))

      val decoded = String(Base64.getDecoder().decode(authValue.removePrefix("Basic ")))
      assertEquals("test-user:test-password", decoded)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `connection without client cert is rejected`() {
    // Build TLS credentials with only a trust store (no client cert) — server requires client auth
    val creds = io.grpc.TlsChannelCredentials.newBuilder().apply {
      val trustKs = java.security.KeyStore.getInstance("PKCS12")
      trustKs.load(Files.newInputStream(certs.trustStore), certs.password.toCharArray())
      val tmf = javax.net.ssl.TrustManagerFactory.getInstance(
        javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm(),
      )
      tmf.init(trustKs)
      @Suppress("SpreadOperator")
      trustManager(*tmf.trustManagers)
    }.build()

    val channel = NettyChannelBuilder
      .forAddress("localhost", mockServer.port, creds)
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())
      fail("Expected connection to fail without client certificate")
    } catch (e: StatusRuntimeException) {
      // Server should reject — either UNAVAILABLE (handshake failure) or UNKNOWN
      assertTrue(
        e.status.code == io.grpc.Status.Code.UNAVAILABLE ||
          e.status.code == io.grpc.Status.Code.UNKNOWN,
        "Expected UNAVAILABLE or UNKNOWN, got ${e.status.code}",
      )
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `connection with wrong CA is rejected`() {
    // Generate a separate CA that the server doesn't trust
    val rogue = TestCertificateAuthority.generate()
    try {
      // Use rogue client cert with the real server's trust store
      val config = mtlsConfig(
        certFile = rogue.clientKeystore.toAbsolutePath().toString(),
        trustFile = certs.trustStore.toAbsolutePath().toString(),
        password = rogue.password,
      )
      val channel = createChannel(config)
      try {
        val stub = VitessGrpc.newBlockingStub(channel)
        stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())
        fail("Expected connection to fail with untrusted client cert")
      } catch (e: StatusRuntimeException) {
        assertTrue(
          e.status.code == io.grpc.Status.Code.UNAVAILABLE ||
            e.status.code == io.grpc.Status.Code.UNKNOWN,
          "Expected UNAVAILABLE or UNKNOWN, got ${e.status.code}",
        )
      } finally {
        channel.shutdownNow()
      }
    } finally {
      Files.walk(rogue.dir).sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
    }
  }
}
