package com.planetscale.debezium.grpc

import io.grpc.Metadata
import io.vitess.proto.Vtgate
import io.vitess.proto.grpc.VitessGrpc
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests that verify auth headers reach the gRPC server correctly.
 * Uses MockVStreamServer to capture metadata from incoming calls.
 */
class GrpcAuthInterceptorTest {
  private lateinit var mockServer: MockVStreamServer

  @BeforeTest
  fun setUp() {
    mockServer = MockVStreamServer().start()
  }

  @AfterTest
  fun tearDown() {
    mockServer.close()
  }

  @Test
  fun `authorization header is attached to gRPC calls`() {
    mockServer.enqueueComplete()

    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .intercept(authInterceptor("testuser", "testpass"))
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())

      assertTrue(mockServer.receivedMetadata.isNotEmpty())
      val metadata = mockServer.receivedMetadata.first()
      val authValue = metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER))
      assertNotNull(authValue)
      assertTrue(authValue.startsWith("Basic "))
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `authorization header is valid Basic auth`() {
    mockServer.enqueueComplete()

    val user = "myuser"
    val pass = "mypassword"
    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .intercept(authInterceptor(user, pass))
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())

      val metadata = mockServer.receivedMetadata.first()
      val authValue = metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER))!!
      val encoded = authValue.removePrefix("Basic ")
      val decoded = String(java.util.Base64.getDecoder().decode(encoded))
      assertEquals("$user:$pass", decoded)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `auth header contains correct base64 encoding`() {
    mockServer.enqueueComplete()

    val user = "user@example.com"
    val pass = "p@ss:word!"
    val expectedEncoded = java.util.Base64.getEncoder().encodeToString("$user:$pass".toByteArray())

    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .intercept(authInterceptor(user, pass))
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())

      val metadata = mockServer.receivedMetadata.first()
      val authValue = metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER))!!
      assertEquals("Basic $expectedEncoded", authValue)
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun `multiple calls each receive auth header`() {
    // Each execute call gets its own response
    val channel = io.grpc.ManagedChannelBuilder
      .forAddress("localhost", mockServer.port)
      .usePlaintext()
      .intercept(authInterceptor("user", "pass"))
      .build()

    try {
      val stub = VitessGrpc.newBlockingStub(channel)
      stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())
      stub.execute(Vtgate.ExecuteRequest.getDefaultInstance())

      assertEquals(2, mockServer.receivedMetadata.size)
      mockServer.receivedMetadata.forEach { metadata ->
        val authValue = metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER))
        assertNotNull(authValue)
        assertTrue(authValue.startsWith("Basic "))
      }
    } finally {
      channel.shutdownNow()
    }
  }

  /**
   * Creates a simple ClientInterceptor that adds Basic auth, similar to what the connector does.
   */
  private fun authInterceptor(user: String, pass: String): io.grpc.ClientInterceptor {
    return object : io.grpc.ClientInterceptor {
      override fun <ReqT, RespT> interceptCall(
        method: io.grpc.MethodDescriptor<ReqT, RespT>,
        callOptions: io.grpc.CallOptions,
        next: io.grpc.Channel,
      ): io.grpc.ClientCall<ReqT, RespT> {
        return object : io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions),
        ) {
          override fun start(responseListener: Listener<RespT>, headers: Metadata) {
            val encoded = java.util.Base64.getEncoder().encodeToString("$user:$pass".toByteArray())
            headers.put(
              Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
              "Basic $encoded",
            )
            super.start(responseListener, headers)
          }
        }
      }
    }
  }
}
