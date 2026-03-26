package com.planetscale.debezium.grpc

import binlogdata.Binlogdata.VEvent
import io.grpc.Metadata
import io.grpc.Server
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerCredentials
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import io.vitess.proto.Vtgate
import io.vitess.proto.grpc.VitessGrpc
import java.io.Closeable
import java.net.ServerSocket
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A configurable mock gRPC server implementing the VStream service for testing.
 */
class MockVStreamServer(
  private val serverCredentials: ServerCredentials? = null,
) : Closeable {
  val port: Int = findAvailablePort()
  val receivedRequests: CopyOnWriteArrayList<Vtgate.VStreamRequest> = CopyOnWriteArrayList()
  val receivedMetadata: CopyOnWriteArrayList<Metadata> = CopyOnWriteArrayList()

  private val responseQueue: ConcurrentLinkedQueue<QueuedAction> = ConcurrentLinkedQueue()

  private val metadataInterceptor = object : ServerInterceptor {
    override fun <ReqT, RespT> interceptCall(
      call: ServerCall<ReqT, RespT>,
      headers: Metadata,
      next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
      receivedMetadata.add(headers)
      return next.startCall(call, headers)
    }
  }

  private val vitessImpl = object : VitessGrpc.VitessImplBase() {
    override fun vStream(
      request: Vtgate.VStreamRequest,
      responseObserver: StreamObserver<Vtgate.VStreamResponse>,
    ) {
      receivedRequests.add(request)

      while (true) {
        val action = responseQueue.poll() ?: break
        when (action) {
          is QueuedAction.Response -> responseObserver.onNext(action.response)
          is QueuedAction.Error -> {
            responseObserver.onError(action.status.asRuntimeException())
            return
          }
          is QueuedAction.Complete -> {
            responseObserver.onCompleted()
            return
          }
        }
      }
      responseObserver.onCompleted()
    }

    override fun execute(
      request: Vtgate.ExecuteRequest,
      responseObserver: StreamObserver<Vtgate.ExecuteResponse>,
    ) {
      responseObserver.onNext(Vtgate.ExecuteResponse.getDefaultInstance())
      responseObserver.onCompleted()
    }
  }

  private val server: Server = run {
    val builder = if (serverCredentials != null) {
      NettyServerBuilder.forPort(port, serverCredentials)
    } else {
      NettyServerBuilder.forPort(port)
    }
    builder.addService(vitessImpl).intercept(metadataInterceptor).build()
  }

  fun enqueueEvents(vararg events: VEvent) {
    val response = Vtgate.VStreamResponse.newBuilder()
      .addAllEvents(events.toList())
      .build()
    responseQueue.add(QueuedAction.Response(response))
  }

  fun enqueueResponse(response: Vtgate.VStreamResponse) {
    responseQueue.add(QueuedAction.Response(response))
  }

  fun enqueueError(status: Status) {
    responseQueue.add(QueuedAction.Error(status))
  }

  fun enqueueComplete() {
    responseQueue.add(QueuedAction.Complete)
  }

  fun start(): MockVStreamServer {
    server.start()
    return this
  }

  override fun close() {
    server.shutdownNow()
    server.awaitTermination()
  }

  fun reset() {
    receivedRequests.clear()
    receivedMetadata.clear()
    responseQueue.clear()
  }

  private sealed interface QueuedAction {
    data class Response(val response: Vtgate.VStreamResponse) : QueuedAction
    data class Error(val status: Status) : QueuedAction
    data object Complete : QueuedAction
  }

  companion object {
    private fun findAvailablePort(): Int = ServerSocket(0).use { it.localPort }
  }
}
