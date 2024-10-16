package io.debezium.connector.vitess.connection;

import java.util.Base64;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;

/**
 * Interceptor to set the BASIC: authorization header for a gRPC call to VTGate service
 */
public class BasicAuthenticationInterceptor implements ClientInterceptor {

    private final String headerValue;

    public BasicAuthenticationInterceptor(String user, String pass) {
        String base64EncodedCredentials = Base64.getEncoder().encodeToString((user + ":" + pass).getBytes());
        headerValue = "Basic " + base64EncodedCredentials;
    }

    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                                                               final Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {

                headers.put(Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), headerValue);
                super.start(responseListener, headers);
            }
        };
    }
}
