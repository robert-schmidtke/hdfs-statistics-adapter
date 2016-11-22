/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.util.concurrent.TimeUnit;

import de.zib.sfs.agent.rpc.proto.ClassTransformationServiceGrpc;
import de.zib.sfs.agent.rpc.proto.Sfs.BeginClassTransformationsRequest;
import de.zib.sfs.agent.rpc.proto.Sfs.BeginClassTransformationsResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ClassTransformationClient {

    private final ManagedChannel channel;

    private final ClassTransformationServiceGrpc.ClassTransformationServiceBlockingStub stub;

    public ClassTransformationClient(int port) {
        this(ManagedChannelBuilder.forAddress("0.0.0.0", port).usePlaintext(
                true));
    }

    private ClassTransformationClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        stub = ClassTransformationServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void beginClassTransformations() {
        BeginClassTransformationsRequest request = BeginClassTransformationsRequest
                .newBuilder().build();
        BeginClassTransformationsResponse response = stub
                .beginClassTransformations(request);
    }

}
