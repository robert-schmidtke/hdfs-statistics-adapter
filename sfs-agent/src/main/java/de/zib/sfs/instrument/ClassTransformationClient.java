/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.util.concurrent.TimeUnit;

import de.zib.sfs.instrument.rpc.ClassTransformationServiceGrpc;
import de.zib.sfs.instrument.rpc.Sfs.BeginClassTransformationsRequest;
import de.zib.sfs.instrument.rpc.Sfs.BeginClassTransformationsResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ClassTransformationClient {

    private final ManagedChannel channel;

    private final ClassTransformationServiceGrpc.ClassTransformationServiceBlockingStub stub;

    public ClassTransformationClient(int port) {
        this(ManagedChannelBuilder.forAddress("0.0.0.0", port)
                .usePlaintext(true));
    }

    private ClassTransformationClient(ManagedChannelBuilder<?> channelBuilder) {
        this.channel = channelBuilder.build();
        this.stub = ClassTransformationServiceGrpc
                .newBlockingStub(this.channel);
    }

    public void shutdown() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void beginClassTransformations(int port) {
        BeginClassTransformationsRequest request = BeginClassTransformationsRequest
                .newBuilder().setPort(port).build();
        BeginClassTransformationsResponse response = this.stub
                .beginClassTransformations(request);
    }

}
