/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

import com.google.protobuf.ByteString;

import de.zib.sfs.agent.rpc.proto.ClassTransformationServiceGrpc;
import de.zib.sfs.agent.rpc.proto.Sfs.ClassTransformationRequest;
import de.zib.sfs.agent.rpc.proto.Sfs.ClassTransformationResponse;
import de.zib.sfs.agent.rpc.proto.Sfs.EndClassTransformationsRequest;
import de.zib.sfs.agent.rpc.proto.Sfs.EndClassTransformationsResponse;

public class ClassTransformationServer extends
        ClassTransformationServiceGrpc.ClassTransformationServiceImplBase {

    private final Server server;

    private boolean isEndClassTransformations;
    private final Lock isEndClassTransformationsLock;
    private final Condition isEndClassTransformationsCondition;

    public ClassTransformationServer(int port) {
        server = ServerBuilder.forPort(port).addService(this).build();

        isEndClassTransformations = false;
        isEndClassTransformationsLock = new ReentrantLock();
        isEndClassTransformationsCondition = isEndClassTransformationsLock
                .newCondition();
    }

    @Override
    public void classTransformation(ClassTransformationRequest request,
            StreamObserver<ClassTransformationResponse> responseObserver) {
        ClassReader cr = new ClassReader(request.getBytecode().toByteArray());
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS
                | ClassWriter.COMPUTE_FRAMES);

        try {
            switch (request.getName()) {
            case "java/io/FileInputStream":
                cr.accept(
                        new FileInputStreamAdapter(cw, request
                                .getNativeMethodPrefix()), 0);
                break;
            case "java/io/FileOutputStream":
                cr.accept(
                        new FileOutputStreamAdapter(cw, request
                                .getNativeMethodPrefix()), 0);
                break;
            case "sun/nio/ch/FileChannelImpl":
                // cr.accept(new FileChannelImplAdapter(cw), 0);
                cr.accept(cw, 0);
                break;
            default:
                cr.accept(cw, 0);
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        responseObserver.onNext(ClassTransformationResponse.newBuilder()
                .setBytecode(ByteString.copyFrom(cw.toByteArray())).build());
        responseObserver.onCompleted();
    }

    @Override
    public void endClassTransformations(EndClassTransformationsRequest request,
            StreamObserver<EndClassTransformationsResponse> responseObserver) {
        responseObserver.onNext(EndClassTransformationsResponse.newBuilder()
                .build());
        responseObserver.onCompleted();

        isEndClassTransformationsLock.lock();
        try {
            isEndClassTransformations = true;
            isEndClassTransformationsCondition.signal();
        } finally {
            isEndClassTransformationsLock.unlock();
        }
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown() throws InterruptedException {
        server.shutdown().awaitTermination();
    }

    public boolean awaitEndClassTransformations(int timeoutSeconds)
            throws InterruptedException {
        isEndClassTransformationsLock.lock();
        try {
            while (!isEndClassTransformations) {
                if (!isEndClassTransformationsCondition.await(timeoutSeconds,
                        TimeUnit.SECONDS)) {
                    // time elapsed
                    break;
                }
            }
        } finally {
            isEndClassTransformationsLock.unlock();
        }

        // this is still false in case of a timeout
        return isEndClassTransformations;
    }

}
