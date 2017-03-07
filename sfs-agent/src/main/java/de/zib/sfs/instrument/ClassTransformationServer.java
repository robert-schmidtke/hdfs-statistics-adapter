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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

import com.google.protobuf.ByteString;

import de.zib.sfs.instrument.rpc.ClassTransformationServiceGrpc;
import de.zib.sfs.instrument.rpc.Sfs.ClassTransformationRequest;
import de.zib.sfs.instrument.rpc.Sfs.ClassTransformationResponse;
import de.zib.sfs.instrument.rpc.Sfs.EndClassTransformationsRequest;
import de.zib.sfs.instrument.rpc.Sfs.EndClassTransformationsResponse;

public class ClassTransformationServer extends
        ClassTransformationServiceGrpc.ClassTransformationServiceImplBase {

    private final Server server;

    private boolean isEndClassTransformations;
    private final Lock isEndClassTransformationsLock;
    private final Condition isEndClassTransformationsCondition;

    private final Map<String, ByteString> transformedClassesCache;

    public ClassTransformationServer(int port) {
        server = ServerBuilder.forPort(port).addService(this).build();

        isEndClassTransformations = false;
        isEndClassTransformationsLock = new ReentrantLock();
        isEndClassTransformationsCondition = isEndClassTransformationsLock
                .newCondition();

        transformedClassesCache = new ConcurrentHashMap<>();
    }

    @Override
    public void classTransformation(ClassTransformationRequest request,
            StreamObserver<ClassTransformationResponse> responseObserver) {
        String className = request.getName();
        ByteString transformedClass = transformedClassesCache.get(className);
        if (transformedClass == null) {
            ClassReader cr = new ClassReader(
                    request.getBytecode().toByteArray());
            ClassWriter cw = new ClassWriter(
                    ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);

            try {
                switch (className) {
                case "java/io/FileInputStream":
                    cr.accept(
                            new FileInputStreamAdapter(cw,
                                    request.getNativeMethodPrefix()),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/io/FileOutputStream":
                    cr.accept(
                            new FileOutputStreamAdapter(cw,
                                    request.getNativeMethodPrefix()),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/io/RandomAccessFile":
                    cr.accept(
                            new RandomAccessFileAdapter(cw,
                                    request.getNativeMethodPrefix()),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/lang/Shutdown":
                    cr.accept(new ShutdownAdapter(cw),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/nio/DirectByteBuffer":
                    cr.accept(
                            new DirectByteBufferAdapter(cw,
                                    request.getNativeMethodPrefix()),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/nio/MappedByteBuffer":
                    cr.accept(new MappedByteBufferAdapter(cw),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "sun/nio/ch/FileChannelImpl":
                    cr.accept(
                            new FileChannelImplAdapter(cw,
                                    request.getNativeMethodPrefix()),
                            ClassReader.EXPAND_FRAMES);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Invalid class: " + className);
                }

                transformedClass = ByteString.copyFrom(cw.toByteArray());
                transformedClassesCache.put(className, transformedClass);
            } catch (Exception e) {
                System.err.println("Error during class transformation:");
                e.printStackTrace();
            }
        }

        responseObserver.onNext(ClassTransformationResponse.newBuilder()
                .setBytecode(transformedClass).build());
        responseObserver.onCompleted();
    }

    @Override
    public void endClassTransformations(EndClassTransformationsRequest request,
            StreamObserver<EndClassTransformationsResponse> responseObserver) {
        responseObserver
                .onNext(EndClassTransformationsResponse.newBuilder().build());
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
                if (timeoutSeconds > 0) {
                    if (!isEndClassTransformationsCondition
                            .await(timeoutSeconds, TimeUnit.SECONDS)) {
                        // time elapsed
                        break;
                    }
                } else {
                    isEndClassTransformationsCondition.awaitUninterruptibly();
                }
            }
        } finally {
            isEndClassTransformationsLock.unlock();
        }

        // this is still false in case of a timeout
        return isEndClassTransformations;
    }

}
