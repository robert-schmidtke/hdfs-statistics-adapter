/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
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
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.util.LogUtil;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class ClassTransformationServer extends
        ClassTransformationServiceGrpc.ClassTransformationServiceImplBase {

    private final Server server;

    private boolean isEndClassTransformations;
    private final Lock isEndClassTransformationsLock;
    private final Condition isEndClassTransformationsCondition;

    private final Map<String, ByteString> transformedClassesCache;

    private final boolean traceMmap;

    private final Set<OperationCategory> skip;

    public ClassTransformationServer(int port, boolean traceMmap,
            Set<OperationCategory> skip) {
        this.server = ServerBuilder.forPort(port).addService(this).build();

        this.isEndClassTransformations = false;
        this.isEndClassTransformationsLock = new ReentrantLock();
        this.isEndClassTransformationsCondition = this.isEndClassTransformationsLock
                .newCondition();

        this.transformedClassesCache = new ConcurrentHashMap<>();

        this.traceMmap = traceMmap;

        this.skip = skip;
    }

    @Override
    public void classTransformation(ClassTransformationRequest request,
            StreamObserver<ClassTransformationResponse> responseObserver) {
        String className = request.getName();

        LogUtil.stderr("Transforming class '%s'.\n", className);

        ByteString transformedClass = this.transformedClassesCache
                .get(className);
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
                                    request.getNativeMethodPrefix(), this.skip),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/io/FileOutputStream":
                    cr.accept(
                            new FileOutputStreamAdapter(cw,
                                    request.getNativeMethodPrefix(), this.skip),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/io/RandomAccessFile":
                    cr.accept(
                            new RandomAccessFileAdapter(cw,
                                    request.getNativeMethodPrefix(), this.skip),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/lang/Shutdown":
                    cr.accept(new ShutdownAdapter(cw),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/nio/DirectByteBuffer":
                case "java/nio/DirectByteBufferR":
                    cr.accept(new DirectByteBufferAdapter(cw,
                            request.getNativeMethodPrefix(), this.skip,
                            className), ClassReader.EXPAND_FRAMES);
                    break;
                case "java/nio/MappedByteBuffer":
                    cr.accept(new MappedByteBufferAdapter(cw, this.skip),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "java/util/zip/ZipFile":
                    cr.accept(
                            new ZipFileAdapter(cw,
                                    request.getNativeMethodPrefix(), this.skip),
                            ClassReader.EXPAND_FRAMES);
                    break;
                case "sun/nio/ch/FileChannelImpl":
                    cr.accept(
                            new FileChannelImplAdapter(cw,
                                    request.getNativeMethodPrefix(),
                                    this.traceMmap, this.skip),
                            ClassReader.EXPAND_FRAMES);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Invalid class: " + className);
                }

                transformedClass = ByteString.copyFrom(cw.toByteArray());
                this.transformedClassesCache.put(className, transformedClass);
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

        this.isEndClassTransformationsLock.lock();
        try {
            this.isEndClassTransformations = true;
            this.isEndClassTransformationsCondition.signal();
        } finally {
            this.isEndClassTransformationsLock.unlock();
        }
    }

    public void start() throws IOException {
        this.server.start();
    }

    public void shutdown() throws InterruptedException {
        this.server.shutdown().awaitTermination();
    }

    public boolean awaitEndClassTransformations(int timeoutSeconds)
            throws InterruptedException {
        this.isEndClassTransformationsLock.lock();
        try {
            while (!this.isEndClassTransformations) {
                if (timeoutSeconds > 0) {
                    if (!this.isEndClassTransformationsCondition
                            .await(timeoutSeconds, TimeUnit.SECONDS)) {
                        // time elapsed
                        break;
                    }
                } else {
                    this.isEndClassTransformationsCondition
                            .awaitUninterruptibly();
                }
            }
        } finally {
            this.isEndClassTransformationsLock.unlock();
        }

        // this is still false in case of a timeout
        return this.isEndClassTransformations;
    }

}
