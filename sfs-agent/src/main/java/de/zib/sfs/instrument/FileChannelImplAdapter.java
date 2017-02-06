/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import sun.nio.ch.FileChannelImpl;

/**
 * Class adapter that instruments {@link sun.nio.ch.FileChannelImpl}.
 * 
 * @author robert
 *
 */
@SuppressWarnings("restriction")
public class FileChannelImplAdapter extends ClassVisitor {

    private final String methodPrefix;

    /**
     * Construct a visitor that modifies an {@link sun.nio.ch.FileChannelImpl}'s
     * read and write calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     * @param methodPrefix
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public FileChannelImplAdapter(ClassVisitor cv, String methodPrefix)
            throws NoSuchMethodException, SecurityException {
        super(Opcodes.ASM5, cv);
        this.methodPrefix = methodPrefix;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if (isReadMethod(access, name, desc, signature, exceptions)
                || isWriteMethod(access, name, desc, signature, exceptions)) {
            // rename native methods so we can wrap them
            mv = cv.visitMethod(access, methodPrefix + name, desc, signature,
                    exceptions);
        } else {
            // simply copy the old method
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        }
        return mv;
    }

    @Override
    public void visitEnd() {
        // general descriptors needed to add methods to FileChannelImpl
        String fileChannelImplInternalName = Type
                .getInternalName(FileChannelImpl.class);
        String fileChannelImplCallbackInternalName = Type
                .getInternalName(FileChannelImplCallback.class);

        // descriptors of the methods we add to FileChannelImpl
        String getCallbackMethodDescriptor = Type.getMethodDescriptor(Type
                .getType(FileChannelImplCallback.class));
        String[] methodDescriptors = new String[] {
                Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(ByteBuffer.class)),
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(ByteBuffer[].class), Type.INT_TYPE,
                        Type.INT_TYPE) };
        MethodVisitor[] readMVs = new MethodVisitor[2];
        MethodVisitor[] writeMVs = new MethodVisitor[2];

        // private FileChannelImplCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE,
                "callback", Type.getDescriptor(FileChannelImplCallback.class),
                null, null);
        callbackFV.visitEnd();

        // private FileChannelImplCallback getCallback() {
        MethodVisitor getCallbackMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "getCallback", getCallbackMethodDescriptor, null, null);
        getCallbackMV.visitCode();

        // if (callback == null) {
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                fileChannelImplInternalName, "callback",
                Type.getDescriptor(FileChannelImplCallback.class));
        Label callbackNonNullLabel = new Label();
        getCallbackMV.visitJumpInsn(Opcodes.IFNONNULL, callbackNonNullLabel);

        // callback = new FileChannelImplCallback(this, parent);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitTypeInsn(Opcodes.NEW,
                fileChannelImplCallbackInternalName);
        getCallbackMV.visitInsn(Opcodes.DUP);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                fileChannelImplInternalName, "parent",
                Type.getDescriptor(Object.class));
        try {
            getCallbackMV
                    .visitMethodInsn(
                            Opcodes.INVOKESPECIAL,
                            Type.getInternalName(FileChannelImplCallback.class),
                            "<init>",
                            Type.getConstructorDescriptor(FileChannelImplCallback.class
                                    .getConstructor(FileChannelImpl.class,
                                            Object.class)), false);
        } catch (Exception e) {
            throw new RuntimeException("Could not access constructor", e);
        }
        getCallbackMV.visitFieldInsn(Opcodes.PUTFIELD,
                fileChannelImplInternalName, "callback",
                Type.getDescriptor(FileChannelImplCallback.class));

        // }
        getCallbackMV.visitLabel(callbackNonNullLabel);

        // return callback;
        // }
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                fileChannelImplInternalName, "callback",
                Type.getDescriptor(FileChannelImplCallback.class));
        getCallbackMV.visitInsn(Opcodes.ARETURN);
        getCallbackMV.visitMaxs(0, 0);
        getCallbackMV.visitEnd();

        // public int read(ByteBuffer dst) {
        readMVs[0] = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                methodDescriptors[0], null,
                new String[] { Type.getInternalName(IOException.class) });
        readMVs[0].visitCode();

        // FileChannelImplCallback cb = getCallback();
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[0].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        readMVs[0].visitVarInsn(Opcodes.ASTORE, 2);

        // long startTime = cb.onReadBegin(dst);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 2);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[0].visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName,
                "onReadBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(ByteBuffer.class)), false);
        readMVs[0].visitVarInsn(Opcodes.LSTORE, 3);

        // int readResult = methodPrefixread(dst);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[0].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "read",
                methodDescriptors[0], false);
        readMVs[0].visitVarInsn(Opcodes.ISTORE, 5);

        // cb.onReadEnd(startTime, readResult, dst);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 2);
        readMVs[0].visitVarInsn(Opcodes.LLOAD, 3);
        readMVs[0].visitVarInsn(Opcodes.ILOAD, 5);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[0].visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName, "onReadEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE, Type.getType(ByteBuffer.class)),
                false);

        // return readResult;
        // }
        readMVs[0].visitVarInsn(Opcodes.ILOAD, 5);
        readMVs[0].visitInsn(Opcodes.IRETURN);
        readMVs[0].visitMaxs(0, 0);
        readMVs[0].visitEnd();

        // public int read(ByteBuffer dsts, int offset, int length) {
        readMVs[1] = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                methodDescriptors[1], null,
                new String[] { Type.getInternalName(IOException.class) });
        readMVs[1].visitCode();

        // FileChannelImplCallback cb = getCallback();
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[1].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        readMVs[1].visitVarInsn(Opcodes.ASTORE, 4);

        // long startTime = cb.onReadBegin(dsts, offset, length);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 4);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        readMVs[1].visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName, "onReadBegin", Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class),
                                Type.INT_TYPE, Type.INT_TYPE), false);
        readMVs[1].visitVarInsn(Opcodes.LSTORE, 5);

        // long readResult = methodPrefixread(dsts, offset, length);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        readMVs[1].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "read",
                methodDescriptors[1], false);
        readMVs[1].visitVarInsn(Opcodes.LSTORE, 7);

        // cb.onReadEnd(startTime, readResult, dsts, offset, length);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 4);
        readMVs[1].visitVarInsn(Opcodes.LLOAD, 5);
        readMVs[1].visitVarInsn(Opcodes.LLOAD, 7);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        readMVs[1].visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName, "onReadEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class),
                                Type.INT_TYPE, Type.INT_TYPE), false);

        // return readResult;
        // }
        readMVs[1].visitVarInsn(Opcodes.LLOAD, 7);
        readMVs[1].visitInsn(Opcodes.LRETURN);
        readMVs[1].visitMaxs(0, 0);
        readMVs[1].visitEnd();

        // public int write(ByteBuffer src) {
        writeMVs[0] = cv.visitMethod(Opcodes.ACC_PUBLIC, "write",
                methodDescriptors[0], null,
                new String[] { Type.getInternalName(IOException.class) });
        writeMVs[0].visitCode();

        // FileChannelImplCallback cb = getCallback();
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[0].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        writeMVs[0].visitVarInsn(Opcodes.ASTORE, 2);

        // long startTime = cb.onWriteBegin(src);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 2);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[0].visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName,
                "onWriteBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(ByteBuffer.class)), false);
        writeMVs[0].visitVarInsn(Opcodes.LSTORE, 3);

        // int writeResult = methodPrefixwrite(src);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[0].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "write",
                methodDescriptors[0], false);
        writeMVs[0].visitVarInsn(Opcodes.ISTORE, 5);

        // cb.onWriteEnd(startTime, writeResult, src);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 2);
        writeMVs[0].visitVarInsn(Opcodes.LLOAD, 3);
        writeMVs[0].visitVarInsn(Opcodes.ILOAD, 5);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[0].visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName, "onWriteEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE, Type.getType(ByteBuffer.class)),
                false);

        // return writeResult;
        // }
        writeMVs[0].visitVarInsn(Opcodes.ILOAD, 5);
        writeMVs[0].visitInsn(Opcodes.IRETURN);
        writeMVs[0].visitMaxs(0, 0);
        writeMVs[0].visitEnd();

        // public int write(ByteBuffer srcs, int offset, int length) {
        writeMVs[1] = cv.visitMethod(Opcodes.ACC_PUBLIC, "write",
                methodDescriptors[1], null,
                new String[] { Type.getInternalName(IOException.class) });
        writeMVs[1].visitCode();

        // FileChannelImplCallback cb = getCallback();
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[1].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        writeMVs[1].visitVarInsn(Opcodes.ASTORE, 4);

        // long startTime = cb.onWriteBegin(srcs, offset, length);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 4);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        writeMVs[1].visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName, "onWriteBegin", Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class),
                                Type.INT_TYPE, Type.INT_TYPE), false);
        writeMVs[1].visitVarInsn(Opcodes.LSTORE, 5);

        // long writeResult = methodPrefixwrite(srcs, offset, length);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        writeMVs[1].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "write",
                methodDescriptors[1], false);
        writeMVs[1].visitVarInsn(Opcodes.LSTORE, 7);

        // cb.onWriteEnd(startTime, writeResult, srcs, offset, length);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 4);
        writeMVs[1].visitVarInsn(Opcodes.LLOAD, 5);
        writeMVs[1].visitVarInsn(Opcodes.LLOAD, 7);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        writeMVs[1].visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName, "onWriteEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class),
                                Type.INT_TYPE, Type.INT_TYPE), false);

        // return writeResult;
        // }
        writeMVs[1].visitVarInsn(Opcodes.LLOAD, 7);
        writeMVs[1].visitInsn(Opcodes.LRETURN);
        writeMVs[1].visitMaxs(0, 0);
        writeMVs[1].visitEnd();

        cv.visitEnd();
    }

    // Helper methods

    private boolean isReadMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC
                && "read".equals(name)
                && (Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(ByteBuffer.class)).equals(desc) || Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class),
                                Type.INT_TYPE, Type.INT_TYPE).equals(desc))
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isWriteMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC
                && "write".equals(name)
                && (Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(ByteBuffer.class)).equals(desc) || Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class),
                                Type.INT_TYPE, Type.INT_TYPE).equals(desc))
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }
}
