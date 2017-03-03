/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

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

    private final String systemInternalName, currentTimeMillisDescriptor;

    private final String fileChannelImplInternalName;

    private final String fileChannelImplCallbackInternalName,
            fileChannelImplCallbackDescriptor;

    /**
     * Construct a visitor that modifies an {@link sun.nio.ch.FileChannelImpl}'s
     * read and write calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     * @param nativeMethodPrefix
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public FileChannelImplAdapter(ClassVisitor cv, String nativeMethodPrefix)
            throws NoSuchMethodException, SecurityException {
        super(Opcodes.ASM5, cv);
        this.methodPrefix = nativeMethodPrefix;

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        fileChannelImplInternalName = Type
                .getInternalName(FileChannelImpl.class);

        fileChannelImplCallbackInternalName = Type
                .getInternalName(FileChannelImplCallback.class);
        fileChannelImplCallbackDescriptor = Type
                .getDescriptor(FileChannelImplCallback.class);
    }

    @Override
    public void visitSource(String source, String debug) {
        // private final FileChannelImplCallback callback;
        FieldVisitor callbackFV = cv.visitField(
                Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "callback",
                fileChannelImplCallbackDescriptor, null, null);
        callbackFV.visitEnd();

        // proceed as intended
        cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if ("<init>".equals(name)) {
            // add initialization of the callback field to constructor
            mv = new ConstructorAdapter(api,
                    cv.visitMethod(access, name, desc, signature, exceptions),
                    access, name, desc);
        } else if (isReadMethod(access, name, desc, signature, exceptions)
                || isWriteMethod(access, name, desc, signature, exceptions)
                || isTransferMethod(access, name, desc, signature,
                        exceptions)) {
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
        // descriptors of the methods we add to FileChannelImpl
        String[] methodDescriptors = new String[] {
                Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(ByteBuffer.class)),
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(ByteBuffer[].class), Type.INT_TYPE,
                        Type.INT_TYPE),
                Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(ByteBuffer.class), Type.LONG_TYPE),
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(FileDescriptor.class), Type.LONG_TYPE,
                        Type.LONG_TYPE, Type.getType(FileDescriptor.class)) };
        MethodVisitor[] readMVs = new MethodVisitor[3];
        MethodVisitor[] writeMVs = new MethodVisitor[3];

        String ioExceptionInternalName = Type
                .getInternalName(IOException.class);

        // public int read(ByteBuffer dst) throws IOException {
        readMVs[0] = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                methodDescriptors[0], null,
                new String[] { ioExceptionInternalName });
        readMVs[0].visitCode();

        // long startTime = System.currentTimeMillis();
        readMVs[0].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMVs[0].visitVarInsn(Opcodes.LSTORE, 2);

        // int readResult = methodPrefixread(dst);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[0].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "read",
                methodDescriptors[0], false);
        readMVs[0].visitVarInsn(Opcodes.ISTORE, 4);

        // long endTime = System.currentTimeMillis();
        readMVs[0].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMVs[0].visitVarInsn(Opcodes.LSTORE, 5);

        // callback.onReadEnd(startTime, endTime, readResult);
        readMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[0].visitFieldInsn(Opcodes.GETFIELD, fileChannelImplInternalName,
                "callback", fileChannelImplCallbackDescriptor);
        readMVs[0].visitVarInsn(Opcodes.LLOAD, 2);
        readMVs[0].visitVarInsn(Opcodes.LLOAD, 5);
        readMVs[0].visitVarInsn(Opcodes.ILOAD, 4);
        readMVs[0]
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        fileChannelImplCallbackInternalName,
                        "onReadEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                        false);

        // return readResult;
        // }
        readMVs[0].visitVarInsn(Opcodes.ILOAD, 4);
        readMVs[0].visitInsn(Opcodes.IRETURN);
        readMVs[0].visitMaxs(0, 0);
        readMVs[0].visitEnd();

        // public int read(ByteBuffer[] dsts, int offset, int length) throws
        // IOException {
        readMVs[1] = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                methodDescriptors[1], null,
                new String[] { ioExceptionInternalName });
        readMVs[1].visitCode();

        // long startTime = System.currentTimeMillis();
        readMVs[1].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMVs[1].visitVarInsn(Opcodes.LSTORE, 4);

        // long readResult = methodPrefixread(dsts, offset, length);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        readMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        readMVs[1].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "read",
                methodDescriptors[1], false);
        readMVs[1].visitVarInsn(Opcodes.LSTORE, 6);

        // long endTime = System.currentTimeMillis();
        readMVs[1].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMVs[1].visitVarInsn(Opcodes.LSTORE, 8);

        // callback.onReadEnd(startTime, endTime, readResult);
        readMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[1].visitFieldInsn(Opcodes.GETFIELD, fileChannelImplInternalName,
                "callback", fileChannelImplCallbackDescriptor);
        readMVs[1].visitVarInsn(Opcodes.LLOAD, 4);
        readMVs[1].visitVarInsn(Opcodes.LLOAD, 8);
        readMVs[1].visitVarInsn(Opcodes.LLOAD, 6);
        readMVs[1]
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        fileChannelImplCallbackInternalName,
                        "onReadEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
                        false);

        // return readResult;
        // }
        readMVs[1].visitVarInsn(Opcodes.LLOAD, 6);
        readMVs[1].visitInsn(Opcodes.LRETURN);
        readMVs[1].visitMaxs(0, 0);
        readMVs[1].visitEnd();

        // public int read(ByteBuffer dst, long position) throws IOException {
        readMVs[2] = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                methodDescriptors[2], null,
                new String[] { ioExceptionInternalName });
        readMVs[2].visitCode();

        // long startTime = System.currentTimeMillis();
        readMVs[2].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMVs[2].visitVarInsn(Opcodes.LSTORE, 4);

        // int readResult = methodPrefixread(dst, position);
        readMVs[2].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[2].visitVarInsn(Opcodes.ALOAD, 1);
        readMVs[2].visitVarInsn(Opcodes.LLOAD, 2);
        readMVs[2].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "read",
                methodDescriptors[2], false);
        readMVs[2].visitVarInsn(Opcodes.ISTORE, 6);

        // long endTime = System.currentTimeMillis();
        readMVs[2].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMVs[2].visitVarInsn(Opcodes.LSTORE, 7);

        // callback.onReadEnd(startTime, endTime, readResult);
        readMVs[2].visitVarInsn(Opcodes.ALOAD, 0);
        readMVs[2].visitFieldInsn(Opcodes.GETFIELD, fileChannelImplInternalName,
                "callback", fileChannelImplCallbackDescriptor);
        readMVs[2].visitVarInsn(Opcodes.LLOAD, 4);
        readMVs[2].visitVarInsn(Opcodes.LLOAD, 7);
        readMVs[2].visitVarInsn(Opcodes.ILOAD, 6);
        readMVs[2]
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        fileChannelImplCallbackInternalName,
                        "onReadEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                        false);

        // return readResult;
        // }
        readMVs[2].visitVarInsn(Opcodes.ILOAD, 6);
        readMVs[2].visitInsn(Opcodes.IRETURN);
        readMVs[2].visitMaxs(0, 0);
        readMVs[2].visitEnd();

        // public int write(ByteBuffer src) throws IOException {
        writeMVs[0] = cv.visitMethod(Opcodes.ACC_PUBLIC, "write",
                methodDescriptors[0], null,
                new String[] { ioExceptionInternalName });
        writeMVs[0].visitCode();

        // long startTime = System.currentTimeMillis();
        writeMVs[0].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMVs[0].visitVarInsn(Opcodes.LSTORE, 2);

        // int writeResult = methodPrefixwrite(src);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[0].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "write",
                methodDescriptors[0], false);
        writeMVs[0].visitVarInsn(Opcodes.ISTORE, 4);

        // long endTime = System.currentTimeMillis();
        writeMVs[0].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMVs[0].visitVarInsn(Opcodes.LSTORE, 5);

        // callback.onWriteEnd(startTime, endTime, writeResult);
        writeMVs[0].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[0].visitFieldInsn(Opcodes.GETFIELD,
                fileChannelImplInternalName, "callback",
                fileChannelImplCallbackDescriptor);
        writeMVs[0].visitVarInsn(Opcodes.LLOAD, 2);
        writeMVs[0].visitVarInsn(Opcodes.LLOAD, 5);
        writeMVs[0].visitVarInsn(Opcodes.ILOAD, 4);
        writeMVs[0]
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        fileChannelImplCallbackInternalName,
                        "onWriteEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                        false);

        // return writeResult;
        // }
        writeMVs[0].visitVarInsn(Opcodes.ILOAD, 4);
        writeMVs[0].visitInsn(Opcodes.IRETURN);
        writeMVs[0].visitMaxs(0, 0);
        writeMVs[0].visitEnd();

        // public int write(ByteBuffer[] srcs, int offset, int length) throws
        // IOException {
        writeMVs[1] = cv.visitMethod(Opcodes.ACC_PUBLIC, "write",
                methodDescriptors[1], null,
                new String[] { ioExceptionInternalName });
        writeMVs[1].visitCode();

        // long startTime = System.currentTimeMillis();
        writeMVs[1].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMVs[1].visitVarInsn(Opcodes.LSTORE, 4);

        // long writeResult = methodPrefixwrite(srcs, offset, length);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 2);
        writeMVs[1].visitVarInsn(Opcodes.ILOAD, 3);
        writeMVs[1].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "write",
                methodDescriptors[1], false);
        writeMVs[1].visitVarInsn(Opcodes.LSTORE, 6);

        // long endTime = System.currentTimeMillis();
        writeMVs[1].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMVs[1].visitVarInsn(Opcodes.LSTORE, 8);

        // callback.onWriteEnd(startTime, endTime, writeResult);
        writeMVs[1].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[1].visitFieldInsn(Opcodes.GETFIELD,
                fileChannelImplInternalName, "callback",
                fileChannelImplCallbackDescriptor);
        writeMVs[1].visitVarInsn(Opcodes.LLOAD, 4);
        writeMVs[1].visitVarInsn(Opcodes.LLOAD, 8);
        writeMVs[1].visitVarInsn(Opcodes.LLOAD, 6);
        writeMVs[1]
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        fileChannelImplCallbackInternalName,
                        "onWriteEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
                        false);

        // return writeResult;
        // }
        writeMVs[1].visitVarInsn(Opcodes.LLOAD, 6);
        writeMVs[1].visitInsn(Opcodes.LRETURN);
        writeMVs[1].visitMaxs(0, 0);
        writeMVs[1].visitEnd();

        // public int write(ByteBuffer src, long position) throws IOException {
        writeMVs[2] = cv.visitMethod(Opcodes.ACC_PUBLIC, "write",
                methodDescriptors[2], null,
                new String[] { ioExceptionInternalName });
        writeMVs[2].visitCode();

        // long startTime = System.currentTimeMillis();
        writeMVs[2].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMVs[2].visitVarInsn(Opcodes.LSTORE, 4);

        // int writeResult = methodPrefixwrite(src, position);
        writeMVs[2].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[2].visitVarInsn(Opcodes.ALOAD, 1);
        writeMVs[2].visitVarInsn(Opcodes.LLOAD, 2);
        writeMVs[2].visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "write",
                methodDescriptors[2], false);
        writeMVs[2].visitVarInsn(Opcodes.ISTORE, 6);

        // long endTime = System.currentTimeMillis();
        writeMVs[2].visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMVs[2].visitVarInsn(Opcodes.LSTORE, 7);

        // callback.onWriteEnd(startTime, endTime, writeResult);
        writeMVs[2].visitVarInsn(Opcodes.ALOAD, 0);
        writeMVs[2].visitFieldInsn(Opcodes.GETFIELD,
                fileChannelImplInternalName, "callback",
                fileChannelImplCallbackDescriptor);
        writeMVs[2].visitVarInsn(Opcodes.LLOAD, 4);
        writeMVs[2].visitVarInsn(Opcodes.LLOAD, 7);
        writeMVs[2].visitVarInsn(Opcodes.ILOAD, 6);
        writeMVs[2]
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        fileChannelImplCallbackInternalName,
                        "onWriteEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                        false);

        // return writeResult;
        // }
        writeMVs[2].visitVarInsn(Opcodes.ILOAD, 6);
        writeMVs[2].visitInsn(Opcodes.IRETURN);
        writeMVs[2].visitMaxs(0, 0);
        writeMVs[2].visitEnd();

        // private long transferTo0(FileDescriptor src, long position, long
        // count, FileDescriptor dst);{
        MethodVisitor transferToMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "transferTo0", methodDescriptors[3], null, null);
        transferToMV.visitCode();

        // long startTime = System.currentTimeMillis();
        transferToMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        transferToMV.visitVarInsn(Opcodes.LSTORE, 7);

        // long transferResult = methodPrefixtransferTo0(src, position, count,
        // dst);
        transferToMV.visitVarInsn(Opcodes.ALOAD, 0);
        transferToMV.visitVarInsn(Opcodes.LLOAD, 1);
        transferToMV.visitVarInsn(Opcodes.LLOAD, 2);
        transferToMV.visitVarInsn(Opcodes.ALOAD, 4);
        transferToMV.visitVarInsn(Opcodes.ALOAD, 6);
        transferToMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileChannelImplInternalName, methodPrefix + "transferTo0",
                methodDescriptors[3], false);
        transferToMV.visitVarInsn(Opcodes.LSTORE, 9);

        // long endTime = System.currentTimeMillis();
        transferToMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        transferToMV.visitVarInsn(Opcodes.LSTORE, 11);

        // callback.onTransferToEnd(startTime, endTime, transferResult);
        transferToMV.visitVarInsn(Opcodes.ALOAD, 0);
        transferToMV.visitFieldInsn(Opcodes.GETFIELD,
                fileChannelImplInternalName, "callback",
                fileChannelImplCallbackDescriptor);
        transferToMV.visitVarInsn(Opcodes.LLOAD, 7);
        transferToMV.visitVarInsn(Opcodes.LLOAD, 11);
        transferToMV.visitVarInsn(Opcodes.LLOAD, 9);
        transferToMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileChannelImplCallbackInternalName, "onTransferToEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE, Type.LONG_TYPE),
                false);

        // return transferResult;
        // }
        transferToMV.visitVarInsn(Opcodes.LLOAD, 9);
        transferToMV.visitInsn(Opcodes.LRETURN);
        transferToMV.visitMaxs(0, 0);
        transferToMV.visitEnd();

        cv.visitEnd();
    }

    private static class ConstructorAdapter extends AdviceAdapter {

        protected ConstructorAdapter(int api, MethodVisitor mv, int access,
                String name, String desc) {
            super(api, mv, access, name, desc);
        }

        @Override
        protected void onMethodEnter() {
            // callback = new FileChannelImplCallback();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitTypeInsn(Opcodes.NEW,
                    Type.getInternalName(FileChannelImplCallback.class));
            mv.visitInsn(Opcodes.DUP);
            try {
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                        Type.getInternalName(FileChannelImplCallback.class),
                        "<init>",
                        Type.getConstructorDescriptor(
                                FileChannelImplCallback.class.getConstructor()),
                        false);
            } catch (Exception e) {
                throw new RuntimeException("Could not access constructor", e);
            }
            mv.visitFieldInsn(Opcodes.PUTFIELD,
                    Type.getInternalName(FileChannelImpl.class), "callback",
                    Type.getDescriptor(FileChannelImplCallback.class));
        }

    }

    // Helper methods

    private boolean isReadMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC && "read".equals(name)
                && (Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(ByteBuffer.class)).equals(
                                desc)
                        || Type.getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class), Type.INT_TYPE,
                                Type.INT_TYPE).equals(desc)
                        || Type.getMethodDescriptor(Type.INT_TYPE,
                                Type.getType(ByteBuffer.class), Type.LONG_TYPE)
                                .equals(desc))
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isWriteMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC && "write".equals(name)
                && (Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(ByteBuffer.class)).equals(
                                desc)
                        || Type.getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(ByteBuffer[].class), Type.INT_TYPE,
                                Type.INT_TYPE).equals(desc)
                        || Type.getMethodDescriptor(Type.INT_TYPE,
                                Type.getType(ByteBuffer.class), Type.LONG_TYPE)
                                .equals(desc))
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isTransferMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == (Opcodes.ACC_PRIVATE | Opcodes.ACC_NATIVE)
                && "transferTo0".equals(name)
                && Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(FileDescriptor.class), Type.LONG_TYPE,
                        Type.LONG_TYPE, Type.getType(FileDescriptor.class))
                        .equals(desc)
                && null == signature
                && (exceptions == null || exceptions.length == 0);
    }
}
