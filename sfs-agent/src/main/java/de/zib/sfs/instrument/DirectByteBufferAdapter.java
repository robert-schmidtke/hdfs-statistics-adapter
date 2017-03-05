/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

public class DirectByteBufferAdapter extends ClassVisitor {

    private final String nativeMethodPrefix;

    private final String systemInternalName, currentTimeMillisDescriptor;

    private final String directByteBufferInternalName;

    private final String directByteBufferCallbackInternalName,
            directByteBufferCallbackDescriptor;

    private static final Map<String, Type> TYPES = new HashMap<String, Type>();
    static {
        TYPES.put("", Type.BYTE_TYPE);
        TYPES.put("Char", Type.CHAR_TYPE);
        TYPES.put("Double", Type.DOUBLE_TYPE);
        TYPES.put("Float", Type.FLOAT_TYPE);
        TYPES.put("Int", Type.INT_TYPE);
        TYPES.put("Long", Type.LONG_TYPE);
        TYPES.put("Short", Type.SHORT_TYPE);
    }

    public DirectByteBufferAdapter(ClassVisitor cv, String nativeMethodPrefix) {
        super(Opcodes.ASM5, cv);
        this.nativeMethodPrefix = nativeMethodPrefix;

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        directByteBufferInternalName = "java/nio/DirectByteBuffer";

        directByteBufferCallbackInternalName = Type
                .getInternalName(DirectByteBufferCallback.class);
        directByteBufferCallbackDescriptor = Type
                .getDescriptor(DirectByteBufferCallback.class);
    }

    @Override
    public void visitSource(String source, String debug) {
        // private DirectByteBufferCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE, "callback",
                directByteBufferCallbackDescriptor, null, null);
        callbackFV.visitEnd();

        // proceed as intended
        cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if ("<init>".equals(name)) {
            mv = new ConstructorAdapter(api,
                    cv.visitMethod(access, name, desc, signature, exceptions),
                    access, name, desc);
        } else if (isGetMethod(access, name, desc, signature, exceptions)
                || isPutMethod(access, name, desc, signature, exceptions)) {
            // rename native methods so we can wrap them
            mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
                    signature, exceptions);
        } else {
            // simply copy the old method
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        }
        return mv;
    }

    @Override
    public void visitEnd() {
        // public ByteBuffer get(byte[] dst, int offset, int length) {
        MethodVisitor bulkGetMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "get",
                Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE),
                null, null);
        bulkGetMV.visitCode();

        // if (!fromFileChannel) {
        bulkGetMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkGetMV.visitFieldInsn(Opcodes.GETFIELD, directByteBufferInternalName,
                "fromFileChannel", Type.getDescriptor(Boolean.TYPE));
        Label bulkGetFromFileChannelLabel = new Label();
        bulkGetMV.visitJumpInsn(Opcodes.IFNE, bulkGetFromFileChannelLabel);

        // return nativeMethodPrefixget(dst[], offset, length);
        bulkGetMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkGetMV.visitVarInsn(Opcodes.ALOAD, 1);
        bulkGetMV.visitVarInsn(Opcodes.ILOAD, 2);
        bulkGetMV.visitVarInsn(Opcodes.ILOAD, 3);
        bulkGetMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                directByteBufferInternalName, nativeMethodPrefix + "get",
                Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE),
                false);
        bulkGetMV.visitInsn(Opcodes.ARETURN);

        // }
        bulkGetMV.visitLabel(bulkGetFromFileChannelLabel);

        // long startTime = System.currentTimeMillis();
        bulkGetMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        bulkGetMV.visitVarInsn(Opcodes.LSTORE, 4);

        // ByteBuffer result = nativeMethodPrefixget(dst, offset, length);
        bulkGetMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkGetMV.visitVarInsn(Opcodes.ALOAD, 1);
        bulkGetMV.visitVarInsn(Opcodes.ILOAD, 2);
        bulkGetMV.visitVarInsn(Opcodes.ILOAD, 3);
        bulkGetMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                directByteBufferInternalName, nativeMethodPrefix + "get",
                Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE),
                false);
        bulkGetMV.visitVarInsn(Opcodes.ASTORE, 6);

        // long endTime = System.currentTimeMillis();
        bulkGetMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        bulkGetMV.visitVarInsn(Opcodes.LSTORE, 7);

        // callback.onGetEnd(startTime, endTime, length);
        bulkGetMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkGetMV.visitFieldInsn(Opcodes.GETFIELD, directByteBufferInternalName,
                "callback", directByteBufferCallbackDescriptor);
        bulkGetMV.visitVarInsn(Opcodes.LLOAD, 4);
        bulkGetMV.visitVarInsn(Opcodes.LLOAD, 7);
        bulkGetMV.visitVarInsn(Opcodes.ILOAD, 3);
        bulkGetMV
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        directByteBufferCallbackInternalName,
                        "onGetEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                        false);

        // return result;
        // }
        bulkGetMV.visitVarInsn(Opcodes.ALOAD, 6);
        bulkGetMV.visitInsn(Opcodes.ARETURN);
        bulkGetMV.visitMaxs(0, 0);
        bulkGetMV.visitEnd();

        // public ByteBuffer put(byte[] dst, int offset, int length) {
        MethodVisitor bulkPutMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "put",
                Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE),
                null, null);
        bulkPutMV.visitCode();

        // if (!fromFileChannel) {
        bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkPutMV.visitFieldInsn(Opcodes.GETFIELD, directByteBufferInternalName,
                "fromFileChannel", Type.getDescriptor(Boolean.TYPE));
        Label bulkPutFromFileChannelLabel = new Label();
        bulkPutMV.visitJumpInsn(Opcodes.IFNE, bulkPutFromFileChannelLabel);

        // return nativeMethodPrefixput(dst, offset, length);
        bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
        bulkPutMV.visitVarInsn(Opcodes.ILOAD, 2);
        bulkPutMV.visitVarInsn(Opcodes.ILOAD, 3);
        bulkPutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                directByteBufferInternalName, nativeMethodPrefix + "put",
                Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE),
                false);
        bulkPutMV.visitInsn(Opcodes.ARETURN);

        // }
        bulkPutMV.visitLabel(bulkPutFromFileChannelLabel);

        // long startTime = System.currentTimeMillis();
        bulkPutMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        bulkPutMV.visitVarInsn(Opcodes.LSTORE, 4);

        // ByteBuffer result = nativeMethodPrefixput(dst, offset, length);
        bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
        bulkPutMV.visitVarInsn(Opcodes.ILOAD, 2);
        bulkPutMV.visitVarInsn(Opcodes.ILOAD, 3);
        bulkPutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                directByteBufferInternalName, nativeMethodPrefix + "put",
                Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE),
                false);
        bulkPutMV.visitVarInsn(Opcodes.ASTORE, 6);

        // long endTime = System.currentTimeMillis();
        bulkPutMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        bulkPutMV.visitVarInsn(Opcodes.LSTORE, 7);

        // callback.onPutEnd(startTime, endTime, length);
        bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
        bulkPutMV.visitFieldInsn(Opcodes.GETFIELD, directByteBufferInternalName,
                "callback", directByteBufferCallbackDescriptor);
        bulkPutMV.visitVarInsn(Opcodes.LLOAD, 4);
        bulkPutMV.visitVarInsn(Opcodes.LLOAD, 7);
        bulkPutMV.visitVarInsn(Opcodes.ILOAD, 3);
        bulkPutMV
                .visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        directByteBufferCallbackInternalName,
                        "onPutEnd", Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                        false);

        // return result;
        // }
        bulkPutMV.visitVarInsn(Opcodes.ALOAD, 6);
        bulkPutMV.visitInsn(Opcodes.ARETURN);
        bulkPutMV.visitMaxs(0, 0);
        bulkPutMV.visitEnd();

        // regular gets and puts
        for (Map.Entry<String, Type> type : TYPES.entrySet()) {
            // public TYPE getTYPE() {
            MethodVisitor relativeGetMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                    "get" + type.getKey(),
                    Type.getMethodDescriptor(type.getValue()), null, null);
            relativeGetMV.visitCode();

            // if (!fromFileChannel) {
            relativeGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativeGetMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "fromFileChannel",
                    Type.getDescriptor(Boolean.TYPE));
            Label relativeGetFromFileChannelLabel = new Label();
            relativeGetMV.visitJumpInsn(Opcodes.IFNE,
                    relativeGetFromFileChannelLabel);

            // return nativeMethodPrefixgetTYPE();
            relativeGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativeGetMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "get" + type.getKey(),
                    Type.getMethodDescriptor(type.getValue()), false);
            relativeGetMV.visitInsn(type.getValue().getOpcode(Opcodes.IRETURN));

            // }
            relativeGetMV.visitLabel(relativeGetFromFileChannelLabel);

            // long startTime = System.currentTimeMillis();
            relativeGetMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            relativeGetMV.visitVarInsn(Opcodes.LSTORE, 1);

            // TYPE result = nativeMethodPrefixgetTYPE();
            relativeGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativeGetMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "get" + type.getKey(),
                    Type.getMethodDescriptor(type.getValue()), false);
            relativeGetMV
                    .visitVarInsn(type.getValue().getOpcode(Opcodes.ISTORE), 3);

            // long endTime = System.currentTimeMillis();
            relativeGetMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            relativeGetMV.visitVarInsn(Opcodes.LSTORE,
                    3 + type.getValue().getSize());

            // callback.onGetTYPEEnd(startTime, endTime);
            relativeGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativeGetMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "callback",
                    directByteBufferCallbackDescriptor);
            relativeGetMV.visitVarInsn(Opcodes.LLOAD, 1);
            relativeGetMV.visitVarInsn(Opcodes.LLOAD,
                    3 + type.getValue().getSize());
            relativeGetMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    directByteBufferCallbackInternalName,
                    "onGet" + type.getKey() + "End",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                            Type.LONG_TYPE),
                    false);

            // return result;
            // }
            relativeGetMV.visitVarInsn(type.getValue().getOpcode(Opcodes.ILOAD),
                    3);
            relativeGetMV.visitInsn(type.getValue().getOpcode(Opcodes.IRETURN));
            relativeGetMV.visitMaxs(0, 0);
            relativeGetMV.visitEnd();

            // public TYPE getTYPE(int index) {
            MethodVisitor absoluteGetMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                    "get" + type.getKey(),
                    Type.getMethodDescriptor(type.getValue(), Type.INT_TYPE),
                    null, null);
            absoluteGetMV.visitCode();

            // if (!fromFileChannel) {
            absoluteGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            absoluteGetMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "fromFileChannel",
                    Type.getDescriptor(Boolean.TYPE));
            Label absoluteGetFromFileChannelLabel = new Label();
            absoluteGetMV.visitJumpInsn(Opcodes.IFNE,
                    absoluteGetFromFileChannelLabel);

            // return nativeMethodPrefixgetTYPE(index);
            absoluteGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            absoluteGetMV.visitVarInsn(Opcodes.ILOAD, 1);
            absoluteGetMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "get" + type.getKey(),
                    Type.getMethodDescriptor(type.getValue(), Type.INT_TYPE),
                    false);
            absoluteGetMV.visitInsn(type.getValue().getOpcode(Opcodes.IRETURN));

            // }
            absoluteGetMV.visitLabel(absoluteGetFromFileChannelLabel);

            // long startTime = System.currentTimeMillis();
            absoluteGetMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            absoluteGetMV.visitVarInsn(Opcodes.LSTORE, 2);

            // TYPE result = nativeMethodPrefixgetTYPE(index);
            absoluteGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            absoluteGetMV.visitVarInsn(Opcodes.ILOAD, 1);
            absoluteGetMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "get" + type.getKey(),
                    Type.getMethodDescriptor(type.getValue(), Type.INT_TYPE),
                    false);
            absoluteGetMV
                    .visitVarInsn(type.getValue().getOpcode(Opcodes.ISTORE), 4);

            // long endTime = System.currentTimeMillis();
            absoluteGetMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            absoluteGetMV.visitVarInsn(Opcodes.LSTORE,
                    4 + type.getValue().getSize());

            // callback.onGetTYPEEnd(startTime, endTime);
            absoluteGetMV.visitVarInsn(Opcodes.ALOAD, 0);
            absoluteGetMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "callback",
                    directByteBufferCallbackDescriptor);
            absoluteGetMV.visitVarInsn(Opcodes.LLOAD, 2);
            absoluteGetMV.visitVarInsn(Opcodes.LLOAD,
                    4 + type.getValue().getSize());
            absoluteGetMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    directByteBufferCallbackInternalName,
                    "onGet" + type.getKey() + "End",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                            Type.LONG_TYPE),
                    false);

            // return result;
            // }
            absoluteGetMV.visitVarInsn(type.getValue().getOpcode(Opcodes.ILOAD),
                    4);
            absoluteGetMV.visitInsn(type.getValue().getOpcode(Opcodes.IRETURN));
            absoluteGetMV.visitMaxs(0, 0);
            absoluteGetMV.visitEnd();

            // public ByteBuffer putTYPE(TYPE value) {
            MethodVisitor relativePutMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                    "put" + type.getKey(),
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            type.getValue()),
                    null, null);
            relativePutMV.visitCode();

            // if (!fromFileChannel) {
            relativePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativePutMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "fromFileChannel",
                    Type.getDescriptor(Boolean.TYPE));
            Label relativePutFromFileChannelLabel = new Label();
            relativePutMV.visitJumpInsn(Opcodes.IFNE,
                    relativePutFromFileChannelLabel);

            // return nativeMethodPrefixputTYPE(value);
            relativePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativePutMV.visitVarInsn(type.getValue().getOpcode(Opcodes.ILOAD),
                    1);
            relativePutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "put" + type.getKey(),
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            type.getValue()),
                    false);
            relativePutMV.visitInsn(Opcodes.ARETURN);

            // }
            relativePutMV.visitLabel(relativePutFromFileChannelLabel);

            // long startTime = System.currentTimeMillis();
            relativePutMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            relativePutMV.visitVarInsn(Opcodes.LSTORE,
                    1 + type.getValue().getSize());

            // ByteBuffer result = nativeMethodPrefixputTYPE(value);
            relativePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativePutMV.visitVarInsn(type.getValue().getOpcode(Opcodes.ILOAD),
                    1);
            relativePutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "put" + type.getKey(),
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            type.getValue()),
                    false);
            relativePutMV.visitVarInsn(Opcodes.ASTORE,
                    3 + type.getValue().getSize());

            // long endTime = System.currentTimeMillis();
            relativePutMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            relativePutMV.visitVarInsn(Opcodes.LSTORE,
                    4 + type.getValue().getSize());

            // callback.onPutTYPEEnd(startTime, endTime);
            relativePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            relativePutMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "callback",
                    directByteBufferCallbackDescriptor);
            relativePutMV.visitVarInsn(Opcodes.LLOAD,
                    1 + type.getValue().getSize());
            relativePutMV.visitVarInsn(Opcodes.LLOAD,
                    4 + type.getValue().getSize());
            relativePutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    directByteBufferCallbackInternalName,
                    "onPut" + type.getKey() + "End",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                            Type.LONG_TYPE),
                    false);

            // return result;
            // }
            relativePutMV.visitVarInsn(Opcodes.ALOAD,
                    3 + type.getValue().getSize());
            relativePutMV.visitInsn(Opcodes.ARETURN);
            relativePutMV.visitMaxs(0, 0);
            relativePutMV.visitEnd();

            // public ByteBuffer putTYPE(int index, TYPE value) {
            MethodVisitor absolutePutMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                    "put" + type.getKey(),
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            Type.INT_TYPE, type.getValue()),
                    null, null);
            absolutePutMV.visitCode();

            // if (!fromFileChannel) {
            absolutePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            absolutePutMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "fromFileChannel",
                    Type.getDescriptor(Boolean.TYPE));
            Label absolutePutFromFileChannelLabel = new Label();
            absolutePutMV.visitJumpInsn(Opcodes.IFNE,
                    absolutePutFromFileChannelLabel);

            // return nativeMethodPrefixputTYPE(index, value);
            absolutePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            absolutePutMV.visitVarInsn(Opcodes.ILOAD, 1);
            absolutePutMV.visitVarInsn(type.getValue().getOpcode(Opcodes.ILOAD),
                    2);
            absolutePutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "put" + type.getKey(),
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            Type.INT_TYPE, type.getValue()),
                    false);
            absolutePutMV.visitInsn(Opcodes.ARETURN);

            // }
            absolutePutMV.visitLabel(absolutePutFromFileChannelLabel);

            // long startTime = System.currentTimeMillis();
            absolutePutMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            absolutePutMV.visitVarInsn(Opcodes.LSTORE,
                    2 + type.getValue().getSize());

            // ByteBuffer result = nativeMethodPrefixputTYPE(index, value);
            absolutePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            absolutePutMV.visitVarInsn(Opcodes.ILOAD, 1);
            absolutePutMV.visitVarInsn(type.getValue().getOpcode(Opcodes.ILOAD),
                    2);
            absolutePutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    directByteBufferInternalName,
                    nativeMethodPrefix + "put" + type.getKey(),
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            Type.INT_TYPE, type.getValue()),
                    false);
            absolutePutMV.visitVarInsn(Opcodes.ASTORE,
                    4 + type.getValue().getSize());

            // long endTime = System.currentTimeMillis();
            absolutePutMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    systemInternalName, "currentTimeMillis",
                    currentTimeMillisDescriptor, false);
            absolutePutMV.visitVarInsn(Opcodes.LSTORE,
                    5 + type.getValue().getSize());

            // callback.onPutTYPEEnd(startTime, endTime);
            absolutePutMV.visitVarInsn(Opcodes.ALOAD, 0);
            absolutePutMV.visitFieldInsn(Opcodes.GETFIELD,
                    directByteBufferInternalName, "callback",
                    directByteBufferCallbackDescriptor);
            absolutePutMV.visitVarInsn(Opcodes.LLOAD,
                    2 + type.getValue().getSize());
            absolutePutMV.visitVarInsn(Opcodes.LLOAD,
                    5 + type.getValue().getSize());
            absolutePutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    directByteBufferCallbackInternalName,
                    "onPut" + type.getKey() + "End",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                            Type.LONG_TYPE),
                    false);

            // return result;
            // }
            absolutePutMV.visitVarInsn(Opcodes.ALOAD,
                    4 + type.getValue().getSize());
            absolutePutMV.visitInsn(Opcodes.ARETURN);
            absolutePutMV.visitMaxs(0, 0);
            absolutePutMV.visitEnd();
        }

        cv.visitEnd();
    }

    private static class ConstructorAdapter extends AdviceAdapter {

        protected ConstructorAdapter(int api, MethodVisitor mv, int access,
                String name, String desc) {
            super(api, mv, access, name, desc);
        }

        @Override
        protected void onMethodEnter() {
            // callback = new DirectByteBufferCallback();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitTypeInsn(Opcodes.NEW,
                    Type.getInternalName(DirectByteBufferCallback.class));
            mv.visitInsn(Opcodes.DUP);
            try {
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                        Type.getInternalName(DirectByteBufferCallback.class),
                        "<init>",
                        Type.getConstructorDescriptor(
                                DirectByteBufferCallback.class
                                        .getConstructor()),
                        false);
            } catch (Exception e) {
                throw new RuntimeException("Could not access constructor", e);
            }
            mv.visitFieldInsn(Opcodes.PUTFIELD, "java/nio/DirectByteBuffer",
                    "callback",
                    Type.getDescriptor(DirectByteBufferCallback.class));
        }

    }

    // Helper methods

    private boolean isGetMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        if (Opcodes.ACC_PUBLIC != access || null != signature
                || (exceptions != null && exceptions.length != 0)) {
            return false;
        }

        // bulk get
        if ("get".equals(name)) {
            if (Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                    Type.getType(byte[].class), Type.INT_TYPE, Type.INT_TYPE)
                    .equals(desc)) {
                return true;
            }
        }

        // regular gets
        for (Map.Entry<String, Type> type : TYPES.entrySet()) {
            if (("get" + type.getKey()).equals(name)) {
                if (Type.getMethodDescriptor(type.getValue()).equals(desc)
                        || Type.getMethodDescriptor(type.getValue(),
                                Type.INT_TYPE).equals(desc)) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean isPutMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        if (Opcodes.ACC_PUBLIC != access || null != signature
                || (exceptions != null && exceptions.length != 0)) {
            return false;
        }

        // bulk put
        if ("put".equals(name)) {
            if (Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                    Type.getType(byte[].class), Type.INT_TYPE, Type.INT_TYPE)
                    .equals(desc)) {
                return true;
            }
        }

        // regular puts
        for (Map.Entry<String, Type> type : TYPES.entrySet()) {
            if (("put" + type.getKey()).equals(name)) {
                if (Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                        type.getValue()).equals(desc)
                        || Type.getMethodDescriptor(
                                Type.getType(ByteBuffer.class), Type.INT_TYPE,
                                type.getValue()).equals(desc)) {
                    return true;
                }
            }
        }

        return false;
    }

}
