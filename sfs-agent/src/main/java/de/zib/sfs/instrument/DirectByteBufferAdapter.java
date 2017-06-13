/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class DirectByteBufferAdapter extends AbstractSfsAdapter {

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

    public DirectByteBufferAdapter(ClassVisitor cv, String methodPrefix) {
        super(cv, "java/nio/DirectByteBuffer", DirectByteBufferCallback.class,
                methodPrefix);
    }

    @Override
    protected boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return isGetMethod(access, name, desc, signature, exceptions)
                || isPutMethod(access, name, desc, signature, exceptions);
    }

    @Override
    protected void initializeFields(MethodVisitor constructorMV,
            String constructorDesc) {
        // if we're constructed from another buffer, make sure we're from a file
        // too if the other buffer is too
        if ("(Lsun/nio/ch/DirectBuffer;IIIII)V".equals(constructorDesc)) {
            // if (db instanceof MappedByteBuffer) {
            constructorMV.visitVarInsn(Opcodes.ALOAD, 1);
            constructorMV.visitTypeInsn(Opcodes.INSTANCEOF,
                    Type.getInternalName(MappedByteBuffer.class));
            Label memoryMappedBufferLabel = new Label();
            constructorMV.visitJumpInsn(Opcodes.IFEQ, memoryMappedBufferLabel);

            // setFromFileChannel(db.isFromFileChannel());
            constructorMV.visitVarInsn(Opcodes.ALOAD, 0);
            constructorMV.visitVarInsn(Opcodes.ALOAD, 1);
            constructorMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(MappedByteBuffer.class),
                    "isFromFileChannel",
                    Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
            constructorMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    instrumentedTypeInternalName, "setFromFileChannel",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);

            // callback.openCallback(db.filename);
            constructorMV.visitVarInsn(Opcodes.ALOAD, 0);
            constructorMV.visitFieldInsn(Opcodes.GETFIELD,
                    instrumentedTypeInternalName, "callback",
                    callbackTypeDescriptor);
            constructorMV.visitVarInsn(Opcodes.ALOAD, 1);
            constructorMV.visitFieldInsn(Opcodes.GETFIELD,
                    Type.getInternalName(MappedByteBuffer.class), "filename",
                    Type.getDescriptor(String.class));
            constructorMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    callbackTypeInternalName, "openCallback",
                    Type.getMethodDescriptor(Type.VOID_TYPE,
                            Type.getType(String.class)),
                    false);

            // }
            constructorMV.visitLabel(memoryMappedBufferLabel);
        }
    }

    @Override
    protected void appendWrappedMethods(ClassVisitor cv) {
        wrapMethod(Opcodes.ACC_PUBLIC, "get", Type.getType(ByteBuffer.class),
                new Type[] { Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE },
                null, null, "getCallback", Type.INT_TYPE,
                new ParameterResultPasser(3));

        wrapMethod(Opcodes.ACC_PUBLIC, "put", Type.getType(ByteBuffer.class),
                new Type[] { Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE },
                null, null, "putCallback", Type.INT_TYPE,
                new ParameterResultPasser(3));

        {
            // public ByteBuffer put(ByteBuffer src) {
            MethodVisitor bulkPutMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "put",
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            Type.getType(ByteBuffer.class)),
                    null, null);
            bulkPutMV.visitCode();

            // if (isInstrumentationActive()) {
            isInstrumentationActive(bulkPutMV);
            Label instrumentationActiveLabel = new Label();
            bulkPutMV.visitJumpInsn(Opcodes.IFEQ, instrumentationActiveLabel);

            // return nativeMethodPrefixput(src);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    instrumentedTypeInternalName, methodPrefix + "put",
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            Type.getType(ByteBuffer.class)),
                    false);
            bulkPutMV.visitInsn(Opcodes.ARETURN);

            // }
            bulkPutMV.visitLabel(instrumentationActiveLabel);

            // setInstrumentationActive(fromFileChannel);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
            bulkPutMV.visitFieldInsn(Opcodes.GETFIELD,
                    instrumentedTypeInternalName, "fromFileChannel",
                    Type.getDescriptor(Boolean.TYPE));
            bulkPutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    instrumentedTypeInternalName, "setInstrumentationActive",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);

            // boolean srcInstrumentationActive = false;
            bulkPutMV.visitInsn(Opcodes.ICONST_0);
            bulkPutMV.visitVarInsn(Opcodes.ISTORE, 2);

            // if (src instanceof MappedByteBuffer) {
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
            bulkPutMV.visitTypeInsn(Opcodes.INSTANCEOF,
                    Type.getInternalName(MappedByteBuffer.class));
            Label srcInstanceofMappedByteBufferLabel = new Label();
            bulkPutMV.visitJumpInsn(Opcodes.IFEQ,
                    srcInstanceofMappedByteBufferLabel);

            // srcInstrumentationActive = src.isFromFileChannel();
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(MappedByteBuffer.class),
                    "isFromFileChannel",
                    Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
            bulkPutMV.visitVarInsn(Opcodes.ISTORE, 2);

            // src.setInstrumentationActive(true);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
            bulkPutMV.visitInsn(Opcodes.ICONST_1);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(MappedByteBuffer.class),
                    "setInstrumentationActive",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);

            // }
            bulkPutMV.visitLabel(srcInstanceofMappedByteBufferLabel);

            // int length = src.remaining();
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(Buffer.class), "remaining",
                    Type.getMethodDescriptor(Type.INT_TYPE), false);
            bulkPutMV.visitVarInsn(Opcodes.ISTORE, 4);

            // long startTime = System.currentTimeMillis();
            bulkPutMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                    "currentTimeMillis", currentTimeMillisDescriptor, false);
            bulkPutMV.visitVarInsn(Opcodes.LSTORE, 5);

            // ByteBuffer result = nativeMethodPrefixput(src);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    instrumentedTypeInternalName, methodPrefix + "put",
                    Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                            Type.getType(ByteBuffer.class)),
                    false);
            bulkPutMV.visitVarInsn(Opcodes.ASTORE, 7);

            // long endTime = System.currentTimeMillis();
            bulkPutMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                    "currentTimeMillis", currentTimeMillisDescriptor, false);
            bulkPutMV.visitVarInsn(Opcodes.LSTORE, 8);

            // if (isInstrumentationActive()) {
            isInstrumentationActive(bulkPutMV);
            Label instrumentationStillActiveLabel = new Label();
            bulkPutMV.visitJumpInsn(Opcodes.IFEQ,
                    instrumentationStillActiveLabel);

            // callback.putCallback(startTime, endTime, length);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
            bulkPutMV.visitFieldInsn(Opcodes.GETFIELD,
                    instrumentedTypeInternalName, "callback",
                    callbackTypeDescriptor);
            bulkPutMV.visitVarInsn(Opcodes.LLOAD, 5);
            bulkPutMV.visitVarInsn(Opcodes.LLOAD, 8);
            bulkPutMV.visitVarInsn(Opcodes.ILOAD, 4);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    callbackTypeInternalName,
                    "putCallback", Type.getMethodDescriptor(Type.VOID_TYPE,
                            Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                    false);

            // setInstrumentationActive(false);
            setInstrumentationActive(bulkPutMV, false);

            // }
            bulkPutMV.visitLabel(instrumentationStillActiveLabel);

            // if (srcInstrumentationActive) {
            bulkPutMV.visitVarInsn(Opcodes.ILOAD, 2);
            Label srcInstrumentationActiveLabel = new Label();
            bulkPutMV.visitJumpInsn(Opcodes.IFEQ,
                    srcInstrumentationActiveLabel);

            // callback.onGetEnd(startTime, endTime, length);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 0);
            bulkPutMV.visitFieldInsn(Opcodes.GETFIELD,
                    instrumentedTypeInternalName, "callback",
                    callbackTypeDescriptor);
            bulkPutMV.visitVarInsn(Opcodes.LLOAD, 5);
            bulkPutMV.visitVarInsn(Opcodes.LLOAD, 8);
            bulkPutMV.visitVarInsn(Opcodes.ILOAD, 4);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    callbackTypeInternalName,
                    "getCallback", Type.getMethodDescriptor(Type.VOID_TYPE,
                            Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE),
                    false);

            // src.setInstrumentationActive(false);
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 1);
            bulkPutMV.visitInsn(Opcodes.ICONST_0);
            bulkPutMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(MappedByteBuffer.class),
                    "setInstrumentationActive",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);

            // }
            bulkPutMV.visitLabel(srcInstrumentationActiveLabel);

            // return result;
            // }
            bulkPutMV.visitVarInsn(Opcodes.ALOAD, 7);
            bulkPutMV.visitInsn(Opcodes.ARETURN);
            bulkPutMV.visitMaxs(0, 0);
            bulkPutMV.visitEnd();
        }

        ResultPasser resultDiscarder = new DiscardResultPasser();

        // regular gets and puts
        for (Map.Entry<String, Type> type : TYPES.entrySet()) {
            // public TYPE getTYPE() { ... }
            wrapMethod(Opcodes.ACC_PUBLIC, "get" + type.getKey(),
                    type.getValue(), null, null, null,
                    "get" + type.getKey() + "Callback", null, resultDiscarder);

            // public TYPE getTYPE(int index) { ... }
            wrapMethod(Opcodes.ACC_PUBLIC, "get" + type.getKey(),
                    type.getValue(), new Type[] { Type.INT_TYPE }, null, null,
                    "get" + type.getKey() + "Callback", null, resultDiscarder);

            // public ByteBuffer putTYPE(TYPE value) { ... }
            wrapMethod(Opcodes.ACC_PUBLIC, "put" + type.getKey(),
                    Type.getType(ByteBuffer.class),
                    new Type[] { type.getValue() }, null, null,
                    "put" + type.getKey() + "Callback", null, resultDiscarder);

            // public ByteBuffer putTYPE(int index, TYPE value) { ... }
            wrapMethod(Opcodes.ACC_PUBLIC, "put" + type.getKey(),
                    Type.getType(ByteBuffer.class),
                    new Type[] { Type.INT_TYPE, type.getValue() }, null, null,
                    "put" + type.getKey() + "Callback", null, resultDiscarder);
        }

        cv.visitEnd();
    }

    @Override
    protected void wrapMethod(int access, String name, Type returnType,
            Type[] argumentTypes, String signature, String[] exceptions,
            String callbackName, Type additionalCallbackArgumentType,
            ResultPasser resultPasser) {
        argumentTypes = argumentTypes == null ? new Type[] {} : argumentTypes;
        String methodDescriptor = Type.getMethodDescriptor(returnType,
                argumentTypes);

        // <access> <returnType> <name>(<argumentTypes> arguments) throws
        // <exceptions> {
        MethodVisitor mv = cv.visitMethod(access, name, methodDescriptor,
                signature, exceptions);
        mv.visitCode();

        // if (isInstrumentationActive() || !fromFileChannel) {
        isInstrumentationActive(mv);
        Label instrumentationActiveLabel = new Label();
        mv.visitJumpInsn(Opcodes.IFNE, instrumentationActiveLabel);

        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, instrumentedTypeInternalName,
                "fromFileChannel", Type.getDescriptor(Boolean.TYPE));
        Label fromFileChannelLabel = new Label();
        mv.visitJumpInsn(Opcodes.IFNE, fromFileChannelLabel);

        mv.visitLabel(instrumentationActiveLabel);

        // return? methodPrefix<name>(arguments);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        int argumentIndex = 1;
        for (Type argument : argumentTypes) {
            mv.visitVarInsn(argument.getOpcode(Opcodes.ILOAD), argumentIndex);
            argumentIndex += argument.getSize();
        }
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, instrumentedTypeInternalName,
                methodPrefix + name, methodDescriptor, false);
        if (!Type.VOID_TYPE.equals(returnType)) {
            mv.visitInsn(returnType.getOpcode(Opcodes.IRETURN));
        } else {
            mv.visitInsn(Opcodes.RETURN);
        }

        // }
        mv.visitLabel(fromFileChannelLabel);

        // setInstrumentationActive(true);
        setInstrumentationActive(mv, true);

        // long startTime = System.currentTimeMillis();
        int startTimeIndex = 1;
        for (Type argument : argumentTypes) {
            startTimeIndex += argument.getSize();
        }
        storeTime(mv, startTimeIndex);

        // <returnType> result =? methodPrefix<name>(arguments);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        argumentIndex = 1;
        for (Type argument : argumentTypes) {
            mv.visitVarInsn(argument.getOpcode(Opcodes.ILOAD), argumentIndex);
            argumentIndex += argument.getSize();
        }
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, instrumentedTypeInternalName,
                methodPrefix + name, methodDescriptor, false);
        int endTimeIndex = startTimeIndex + 2;
        if (!Type.VOID_TYPE.equals(returnType)) {
            mv.visitVarInsn(returnType.getOpcode(Opcodes.ISTORE),
                    startTimeIndex + 2);
            endTimeIndex += returnType.getSize();
        }

        // long endTime = System.currentTimeMillis();
        storeTime(mv, endTimeIndex);

        // callback.<callbackMethod>(startTime, endTime, result?);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, instrumentedTypeInternalName,
                "callback", callbackTypeDescriptor);
        mv.visitVarInsn(Opcodes.LLOAD, startTimeIndex);
        mv.visitVarInsn(Opcodes.LLOAD, endTimeIndex);

        // -1 indicates no result should be passed
        int resultIndex = resultPasser.getResultIndex();
        if (resultIndex != -1) {
            // result of the actual operation requested
            if (resultIndex == 0) {
                mv.visitVarInsn(returnType.getOpcode(Opcodes.ILOAD),
                        startTimeIndex + 2);
                resultPasser.passResult(mv);
            } else {
                // some parameter requested
                mv.visitVarInsn(
                        argumentTypes[resultIndex - 1].getOpcode(Opcodes.ILOAD),
                        resultIndex);
                resultPasser.passResult(mv);
            }
        }

        Type[] callbackArgumentTypes;
        if (additionalCallbackArgumentType == null) {
            callbackArgumentTypes = new Type[] { Type.LONG_TYPE,
                    Type.LONG_TYPE };
        } else {
            callbackArgumentTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE,
                    additionalCallbackArgumentType };
        }
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, callbackTypeInternalName,
                callbackName,
                Type.getMethodDescriptor(Type.VOID_TYPE, callbackArgumentTypes),
                false);

        // setInstrumentationActive(false);
        setInstrumentationActive(mv, false);

        // return result;?
        // }
        if (!Type.VOID_TYPE.equals(returnType)) {
            mv.visitVarInsn(returnType.getOpcode(Opcodes.ILOAD),
                    startTimeIndex + 2);
            mv.visitInsn(returnType.getOpcode(Opcodes.IRETURN));
        } else {
            mv.visitInsn(Opcodes.RETURN);
        }
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

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

        // bulk puts
        if ("put".equals(name)) {
            if (Type.getMethodDescriptor(Type.getType(ByteBuffer.class),
                    Type.getType(byte[].class), Type.INT_TYPE, Type.INT_TYPE)
                    .equals(desc) || Type
                            .getMethodDescriptor(Type.getType(ByteBuffer.class),
                                    Type.getType(ByteBuffer.class))
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
