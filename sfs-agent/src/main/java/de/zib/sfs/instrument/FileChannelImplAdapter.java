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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.objectweb.asm.ClassVisitor;
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
public class FileChannelImplAdapter extends AbstractSfsAdapter {

    public FileChannelImplAdapter(ClassVisitor cv, String methodPrefix)
            throws NoSuchMethodException, SecurityException {
        super(cv, FileChannelImpl.class, FileChannelImplCallback.class,
                methodPrefix);
    }

    @Override
    protected boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return isReadMethod(access, name, desc, signature, exceptions)

                || isWriteMethod(access, name, desc, signature, exceptions)
                || isTransferToMethod(access, name, desc, signature, exceptions)
                || isTransferFromMethod(access, name, desc, signature,
                        exceptions)
                || isMapMethod(access, name, desc, signature, exceptions);
    }

    @Override
    protected void appendWrappedMethods(ClassVisitor cv) {
        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "read", Type.INT_TYPE,
                new Type[] { Type.getType(ByteBuffer.class) }, null,
                new String[] { Type.getInternalName(IOException.class) },
                "readCallback", "writeCallback", Type.INT_TYPE, 0, false);

        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "read", Type.INT_TYPE,
                new Type[] { Type.getType(ByteBuffer.class), Type.LONG_TYPE },
                null, new String[] { Type.getInternalName(IOException.class) },
                "readCallback", "writeCallback", Type.INT_TYPE, 0, false);

        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "read", Type.LONG_TYPE,
                new Type[] { Type.getType(ByteBuffer[].class), Type.INT_TYPE,
                        Type.INT_TYPE },
                null, new String[] { Type.getInternalName(IOException.class) },
                "readCallback", "writeCallback", Type.LONG_TYPE, 0, false);

        // repeat for write methods

        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "write", Type.INT_TYPE,
                new Type[] { Type.getType(ByteBuffer.class) }, null,
                new String[] { Type.getInternalName(IOException.class) },
                "writeCallback", "readCallback", Type.INT_TYPE, 0, false);

        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "write", Type.INT_TYPE,
                new Type[] { Type.getType(ByteBuffer.class), Type.LONG_TYPE },
                null, new String[] { Type.getInternalName(IOException.class) },
                "writeCallback", "readCallback", Type.INT_TYPE, 0, false);

        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "write", Type.LONG_TYPE,
                new Type[] { Type.getType(ByteBuffer[].class), Type.INT_TYPE,
                        Type.INT_TYPE },
                null, new String[] { Type.getInternalName(IOException.class) },
                "writeCallback", "readCallback", Type.LONG_TYPE, 0, false);

        // transferTo is basically a read

        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "transferTo",
                Type.LONG_TYPE,
                new Type[] { Type.LONG_TYPE, Type.LONG_TYPE,
                        Type.getType(WritableByteChannel.class) },
                null, new String[] { Type.getInternalName(IOException.class) },
                "readCallback", "writeCallback", Type.LONG_TYPE, 2, true);

        // transferFrom is basically a write

        wrapFileChannelImplMethod(Opcodes.ACC_PUBLIC, "transferFrom",
                Type.LONG_TYPE,
                new Type[] { Type.getType(ReadableByteChannel.class),
                        Type.LONG_TYPE, Type.LONG_TYPE },
                null, new String[] { Type.getInternalName(IOException.class) },
                "writeCallback", "readCallback", Type.LONG_TYPE, 0, true);

        String mapMethodDescriptor = Type.getMethodDescriptor(
                Type.getType(MappedByteBuffer.class),
                Type.getType(MapMode.class), Type.LONG_TYPE, Type.LONG_TYPE);

        // public MappedByteBuffer map(MapMode mode, long position, long size)
        // throws IOException {
        MethodVisitor mapMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "map",
                mapMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        mapMV.visitCode();

        // MappedByteBuffer mbb = nativeMethodPrefixmap(mode, position, size);
        mapMV.visitVarInsn(Opcodes.ALOAD, 0);
        mapMV.visitVarInsn(Opcodes.ALOAD, 1);
        mapMV.visitVarInsn(Opcodes.LLOAD, 2);
        mapMV.visitVarInsn(Opcodes.LLOAD, 4);
        mapMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                instrumentedTypeInternalName, methodPrefix + "map",
                mapMethodDescriptor, false);
        mapMV.visitVarInsn(Opcodes.ASTORE, 6);

        // mbb.setFromFileChannel(true);
        mapMV.visitVarInsn(Opcodes.ALOAD, 6);
        mapMV.visitInsn(Opcodes.ICONST_1);
        mapMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(MappedByteBuffer.class),
                "setFromFileChannel",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                false);

        // return mbb;
        // }
        mapMV.visitVarInsn(Opcodes.ALOAD, 6);
        mapMV.visitInsn(Opcodes.ARETURN);
        mapMV.visitMaxs(0, 0);
        mapMV.visitEnd();
    }

    protected void wrapFileChannelImplMethod(int access, String name,
            Type returnType, Type[] argumentTypes, String signature,
            String[] exceptions, String callbackName,
            String oppositeCallbackName, Type additionalCallbackArgumentType,
            int bufferArgumentTypeIndex, boolean isTransferMethod) {
        String methodDescriptor = Type.getMethodDescriptor(returnType,
                argumentTypes);

        // <access> <returnType> <name>(<argumentTypes> arguments) throws
        // <exceptions> {
        MethodVisitor mv = cv.visitMethod(access, name, methodDescriptor,
                signature, exceptions);
        mv.visitCode();

        // if (instrumentationActive) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, instrumentedTypeInternalName,
                "instrumentationActive", Type.getDescriptor(Boolean.TYPE));
        Label instrumentationActiveLabel = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, instrumentationActiveLabel);

        // return methodPrefix<name>(arguments);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        int argumentIndex = 1;
        for (Type argument : argumentTypes) {
            mv.visitVarInsn(argument.getOpcode(Opcodes.ILOAD), argumentIndex);
            argumentIndex += argument.getSize();
        }
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, instrumentedTypeInternalName,
                methodPrefix + name, methodDescriptor, false);
        mv.visitInsn(returnType.getOpcode(Opcodes.IRETURN));

        // }
        mv.visitLabel(instrumentationActiveLabel);

        // instrumentationActive = true;
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.ICONST_1);
        mv.visitFieldInsn(Opcodes.PUTFIELD, instrumentedTypeInternalName,
                "instrumentationActive", Type.getDescriptor(Boolean.TYPE));

        // we need to set the instrumentation flag for the source/destination
        // buffer(s) as well

        // boolean bufferInstrumentationActive = false;
        int bufferInstrumentationActiveIndex = 1;
        for (Type argument : argumentTypes) {
            bufferInstrumentationActiveIndex += argument.getSize();
        }
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitVarInsn(Opcodes.ISTORE, bufferInstrumentationActiveIndex);

        // obtain actual index of the buffer(s) in the argument list
        int bufferArgumentIndex = 1;
        for (int i = 0; i < bufferArgumentTypeIndex; ++i) {
            bufferArgumentIndex += argumentTypes[i].getSize();
        }

        if (argumentTypes[bufferArgumentTypeIndex].getSort() == Type.ARRAY) {
            // If the first buffer in the array is a MappedByteBuffer, assume
            // they all are. If not, this will crash the users' program.

            // if (<buffers>[0] instanceof MappedByteBuffer) {
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitInsn(Opcodes.AALOAD);
            mv.visitTypeInsn(Opcodes.INSTANCEOF,
                    Type.getInternalName(MappedByteBuffer.class));
            Label bufferInstanceofMappedByteBufferLabel = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ,
                    bufferInstanceofMappedByteBufferLabel);

            // if (<buffers>[0].isFromFileChannel()) {
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitInsn(Opcodes.AALOAD);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(MappedByteBuffer.class),
                    "isFromFileChannel",
                    Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
            Label fromFileChannelLabel = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, fromFileChannelLabel);

            int iIndex = bufferInstrumentationActiveIndex + 1;

            // for (int i = 0; i < <buffers>.length; ++i) {
            // <buffers>[i].setInstrumentationActive(true);
            // }
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitVarInsn(Opcodes.ISTORE, iIndex);
            Label loopConditionLabel = new Label();
            mv.visitJumpInsn(Opcodes.GOTO, loopConditionLabel);
            Label loopStartLabel = new Label();
            mv.visitLabel(loopStartLabel);
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitVarInsn(Opcodes.ILOAD, iIndex);
            mv.visitInsn(Opcodes.AALOAD);
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(MappedByteBuffer.class),
                    "setInstrumentationActive",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);
            mv.visitIincInsn(iIndex, 1);
            mv.visitLabel(loopConditionLabel);
            mv.visitVarInsn(Opcodes.ILOAD, iIndex);
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitInsn(Opcodes.ARRAYLENGTH);
            mv.visitJumpInsn(Opcodes.IF_ICMPLT, loopStartLabel);

            // bufferInstrumentationActive = true;
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitVarInsn(Opcodes.ISTORE, bufferInstrumentationActiveIndex);

            // }
            mv.visitLabel(fromFileChannelLabel);

            // }
            mv.visitLabel(bufferInstanceofMappedByteBufferLabel);
        } else {
            // We need to handle the transferFrom/transferTo methods a little
            // differently. Their "buffers" only need to be FileChannelImpls,
            // the rest remains the same.

            // if (buffer instanceof MappedByteBuffer) {
            // if (buffer instanceof FileChannelImpl) {
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            if (!isTransferMethod) {
                mv.visitTypeInsn(Opcodes.INSTANCEOF,
                        Type.getInternalName(MappedByteBuffer.class));
            } else {
                mv.visitTypeInsn(Opcodes.INSTANCEOF,
                        Type.getInternalName(FileChannelImpl.class));
            }
            Label bufferInstanceofMappedByteBufferLabel = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ,
                    bufferInstanceofMappedByteBufferLabel);

            // additional check required if the buffer is a MappedByteBuffer
            Label fromFileChannelLabel = new Label();
            if (!isTransferMethod) {
                // if (buffer.isFromFileChannel()) {
                mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(MappedByteBuffer.class),
                        "isFromFileChannel",
                        Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
                mv.visitJumpInsn(Opcodes.IFEQ, fromFileChannelLabel);
            }

            // buffer.setInstrumentationActive(true);
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    !isTransferMethod
                            ? Type.getInternalName(MappedByteBuffer.class)
                            : Type.getInternalName(FileChannelImpl.class),
                    "setInstrumentationActive",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);

            // bufferInstrumentationActive = true;
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitVarInsn(Opcodes.ISTORE, bufferInstrumentationActiveIndex);

            if (!isTransferMethod) {
                // }
                mv.visitLabel(fromFileChannelLabel);
            }

            // }
            mv.visitLabel(bufferInstanceofMappedByteBufferLabel);
        }

        // long startTime = System.currentTimeMillis();
        int startTimeIndex = bufferInstrumentationActiveIndex + 1;
        storeTime(mv, startTimeIndex);

        // <returnType> result = methodPrefix<name>(arguments);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        argumentIndex = 1;
        for (Type argument : argumentTypes) {
            mv.visitVarInsn(argument.getOpcode(Opcodes.ILOAD), argumentIndex);
            argumentIndex += argument.getSize();
        }
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, instrumentedTypeInternalName,
                methodPrefix + name, methodDescriptor, false);
        int resultIndex = startTimeIndex + 2;
        mv.visitVarInsn(returnType.getOpcode(Opcodes.ISTORE), resultIndex);
        int endTimeIndex = resultIndex + returnType.getSize();

        // long endTime = System.currentTimeMillis();
        storeTime(mv, endTimeIndex);

        // callback.<callbackName>(startTime, endTime, result);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, instrumentedTypeInternalName,
                "callback", callbackTypeDescriptor);
        mv.visitVarInsn(Opcodes.LLOAD, startTimeIndex);
        mv.visitVarInsn(Opcodes.LLOAD, endTimeIndex);
        mv.visitVarInsn(returnType.getOpcode(Opcodes.ILOAD), resultIndex);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, callbackTypeInternalName,
                callbackName,
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE, additionalCallbackArgumentType),
                false);

        // if (bufferInstrumentationActive) {
        mv.visitVarInsn(Opcodes.ILOAD, bufferInstrumentationActiveIndex);
        Label bufferInstrumentationActiveLabel = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, bufferInstrumentationActiveLabel);

        // callback.<oppositeCallbackName>(startTime, endTime, result);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, instrumentedTypeInternalName,
                "callback", callbackTypeDescriptor);
        mv.visitVarInsn(Opcodes.LLOAD, startTimeIndex);
        mv.visitVarInsn(Opcodes.LLOAD, endTimeIndex);
        mv.visitVarInsn(returnType.getOpcode(Opcodes.ILOAD), resultIndex);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, callbackTypeInternalName,
                oppositeCallbackName,
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE, additionalCallbackArgumentType),
                false);

        // revert the active instrumentation flag for the buffer
        if (argumentTypes[bufferArgumentTypeIndex].getSort() == Type.ARRAY) {
            int iIndex = bufferInstrumentationActiveIndex + 1;

            // for (int i = 0; i < <buffers>.length; ++i) {
            // <buffers>[i].setInstrumentationActive(false);
            // }
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitVarInsn(Opcodes.ISTORE, iIndex);
            Label loopConditionLabel = new Label();
            mv.visitJumpInsn(Opcodes.GOTO, loopConditionLabel);
            Label loopStartLabel = new Label();
            mv.visitLabel(loopStartLabel);
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitVarInsn(Opcodes.ILOAD, iIndex);
            mv.visitInsn(Opcodes.AALOAD);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(MappedByteBuffer.class),
                    "setInstrumentationActive",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);
            mv.visitIincInsn(iIndex, 1);
            mv.visitLabel(loopConditionLabel);
            mv.visitVarInsn(Opcodes.ILOAD, iIndex);
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitInsn(Opcodes.ARRAYLENGTH);
            mv.visitJumpInsn(Opcodes.IF_ICMPLT, loopStartLabel);
        } else {
            // buffer.setInstrumentationActive(false);
            mv.visitVarInsn(Opcodes.ALOAD, bufferArgumentIndex);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    !isTransferMethod
                            ? Type.getInternalName(MappedByteBuffer.class)
                            : Type.getInternalName(FileChannelImpl.class),
                    "setInstrumentationActive",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                    false);
        }

        // }
        mv.visitLabel(bufferInstrumentationActiveLabel);

        // instrumentationActive = false;
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitFieldInsn(Opcodes.PUTFIELD, instrumentedTypeInternalName,
                "instrumentationActive", Type.getDescriptor(Boolean.TYPE));

        // return result;
        // }
        mv.visitVarInsn(returnType.getOpcode(Opcodes.ILOAD), resultIndex);
        mv.visitInsn(returnType.getOpcode(Opcodes.IRETURN));
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

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

    private boolean isTransferToMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC && "transferTo".equals(name)
                && Type.getMethodDescriptor(Type.LONG_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE, Type.getType(WritableByteChannel.class))
                        .equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isTransferFromMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC && "transferFrom".equals(name)
                && Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(ReadableByteChannel.class), Type.LONG_TYPE,
                        Type.LONG_TYPE).equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isMapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC && "map".equals(name)
                && Type.getMethodDescriptor(
                        Type.getType(MappedByteBuffer.class),
                        Type.getType(MapMode.class), Type.LONG_TYPE,
                        Type.LONG_TYPE).equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }
}
