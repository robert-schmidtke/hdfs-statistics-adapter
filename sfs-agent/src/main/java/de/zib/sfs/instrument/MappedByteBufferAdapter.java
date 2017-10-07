/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;
import java.nio.MappedByteBuffer;
import java.util.Set;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import de.zib.sfs.instrument.statistics.OperationCategory;

public class MappedByteBufferAdapter extends AbstractSfsAdapter {

    public MappedByteBufferAdapter(ClassVisitor cv,
            Set<OperationCategory> skip) {
        super(cv, MappedByteBuffer.class, skip);
    }

    @Override
    protected void appendFields(ClassVisitor cv) {
        // add protected field that DirectByteBuffer can access to see if it was
        // created by a FileChannelImpl

        // protected boolean fromFileChannel;
        FieldVisitor fromFileChannelFV = cv.visitField(Opcodes.ACC_PROTECTED,
                "fromFileChannel", Type.getDescriptor(Boolean.TYPE), null,
                null);
        fromFileChannelFV.visitEnd();

        // protected FileDescriptor fileDescriptor;
        FieldVisitor filenameFV = cv.visitField(Opcodes.ACC_PROTECTED,
                "fileDescriptor", Type.getDescriptor(FileDescriptor.class),
                null, null);
        filenameFV.visitEnd();
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc,
            String signature, Object value) {
        return super.visitField(access, name, desc, signature, value);
    }

    @Override
    protected void initializeFields(MethodVisitor constructorMV,
            String constructorDesc) {
        // fromFileChannel = false;
        constructorMV.visitVarInsn(Opcodes.ALOAD, 0);
        constructorMV.visitInsn(Opcodes.ICONST_0);
        constructorMV.visitFieldInsn(Opcodes.PUTFIELD,
                Type.getInternalName(MappedByteBuffer.class), "fromFileChannel",
                Type.getDescriptor(Boolean.TYPE));
    }

    @Override
    protected boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return false;
    }

    @Override
    protected void appendWrappedMethods(ClassVisitor cv) {
        // public void setFromFileChannel(boolean fromFileChannel) {
        MethodVisitor setFromFileChannelMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                "setFromFileChannel",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                null, null);
        setFromFileChannelMV.visitCode();

        // this.fromFileChannel = fromFileChannel;
        setFromFileChannelMV.visitVarInsn(Opcodes.ALOAD, 0);
        setFromFileChannelMV.visitVarInsn(Opcodes.ILOAD, 1);
        setFromFileChannelMV.visitFieldInsn(Opcodes.PUTFIELD,
                Type.getInternalName(MappedByteBuffer.class), "fromFileChannel",
                Type.getDescriptor(Boolean.TYPE));

        // }
        setFromFileChannelMV.visitInsn(Opcodes.RETURN);
        setFromFileChannelMV.visitMaxs(0, 0);
        setFromFileChannelMV.visitEnd();

        // public boolean isFromFileChannel() {
        MethodVisitor isFromFileChannelMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                "isFromFileChannel",
                Type.getMethodDescriptor(Type.BOOLEAN_TYPE), null, null);
        isFromFileChannelMV.visitCode();

        // return fromFileChannel;
        // }
        isFromFileChannelMV.visitVarInsn(Opcodes.ALOAD, 0);
        isFromFileChannelMV.visitFieldInsn(Opcodes.GETFIELD,
                Type.getInternalName(MappedByteBuffer.class), "fromFileChannel",
                Type.getDescriptor(Boolean.TYPE));
        isFromFileChannelMV.visitInsn(Opcodes.IRETURN);
        isFromFileChannelMV.visitMaxs(0, 0);
        isFromFileChannelMV.visitEnd();

        // public void setFileDescriptor(FileDescriptor fileDescriptor) {
        MethodVisitor settFileDescriptorMV = cv
                .visitMethod(Opcodes.ACC_PUBLIC, "setFileDescriptor",
                        Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.getType(FileDescriptor.class)),
                        null, null);
        settFileDescriptorMV.visitCode();

        // this.fileDescriptor = fileDescriptor;
        settFileDescriptorMV.visitVarInsn(Opcodes.ALOAD, 0);
        settFileDescriptorMV.visitVarInsn(Opcodes.ALOAD, 1);
        settFileDescriptorMV.visitFieldInsn(Opcodes.PUTFIELD,
                Type.getInternalName(MappedByteBuffer.class), "fileDescriptor",
                Type.getDescriptor(FileDescriptor.class));

        // }
        settFileDescriptorMV.visitInsn(Opcodes.RETURN);
        settFileDescriptorMV.visitMaxs(0, 0);
        settFileDescriptorMV.visitEnd();

        // public FileDescriptor getFileDescriptor() {
        MethodVisitor getFileDescriptorMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                "getFileDescriptor",
                Type.getMethodDescriptor(Type.getType(FileDescriptor.class)),
                null, null);
        getFileDescriptorMV.visitCode();

        // return getFileDescriptorImpl();
        // }
        getFileDescriptorMV.visitVarInsn(Opcodes.ALOAD, 0);
        getFileDescriptorMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                instrumentedTypeInternalName, "getFileDescriptorImpl",
                Type.getMethodDescriptor(Type.getType(FileDescriptor.class)),
                false);
        getFileDescriptorMV.visitInsn(Opcodes.ARETURN);
        getFileDescriptorMV.visitMaxs(0, 0);
        getFileDescriptorMV.visitEnd();

        // protected FileDescriptor getFileDescriptorImpl() {
        MethodVisitor getFileDescriptorImplMV = cv.visitMethod(
                Opcodes.ACC_PROTECTED, "getFileDescriptorImpl",
                Type.getMethodDescriptor(Type.getType(FileDescriptor.class)),
                null, null);
        getFileDescriptorImplMV.visitCode();

        // return fileDescriptor;
        // }
        getFileDescriptorImplMV.visitVarInsn(Opcodes.ALOAD, 0);
        getFileDescriptorImplMV.visitFieldInsn(Opcodes.GETFIELD,
                Type.getInternalName(MappedByteBuffer.class), "fileDescriptor",
                Type.getDescriptor(FileDescriptor.class));
        getFileDescriptorImplMV.visitInsn(Opcodes.ARETURN);
        getFileDescriptorImplMV.visitMaxs(0, 0);
        getFileDescriptorImplMV.visitEnd();

        cv.visitEnd();
    }

}
