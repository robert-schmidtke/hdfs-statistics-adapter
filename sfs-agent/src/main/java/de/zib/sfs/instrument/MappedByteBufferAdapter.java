/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

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

        // protected String filename;
        FieldVisitor filenameFV = cv.visitField(Opcodes.ACC_PROTECTED,
                "filename", Type.getDescriptor(String.class), null, null);
        filenameFV.visitEnd();
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

        // public void setFilename(String filename) {
        MethodVisitor setFilenameMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                "setFilename", Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class)),
                null, null);
        setFilenameMV.visitCode();

        // this.filename = filename;
        setFilenameMV.visitVarInsn(Opcodes.ALOAD, 0);
        setFilenameMV.visitVarInsn(Opcodes.ALOAD, 1);
        setFilenameMV.visitFieldInsn(Opcodes.PUTFIELD,
                Type.getInternalName(MappedByteBuffer.class), "filename",
                Type.getDescriptor(String.class));

        // }
        setFilenameMV.visitInsn(Opcodes.RETURN);
        setFilenameMV.visitMaxs(0, 0);
        setFilenameMV.visitEnd();

        cv.visitEnd();
    }

}
