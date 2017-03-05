/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.nio.MappedByteBuffer;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

public class MappedByteBufferAdapter extends ClassVisitor {

    public MappedByteBufferAdapter(ClassVisitor cv, String nativeMethodPrefix) {
        super(Opcodes.ASM5, cv);
    }

    @Override
    public void visitSource(String source, String debug) {
        // add protected field that DirectByteBuffer can access to see if it was
        // created by a FileChannelImpl

        // protected boolean fromFileChannel;
        FieldVisitor fromFileChannelFV = cv.visitField(Opcodes.ACC_PROTECTED,
                "fromFileChannel", Type.getDescriptor(Boolean.TYPE), null,
                null);
        fromFileChannelFV.visitEnd();

        cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        if ("<init>".equals(name)) {
            return new AdviceAdapter(api,
                    cv.visitMethod(access, name, desc, signature, exceptions),
                    access, name, desc) {

                @Override
                protected void onMethodEnter() {
                    // fromFileChannel = false;
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitInsn(Opcodes.ICONST_0);
                    mv.visitFieldInsn(Opcodes.PUTFIELD,
                            Type.getInternalName(MappedByteBuffer.class),
                            "fromFileChannel",
                            Type.getDescriptor(Boolean.TYPE));
                }

            };
        } else {
            return cv.visitMethod(access, name, desc, signature, exceptions);
        }
    }

    @Override
    public void visitEnd() {
        // public void setFromFileChannel(boolean fromFileChannel) {
        MethodVisitor setFromFileChannelMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                "setFromFileChannel",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                null, null);
        setFromFileChannelMV.visitCode();

        // fromFileChannel = false;
        setFromFileChannelMV.visitVarInsn(Opcodes.ALOAD, 0);
        setFromFileChannelMV.visitVarInsn(Opcodes.ILOAD, 1);
        setFromFileChannelMV.visitFieldInsn(Opcodes.PUTFIELD,
                Type.getInternalName(MappedByteBuffer.class), "fromFileChannel",
                Type.getDescriptor(Boolean.TYPE));

        // }
        setFromFileChannelMV.visitInsn(Opcodes.RETURN);
        setFromFileChannelMV.visitMaxs(0, 0);
        setFromFileChannelMV.visitEnd();

        cv.visitEnd();
    }

}
