/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.zip.ZipFile;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

import de.zib.sfs.instrument.statistics.OperationCategory;

public class ZipFileAdapter extends ClassVisitor {

    private final String methodPrefix;

    private final String constructorDescriptor;

    private final Set<OperationCategory> skip;

    protected ZipFileAdapter(ClassVisitor cv, String methodPrefix,
            Set<OperationCategory> skip) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = methodPrefix;

        Constructor<ZipFile> constructor;
        try {
            constructor = ZipFile.class.getConstructor(File.class, Integer.TYPE,
                    Charset.class);
            this.constructorDescriptor = Type
                    .getConstructorDescriptor(constructor);
        } catch (Exception e) {
            throw new RuntimeException("Could not access constructor", e);
        }

        this.skip = skip;
    }

    @Override
    public void visitSource(String source, String debug) {
        if (!this.skip.contains(OperationCategory.ZIP)) {
            // private long startTime;
            FieldVisitor startTimeFV = this.cv.visitField(Opcodes.ACC_PRIVATE,
                    "startTime", Type.getDescriptor(Long.TYPE), null, null);
            startTimeFV.visitEnd();
        }

        this.cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if (isConstructor(access, name, desc, signature, exceptions)) {
            mv = new ConstructorAdapter(this.cv.visitMethod(access, name, desc,
                    signature, exceptions), access, name, desc);
        } else if (isCloseMethod(access, name, desc, signature, exceptions)) {
            mv = this.cv.visitMethod(access, this.methodPrefix + name, desc,
                    signature, exceptions);
        } else {
            mv = this.cv.visitMethod(access, name, desc, signature, exceptions);
        }
        return mv;
    }

    @Override
    public void visitEnd() {
        if (!this.skip.contains(OperationCategory.ZIP)) {
            // public void close() {
            MethodVisitor closeMethodMV = this.cv.visitMethod(
                    Opcodes.ACC_PUBLIC, "close",
                    Type.getMethodDescriptor(Type.VOID_TYPE), null,
                    new String[] { Type.getInternalName(IOException.class) });
            closeMethodMV.visitCode();

            // ZipFileCallback.closeCallback(jzfile);
            closeMethodMV.visitVarInsn(Opcodes.ALOAD, 0);
            closeMethodMV.visitFieldInsn(Opcodes.GETFIELD,
                    Type.getInternalName(ZipFile.class), "jzfile",
                    Type.getDescriptor(Long.TYPE));
            closeMethodMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                    Type.getInternalName(ZipFileCallback.class),
                    "closeCallback",
                    Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE),
                    false);

            // methodPrefixclose();
            closeMethodMV.visitVarInsn(Opcodes.ALOAD, 0);
            closeMethodMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    Type.getInternalName(ZipFile.class),
                    this.methodPrefix + "close",
                    Type.getMethodDescriptor(Type.VOID_TYPE), false);

            // }
            closeMethodMV.visitInsn(Opcodes.RETURN);
            closeMethodMV.visitMaxs(0, 0);
            closeMethodMV.visitEnd();
        }

        this.cv.visitEnd();
    }

    /**
     * Times execution of the entire constructor, storing the startTime in a
     * private field to avoid cluttering the local stack of the constructor.
     * 
     * @author robert
     *
     */
    protected class ConstructorAdapter extends AdviceAdapter {

        protected ConstructorAdapter(MethodVisitor mv, int access, String name,
                String desc) {
            super(Opcodes.ASM5, mv, access, name, desc);
        }

        @Override
        protected void onMethodEnter() {
            // startTime = System.nanoTime();
            this.mv.visitVarInsn(Opcodes.ALOAD, 0);
            this.mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                    Type.getInternalName(System.class), "nanoTime",
                    Type.getMethodDescriptor(Type.LONG_TYPE), false);
            this.mv.visitFieldInsn(Opcodes.PUTFIELD,
                    Type.getInternalName(ZipFile.class), "startTime",
                    Type.getDescriptor(Long.TYPE));
        }

        @Override
        protected void onMethodExit(int opcode) {
            if (opcode == Opcodes.RETURN) {
                // ZipFileCallback.constructorCallback(startTime,
                // System.nanoTime(), name, jzfile, file.length());
                this.mv.visitVarInsn(Opcodes.ALOAD, 0);
                this.mv.visitFieldInsn(Opcodes.GETFIELD,
                        Type.getInternalName(ZipFile.class), "startTime",
                        Type.getDescriptor(Long.TYPE));
                this.mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(System.class), "nanoTime",
                        Type.getMethodDescriptor(Type.LONG_TYPE), false);
                this.mv.visitVarInsn(Opcodes.ALOAD, 0);
                this.mv.visitFieldInsn(Opcodes.GETFIELD,
                        Type.getInternalName(ZipFile.class), "name",
                        Type.getDescriptor(String.class));
                this.mv.visitVarInsn(Opcodes.ALOAD, 0);
                this.mv.visitFieldInsn(Opcodes.GETFIELD,
                        Type.getInternalName(ZipFile.class), "jzfile",
                        Type.getDescriptor(Long.TYPE));
                this.mv.visitVarInsn(Opcodes.ALOAD, 1);
                this.mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(File.class), "length",
                        Type.getMethodDescriptor(Type.LONG_TYPE), false);
                this.mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(ZipFileCallback.class),
                        "constructorCallback",
                        Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE, Type.getType(String.class),
                                Type.LONG_TYPE, Type.LONG_TYPE),
                        false);
            }
        }

    }

    private boolean isConstructor(int access, String name, String desc,
            String signature, String[] exceptions) {
        return Opcodes.ACC_PUBLIC == access && "<init>".equals(name)
                && desc.equals(this.constructorDescriptor) && null == signature
                && null != exceptions && exceptions.length == 1
                && Type.getInternalName(IOException.class).equals(exceptions[0])
                && !this.skip.contains(OperationCategory.ZIP);
    }

    private boolean isCloseMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return Opcodes.ACC_PUBLIC == access && "close".equals(name)
                && desc.equals(Type.getMethodDescriptor(Type.VOID_TYPE))
                && signature == null && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class).equals(exceptions[0])
                && !this.skip.contains(OperationCategory.ZIP);
    }

}
