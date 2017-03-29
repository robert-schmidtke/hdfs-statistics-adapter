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
import java.util.zip.ZipFile;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

public class ZipFileAdapter extends ClassVisitor {

    private final String methodPrefix;

    private final String constructorDescriptor;

    protected ZipFileAdapter(ClassVisitor cv, String methodPrefix) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = methodPrefix;

        Constructor<ZipFile> constructor;
        try {
            constructor = ZipFile.class.getConstructor(File.class, Integer.TYPE,
                    Charset.class);
            constructorDescriptor = Type.getConstructorDescriptor(constructor);
        } catch (Exception e) {
            throw new RuntimeException("Could not access constructor", e);
        }
    }

    @Override
    public void visitSource(String source, String debug) {
        // private long startTime;
        FieldVisitor startTimeFV = cv.visitField(Opcodes.ACC_PRIVATE,
                "startTime", Type.getDescriptor(Long.TYPE), null, null);
        startTimeFV.visitEnd();

        cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if (isConstructor(access, name, desc, signature, exceptions)) {
            mv = new ConstructorAdapter(
                    cv.visitMethod(access, name, desc, signature, exceptions),
                    access, name, desc);
        } else if (isCloseMethod(access, name, desc, signature, exceptions)) {
            mv = cv.visitMethod(access, methodPrefix + name, desc, signature,
                    exceptions);
        } else {
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        }
        return mv;
    }

    @Override
    public void visitEnd() {
        // public void close() {
        MethodVisitor closeMethodMV = cv.visitMethod(Opcodes.ACC_PUBLIC,
                "close", Type.getMethodDescriptor(Type.VOID_TYPE), null,
                new String[] { Type.getInternalName(IOException.class) });
        closeMethodMV.visitCode();

        // ZipFileCallback.closeCallback(jzfile);
        closeMethodMV.visitVarInsn(Opcodes.ALOAD, 0);
        closeMethodMV.visitFieldInsn(Opcodes.GETFIELD,
                Type.getInternalName(ZipFile.class), "jzfile",
                Type.getDescriptor(Long.TYPE));
        closeMethodMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                Type.getInternalName(ZipFileCallback.class), "closeCallback",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE),
                false);

        // methodPrefixclose();
        closeMethodMV.visitVarInsn(Opcodes.ALOAD, 0);
        closeMethodMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                Type.getInternalName(ZipFile.class), methodPrefix + "close",
                Type.getMethodDescriptor(Type.VOID_TYPE), false);

        // }
        closeMethodMV.visitInsn(Opcodes.RETURN);
        closeMethodMV.visitMaxs(0, 0);
        closeMethodMV.visitEnd();

        cv.visitEnd();
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
            // startTime = System.currentTimeMillis();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                    Type.getInternalName(System.class), "currentTimeMillis",
                    Type.getMethodDescriptor(Type.LONG_TYPE), false);
            mv.visitFieldInsn(Opcodes.PUTFIELD,
                    Type.getInternalName(ZipFile.class), "startTime",
                    Type.getDescriptor(Long.TYPE));
        }

        @Override
        protected void onMethodExit(int opcode) {
            if (opcode == Opcodes.RETURN) {
                // ZipFileCallback.constructorCallback(startTime,
                // System.currentTimeMillis(), jzfile, file.length());
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD,
                        Type.getInternalName(ZipFile.class), "startTime",
                        Type.getDescriptor(Long.TYPE));
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(System.class), "currentTimeMillis",
                        Type.getMethodDescriptor(Type.LONG_TYPE), false);
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD,
                        Type.getInternalName(ZipFile.class), "jzfile",
                        Type.getDescriptor(Long.TYPE));
                mv.visitVarInsn(Opcodes.ALOAD, 1);
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(File.class), "length",
                        Type.getMethodDescriptor(Type.LONG_TYPE), false);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(ZipFileCallback.class),
                        "constructorCallback",
                        Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE),
                        false);
            }
        }

    }

    private boolean isConstructor(int access, String name, String desc,
            String signature, String[] exceptions) {
        return Opcodes.ACC_PUBLIC == access && "<init>".equals(name)
                && desc.equals(constructorDescriptor) && null == signature
                && null != exceptions && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isCloseMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return Opcodes.ACC_PUBLIC == access && "close".equals(name)
                && desc.equals(Type.getMethodDescriptor(Type.VOID_TYPE))
                && signature == null && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

}
