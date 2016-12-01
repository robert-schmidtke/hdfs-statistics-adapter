/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Class adapter that instruments {@link java.io.FileInputStream}.
 * 
 * @author robert
 *
 */
public class FileInputStreamAdapter extends ClassVisitor {

    private final String nativeMethodPrefix;

    /**
     * Construct a visitor that modifies an {@link java.io.FileInputStream}'s
     * read calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     * @param nativeMethodPrefix
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public FileInputStreamAdapter(ClassVisitor cv, String nativeMethodPrefix)
            throws NoSuchMethodException, SecurityException {
        super(Opcodes.ASM5, cv);
        this.nativeMethodPrefix = nativeMethodPrefix;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // rename native methods so we can wrap them
        MethodVisitor mv;
        if (isOpenMethod(access, name, desc, signature, exceptions)) {
            mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
                    signature, exceptions);
        } else if (isReadMethod(access, name, desc, signature, exceptions)) {
            mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
                    signature, exceptions);
        } else if (isReadBytesMethod(access, name, desc, signature, exceptions)) {
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
        String fileInputStreamInternalName = Type
                .getInternalName(FileInputStream.class);
        String fileInputStreamCallbackInternalName = Type
                .getInternalName(FileInputStreamCallback.class);
        String fileInputStreamCallbackGetInstanceMethodDescriptor = Type
                .getMethodDescriptor(
                        Type.getType(FileInputStreamCallback.class),
                        Type.getType(FileInputStream.class));

        String openMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.getType(String.class));
        String readMethodDescriptor = Type.getMethodDescriptor(Type.INT_TYPE);
        String readBytesMethodDescriptor = Type.getMethodDescriptor(
                Type.INT_TYPE, Type.getType(byte[].class), Type.INT_TYPE,
                Type.INT_TYPE);

        // private void open(String name) {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                openMethodDescriptor, null, new String[] { Type
                        .getInternalName(FileNotFoundException.class) });
        openMV.visitCode();

        // long startTime =
        // FileInputStreamCallback.getInstance(this).onOpenBegin(name);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName,
                "onOpenBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(String.class)), false);
        openMV.visitVarInsn(Opcodes.LSTORE, 2);

        // nativeMethodPrefixopen(name);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "open",
                openMethodDescriptor, false);

        // FileInputStreamCallback.getInstance(this).onOpenEnd(startTime, name);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        openMV.visitVarInsn(Opcodes.LLOAD, 2);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName,
                "onOpenEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.getType(String.class)), false);

        // }
        openMV.visitInsn(Opcodes.RETURN);
        openMV.visitMaxs(0, 0);
        openMV.visitEnd();

        // public int read() {
        MethodVisitor readMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                readMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        readMV.visitCode();

        // long startTime =
        // FileInputStreamCallback.getInstance(this).onReadBegin();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE), false);
        readMV.visitVarInsn(Opcodes.LSTORE, 1);

        // int readResult = nativeMethodPrefixread();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "read",
                readMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.ISTORE, 3);

        // FileInputStreamCallback.getInstance(this).onReadEnd(startTime,
        // readResult);
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.LLOAD, 1);
        readMV.visitVarInsn(Opcodes.ILOAD, 3);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE), false);

        // return readResult;
        // }
        readMV.visitVarInsn(Opcodes.ILOAD, 3);
        readMV.visitInsn(Opcodes.IRETURN);
        readMV.visitMaxs(0, 0);
        readMV.visitEnd();

        // private int readBytes(byte[] b, int off, int len) {
        MethodVisitor readBytesMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "readBytes", readBytesMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        readBytesMV.visitCode();

        // long startTime =
        // FileInputStreamCallback.getInstance(this).onReadBytesBegin(b, off,
        // len);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        readBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadBytesBegin", Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE), false);
        readBytesMV.visitVarInsn(Opcodes.LSTORE, 4);

        // int readBytesResult = nativeMethodPrefixreadBytes(b, off, len);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        readBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "readBytes",
                readBytesMethodDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.ISTORE, 6);

        // FileInputStreamCallback.getInstance(this).onReadBytesEnd(startTime,
        // readBytesResult, b, off, len);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.LLOAD, 4);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 6);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        readBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadBytesEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE, Type.getType(byte[].class),
                                Type.INT_TYPE, Type.INT_TYPE), false);

        // return readBytesResult;
        // }
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 6);
        readBytesMV.visitInsn(Opcodes.IRETURN);
        readBytesMV.visitMaxs(0, 0);
        readBytesMV.visitEnd();

        cv.visitEnd();
    }

    // Helper methods

    private boolean isOpenMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the generated accessor method and not the native open0 method
        // itself
        return access == Opcodes.ACC_PRIVATE
                && "open".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class)).equals(desc)
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(FileNotFoundException.class).equals(
                        exceptions[0]);
    }

    private boolean isReadMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the generated accessor method and not the native read0 method
        // itself
        return access == Opcodes.ACC_PUBLIC
                && "read".equals(name)
                && Type.getMethodDescriptor(Type.INT_TYPE).equals(desc)
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isReadBytesMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the native method itself, because readBytes has no accessor
        // method and there is no native readBytes0 method
        return access == (Opcodes.ACC_PRIVATE | Opcodes.ACC_NATIVE)
                && "readBytes".equals(name)
                && Type.getMethodDescriptor(Type.INT_TYPE,
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE).equals(desc)
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }
}
