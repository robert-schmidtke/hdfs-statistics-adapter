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
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
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
        MethodVisitor mv;
        if (isOpenMethod(access, name, desc, signature, exceptions)
                || isReadMethod(access, name, desc, signature, exceptions)
                || isReadBytesMethod(access, name, desc, signature, exceptions)) {
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
        // general descriptors needed to add methods to FileInputStream
        String fileInputStreamInternalName = Type
                .getInternalName(FileInputStream.class);
        String fileInputStreamCallbackInternalName = Type
                .getInternalName(FileInputStreamCallback.class);

        // descriptors of the methods we add to FileInputStream
        String getCallbackMethodDescriptor = Type.getMethodDescriptor(Type
                .getType(FileInputStreamCallback.class));
        String openMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.getType(String.class));
        String readMethodDescriptor = Type.getMethodDescriptor(Type.INT_TYPE);
        String readBytesMethodDescriptor = Type.getMethodDescriptor(
                Type.INT_TYPE, Type.getType(byte[].class), Type.INT_TYPE,
                Type.INT_TYPE);

        // private FileInputStreamCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE,
                "callback", Type.getDescriptor(FileInputStreamCallback.class),
                null, null);
        callbackFV.visitEnd();

        // private FileInputStreamCallback getCallback() {
        MethodVisitor getCallbackMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "getCallback", getCallbackMethodDescriptor, null, null);
        getCallbackMV.visitCode();

        // if (callback == null) {
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                fileInputStreamInternalName, "callback",
                Type.getDescriptor(FileInputStreamCallback.class));
        Label callbackNonNullLabel = new Label();
        getCallbackMV.visitJumpInsn(Opcodes.IFNONNULL, callbackNonNullLabel);

        // callback = new FileInputStreamCallback(this);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitTypeInsn(Opcodes.NEW,
                fileInputStreamCallbackInternalName);
        getCallbackMV.visitInsn(Opcodes.DUP);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        try {
            getCallbackMV.visitMethodInsn(Opcodes.INVOKESPECIAL, Type
                    .getInternalName(FileInputStreamCallback.class), "<init>",
                    Type.getConstructorDescriptor(FileInputStreamCallback.class
                            .getConstructor(FileInputStream.class)), false);
        } catch (Exception e) {
            throw new RuntimeException("Could not access constructor", e);
        }
        getCallbackMV.visitFieldInsn(Opcodes.PUTFIELD,
                fileInputStreamInternalName, "callback",
                Type.getDescriptor(FileInputStreamCallback.class));

        // }
        getCallbackMV.visitLabel(callbackNonNullLabel);

        // return callback;
        // }
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                fileInputStreamInternalName, "callback",
                Type.getDescriptor(FileInputStreamCallback.class));
        getCallbackMV.visitInsn(Opcodes.ARETURN);
        getCallbackMV.visitMaxs(0, 0);
        getCallbackMV.visitEnd();

        // private void open(String name) {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                openMethodDescriptor, null, new String[] { Type
                        .getInternalName(FileNotFoundException.class) });
        openMV.visitCode();

        // FileInputStreamCallback cb = getCallback();
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        openMV.visitVarInsn(Opcodes.ASTORE, 2);

        // long startTime = cb.onOpenBegin(name);
        openMV.visitVarInsn(Opcodes.ALOAD, 2);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName,
                "onOpenBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(String.class)), false);
        openMV.visitVarInsn(Opcodes.LSTORE, 3);

        // nativeMethodPrefixopen(name);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "open",
                openMethodDescriptor, false);

        // cb.onOpenEnd(startTime, name);
        openMV.visitVarInsn(Opcodes.ALOAD, 2);
        openMV.visitVarInsn(Opcodes.LLOAD, 3);
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

        // FileInputStreamCallback cb = getCallback();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.ASTORE, 1);

        // long startTime = cb.onReadBegin();
        readMV.visitVarInsn(Opcodes.ALOAD, 1);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE), false);
        readMV.visitVarInsn(Opcodes.LSTORE, 2);

        // int readResult = nativeMethodPrefixread();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "read",
                readMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.ISTORE, 4);

        // cb.onReadEnd(startTime, readResult);
        readMV.visitVarInsn(Opcodes.ALOAD, 1);
        readMV.visitVarInsn(Opcodes.LLOAD, 2);
        readMV.visitVarInsn(Opcodes.ILOAD, 4);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE), false);

        // return readResult;
        // }
        readMV.visitVarInsn(Opcodes.ILOAD, 4);
        readMV.visitInsn(Opcodes.IRETURN);
        readMV.visitMaxs(0, 0);
        readMV.visitEnd();

        // private int readBytes(byte[] b, int off, int len) {
        MethodVisitor readBytesMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "readBytes", readBytesMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        readBytesMV.visitCode();

        // FileInputStreamCallback cb = getCallback();
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.ASTORE, 4);

        // long startTime = cb.onReadBytesBegin(b, off, len);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 4);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        readBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadBytesBegin", Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE), false);
        readBytesMV.visitVarInsn(Opcodes.LSTORE, 5);

        // int readBytesResult = nativeMethodPrefixreadBytes(b, off, len);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        readBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "readBytes",
                readBytesMethodDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.ISTORE, 7);

        // cb.onReadBytesEnd(startTime, readBytesResult, b, off, len);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 4);
        readBytesMV.visitVarInsn(Opcodes.LLOAD, 5);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 7);
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
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 7);
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
