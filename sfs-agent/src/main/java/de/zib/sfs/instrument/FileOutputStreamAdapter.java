/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Class adapter that instruments {@link java.io.FileOutputStream}.
 * 
 * @author robert
 *
 */
public class FileOutputStreamAdapter extends ClassVisitor {

    private final String nativeMethodPrefix;

    /**
     * Construct a visitor that modifies an {@link java.io.FileOutputStream}'s
     * write calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     * @param nativeMethodPrefix
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public FileOutputStreamAdapter(ClassVisitor cv, String nativeMethodPrefix)
            throws NoSuchMethodException, SecurityException {
        super(Opcodes.ASM5, cv);
        this.nativeMethodPrefix = nativeMethodPrefix;
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if (isOpenMethod(access, name, desc, signature, exceptions)
                || isWriteMethod(access, name, desc, signature, exceptions)
                || isWriteBytesMethod(access, name, desc, signature, exceptions)) {
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
        // general descriptors needed to add methods to FileOutputStream
        String fileOutputStreamInternalName = Type
                .getInternalName(FileOutputStream.class);
        String fileOutputStreamCallbackInternalName = Type
                .getInternalName(FileOutputStreamCallback.class);
        String fileOutputStreamCallbackGetInstanceMethodDescriptor = Type
                .getMethodDescriptor(
                        Type.getType(FileOutputStreamCallback.class),
                        Type.getType(FileOutputStream.class));

        // descriptors of the methods we add to FileOutputStream
        String getCallbackMethodDescriptor = Type.getMethodDescriptor(Type
                .getType(FileOutputStreamCallback.class));
        String openMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.getType(String.class), Type.BOOLEAN_TYPE);
        String writeMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.INT_TYPE, Type.BOOLEAN_TYPE);
        String writeBytesMethodDescriptor = Type.getMethodDescriptor(
                Type.VOID_TYPE, Type.getType(byte[].class), Type.INT_TYPE,
                Type.INT_TYPE, Type.BOOLEAN_TYPE);

        // private FileOutputStreamCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE,
                "callback", Type.getDescriptor(FileOutputStreamCallback.class),
                null, null);
        callbackFV.visitEnd();

        // private FileOutputStreamCallback getCallback() {
        MethodVisitor getCallbackMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "getCallback", getCallbackMethodDescriptor, null, null);
        getCallbackMV.visitCode();

        // if (callback == null) {
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                fileOutputStreamInternalName, "callback",
                Type.getDescriptor(FileOutputStreamCallback.class));
        Label callbackNonNullLabel = new Label();
        getCallbackMV.visitJumpInsn(Opcodes.IFNONNULL, callbackNonNullLabel);

        // callback = FileOutputStreamCallback.getInstance(this);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileOutputStreamCallbackInternalName, "getInstance",
                fileOutputStreamCallbackGetInstanceMethodDescriptor, false);
        getCallbackMV.visitFieldInsn(Opcodes.PUTFIELD,
                fileOutputStreamInternalName, "callback",
                Type.getDescriptor(FileOutputStreamCallback.class));

        // }
        getCallbackMV.visitLabel(callbackNonNullLabel);

        // return callback;
        // }
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                fileOutputStreamInternalName, "callback",
                Type.getDescriptor(FileOutputStreamCallback.class));
        getCallbackMV.visitInsn(Opcodes.ARETURN);
        getCallbackMV.visitMaxs(0, 0);
        getCallbackMV.visitEnd();

        // private void open(String name, boolean append) {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                openMethodDescriptor, null, new String[] { Type
                        .getInternalName(FileNotFoundException.class) });
        openMV.visitCode();

        // FileOutputStreamCallback cb = getCallback();
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        openMV.visitVarInsn(Opcodes.ASTORE, 3);

        // long startTime = cb.onOpenBegin(name, append);
        openMV.visitVarInsn(Opcodes.ALOAD, 3);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ILOAD, 2);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName,
                "onOpenBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(String.class), Type.BOOLEAN_TYPE), false);
        openMV.visitVarInsn(Opcodes.LSTORE, 4);

        // nativeMethodPrefixopen(name, append);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ILOAD, 2);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName, nativeMethodPrefix + "open",
                openMethodDescriptor, false);

        // cb.onOpenEnd(startTime, name, append);
        openMV.visitVarInsn(Opcodes.ALOAD, 3);
        openMV.visitVarInsn(Opcodes.LLOAD, 4);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ILOAD, 2);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName,
                "onOpenEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.getType(String.class), Type.BOOLEAN_TYPE), false);

        // }
        openMV.visitInsn(Opcodes.RETURN);
        openMV.visitMaxs(0, 0);
        openMV.visitEnd();

        // private void write(int b, boolean append) {
        MethodVisitor writeMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "write",
                writeMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        writeMV.visitCode();

        // FileOutputStreamCallback cb = getCallback();
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        writeMV.visitVarInsn(Opcodes.ASTORE, 3);

        // long startTime = cb.onWriteBegin(b, append);
        writeMV.visitVarInsn(Opcodes.ALOAD, 3);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName, "onWriteBegin", Type
                        .getMethodDescriptor(Type.LONG_TYPE, Type.INT_TYPE,
                                Type.BOOLEAN_TYPE), false);
        writeMV.visitVarInsn(Opcodes.LSTORE, 4);

        // nativeMethodPrefixwrite(b, append);
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName, nativeMethodPrefix + "write",
                writeMethodDescriptor, false);

        // cb.onWriteEnd(startTime, b, append);
        writeMV.visitVarInsn(Opcodes.ALOAD, 3);
        writeMV.visitVarInsn(Opcodes.LLOAD, 4);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName, "onWriteEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE, Type.BOOLEAN_TYPE), false);

        // }
        writeMV.visitInsn(Opcodes.RETURN);
        writeMV.visitMaxs(0, 0);
        writeMV.visitEnd();

        // private void writeBytes(byte[] b, int off, int len, boolean append) {
        MethodVisitor writeBytesMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "writeBytes", writeBytesMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        writeBytesMV.visitCode();

        // FileOutputStreamCallback cb = getCallback();
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        writeBytesMV.visitVarInsn(Opcodes.ASTORE, 5);

        // long startTime = cb.onWriteBytesBegin(b, off, len, append);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 5);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 4);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName, "onWriteBytesBegin", Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE, Type.BOOLEAN_TYPE), false);
        writeBytesMV.visitVarInsn(Opcodes.LSTORE, 6);

        // nativeMethodPrefixwriteBytes(b, off, len, append);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 4);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName,
                nativeMethodPrefix + "writeBytes", writeBytesMethodDescriptor,
                false);

        // cb.onWriteBytesEnd(startTime, b, off, len, append);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 5);
        writeBytesMV.visitVarInsn(Opcodes.LLOAD, 6);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 4);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName, "onWriteBytesEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE, Type.BOOLEAN_TYPE), false);

        // }
        writeBytesMV.visitInsn(Opcodes.RETURN);
        writeBytesMV.visitMaxs(0, 0);
        writeBytesMV.visitEnd();

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
                        Type.getType(String.class), Type.BOOLEAN_TYPE).equals(
                        desc)
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(FileNotFoundException.class).equals(
                        exceptions[0]);
    }

    private boolean isWriteMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the native method itself, because write has no accessor
        // method and there is no native write0 method
        return access == (Opcodes.ACC_PRIVATE | Opcodes.ACC_NATIVE)
                && "write".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE,
                        Type.BOOLEAN_TYPE).equals(desc)
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isWriteBytesMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the native method itself, because writeBytes has no accessor
        // method and there is no native writeBytes0 method
        return access == (Opcodes.ACC_PRIVATE | Opcodes.ACC_NATIVE)
                && "writeBytes".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE, Type.BOOLEAN_TYPE).equals(desc)
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }
}
