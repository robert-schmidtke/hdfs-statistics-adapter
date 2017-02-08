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

    private final String systemInternalName, currentTimeMillisDescriptor;

    private final String fileOutputStreamInternalName;

    private final String fileOutputStreamCallbackInternalName,
            fileOutputStreamCallbackDescriptor;

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

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        fileOutputStreamInternalName = Type
                .getInternalName(FileOutputStream.class);

        fileOutputStreamCallbackInternalName = Type
                .getInternalName(FileOutputStreamCallback.class);
        fileOutputStreamCallbackDescriptor = Type
                .getDescriptor(FileOutputStreamCallback.class);
    }

    @Override
    public void visitSource(String source, String debug) {
        // private FileOutputStreamCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE,
                "callback", fileOutputStreamCallbackDescriptor, null, null);
        callbackFV.visitEnd();

        // proceed as intended
        cv.visitSource(source, debug);
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
        // descriptors of the methods we add to FileOutputStream
        String openMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.getType(String.class), Type.BOOLEAN_TYPE);
        String writeMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.INT_TYPE, Type.BOOLEAN_TYPE);
        String writeBytesMethodDescriptor = Type.getMethodDescriptor(
                Type.VOID_TYPE, Type.getType(byte[].class), Type.INT_TYPE,
                Type.INT_TYPE, Type.BOOLEAN_TYPE);

        String ioExceptionInternalName = Type
                .getInternalName(IOException.class);

        // private void open(String name, boolean append) throw
        // FileNotFoundException {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                openMethodDescriptor, null, new String[] { Type
                        .getInternalName(FileNotFoundException.class) });
        openMV.visitCode();

        // callback = new FileOutputStreamCallback();
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitTypeInsn(Opcodes.NEW, fileOutputStreamCallbackInternalName);
        openMV.visitInsn(Opcodes.DUP);
        try {
            openMV.visitMethodInsn(
                    Opcodes.INVOKESPECIAL,
                    fileOutputStreamCallbackInternalName,
                    "<init>",
                    Type.getConstructorDescriptor(FileOutputStreamCallback.class
                            .getConstructor()), false);
        } catch (Exception e) {
            throw new RuntimeException("Could not access constructor", e);
        }
        openMV.visitFieldInsn(Opcodes.PUTFIELD, fileOutputStreamInternalName,
                "callback", fileOutputStreamCallbackDescriptor);

        // long startTime = System.currentTimeMillis();
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        openMV.visitVarInsn(Opcodes.LSTORE, 3);

        // nativeMethodPrefixopen(name, append);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ILOAD, 2);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName, nativeMethodPrefix + "open",
                openMethodDescriptor, false);

        // long endTime = System.currentTimeMillis();
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        openMV.visitVarInsn(Opcodes.LSTORE, 5);

        // callback.onOpenEnd(startTime, endTime);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitFieldInsn(Opcodes.GETFIELD, fileOutputStreamInternalName,
                "callback", fileOutputStreamCallbackDescriptor);
        openMV.visitVarInsn(Opcodes.LLOAD, 3);
        openMV.visitVarInsn(Opcodes.LLOAD, 5);
        openMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName, "onOpenEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE), false);

        // }
        openMV.visitInsn(Opcodes.RETURN);
        openMV.visitMaxs(0, 0);
        openMV.visitEnd();

        // private void write(int b, boolean append) throws IOException {
        MethodVisitor writeMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "write",
                writeMethodDescriptor, null,
                new String[] { ioExceptionInternalName });
        writeMV.visitCode();

        // long startTime = System.currentTimeMillis();
        writeMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMV.visitVarInsn(Opcodes.LSTORE, 3);

        // nativeMethodPrefixwrite(b, append);
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileOutputStreamInternalName, nativeMethodPrefix + "write",
                writeMethodDescriptor, false);

        // long endTime = System.currentTimeMillis();
        writeMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMV.visitVarInsn(Opcodes.LSTORE, 5);

        // callback.onWriteEnd(startTime, endTime);
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitFieldInsn(Opcodes.GETFIELD, fileOutputStreamInternalName,
                "callback", fileOutputStreamCallbackDescriptor);
        writeMV.visitVarInsn(Opcodes.LLOAD, 3);
        writeMV.visitVarInsn(Opcodes.LLOAD, 5);
        writeMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName, "onWriteEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE), false);

        // }
        writeMV.visitInsn(Opcodes.RETURN);
        writeMV.visitMaxs(0, 0);
        writeMV.visitEnd();

        // private void writeBytes(byte[] b, int off, int len, boolean append)
        // throws IOException {
        MethodVisitor writeBytesMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "writeBytes", writeBytesMethodDescriptor, null,
                new String[] { ioExceptionInternalName });
        writeBytesMV.visitCode();

        // long startTime = System.currentTimeMillis();
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeBytesMV.visitVarInsn(Opcodes.LSTORE, 5);

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

        // long endTime = System.currentTimeMillis();
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeBytesMV.visitVarInsn(Opcodes.LSTORE, 7);

        // callback.onWriteBytesEnd(startTime, endTime, len);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeBytesMV.visitFieldInsn(Opcodes.GETFIELD,
                fileOutputStreamInternalName, "callback",
                fileOutputStreamCallbackDescriptor);
        writeBytesMV.visitVarInsn(Opcodes.LLOAD, 5);
        writeBytesMV.visitVarInsn(Opcodes.LLOAD, 7);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileOutputStreamCallbackInternalName, "onWriteBytesEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE, Type.INT_TYPE), false);

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
