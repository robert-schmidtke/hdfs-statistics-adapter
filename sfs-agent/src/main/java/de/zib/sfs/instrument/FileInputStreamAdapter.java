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

    private final String systemInternalName, currentTimeMillisDescriptor;

    private final String fileInputStreamInternalName;

    private final String fileInputStreamCallbackInternalName,
            fileInputStreamCallbackDescriptor;

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

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        fileInputStreamInternalName = Type
                .getInternalName(FileInputStream.class);

        fileInputStreamCallbackInternalName = Type
                .getInternalName(FileInputStreamCallback.class);
        fileInputStreamCallbackDescriptor = Type
                .getDescriptor(FileInputStreamCallback.class);
    }

    @Override
    public void visitSource(String source, String debug) {
        // private FileInputStreamCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE,
                "callback", fileInputStreamCallbackDescriptor, null, null);
        callbackFV.visitEnd();

        // proceed as intended
        cv.visitSource(source, debug);
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
        // descriptors of the methods we add to FileInputStream
        String openMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.getType(String.class));
        String readMethodDescriptor = Type.getMethodDescriptor(Type.INT_TYPE);
        String readBytesMethodDescriptor = Type.getMethodDescriptor(
                Type.INT_TYPE, Type.getType(byte[].class), Type.INT_TYPE,
                Type.INT_TYPE);

        String ioExceptionInternalName = Type
                .getInternalName(IOException.class);

        // private void open(String name) throws FileNotFoundException {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                openMethodDescriptor, null, new String[] { Type
                        .getInternalName(FileNotFoundException.class) });
        openMV.visitCode();

        // callback = new FileInputStreamCallback();
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitTypeInsn(Opcodes.NEW, fileInputStreamCallbackInternalName);
        openMV.visitInsn(Opcodes.DUP);
        try {
            openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    fileInputStreamCallbackInternalName, "<init>",
                    Type.getConstructorDescriptor(FileInputStreamCallback.class
                            .getConstructor()), false);
        } catch (Exception e) {
            throw new RuntimeException("Could not access constructor", e);
        }
        openMV.visitFieldInsn(Opcodes.PUTFIELD, fileInputStreamInternalName,
                "callback", fileInputStreamCallbackDescriptor);

        // long startTime = System.currentTimeMillis();
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        openMV.visitVarInsn(Opcodes.LSTORE, 2);

        // nativeMethodPrefixopen(name);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "open",
                openMethodDescriptor, false);

        // long endTime = System.currentTimeMillis();
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        openMV.visitVarInsn(Opcodes.LSTORE, 4);

        // callback.onOpenEnd(startTime, endTime);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitFieldInsn(Opcodes.GETFIELD, fileInputStreamInternalName,
                "callback", fileInputStreamCallbackDescriptor);
        openMV.visitVarInsn(Opcodes.LLOAD, 2);
        openMV.visitVarInsn(Opcodes.LLOAD, 4);
        openMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onOpenEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE), false);

        // }
        openMV.visitInsn(Opcodes.RETURN);
        openMV.visitMaxs(0, 0);
        openMV.visitEnd();

        // public int read() throws IOException {
        MethodVisitor readMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                readMethodDescriptor, null,
                new String[] { ioExceptionInternalName });
        readMV.visitCode();

        // long startTime = System.currentTimeMillis();
        readMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMV.visitVarInsn(Opcodes.LSTORE, 2);

        // int readResult = nativeMethodPrefixread();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "read",
                readMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.ISTORE, 4);

        // long endTime = System.currentTimeMillis();
        readMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMV.visitVarInsn(Opcodes.LSTORE, 5);

        // callback.onReadEnd(startTime, endTime, readResult);
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitFieldInsn(Opcodes.GETFIELD, fileInputStreamInternalName,
                "callback", fileInputStreamCallbackDescriptor);
        readMV.visitVarInsn(Opcodes.LLOAD, 2);
        readMV.visitVarInsn(Opcodes.LLOAD, 5);
        readMV.visitVarInsn(Opcodes.ILOAD, 4);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE, Type.INT_TYPE), false);

        // return readResult;
        // }
        readMV.visitVarInsn(Opcodes.ILOAD, 4);
        readMV.visitInsn(Opcodes.IRETURN);
        readMV.visitMaxs(0, 0);
        readMV.visitEnd();

        // private int readBytes(byte[] b, int off, int len) throws IOException
        // {
        MethodVisitor readBytesMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "readBytes", readBytesMethodDescriptor, null,
                new String[] { ioExceptionInternalName });
        readBytesMV.visitCode();

        // long startTime = System.currentTimeMillis();
        readBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
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

        // long endTime = System.currentTimeMillis();
        readBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.LSTORE, 7);

        // callback.onReadBytesEnd(startTime, endTime, readBytesResult);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitFieldInsn(Opcodes.GETFIELD,
                fileInputStreamInternalName, "callback",
                fileInputStreamCallbackDescriptor);
        readBytesMV.visitVarInsn(Opcodes.LLOAD, 4);
        readBytesMV.visitVarInsn(Opcodes.LLOAD, 7);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 6);
        readBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onReadBytesEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.LONG_TYPE, Type.INT_TYPE), false);

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
