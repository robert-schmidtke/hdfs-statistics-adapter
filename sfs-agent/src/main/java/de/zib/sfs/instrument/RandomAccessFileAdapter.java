/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Class adapter that instruments {@link java.io.RandomAccessFile}.
 * 
 * @author robert
 *
 */
public class RandomAccessFileAdapter extends ClassVisitor {

    private final String nativeMethodPrefix;

    /**
     * Construct a visitor that modifies an {@link java.io.RandomAccessFile}'s
     * read and write calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     * @param nativeMethodPrefix
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public RandomAccessFileAdapter(ClassVisitor cv, String nativeMethodPrefix)
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
                || isReadBytesMethod(access, name, desc, signature, exceptions)
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
        // general descriptors needed to add methods to RandomAccessFile
        String randomAccessFileInternalName = Type
                .getInternalName(RandomAccessFile.class);
        String randomAccessFileCallbackInternalName = Type
                .getInternalName(RandomAccessFileCallback.class);
        String randomAccessFileCallbackGetInstanceMethodDescriptor = Type
                .getMethodDescriptor(
                        Type.getType(RandomAccessFileCallback.class),
                        Type.getType(RandomAccessFile.class));

        // descriptors of the methods we add to RandomAccessFile
        String getCallbackMethodDescriptor = Type.getMethodDescriptor(Type
                .getType(RandomAccessFileCallback.class));
        String openMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.getType(String.class), Type.INT_TYPE);
        String readMethodDescriptor = Type.getMethodDescriptor(Type.INT_TYPE);
        String readBytesMethodDescriptor = Type.getMethodDescriptor(
                Type.INT_TYPE, Type.getType(byte[].class), Type.INT_TYPE,
                Type.INT_TYPE);
        String writeMethodDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE,
                Type.INT_TYPE);
        String writeBytesMethodDescriptor = Type.getMethodDescriptor(
                Type.VOID_TYPE, Type.getType(byte[].class), Type.INT_TYPE,
                Type.INT_TYPE);

        // private RandomAccessFileCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE,
                "callback", Type.getDescriptor(RandomAccessFileCallback.class),
                null, null);
        callbackFV.visitEnd();

        // private RandomAccessFileCallback getCallback() {
        MethodVisitor getCallbackMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "getCallback", getCallbackMethodDescriptor, null, null);
        getCallbackMV.visitCode();

        // if (callback == null) {
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                randomAccessFileInternalName, "callback",
                Type.getDescriptor(RandomAccessFileCallback.class));
        Label callbackNonNullLabel = new Label();
        getCallbackMV.visitJumpInsn(Opcodes.IFNONNULL, callbackNonNullLabel);

        // callback = RandomAccessFileCallback.getInstance(this);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                randomAccessFileCallbackInternalName, "getInstance",
                randomAccessFileCallbackGetInstanceMethodDescriptor, false);
        getCallbackMV.visitFieldInsn(Opcodes.PUTFIELD,
                randomAccessFileInternalName, "callback",
                Type.getDescriptor(RandomAccessFileCallback.class));

        // }
        getCallbackMV.visitLabel(callbackNonNullLabel);

        // return callback;
        // }
        getCallbackMV.visitVarInsn(Opcodes.ALOAD, 0);
        getCallbackMV.visitFieldInsn(Opcodes.GETFIELD,
                randomAccessFileInternalName, "callback",
                Type.getDescriptor(RandomAccessFileCallback.class));
        getCallbackMV.visitInsn(Opcodes.ARETURN);
        getCallbackMV.visitMaxs(0, 0);
        getCallbackMV.visitEnd();

        // private void open(String name, int mode) {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                openMethodDescriptor, null, new String[] { Type
                        .getInternalName(FileNotFoundException.class) });
        openMV.visitCode();

        // RandomAccessFileCallback cb = getCallback();
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        openMV.visitVarInsn(Opcodes.ASTORE, 3);

        // long startTime = cb.onOpenBegin(name, mode);
        openMV.visitVarInsn(Opcodes.ALOAD, 3);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ILOAD, 2);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName,
                "onOpenBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(String.class), Type.INT_TYPE), false);
        openMV.visitVarInsn(Opcodes.LSTORE, 4);

        // nativeMethodPrefixopen(name, mode);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ILOAD, 2);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, nativeMethodPrefix + "open",
                openMethodDescriptor, false);

        // cb.onOpenEnd(startTime, name, mode);
        openMV.visitVarInsn(Opcodes.ALOAD, 3);
        openMV.visitVarInsn(Opcodes.LLOAD, 4);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ALOAD, 2);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName,
                "onOpenEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.getType(String.class), Type.INT_TYPE), false);

        // }
        openMV.visitInsn(Opcodes.RETURN);
        openMV.visitMaxs(0, 0);
        openMV.visitEnd();

        // public int read() {
        MethodVisitor readMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "read",
                readMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        readMV.visitCode();

        // RandomAccessFileCallback cb = getCallback();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.ASTORE, 1);

        // long startTime = cb.onReadBegin();
        readMV.visitVarInsn(Opcodes.ALOAD, 1);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onReadBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE), false);
        readMV.visitVarInsn(Opcodes.LSTORE, 2);

        // int readResult = nativeMethodPrefixread();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, nativeMethodPrefix + "read",
                readMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.ISTORE, 4);

        // cb.onReadEnd(startTime, readResult);
        readMV.visitVarInsn(Opcodes.ALOAD, 1);
        readMV.visitVarInsn(Opcodes.LLOAD, 2);
        readMV.visitVarInsn(Opcodes.ILOAD, 4);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onReadEnd", Type
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

        // RandomAccessFileCallback cb = getCallback();
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.ASTORE, 4);

        // long startTime = cb.onReadBytesBegin(b, off, len);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 4);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        readBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onReadBytesBegin", Type
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
                randomAccessFileInternalName, nativeMethodPrefix + "readBytes",
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
                randomAccessFileCallbackInternalName, "onReadBytesEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE, Type.getType(byte[].class),
                                Type.INT_TYPE, Type.INT_TYPE), false);

        // return readBytesResult;
        // }
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 7);
        readBytesMV.visitInsn(Opcodes.IRETURN);
        readBytesMV.visitMaxs(0, 0);
        readBytesMV.visitEnd();

        // public void write(int b) {
        MethodVisitor writeMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "write",
                writeMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        writeMV.visitCode();

        // RandomAccessFileCallback cb = getCallback();
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        writeMV.visitVarInsn(Opcodes.ASTORE, 2);

        // long startTime = cb.onWriteBegin(b);
        writeMV.visitVarInsn(Opcodes.ALOAD, 2);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onWriteBegin",
                Type.getMethodDescriptor(Type.LONG_TYPE, Type.INT_TYPE), false);
        writeMV.visitVarInsn(Opcodes.LSTORE, 3);

        // nativeMethodPrefixwrite(b);
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, nativeMethodPrefix + "write",
                writeMethodDescriptor, false);

        // cb.onWriteEnd(startTime, b);
        writeMV.visitVarInsn(Opcodes.ALOAD, 2);
        writeMV.visitVarInsn(Opcodes.LLOAD, 3);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onWriteEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.INT_TYPE), false);

        // }
        writeMV.visitInsn(Opcodes.RETURN);
        writeMV.visitMaxs(0, 0);
        writeMV.visitEnd();

        // private void writeBytes(byte[] b, int off, int len) {
        MethodVisitor writeBytesMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "writeBytes", writeBytesMethodDescriptor, null,
                new String[] { Type.getInternalName(IOException.class) });
        writeBytesMV.visitCode();

        // RandomAccessFileCallback cb = getCallback();
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, "getCallback",
                getCallbackMethodDescriptor, false);
        writeBytesMV.visitVarInsn(Opcodes.ASTORE, 4);

        // long startTime = cb.onWriteBytesBegin(b, off, len);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 4);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onWriteBytesBegin", Type
                        .getMethodDescriptor(Type.LONG_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE), false);
        writeBytesMV.visitVarInsn(Opcodes.LSTORE, 5);

        // nativeMethodPrefixwriteBytes(b, off, len);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName,
                nativeMethodPrefix + "writeBytes", writeBytesMethodDescriptor,
                false);

        // cb.onWriteBytesEnd(startTime, b, off, len);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 4);
        writeBytesMV.visitVarInsn(Opcodes.LLOAD, 5);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onWriteBytesEnd", Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE), false);

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
                        Type.getType(String.class), Type.INT_TYPE).equals(desc)
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

    private boolean isWriteMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the generated accessor method and not the native write0 method
        // itself
        return access == Opcodes.ACC_PUBLIC
                && "write".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE)
                        .equals(desc)
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
                        Type.INT_TYPE).equals(desc)
                && null == signature
                && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }
}
