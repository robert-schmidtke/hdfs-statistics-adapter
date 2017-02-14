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
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

/**
 * Class adapter that instruments {@link java.io.RandomAccessFile}.
 * 
 * @author robert
 *
 */
public class RandomAccessFileAdapter extends ClassVisitor {

    private final String nativeMethodPrefix;

    private final String systemInternalName, currentTimeMillisDescriptor;

    private final String randomAccessFileInternalName;

    private final String randomAccessFileCallbackInternalName,
            randomAccessFileCallbackDescriptor;

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

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        randomAccessFileInternalName = Type
                .getInternalName(RandomAccessFile.class);

        randomAccessFileCallbackInternalName = Type
                .getInternalName(RandomAccessFileCallback.class);
        randomAccessFileCallbackDescriptor = Type
                .getDescriptor(RandomAccessFileCallback.class);
    }

    @Override
    public void visitSource(String source, String debug) {
        // private RandomAccessFileCallback callback;
        FieldVisitor callbackFV = cv.visitField(Opcodes.ACC_PRIVATE, "callback",
                randomAccessFileCallbackDescriptor, null, null);
        callbackFV.visitEnd();

        // proceed as intended
        cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if ("<init>".equals(name)) {
            mv = new ConstructorAdapter(api,
                    cv.visitMethod(access, name, desc, signature, exceptions),
                    access, name, desc);
        } else if (isOpenMethod(access, name, desc, signature, exceptions)
                || isReadMethod(access, name, desc, signature, exceptions)
                || isReadBytesMethod(access, name, desc, signature, exceptions)
                || isWriteMethod(access, name, desc, signature, exceptions)
                || isWriteBytesMethod(access, name, desc, signature,
                        exceptions)) {
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
        // descriptors of the methods we add to RandomAccessFile
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

        String ioExceptionInternalName = Type
                .getInternalName(IOException.class);

        // private void open(String name, int mode) throws FileNotFoundException
        // {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                openMethodDescriptor, null, new String[] {
                        Type.getInternalName(FileNotFoundException.class) });
        openMV.visitCode();

        // long startTime = System.currentTimeMillis();
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        openMV.visitVarInsn(Opcodes.LSTORE, 3);

        // nativeMethodPrefixopen(name, mode);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitVarInsn(Opcodes.ILOAD, 2);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, nativeMethodPrefix + "open",
                openMethodDescriptor, false);

        // long endTime = System.currentTimeMillis();
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        openMV.visitVarInsn(Opcodes.LSTORE, 5);

        // callback.onOpenEnd(startTime, endTime);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitFieldInsn(Opcodes.GETFIELD, randomAccessFileInternalName,
                "callback", randomAccessFileCallbackDescriptor);
        openMV.visitVarInsn(Opcodes.LLOAD, 3);
        openMV.visitVarInsn(Opcodes.LLOAD, 5);
        openMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onOpenEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE),
                false);

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
        readMV.visitVarInsn(Opcodes.LSTORE, 1);

        // int readResult = nativeMethodPrefixread();
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, nativeMethodPrefix + "read",
                readMethodDescriptor, false);
        readMV.visitVarInsn(Opcodes.ISTORE, 3);

        // long endTime = System.currentTimeMillis();
        readMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readMV.visitVarInsn(Opcodes.LSTORE, 4);

        // callback.onReadEnd(startTime, endTime, readResult);
        readMV.visitVarInsn(Opcodes.ALOAD, 0);
        readMV.visitFieldInsn(Opcodes.GETFIELD, randomAccessFileInternalName,
                "callback", randomAccessFileCallbackDescriptor);
        readMV.visitVarInsn(Opcodes.LLOAD, 1);
        readMV.visitVarInsn(Opcodes.LLOAD, 4);
        readMV.visitVarInsn(Opcodes.ILOAD, 3);
        readMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onReadEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE, Type.INT_TYPE),
                false);

        // return readResult;
        // }
        readMV.visitVarInsn(Opcodes.ILOAD, 3);
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
                randomAccessFileInternalName, nativeMethodPrefix + "readBytes",
                readBytesMethodDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.ISTORE, 6);

        // long endTime = System.currentTimeMillis();
        readBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        readBytesMV.visitVarInsn(Opcodes.LSTORE, 7);

        // callback.onReadBytesEnd(startTime, endTime, readBytesResult);
        readBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        readBytesMV.visitFieldInsn(Opcodes.GETFIELD,
                randomAccessFileInternalName, "callback",
                randomAccessFileCallbackDescriptor);
        readBytesMV.visitVarInsn(Opcodes.LLOAD, 4);
        readBytesMV.visitVarInsn(Opcodes.LLOAD, 7);
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 6);
        readBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onReadBytesEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE, Type.INT_TYPE),
                false);

        // return readBytesResult;
        // }
        readBytesMV.visitVarInsn(Opcodes.ILOAD, 6);
        readBytesMV.visitInsn(Opcodes.IRETURN);
        readBytesMV.visitMaxs(0, 0);
        readBytesMV.visitEnd();

        // public void write(int b) throws IOException {
        MethodVisitor writeMV = cv.visitMethod(Opcodes.ACC_PUBLIC, "write",
                writeMethodDescriptor, null,
                new String[] { ioExceptionInternalName });
        writeMV.visitCode();

        // long startTime = System.currentTimeMillis();
        writeMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMV.visitVarInsn(Opcodes.LSTORE, 2);

        // nativeMethodPrefixwrite(b);
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitVarInsn(Opcodes.ILOAD, 1);
        writeMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, nativeMethodPrefix + "write",
                writeMethodDescriptor, false);

        // long endTime = System.currentTimeMillis();
        writeMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeMV.visitVarInsn(Opcodes.LSTORE, 4);

        // callback.onWriteEnd(startTime, endTime);
        writeMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeMV.visitFieldInsn(Opcodes.GETFIELD, randomAccessFileInternalName,
                "callback", randomAccessFileCallbackDescriptor);
        writeMV.visitVarInsn(Opcodes.LLOAD, 2);
        writeMV.visitVarInsn(Opcodes.LLOAD, 4);
        writeMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onWriteEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE),
                false);

        // }
        writeMV.visitInsn(Opcodes.RETURN);
        writeMV.visitMaxs(0, 0);
        writeMV.visitEnd();

        // private void writeBytes(byte[] b, int off, int len) throws
        // IOException {
        MethodVisitor writeBytesMV = cv.visitMethod(Opcodes.ACC_PRIVATE,
                "writeBytes", writeBytesMethodDescriptor, null,
                new String[] { ioExceptionInternalName });
        writeBytesMV.visitCode();

        // long startTime = System.currentTimeMillis();
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeBytesMV.visitVarInsn(Opcodes.LSTORE, 4);

        // nativeMethodPrefixwriteBytes(b, off, len);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 1);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 2);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                randomAccessFileInternalName, nativeMethodPrefix + "writeBytes",
                writeBytesMethodDescriptor, false);

        // long endTime = System.currentTimeMillis();
        writeBytesMV.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        writeBytesMV.visitVarInsn(Opcodes.LSTORE, 6);

        // callback.onWriteBytesEnd(startTime, endTime, len);
        writeBytesMV.visitVarInsn(Opcodes.ALOAD, 0);
        writeBytesMV.visitFieldInsn(Opcodes.GETFIELD,
                randomAccessFileInternalName, "callback",
                randomAccessFileCallbackDescriptor);
        writeBytesMV.visitVarInsn(Opcodes.LLOAD, 4);
        writeBytesMV.visitVarInsn(Opcodes.LLOAD, 6);
        writeBytesMV.visitVarInsn(Opcodes.ILOAD, 3);
        writeBytesMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                randomAccessFileCallbackInternalName, "onWriteBytesEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.LONG_TYPE,
                        Type.LONG_TYPE, Type.INT_TYPE),
                false);

        // }
        writeBytesMV.visitInsn(Opcodes.RETURN);
        writeBytesMV.visitMaxs(0, 0);
        writeBytesMV.visitEnd();

        cv.visitEnd();
    }

    private static class ConstructorAdapter extends AdviceAdapter {

        protected ConstructorAdapter(int api, MethodVisitor mv, int access,
                String name, String desc) {
            super(api, mv, access, name, desc);
        }

        @Override
        protected void onMethodEnter() {
            // callback = new RandomAccessFileCallback();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitTypeInsn(Opcodes.NEW,
                    Type.getInternalName(RandomAccessFileCallback.class));
            mv.visitInsn(Opcodes.DUP);
            try {
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                        Type.getInternalName(RandomAccessFileCallback.class),
                        "<init>",
                        Type.getConstructorDescriptor(
                                RandomAccessFileCallback.class
                                        .getConstructor()),
                        false);
            } catch (Exception e) {
                throw new RuntimeException("Could not access constructor", e);
            }
            mv.visitFieldInsn(Opcodes.PUTFIELD,
                    Type.getInternalName(RandomAccessFile.class), "callback",
                    Type.getDescriptor(RandomAccessFileCallback.class));
        }

    }

    // Helper methods

    private boolean isOpenMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the generated accessor method and not the native open0 method
        // itself
        return access == Opcodes.ACC_PRIVATE && "open".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class), Type.INT_TYPE).equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(FileNotFoundException.class)
                        .equals(exceptions[0]);
    }

    private boolean isReadMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the generated accessor method and not the native read0 method
        // itself
        return access == Opcodes.ACC_PUBLIC && "read".equals(name)
                && Type.getMethodDescriptor(Type.INT_TYPE).equals(desc)
                && null == signature && exceptions != null
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
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

    private boolean isWriteMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // wrap the generated accessor method and not the native write0 method
        // itself
        return access == Opcodes.ACC_PUBLIC && "write".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE)
                        .equals(desc)
                && null == signature && exceptions != null
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
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }
}
