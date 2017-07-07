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
import java.util.Set;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import de.zib.sfs.instrument.statistics.OperationCategory;

/**
 * Class adapter that instruments {@link java.io.RandomAccessFile}.
 * 
 * @author robert
 *
 */
public class RandomAccessFileAdapter extends AbstractSfsAdapter {

    public RandomAccessFileAdapter(ClassVisitor cv, String methodPrefix,
            Set<OperationCategory> skip)
            throws NoSuchMethodException, SecurityException {
        super(cv, RandomAccessFile.class, RandomAccessFileCallback.class,
                methodPrefix, skip);
    }

    @Override
    protected boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return isOpenMethod(access, name, desc, signature, exceptions)
                || isReadMethod(access, name, desc, signature, exceptions)
                || isReadFullyMethod(access, name, desc, signature, exceptions)
                || isReadStringMethod(access, name, desc, signature, exceptions)
                || isWriteMethod(access, name, desc, signature, exceptions)
                || isWriteStringMethod(access, name, desc, signature,
                        exceptions);
    }

    @Override
    protected void appendWrappedMethods(ClassVisitor cv) {
        ResultPasser resultDiscarder = new DiscardResultPasser();

        wrapMethod(Opcodes.ACC_PRIVATE, "open", Type.VOID_TYPE,
                new Type[] { Type.getType(String.class), Type.INT_TYPE }, null,
                new String[] {
                        Type.getInternalName(FileNotFoundException.class) },
                "openCallback", null, resultDiscarder);

        // for all read methods pass the read result to the callback
        ReturnResultPasser resultPasser = new ReturnResultPasser();

        // invokes .length on an array
        ResultPasser arrayLengthPasser = new ParameterResultPasser(1) {
            @Override
            public void passResult(MethodVisitor mv) {
                mv.visitInsn(Opcodes.ARRAYLENGTH);
            }
        };

        // invokes .length(); on the current local variable
        ResultPasser stringLengthPasser = new ReturnResultPasser() {
            @Override
            public void passResult(MethodVisitor mv) {
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(String.class), "length",
                        Type.getMethodDescriptor(Type.INT_TYPE), false);
            }
        };

        if (!skipReads()) {
            wrapMethod(Opcodes.ACC_PUBLIC, "read", Type.INT_TYPE, null, null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "readCallback", Type.INT_TYPE, resultPasser);

            wrapMethod(Opcodes.ACC_PUBLIC, "read", Type.INT_TYPE,
                    new Type[] { Type.getType(byte[].class) }, null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "readBytesCallback", Type.INT_TYPE, resultPasser);

            wrapMethod(Opcodes.ACC_PUBLIC, "read", Type.INT_TYPE,
                    new Type[] { Type.getType(byte[].class), Type.INT_TYPE,
                            Type.INT_TYPE },
                    null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "readBytesCallback", Type.INT_TYPE, resultPasser);

            wrapMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "readFully",
                    Type.VOID_TYPE, new Type[] { Type.getType(byte[].class) },
                    null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "readBytesCallback", Type.INT_TYPE, arrayLengthPasser);

            wrapMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "readFully",
                    Type.VOID_TYPE,
                    new Type[] { Type.getType(byte[].class), Type.INT_TYPE,
                            Type.INT_TYPE },
                    null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "readBytesCallback", Type.INT_TYPE,
                    new ParameterResultPasser(3));

            // record length of read string
            wrapMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "readLine",
                    Type.getType(String.class), null, null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "readBytesCallback", Type.INT_TYPE, stringLengthPasser);

            wrapMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "readUTF",
                    Type.getType(String.class), null, null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "readBytesCallback", Type.INT_TYPE, stringLengthPasser);
        }

        // take length of parameter string instead of return value
        stringLengthPasser = new ParameterResultPasser(1) {
            @Override
            public void passResult(MethodVisitor mv) {
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(String.class), "length",
                        Type.getMethodDescriptor(Type.INT_TYPE), false);
            }
        };

        if (!skipWrites()) {
            // 1 byte write, no result needed
            wrapMethod(Opcodes.ACC_PUBLIC, "write", Type.VOID_TYPE,
                    new Type[] { Type.INT_TYPE }, null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "writeCallback", null, resultDiscarder);

            // have the byte array put on top of the stack, then pass its length
            // to
            // the callback
            wrapMethod(Opcodes.ACC_PUBLIC, "write", Type.VOID_TYPE,
                    new Type[] { Type.getType(byte[].class) }, null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "writeBytesCallback", Type.INT_TYPE, arrayLengthPasser);

            // have the len parameter put on top of the stack
            wrapMethod(Opcodes.ACC_PUBLIC, "write", Type.VOID_TYPE,
                    new Type[] { Type.getType(byte[].class), Type.INT_TYPE,
                            Type.INT_TYPE },
                    null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "writeBytesCallback", Type.INT_TYPE,
                    new ParameterResultPasser(3));

            wrapMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "writeBytes",
                    Type.VOID_TYPE, new Type[] { Type.getType(String.class) },
                    null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "writeBytesCallback", Type.INT_TYPE, stringLengthPasser);

            // twice the data if written as characters
            wrapMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "writeChars",
                    Type.VOID_TYPE, new Type[] { Type.getType(String.class) },
                    null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "writeBytesCallback", Type.INT_TYPE,
                    new ParameterResultPasser(1) {
                        @Override
                        public void passResult(MethodVisitor mv) {
                            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                    Type.getInternalName(String.class),
                                    "length",
                                    Type.getMethodDescriptor(Type.INT_TYPE),
                                    false);
                            mv.visitInsn(Opcodes.ICONST_2);
                            mv.visitInsn(Opcodes.IMUL);
                        }
                    });

            wrapMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "writeUTF",
                    Type.VOID_TYPE, new Type[] { Type.getType(String.class) },
                    null,
                    new String[] { Type.getInternalName(IOException.class) },
                    "writeBytesCallback", Type.INT_TYPE, stringLengthPasser);
        }

    }

    private boolean isOpenMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
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
        return access == Opcodes.ACC_PUBLIC && "read".equals(name)
                && (Type.getMethodDescriptor(Type.INT_TYPE).equals(desc)
                        || Type.getMethodDescriptor(Type.INT_TYPE,
                                Type.getType(byte[].class)).equals(desc)
                        || Type.getMethodDescriptor(Type.INT_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE).equals(desc))
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class).equals(exceptions[0])
                && !skipReads();
    }

    private boolean isReadFullyMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == (Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL)
                && "readFully".equals(name)
                && (Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(byte[].class)).equals(desc)
                        || Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE).equals(desc))
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class).equals(exceptions[0])
                && !skipReads();
    }

    private boolean isReadStringMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == (Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL)
                && ("readLine".equals(name) || "readUTF".equals(name))
                && Type.getMethodDescriptor(Type.getType(String.class))
                        .equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class).equals(exceptions[0])
                && !skipReads();
    }

    private boolean isWriteMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PUBLIC && "write".equals(name)
                && (Type.getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE)
                        .equals(desc)
                        || Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.getType(byte[].class)).equals(desc)
                        || Type.getMethodDescriptor(Type.VOID_TYPE,
                                Type.getType(byte[].class), Type.INT_TYPE,
                                Type.INT_TYPE).equals(desc))
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class).equals(exceptions[0])
                && !skipWrites();
    }

    private boolean isWriteStringMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == (Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL)
                && ("writeBytes".equals(name) || "writeChars".equals(name)
                        || "writeUTF".equals(name))
                && Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class)).equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class).equals(exceptions[0])
                && !skipWrites();
    }
}
