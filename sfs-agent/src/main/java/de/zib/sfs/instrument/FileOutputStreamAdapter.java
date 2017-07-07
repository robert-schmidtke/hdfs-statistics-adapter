/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Class adapter that instruments {@link java.io.FileOutputStream}.
 * 
 * @author robert
 *
 */
public class FileOutputStreamAdapter extends AbstractSfsAdapter {

    public FileOutputStreamAdapter(ClassVisitor cv, String methodPrefix)
            throws NoSuchMethodException, SecurityException {
        super(cv, FileOutputStream.class, FileOutputStreamCallback.class,
                methodPrefix);
    }

    @Override
    protected boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return isOpenMethod(access, name, desc, signature, exceptions)
                || isWriteMethod(access, name, desc, signature, exceptions);
    }

    @Override
    protected void appendWrappedMethods(ClassVisitor cv) {
        ResultPasser resultDiscarder = new DiscardResultPasser();

        wrapMethod(Opcodes.ACC_PRIVATE, "open", Type.VOID_TYPE,
                new Type[] { Type.getType(String.class), Type.BOOLEAN_TYPE },
                null,
                new String[] {
                        Type.getInternalName(FileNotFoundException.class) },
                "openCallback",
                new Type[] { Type.getType(String.class),
                        Type.getType(FileDescriptor.class) },
                new ResultPasser[] { new ParameterResultPasser(1),
                        new FieldResultPasser("fd") });

        // 1 byte write, no result needed
        wrapMethod(Opcodes.ACC_PUBLIC, "write", Type.VOID_TYPE,
                new Type[] { Type.INT_TYPE }, null,
                new String[] { Type.getInternalName(IOException.class) },
                "writeCallback", null, resultDiscarder);

        // have the byte array put on top of the stack, then pass its length to
        // the callback
        wrapMethod(Opcodes.ACC_PUBLIC, "write", Type.VOID_TYPE,
                new Type[] { Type.getType(byte[].class) }, null,
                new String[] { Type.getInternalName(IOException.class) },
                "writeBytesCallback", Type.INT_TYPE,
                new ParameterResultPasser(1) {
                    @Override
                    public void passResult(MethodVisitor mv) {
                        mv.visitInsn(Opcodes.ARRAYLENGTH);
                    }
                });

        // have the len parameter put on top of the stack
        wrapMethod(Opcodes.ACC_PUBLIC, "write", Type.VOID_TYPE,
                new Type[] { Type.getType(byte[].class), Type.INT_TYPE,
                        Type.INT_TYPE },
                null, new String[] { Type.getInternalName(IOException.class) },
                "writeBytesCallback", Type.INT_TYPE,
                new ParameterResultPasser(3));
    }

    private boolean isOpenMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PRIVATE && "open".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class), Type.BOOLEAN_TYPE)
                        .equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(FileNotFoundException.class)
                        .equals(exceptions[0]);
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
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }
}
