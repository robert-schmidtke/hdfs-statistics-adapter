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
import java.util.Set;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import de.zib.sfs.instrument.statistics.OperationCategory;

/**
 * Class adapter that instruments {@link java.io.FileInputStream}.
 * 
 * @author robert
 *
 */
public class FileInputStreamAdapter extends AbstractSfsAdapter {

    public FileInputStreamAdapter(ClassVisitor cv, String methodPrefix,
            Set<OperationCategory> skip) throws SecurityException {
        super(cv, FileInputStream.class, FileInputStreamCallback.class,
                methodPrefix, skip);
    }

    @Override
    protected boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return isOpenMethod(access, name, desc, signature, exceptions)
                || isReadMethod(access, name, desc, signature, exceptions);
    }

    @Override
    protected void appendWrappedMethods(ClassVisitor visitor) {
        wrapMethod(Opcodes.ACC_PRIVATE, "open", Type.VOID_TYPE,
                new Type[] { Type.getType(String.class) }, null,
                new String[] {
                        Type.getInternalName(FileNotFoundException.class) },
                "openCallback", Type.getType(String.class),
                new ParameterResultPasser(1));

        // for all read methods pass the read result to the callback
        ReturnResultPasser resultPasser = new ReturnResultPasser();

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
        }
    }

    private static boolean isOpenMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return access == Opcodes.ACC_PRIVATE && "open".equals(name)
                && Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class)).equals(desc)
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
}
