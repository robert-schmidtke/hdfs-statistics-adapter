/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.IOException;
import java.util.zip.ZipFile;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class ZipFileAdapter extends AbstractSfsAdapter {

    protected ZipFileAdapter(ClassVisitor cv, String methodPrefix) {
        super(cv, ZipFile.class, ZipFileCallback.class, methodPrefix);
    }

    @Override
    protected boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return isOpenMethod(access, name, desc, signature, exceptions);
    }

    @Override
    protected void appendWrappedMethods(ClassVisitor cv) {
        // pass the filename to the callback
        wrapMethod(Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC, "open",
                Type.LONG_TYPE,
                new Type[] { Type.getType(String.class), Type.INT_TYPE,
                        Type.LONG_TYPE, Type.BOOLEAN_TYPE },
                null, new String[] { Type.getInternalName(IOException.class) },
                "openCallback", Type.getType(String.class),
                new ParameterResultPasser(1));
    }

    // Helper methods

    private boolean isOpenMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        return (Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC
                | Opcodes.ACC_NATIVE) == access
                && "open".equals(name)
                && Type.getMethodDescriptor(Type.LONG_TYPE,
                        Type.getType(String.class), Type.INT_TYPE,
                        Type.LONG_TYPE, Type.BOOLEAN_TYPE).equals(desc)
                && null == signature && exceptions != null
                && exceptions.length == 1
                && Type.getInternalName(IOException.class)
                        .equals(exceptions[0]);
    }

}
