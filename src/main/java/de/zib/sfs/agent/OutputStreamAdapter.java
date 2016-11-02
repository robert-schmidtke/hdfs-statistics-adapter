/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.agent;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class OutputStreamAdapter extends ClassVisitor {

    public static class WriteMethodAdapter extends MethodVisitor {
        public WriteMethodAdapter(MethodVisitor mv) {
            super(Opcodes.ASM5, mv);
        }
    }

    /**
     * Construct a visitor that modifies an {@link java.io.OutputStream}'s write
     * calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     */
    public OutputStreamAdapter(ClassVisitor cv) {
        super(Opcodes.ASM5, cv);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature,
                exceptions);
        if (mv != null) {
            // TODO and the name and signature match a write call
            mv = new WriteMethodAdapter(mv);
        }
        return mv;
    }

}
