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

public class InputStreamAdapter extends ClassVisitor {

    public static class ReadMethodAdapter extends MethodVisitor {
        public ReadMethodAdapter(MethodVisitor mv) {
            super(Opcodes.ASM5, mv);
        }
    }

    /**
     * Construct a visitor that modifies an {@link java.io.InputStream}'s read
     * calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     */
    public InputStreamAdapter(ClassVisitor cv) {
        super(Opcodes.ASM5, cv);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature,
                exceptions);
        if (mv != null) {
            // TODO and the name and signature match a read call
            mv = new ReadMethodAdapter(mv);
        }
        return mv;
    }

}
