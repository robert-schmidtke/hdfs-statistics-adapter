/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.util.HashMap;
import java.util.Map;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Class adapter that instruments {@link java.io.FileInputStream}.
 * 
 * @author robert
 *
 */
public class FileInputStreamAdapter extends ClassVisitor {

    private final String nativeMethodPrefix;

    private final String ignoreFileName;

    private final Map<String, String> methodDescriptors, methodSignatures;
    private final Map<String, String[]> methodExceptions;

    /**
     * Construct a visitor that modifies an {@link java.io.FileInputStream}'s
     * read calls. Delegate to {@code cv} by default.
     * 
     * @param cv
     *            {@link org.objectweb.asm.ClassVisitor} to delegate all visit
     *            calls to which are not explicitly overridden here. Most likely
     *            a {@link org.objectweb.asm.ClassWriter}.
     * @param nativeMethodPrefix
     * @param ignoreFileName
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public FileInputStreamAdapter(ClassVisitor cv, String nativeMethodPrefix,
            String ignoreFileName) throws NoSuchMethodException,
            SecurityException {
        super(Opcodes.ASM5, cv);

        this.nativeMethodPrefix = nativeMethodPrefix;
        this.ignoreFileName = ignoreFileName;

        methodDescriptors = new HashMap<String, String>();
        methodSignatures = new HashMap<String, String>();
        methodExceptions = new HashMap<String, String[]>();
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if ("open".equals(name)) {
            // TODO rename native open method so we can wrap it
            // mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
            // signature, exceptions);
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        } else if ("read".equals(name)) {
            // TODO rename native read method so we can wrap it
            // mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
            // signature, exceptions);
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        } else if ("readBytes".equals(name)) {
            // TODO rename native readBytes method so we can wrap it
            // mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
            // signature, exceptions);
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        } else {
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        }

        // remember method information for generating wrappers
        methodDescriptors.put(name, desc);
        methodSignatures.put(name, signature);
        methodExceptions.put(name, exceptions);

        return mv;
    }

    @Override
    public void visitEnd() {

        cv.visitEnd();

        // TODO add wrapper methods for above renamed methods
    }
}
