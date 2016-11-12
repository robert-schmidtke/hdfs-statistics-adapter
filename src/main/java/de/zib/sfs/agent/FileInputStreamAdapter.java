/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.agent;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;
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

    private final Logger fsLogger;

    private final FileDescriptorBlacklist fileDescriptorBlacklist;

    private final String nativeMethodPrefix;

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
     * @param fsLogger
     * @param fileDescriptorBlacklist
     * @param nativeMethodPrefix
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public FileInputStreamAdapter(ClassVisitor cv, Logger fsLogger,
            FileDescriptorBlacklist fileDescriptorBlacklist,
            String nativeMethodPrefix) throws NoSuchMethodException,
            SecurityException {
        super(Opcodes.ASM5, cv);

        this.fsLogger = fsLogger;
        this.fileDescriptorBlacklist = fileDescriptorBlacklist;
        this.nativeMethodPrefix = nativeMethodPrefix;

        methodDescriptors = new HashMap<String, String>();
        methodSignatures = new HashMap<String, String>();
        methodExceptions = new HashMap<String, String[]>();
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if ("open".equals(name)) {
            // rename native open method
            mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
                    signature, exceptions);
        } else if ("read".equals(name)) {
            // TODO rename native read method
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        } else if ("readBytes".equals(name)) {
            // TODO rename native readBytes method
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
        // add open method that calls the renamed native version
        MethodVisitor mv = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                methodDescriptors.get("open"), methodSignatures.get("open"),
                methodExceptions.get("open"));

        // begin code generation
        mv.visitCode();

        // TODO add (fd,name) pair to blacklist mappings

        // load this pointer onto stack
        mv.visitVarInsn(Opcodes.ALOAD, 0);

        // load name argument onto stack
        mv.visitVarInsn(Opcodes.ALOAD, 1);

        // this.nativeMethodPrefix + open(name);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java.io.FileInputStream",
                nativeMethodPrefix + "open", methodDescriptors.get("open"),
                false);

        // return;
        mv.visitInsn(Opcodes.RETURN);

        // this and name both on the stack and locally
        mv.visitMaxs(2, 2);

        // end code generation
        mv.visitEnd();

        cv.visitEnd();
    }

}
