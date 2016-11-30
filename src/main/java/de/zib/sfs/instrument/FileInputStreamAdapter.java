/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Class adapter that instruments {@link java.io.FileInputStream}.
 * 
 * @author robert
 *
 */
public class FileInputStreamAdapter extends ClassVisitor {

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
     * @param nativeMethodPrefix
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    public FileInputStreamAdapter(ClassVisitor cv, String nativeMethodPrefix)
            throws NoSuchMethodException, SecurityException {
        super(Opcodes.ASM5, cv);

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
            // rename native open method so we can wrap it
            mv = cv.visitMethod(access, nativeMethodPrefix + name, desc,
                    signature, exceptions);
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
        String fileInputStreamInternalName = Type
                .getInternalName(FileInputStream.class);
        String fileInputStreamCallbackInternalName = Type
                .getInternalName(FileInputStreamCallback.class);
        String fileInputStreamCallbackGetInstanceMethodDescriptor = Type
                .getMethodDescriptor(
                        Type.getType(FileInputStreamCallback.class),
                        Type.getType(FileInputStream.class));

        // private void open(String name) {
        MethodVisitor openMV = cv.visitMethod(Opcodes.ACC_PRIVATE, "open",
                methodDescriptors.get("open"), methodSignatures.get("open"),
                methodExceptions.get("open"));
        openMV.visitCode();

        // FileInputStreamCallback.getInstance(this).onOpenBegin(name);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName,
                "onOpenBegin",
                Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class)), false);

        // nativeMethodPrefixopen(name);
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitVarInsn(Opcodes.ALOAD, 1);
        openMV.visitMethodInsn(Opcodes.INVOKESPECIAL,
                fileInputStreamInternalName, nativeMethodPrefix + "open",
                methodDescriptors.get("open"), false);

        // FileInputStreamCallback.getInstance(this).onOpenEnd();
        openMV.visitVarInsn(Opcodes.ALOAD, 0);
        openMV.visitMethodInsn(Opcodes.INVOKESTATIC,
                fileInputStreamCallbackInternalName, "getInstance",
                fileInputStreamCallbackGetInstanceMethodDescriptor, false);
        openMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                fileInputStreamCallbackInternalName, "onOpenEnd",
                Type.getMethodDescriptor(Type.VOID_TYPE), false);

        // }
        openMV.visitInsn(Opcodes.RETURN);
        openMV.visitMaxs(0, 0);
        openMV.visitEnd();

        cv.visitEnd();

        // TODO add wrapper methods for above renamed methods
    }
}
