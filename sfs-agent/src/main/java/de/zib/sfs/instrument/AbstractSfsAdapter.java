/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.PrintStream;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

public abstract class AbstractSfsAdapter extends ClassVisitor {

    protected final String methodPrefix;

    protected final String instrumentedTypeInternalName;
    protected final String callbackTypeInternalName, callbackTypeDescriptor,
            callbackTypeConstructorDescriptor;

    protected final String systemInternalName, currentTimeMillisDescriptor;

    protected <InstrumentedType, CallbackType> AbstractSfsAdapter(
            ClassVisitor cv, Class<InstrumentedType> instrumentedTypeClass,
            Class<CallbackType> callbackTypeClass, String methodPrefix) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = methodPrefix;

        instrumentedTypeInternalName = Type
                .getInternalName(instrumentedTypeClass);
        callbackTypeInternalName = Type.getInternalName(callbackTypeClass);
        callbackTypeDescriptor = Type.getDescriptor(callbackTypeClass);
        try {
            callbackTypeConstructorDescriptor = Type.getConstructorDescriptor(
                    callbackTypeClass.getConstructor());
        } catch (Exception e) {
            throw new IllegalArgumentException("No-arg constructor of "
                    + callbackTypeInternalName + " is not accessible.");
        }

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);
    }

    protected <InstrumentedType> AbstractSfsAdapter(ClassVisitor cv,
            Class<InstrumentedType> instrumentedTypeClass) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = null;

        instrumentedTypeInternalName = Type
                .getInternalName(instrumentedTypeClass);
        callbackTypeInternalName = null;
        callbackTypeDescriptor = null;
        callbackTypeConstructorDescriptor = null;

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);
    }

    protected <CallbackType> AbstractSfsAdapter(ClassVisitor cv,
            String instrumentedTypeInternalName,
            Class<CallbackType> callbackTypeClass, String methodPrefix) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = methodPrefix;

        this.instrumentedTypeInternalName = instrumentedTypeInternalName;
        callbackTypeInternalName = Type.getInternalName(callbackTypeClass);
        callbackTypeDescriptor = Type.getDescriptor(callbackTypeClass);
        try {
            callbackTypeConstructorDescriptor = Type.getConstructorDescriptor(
                    callbackTypeClass.getConstructor());
        } catch (Exception e) {
            throw new IllegalArgumentException("No-arg constructor of "
                    + callbackTypeInternalName + " is not accessible.");
        }

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);
    }

    protected void appendFields(ClassVisitor cv) {
    }

    @Override
    public void visitSource(String source, String debug) {
        if (callbackTypeDescriptor != null) {
            // private final <CallbackType> callback;
            FieldVisitor callbackFV = cv.visitField(
                    Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "callback",
                    callbackTypeDescriptor, null, null);
            callbackFV.visitEnd();
        }

        // protected boolean instrumentationActive;
        FieldVisitor instrumentationActiveFV = cv.visitField(
                Opcodes.ACC_PROTECTED, "instrumentationActive",
                Type.getDescriptor(Boolean.TYPE), null, null);
        instrumentationActiveFV.visitEnd();

        appendFields(cv);

        cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if ("<init>".equals(name)) {
            mv = new ConstructorAdapter(
                    cv.visitMethod(access, name, desc, signature, exceptions),
                    access, name, desc);
        } else if (wrapMethod(access, name, desc, signature, exceptions)) {
            // rename methods so we can wrap them
            mv = cv.visitMethod(access, methodPrefix + name, desc, signature,
                    exceptions);
        } else {
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        }
        return mv;
    }

    protected abstract boolean wrapMethod(int access, String name, String desc,
            String signature, String[] exceptions);

    protected void wrapMethod(int access, String name, Type returnType,
            Type[] argumentTypes, String signature, String[] exceptions,
            String callbackName, Type additionalCallbackArgumentType,
            ResultPasser resultPasser) {
        argumentTypes = argumentTypes == null ? new Type[] {} : argumentTypes;
        String methodDescriptor = Type.getMethodDescriptor(returnType,
                argumentTypes);

        // <access> <returnType> <name>(<argumentTypes> arguments) throws
        // <exceptions> {
        MethodVisitor mv = cv.visitMethod(access, name, methodDescriptor,
                signature, exceptions);
        mv.visitCode();

        // if (instrumentationActive) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, instrumentedTypeInternalName,
                "instrumentationActive", Type.getDescriptor(Boolean.TYPE));
        Label instrumentationActiveLabel = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, instrumentationActiveLabel);

        // return? methodPrefix<name>(arguments);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        int argumentIndex = 1;
        for (Type argument : argumentTypes) {
            mv.visitVarInsn(argument.getOpcode(Opcodes.ILOAD), argumentIndex);
            argumentIndex += argument.getSize();
        }
        mv.visitMethodInsn(
                (access & Opcodes.ACC_STATIC) == 0 ? Opcodes.INVOKESPECIAL
                        : Opcodes.INVOKESTATIC,
                instrumentedTypeInternalName, methodPrefix + name,
                methodDescriptor, false);
        if (!Type.VOID_TYPE.equals(returnType)) {
            mv.visitInsn(returnType.getOpcode(Opcodes.IRETURN));
        } else {
            mv.visitInsn(Opcodes.RETURN);
        }

        // }
        mv.visitLabel(instrumentationActiveLabel);

        // instrumentationActive = true;
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.ICONST_1);
        mv.visitFieldInsn(Opcodes.PUTFIELD, instrumentedTypeInternalName,
                "instrumentationActive", Type.getDescriptor(Boolean.TYPE));

        // long startTime = System.currentTimeMillis();
        int startTimeIndex = 1;
        for (Type argument : argumentTypes) {
            startTimeIndex += argument.getSize();
        }
        storeTime(mv, startTimeIndex);

        // <returnType> result =? methodPrefix<name>(arguments);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        argumentIndex = 1;
        for (Type argument : argumentTypes) {
            mv.visitVarInsn(argument.getOpcode(Opcodes.ILOAD), argumentIndex);
            argumentIndex += argument.getSize();
        }
        mv.visitMethodInsn(
                (access & Opcodes.ACC_STATIC) == 0 ? Opcodes.INVOKESPECIAL
                        : Opcodes.INVOKESTATIC,
                instrumentedTypeInternalName, methodPrefix + name,
                methodDescriptor, false);
        int endTimeIndex = startTimeIndex + 2;
        if (!Type.VOID_TYPE.equals(returnType)) {
            mv.visitVarInsn(returnType.getOpcode(Opcodes.ISTORE),
                    startTimeIndex + 2);
            endTimeIndex += returnType.getSize();
        }

        // long endTime = System.currentTimeMillis();
        storeTime(mv, endTimeIndex);

        // callback.<callbackMethod>(startTime, endTime, result?);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, instrumentedTypeInternalName,
                "callback", callbackTypeDescriptor);
        mv.visitVarInsn(Opcodes.LLOAD, startTimeIndex);
        mv.visitVarInsn(Opcodes.LLOAD, endTimeIndex);

        // -1 indicates no result should be passed
        int resultIndex = resultPasser.getResultIndex();
        if (resultIndex != -1) {
            // result of the actual operation requested
            if (resultIndex == 0) {
                mv.visitVarInsn(returnType.getOpcode(Opcodes.ILOAD),
                        startTimeIndex + 2);
                resultPasser.passResult(mv);
            } else {
                // some parameter requested
                mv.visitVarInsn(
                        argumentTypes[resultIndex - 1].getOpcode(Opcodes.ILOAD),
                        resultIndex);
                resultPasser.passResult(mv);
            }
        }

        Type[] callbackArgumentTypes;
        if (additionalCallbackArgumentType == null) {
            callbackArgumentTypes = new Type[] { Type.LONG_TYPE,
                    Type.LONG_TYPE };
        } else {
            callbackArgumentTypes = new Type[] { Type.LONG_TYPE, Type.LONG_TYPE,
                    additionalCallbackArgumentType };
        }
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, callbackTypeInternalName,
                callbackName,
                Type.getMethodDescriptor(Type.VOID_TYPE, callbackArgumentTypes),
                false);

        // instrumentationActive = false;
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitFieldInsn(Opcodes.PUTFIELD, instrumentedTypeInternalName,
                "instrumentationActive", Type.getDescriptor(Boolean.TYPE));

        // return result;?
        // }
        if (!Type.VOID_TYPE.equals(returnType)) {
            mv.visitVarInsn(returnType.getOpcode(Opcodes.ILOAD),
                    startTimeIndex + 2);
            mv.visitInsn(returnType.getOpcode(Opcodes.IRETURN));
        } else {
            mv.visitInsn(Opcodes.RETURN);
        }
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    protected void appendWrappedMethods(ClassVisitor cv) {
    }

    @Override
    public void visitEnd() {
        // public void setInstrumentationActive(boolean instrumentationActive) {
        MethodVisitor setInstrumentationActiveMV = cv.visitMethod(
                Opcodes.ACC_PUBLIC, "setInstrumentationActive",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                null, null);
        setInstrumentationActiveMV.visitCode();

        // this.fromFileChannel = instrumentationActive;
        setInstrumentationActiveMV.visitVarInsn(Opcodes.ALOAD, 0);
        setInstrumentationActiveMV.visitVarInsn(Opcodes.ILOAD, 1);
        setInstrumentationActiveMV.visitFieldInsn(Opcodes.PUTFIELD,
                instrumentedTypeInternalName, "instrumentationActive",
                Type.getDescriptor(Boolean.TYPE));

        // }
        setInstrumentationActiveMV.visitInsn(Opcodes.RETURN);
        setInstrumentationActiveMV.visitMaxs(0, 0);
        setInstrumentationActiveMV.visitEnd();

        appendWrappedMethods(cv);

        cv.visitEnd();
    }

    protected void println(MethodVisitor mv, String string) {
        // System.err.println(<string>);
        mv.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(System.class),
                "err", Type.getDescriptor(PrintStream.class));
        mv.visitLdcInsn(string);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(PrintStream.class), "println",
                Type.getMethodDescriptor(Type.VOID_TYPE,
                        Type.getType(String.class)),
                false);
    }

    protected void storeTime(MethodVisitor mv, int index) {
        // long time = System.currentTimeMillis();
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, systemInternalName,
                "currentTimeMillis", currentTimeMillisDescriptor, false);
        mv.visitVarInsn(Opcodes.LSTORE, index);
    }

    protected void initializeFields(MethodVisitor constructorMV) {
    }

    protected class ConstructorAdapter extends AdviceAdapter {

        protected ConstructorAdapter(MethodVisitor mv, int access, String name,
                String desc) {
            super(Opcodes.ASM5, mv, access, name, desc);
        }

        @Override
        protected void onMethodEnter() {
            if (callbackTypeInternalName != null) {
                // callback = new <CallbackType>();
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitTypeInsn(Opcodes.NEW, callbackTypeInternalName);
                mv.visitInsn(Opcodes.DUP);
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                        callbackTypeInternalName, "<init>",
                        callbackTypeConstructorDescriptor, false);
                mv.visitFieldInsn(Opcodes.PUTFIELD,
                        instrumentedTypeInternalName, "callback",
                        callbackTypeDescriptor);
            }

            // instrumentationActive = false;
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitFieldInsn(Opcodes.PUTFIELD, instrumentedTypeInternalName,
                    "instrumentationActive", Type.getDescriptor(Boolean.TYPE));

            initializeFields(mv);
        }

    }

    protected static interface ResultPasser {
        public int getResultIndex();

        public void passResult(MethodVisitor mv);
    }

    protected static class DiscardResultPasser implements ResultPasser {
        @Override
        public int getResultIndex() {
            return -1;
        }

        @Override
        public void passResult(MethodVisitor mv) {
        }
    }

    protected static class ReturnResultPasser implements ResultPasser {
        @Override
        public int getResultIndex() {
            return 0;
        }

        @Override
        public void passResult(MethodVisitor mv) {
        }
    }

    protected static class ParameterResultPasser implements ResultPasser {
        protected final int index;

        protected ParameterResultPasser(int index) {
            this.index = index;
        }

        @Override
        public int getResultIndex() {
            return index;
        }

        @Override
        public void passResult(MethodVisitor mv) {
        }
    }

}
