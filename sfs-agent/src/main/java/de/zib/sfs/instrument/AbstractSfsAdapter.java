/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

import de.zib.sfs.instrument.statistics.OperationCategory;

public abstract class AbstractSfsAdapter extends ClassVisitor {

    protected final String methodPrefix;

    protected final String instrumentedTypeInternalName;
    protected final String callbackTypeInternalName, callbackTypeDescriptor,
            callbackTypeConstructorDescriptor;

    protected final String systemInternalName, currentTimeMillisDescriptor;

    private final Set<OperationCategory> skip;

    protected <InstrumentedType, CallbackType> AbstractSfsAdapter(
            ClassVisitor cv, Class<InstrumentedType> instrumentedTypeClass,
            Class<CallbackType> callbackTypeClass, String methodPrefix,
            Set<OperationCategory> skip) {
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

        this.skip = skip;
    }

    protected <InstrumentedType> AbstractSfsAdapter(ClassVisitor cv,
            Class<InstrumentedType> instrumentedTypeClass,
            Set<OperationCategory> skip) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = null;

        instrumentedTypeInternalName = Type
                .getInternalName(instrumentedTypeClass);
        callbackTypeInternalName = null;
        callbackTypeDescriptor = null;
        callbackTypeConstructorDescriptor = null;

        systemInternalName = Type.getInternalName(System.class);
        currentTimeMillisDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        this.skip = skip;
    }

    protected <CallbackType> AbstractSfsAdapter(ClassVisitor cv,
            String instrumentedTypeInternalName,
            Class<CallbackType> callbackTypeClass, String methodPrefix,
            Set<OperationCategory> skip) {
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

        this.skip = skip;
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

        // private final InstrumentationActive instrumentationActive;
        FieldVisitor instrumentationActiveFV = cv.visitField(
                Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
                "instrumentationActive",
                Type.getDescriptor(InstrumentationActive.class), null, null);
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

        // if (isInstrumentationActive()) {
        isInstrumentationActive(mv);
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

        // setInstrumentationActive(true);
        setInstrumentationActive(mv, true);

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

        // setInstrumentationActive(false);
        setInstrumentationActive(mv, false);

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

        // this.instrumentationActive.setInstrumentationActive(instrumentationActive);
        setInstrumentationActiveMV.visitVarInsn(Opcodes.ALOAD, 0);
        setInstrumentationActiveMV.visitFieldInsn(Opcodes.GETFIELD,
                instrumentedTypeInternalName, "instrumentationActive",
                Type.getDescriptor(InstrumentationActive.class));
        setInstrumentationActiveMV.visitVarInsn(Opcodes.ILOAD, 1);
        setInstrumentationActiveMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(InstrumentationActive.class),
                "setInstrumentationActive",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                false);

        // }
        setInstrumentationActiveMV.visitInsn(Opcodes.RETURN);
        setInstrumentationActiveMV.visitMaxs(0, 0);
        setInstrumentationActiveMV.visitEnd();

        // public boolean isInstrumentationActive() {
        MethodVisitor isInstrumentationActiveMV = cv.visitMethod(
                Opcodes.ACC_PUBLIC, "isInstrumentationActive",
                Type.getMethodDescriptor(Type.BOOLEAN_TYPE), null, null);
        isInstrumentationActiveMV.visitCode();

        // return instrumentationActive.isInstrumentationActive();
        // }
        isInstrumentationActiveMV.visitVarInsn(Opcodes.ALOAD, 0);
        isInstrumentationActiveMV.visitFieldInsn(Opcodes.GETFIELD,
                instrumentedTypeInternalName, "instrumentationActive",
                Type.getDescriptor(InstrumentationActive.class));
        isInstrumentationActiveMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(InstrumentationActive.class),
                "isInstrumentationActive",
                Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
        isInstrumentationActiveMV.visitInsn(Opcodes.IRETURN);
        isInstrumentationActiveMV.visitMaxs(0, 0);
        isInstrumentationActiveMV.visitEnd();

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

    protected void isInstrumentationActive(MethodVisitor mv) {
        // isInstrumentationActive();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, instrumentedTypeInternalName,
                "isInstrumentationActive",
                Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
    }

    protected void setInstrumentationActive(MethodVisitor mv,
            boolean instrumentationActive) {
        // setInstrumentationActive(<instrumentationActive>);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(
                instrumentationActive ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, instrumentedTypeInternalName,
                "setInstrumentationActive",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                false);
    }

    protected void initializeFields(MethodVisitor constructorMV,
            String constructorDesc) {
    }

    protected boolean skipReads() {
        return skip.contains(OperationCategory.READ);
    }

    protected boolean skipWrites() {
        return skip.contains(OperationCategory.WRITE);
    }

    protected boolean skipOther() {
        return skip.contains(OperationCategory.OTHER);
    }

    protected boolean skipZip() {
        return skip.contains(OperationCategory.ZIP);
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

            // instrumentationActive = new InstrumentationActive();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitTypeInsn(Opcodes.NEW,
                    Type.getInternalName(InstrumentationActive.class));
            mv.visitInsn(Opcodes.DUP);
            try {
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                        Type.getInternalName(InstrumentationActive.class),
                        "<init>",
                        Type.getConstructorDescriptor(
                                InstrumentationActive.class.getConstructor()),
                        false);
            } catch (Exception e) {
                throw new RuntimeException("Could not access constructor", e);
            }
            mv.visitFieldInsn(Opcodes.PUTFIELD, instrumentedTypeInternalName,
                    "instrumentationActive",
                    Type.getDescriptor(InstrumentationActive.class));
        }

        @Override
        protected void onMethodExit(int opcode) {
            if (opcode == Opcodes.RETURN) {
                initializeFields(mv, methodDesc);
            }
        }

    }

    /**
     * Custom "thread local" variable that tracks instrumentation.
     * 
     * @author robert
     *
     */
    public static class InstrumentationActive {
        private final Map<Thread, Boolean> instrumentingThreads = new ConcurrentHashMap<Thread, Boolean>();

        public boolean isInstrumentationActive() {
            return instrumentingThreads.containsKey(Thread.currentThread());
        }

        public void setInstrumentationActive(boolean instrumentationActive) {
            if (instrumentationActive) {
                instrumentingThreads.put(Thread.currentThread(), true);
            } else {
                instrumentingThreads.remove(Thread.currentThread());
            }
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
