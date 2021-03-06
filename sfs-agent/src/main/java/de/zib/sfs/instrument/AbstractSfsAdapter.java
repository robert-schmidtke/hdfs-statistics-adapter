/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.PrintStream;
import java.util.Set;

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

    protected final String systemInternalName, nanoTimeDescriptor;

    private final Set<OperationCategory> skip;

    protected <InstrumentedType, CallbackType> AbstractSfsAdapter(
            ClassVisitor cv, Class<InstrumentedType> instrumentedTypeClass,
            Class<CallbackType> callbackTypeClass, String methodPrefix,
            Set<OperationCategory> skip) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = methodPrefix;

        this.instrumentedTypeInternalName = Type
                .getInternalName(instrumentedTypeClass);
        this.callbackTypeInternalName = Type.getInternalName(callbackTypeClass);
        this.callbackTypeDescriptor = Type.getDescriptor(callbackTypeClass);
        try {
            this.callbackTypeConstructorDescriptor = Type
                    .getConstructorDescriptor(callbackTypeClass
                            .getConstructor(instrumentedTypeClass));
        } catch (Exception e) {
            throw new IllegalArgumentException("Constructor of "
                    + this.callbackTypeInternalName + " is not accessible.", e);
        }

        this.systemInternalName = Type.getInternalName(System.class);
        this.nanoTimeDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        this.skip = skip;
    }

    protected <InstrumentedType> AbstractSfsAdapter(ClassVisitor cv,
            Class<InstrumentedType> instrumentedTypeClass,
            Set<OperationCategory> skip) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = null;

        this.instrumentedTypeInternalName = Type
                .getInternalName(instrumentedTypeClass);
        this.callbackTypeInternalName = null;
        this.callbackTypeDescriptor = null;
        this.callbackTypeConstructorDescriptor = null;

        this.systemInternalName = Type.getInternalName(System.class);
        this.nanoTimeDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        this.skip = skip;
    }

    protected <CallbackType> AbstractSfsAdapter(ClassVisitor cv,
            String instrumentedTypeInternalName,
            Class<CallbackType> callbackTypeClass, String methodPrefix,
            Set<OperationCategory> skip) {
        super(Opcodes.ASM5, cv);

        this.methodPrefix = methodPrefix;

        this.instrumentedTypeInternalName = instrumentedTypeInternalName;
        this.callbackTypeInternalName = Type.getInternalName(callbackTypeClass);
        this.callbackTypeDescriptor = Type.getDescriptor(callbackTypeClass);
        try {
            this.callbackTypeConstructorDescriptor = Type
                    .getConstructorDescriptor(
                            callbackTypeClass.getConstructor());
        } catch (Exception e) {
            throw new IllegalArgumentException("No-arg constructor of "
                    + this.callbackTypeInternalName + " is not accessible.", e);
        }

        this.systemInternalName = Type.getInternalName(System.class);
        this.nanoTimeDescriptor = Type.getMethodDescriptor(Type.LONG_TYPE);

        this.skip = skip;
    }

    /**
     * @param visitor
     */
    protected void appendFields(ClassVisitor visitor) {
        // children may override
    }

    @Override
    public void visitSource(String source, String debug) {
        if (this.callbackTypeDescriptor != null) {
            // private final <CallbackType> callback;
            FieldVisitor callbackFV = this.cv.visitField(
                    Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "callback",
                    this.callbackTypeDescriptor, null, null);
            callbackFV.visitEnd();
        }

        // private final InstrumentationActive instrumentationActive;
        FieldVisitor instrumentationActiveFV = this.cv.visitField(
                Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
                "instrumentationActive",
                Type.getDescriptor(InstrumentationActive.class), null, null);
        instrumentationActiveFV.visitEnd();

        appendFields(this.cv);

        this.cv.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if ("<init>".equals(name)) {
            mv = new ConstructorAdapter(this.cv.visitMethod(access, name, desc,
                    signature, exceptions), access, name, desc);
        } else if (wrapMethod(access, name, desc, signature, exceptions)) {
            // rename methods so we can wrap them
            mv = this.cv.visitMethod(access, this.methodPrefix + name, desc,
                    signature, exceptions);
        } else {
            mv = this.cv.visitMethod(access, name, desc, signature, exceptions);
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
        MethodVisitor mv = this.cv.visitMethod(access, name, methodDescriptor,
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
                this.instrumentedTypeInternalName, this.methodPrefix + name,
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

        // long startTime = System.nanoTime();
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
                this.instrumentedTypeInternalName, this.methodPrefix + name,
                methodDescriptor, false);
        int endTimeIndex = startTimeIndex + 2;
        if (!Type.VOID_TYPE.equals(returnType)) {
            mv.visitVarInsn(returnType.getOpcode(Opcodes.ISTORE),
                    startTimeIndex + 2);
            endTimeIndex += returnType.getSize();
        }

        // long endTime = System.nanoTime();
        storeTime(mv, endTimeIndex);

        // callback.<callbackMethod>(startTime, endTime, result?);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, this.instrumentedTypeInternalName,
                "callback", this.callbackTypeDescriptor);
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
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, this.callbackTypeInternalName,
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

    /**
     * @param visitor
     */
    protected void appendWrappedMethods(ClassVisitor visitor) {
        // children may override
    }

    @Override
    public void visitEnd() {
        // public void setInstrumentationActive(boolean instrumentationActive) {
        MethodVisitor setInstrumentationActiveMV = this.cv.visitMethod(
                Opcodes.ACC_PUBLIC, "setInstrumentationActive",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                null, null);
        setInstrumentationActiveMV.visitCode();

        // this.instrumentationActive.setInstrumentationActive(instrumentationActive);
        setInstrumentationActiveMV.visitVarInsn(Opcodes.ALOAD, 0);
        setInstrumentationActiveMV.visitFieldInsn(Opcodes.GETFIELD,
                this.instrumentedTypeInternalName, "instrumentationActive",
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
        MethodVisitor isInstrumentationActiveMV = this.cv.visitMethod(
                Opcodes.ACC_PUBLIC, "isInstrumentationActive",
                Type.getMethodDescriptor(Type.BOOLEAN_TYPE), null, null);
        isInstrumentationActiveMV.visitCode();

        // return instrumentationActive.isInstrumentationActive();
        // }
        isInstrumentationActiveMV.visitVarInsn(Opcodes.ALOAD, 0);
        isInstrumentationActiveMV.visitFieldInsn(Opcodes.GETFIELD,
                this.instrumentedTypeInternalName, "instrumentationActive",
                Type.getDescriptor(InstrumentationActive.class));
        isInstrumentationActiveMV.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(InstrumentationActive.class),
                "isInstrumentationActive",
                Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
        isInstrumentationActiveMV.visitInsn(Opcodes.IRETURN);
        isInstrumentationActiveMV.visitMaxs(0, 0);
        isInstrumentationActiveMV.visitEnd();

        appendWrappedMethods(this.cv);

        this.cv.visitEnd();
    }

    protected static void println(MethodVisitor mv, String string) {
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
        // long time = System.nanoTime();
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, this.systemInternalName,
                "nanoTime", this.nanoTimeDescriptor, false);
        mv.visitVarInsn(Opcodes.LSTORE, index);
    }

    protected void isInstrumentationActive(MethodVisitor mv) {
        // isInstrumentationActive();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                this.instrumentedTypeInternalName, "isInstrumentationActive",
                Type.getMethodDescriptor(Type.BOOLEAN_TYPE), false);
    }

    protected void setInstrumentationActive(MethodVisitor mv,
            boolean instrumentationActive) {
        // setInstrumentationActive(<instrumentationActive>);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(
                instrumentationActive ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                this.instrumentedTypeInternalName, "setInstrumentationActive",
                Type.getMethodDescriptor(Type.VOID_TYPE, Type.BOOLEAN_TYPE),
                false);
    }

    /**
     * @param constructorMV
     * @param constructorDesc
     */
    protected void initializeFields(MethodVisitor constructorMV,
            String constructorDesc) {
        // children may override
    }

    protected boolean skipReads() {
        return this.skip.contains(OperationCategory.READ);
    }

    protected boolean skipWrites() {
        return this.skip.contains(OperationCategory.WRITE);
    }

    protected boolean skipOther() {
        return this.skip.contains(OperationCategory.OTHER);
    }

    protected boolean skipZip() {
        return this.skip.contains(OperationCategory.ZIP);
    }

    protected class ConstructorAdapter extends AdviceAdapter {

        protected ConstructorAdapter(MethodVisitor mv, int access, String name,
                String desc) {
            super(Opcodes.ASM5, mv, access, name, desc);
        }

        @Override
        protected void onMethodEnter() {
            if (AbstractSfsAdapter.this.callbackTypeInternalName != null) {
                // callback = new <CallbackType>(this?);
                this.mv.visitVarInsn(Opcodes.ALOAD, 0);
                this.mv.visitTypeInsn(Opcodes.NEW,
                        AbstractSfsAdapter.this.callbackTypeInternalName);
                this.mv.visitInsn(Opcodes.DUP);
                if (!"()V".equals(
                        AbstractSfsAdapter.this.callbackTypeConstructorDescriptor)) {
                    this.mv.visitVarInsn(Opcodes.ALOAD, 0);
                }
                this.mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                        AbstractSfsAdapter.this.callbackTypeInternalName,
                        "<init>",
                        AbstractSfsAdapter.this.callbackTypeConstructorDescriptor,
                        false);
                this.mv.visitFieldInsn(Opcodes.PUTFIELD,
                        AbstractSfsAdapter.this.instrumentedTypeInternalName,
                        "callback",
                        AbstractSfsAdapter.this.callbackTypeDescriptor);

                if (skipOther()) {
                    // callback.skipOther();
                    this.mv.visitVarInsn(Opcodes.ALOAD, 0);
                    this.mv.visitFieldInsn(Opcodes.GETFIELD,
                            AbstractSfsAdapter.this.instrumentedTypeInternalName,
                            "callback",
                            AbstractSfsAdapter.this.callbackTypeDescriptor);
                    this.mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                            AbstractSfsAdapter.this.callbackTypeInternalName,
                            "skipOther",
                            Type.getMethodDescriptor(Type.VOID_TYPE), false);
                }
            }

            // instrumentationActive = new InstrumentationActive();
            this.mv.visitVarInsn(Opcodes.ALOAD, 0);
            this.mv.visitTypeInsn(Opcodes.NEW,
                    Type.getInternalName(InstrumentationActive.class));
            this.mv.visitInsn(Opcodes.DUP);
            try {
                this.mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
                        Type.getInternalName(InstrumentationActive.class),
                        "<init>",
                        Type.getConstructorDescriptor(
                                InstrumentationActive.class.getConstructor()),
                        false);
            } catch (Exception e) {
                throw new RuntimeException("Could not access constructor", e);
            }
            this.mv.visitFieldInsn(Opcodes.PUTFIELD,
                    AbstractSfsAdapter.this.instrumentedTypeInternalName,
                    "instrumentationActive",
                    Type.getDescriptor(InstrumentationActive.class));
        }

        @Override
        protected void onMethodExit(int opcode) {
            if (opcode == Opcodes.RETURN) {
                initializeFields(this.mv, this.methodDesc);
            }
        }

    }

    public static class InstrumentationActive extends ThreadLocal<Boolean> {
        public boolean isInstrumentationActive() {
            return get();
        }

        @Override
        protected Boolean initialValue() {
            return false;
        }

        public void setInstrumentationActive(boolean instrumentationActive) {
            set(instrumentationActive);
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
            // nothing to do
        }
    }

    protected static class ReturnResultPasser implements ResultPasser {
        @Override
        public int getResultIndex() {
            return 0;
        }

        @Override
        public void passResult(MethodVisitor mv) {
            // nothing to do
        }
    }

    protected static class ParameterResultPasser implements ResultPasser {
        protected final int index;

        protected ParameterResultPasser(int index) {
            this.index = index;
        }

        @Override
        public int getResultIndex() {
            return this.index;
        }

        @Override
        public void passResult(MethodVisitor mv) {
            // nothing to do
        }
    }

}
