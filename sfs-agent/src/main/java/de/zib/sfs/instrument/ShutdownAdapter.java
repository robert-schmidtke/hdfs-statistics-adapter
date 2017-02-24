/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;

public class ShutdownAdapter extends ClassVisitor {

    public ShutdownAdapter(ClassVisitor cv) {
        super(Opcodes.ASM5, cv);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        MethodVisitor mv;
        if (isHaltOrShutdownMethod(access, name, desc, signature, exceptions)) {
            // because who needs shutdown hooks anyway
            mv = new HaltAndShutdownMethodAdapter(api,
                    cv.visitMethod(access, name, desc, signature, exceptions),
                    access, name, desc);
        } else {
            mv = cv.visitMethod(access, name, desc, signature, exceptions);
        }
        return mv;
    }

    private static class HaltAndShutdownMethodAdapter extends AdviceAdapter {

        protected HaltAndShutdownMethodAdapter(int api, MethodVisitor mv,
                int access, String name, String desc) {
            super(api, mv, access, name, desc);
        }

        @Override
        protected void onMethodEnter() {
            String liveOperationStatisticsAggregatorInternalName = Type
                    .getInternalName(LiveOperationStatisticsAggregator.class);
            mv.visitFieldInsn(Opcodes.GETSTATIC,
                    liveOperationStatisticsAggregatorInternalName, "instance",
                    Type.getDescriptor(
                            LiveOperationStatisticsAggregator.class));
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                    liveOperationStatisticsAggregatorInternalName, "shutdown",
                    Type.getMethodDescriptor(Type.VOID_TYPE), false);
        }

    }

    // Helper methods

    private boolean isHaltOrShutdownMethod(int access, String name, String desc,
            String signature, String[] exceptions) {
        // shutdown() is called by JNI during regular termination of the JVM,
        // halt(exitCode) is called when the user terminates the JVM via
        // System.exit(errorCode); or Runtime.halt(exitCode);
        return access == Opcodes.ACC_STATIC
                && (("halt".equals(name) && Type
                        .getMethodDescriptor(Type.VOID_TYPE, Type.INT_TYPE)
                        .equals(desc))
                        || ("shutdown".equals(name)
                                && Type.getMethodDescriptor(Type.VOID_TYPE)
                                        .equals(desc)))
                && null == signature
                && (exceptions == null || exceptions.length == 0);
    }

}
