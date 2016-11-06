/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.agent;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.security.ProtectionDomain;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

import sun.tools.attach.BsdAttachProvider;
import sun.tools.attach.LinuxAttachProvider;
import sun.tools.attach.SolarisAttachProvider;
import sun.tools.attach.WindowsAttachProvider;

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.spi.AttachProvider;

public class StatisticsFileSystemAgent {

    public static final String SFS_AGENT_LOGGER_NAME_KEY = "logger.name";

    public static final String SFS_AGENT_INPUTSTREAM_CLASSES_KEY = "inputStream.classes";

    public static final String SFS_AGENT_OUTPUSTREAM_CLASSES_KEY = "outputStream.classes";

    private static StatisticsFileSystemAgent instance = null;

    private final Instrumentation inst;

    private final Logger fsLogger;

    private static final Log LOG = LogFactory
            .getLog(StatisticsFileSystemAgent.class);

    private StatisticsFileSystemAgent(String agentArgs, Instrumentation inst)
            throws Exception {
        this.inst = inst;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing agent with '" + agentArgs + "'");
        }

        // Make options easily accesible through lookup
        Map<String, String> options = new HashMap<String, String>();
        for (String arg : agentArgs.split(",")) {
            String[] keyValue = arg.split("=");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid argument: " + arg);
            }
            options.put(keyValue[0], keyValue[1]);
        }

        // Obtain logger
        fsLogger = LogManager.getLogger(options.get(SFS_AGENT_LOGGER_NAME_KEY));

        // Transformer that adds log calls to an InputStreams read calls
        ClassFileTransformer inputStreamClassTransformer = new ClassFileTransformer() {
            @Override
            public byte[] transform(ClassLoader loader, String className,
                    Class<?> classBeingRedefined,
                    ProtectionDomain protectionDomain, byte[] classfileBuffer)
                    throws IllegalClassFormatException {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Transforming InputStream class: "
                            + classBeingRedefined.getName());
                }

                ClassReader cr = new ClassReader(classfileBuffer);
                ClassWriter cw = new ClassWriter(0);
                cr.accept(new InputStreamAdapter(cw), 0);
                return cw.toByteArray();
            }
        };
        this.inst.addTransformer(inputStreamClassTransformer, true);
        this.inst.retransformClasses(getClasses(
                options.get(SFS_AGENT_INPUTSTREAM_CLASSES_KEY),
                InputStream.class));
        this.inst.removeTransformer(inputStreamClassTransformer);

        // Repeat for OutputStream
        ClassFileTransformer outputStreamClassFileTransformer = new ClassFileTransformer() {
            @Override
            public byte[] transform(ClassLoader loader, String className,
                    Class<?> classBeingRedefined,
                    ProtectionDomain protectionDomain, byte[] classfileBuffer)
                    throws IllegalClassFormatException {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Transforming OutputStream class: "
                            + classBeingRedefined.getName());
                }

                ClassReader cr = new ClassReader(classfileBuffer);
                ClassWriter cw = new ClassWriter(0);
                cr.accept(new OutputStreamAdapter(cw), 0);
                return cw.toByteArray();
            }
        };
        this.inst.addTransformer(outputStreamClassFileTransformer, true);
        this.inst.retransformClasses(getClasses(
                options.get(SFS_AGENT_OUTPUSTREAM_CLASSES_KEY),
                OutputStream.class));
        this.inst.removeTransformer(outputStreamClassFileTransformer);
    }

    public static StatisticsFileSystemAgent loadAgent(String agentArgs)
            throws Exception {
        // Get proper VM provider depending on the OS
        AttachProvider ap = null;
        if (SystemUtils.IS_OS_LINUX) {
            ap = new LinuxAttachProvider();
        } else if (SystemUtils.IS_OS_SOLARIS) {
            ap = new SolarisAttachProvider();
        } else if (SystemUtils.IS_OS_WINDOWS) {
            ap = new WindowsAttachProvider();
        } else if (SystemUtils.IS_OS_UNIX) {
            ap = new BsdAttachProvider();
        } else {
            throw new RuntimeException("Unsupported OS: " + SystemUtils.OS_NAME);
        }

        // Get full name of our jar file, which includes the agent as well
        String jarFilePath = null;
        String classpath = System.getProperty("java.class.path");
        String[] classpathEntries = classpath.split(File.pathSeparator);
        for (String classpathEntry : classpathEntries) {
            if (classpathEntry.endsWith("hdfs-statistics-adapter.jar")) {
                jarFilePath = new File(classpathEntry).getAbsolutePath();
                break;
            }
        }
        if (jarFilePath == null) {
            throw new RuntimeException("Could not obtain full path to jar file");
        }

        // Get VM id, usually 'PID@hostname'
        String vmName = ManagementFactory.getRuntimeMXBean().getName();
        String[] vmNameParts = vmName.split("@");
        if (vmNameParts.length != 2) {
            throw new RuntimeException("Unexpected VM name found: " + vmName);
        }

        // Attach to this VM, loadAgent returns when agentmain() has completed
        VirtualMachine vm = ap.attachVirtualMachine(vmNameParts[0]);
        vm.loadAgent(jarFilePath, agentArgs);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded agent from jar '" + jarFilePath
                    + "' with arguments '" + agentArgs + "'");
        }
        vm.detach();

        return instance;
    }

    public static void agentmain(String agentArgs, Instrumentation inst)
            throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("agentmain(" + agentArgs + "," + inst + ")");
        }
        instance = new StatisticsFileSystemAgent(agentArgs, inst);
    }

    public static void premain(String agentArgs, Instrumentation inst)
            throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("premain(" + agentArgs + "," + inst + ")");
        }
        agentmain(agentArgs, inst);
    }

    // Helper methods

    /**
     * Transforms a string of the form 'className1:className2' into an array of
     * the according classes.
     * 
     * @param classNames
     * @return
     * @throws ClassNotFoundException
     */
    private static Class<?>[] getClasses(String classNamesString,
            Class<?> commonBaseClass) throws ClassNotFoundException {
        String[] classNames = classNamesString.split(":");
        Class<?>[] classes = new Class<?>[classNames.length];
        for (int i = 0; i < classNames.length; ++i) {
            classes[i] = Class.forName(classNames[i]).asSubclass(
                    commonBaseClass);
        }
        return classes;
    }
}