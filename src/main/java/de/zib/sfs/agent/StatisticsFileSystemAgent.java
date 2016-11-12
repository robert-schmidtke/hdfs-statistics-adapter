/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

import sun.nio.ch.FileChannelImpl;
import sun.tools.attach.BsdAttachProvider;
import sun.tools.attach.LinuxAttachProvider;
import sun.tools.attach.SolarisAttachProvider;
import sun.tools.attach.WindowsAttachProvider;

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.spi.AttachProvider;

public class StatisticsFileSystemAgent {

    public static final String SFS_AGENT_LOGGER_NAME_KEY = "logger.name";

    public static final String SFS_AGENT_BLACKLISTED_FILENAMES_KEY = "blacklist.filenames";

    private static StatisticsFileSystemAgent instance = null;

    private final Instrumentation inst;

    private final Logger fsLogger;

    private final FileDescriptorBlacklist fileDescriptorBlacklist;

    private static final Log LOG = LogFactory
            .getLog(StatisticsFileSystemAgent.class);

    private StatisticsFileSystemAgent(String agentArgs, Instrumentation inst)
            throws Exception {
        this.inst = inst;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing agent with '" + agentArgs + "'");
        }

        // Make options easily accessible through lookup
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

        // Build blacklist of file names not to log access to
        fileDescriptorBlacklist = new FileDescriptorBlacklist();
        if (options.get(SFS_AGENT_BLACKLISTED_FILENAMES_KEY) != null) {
            for (String blacklistedFilename : options.get(
                    SFS_AGENT_BLACKLISTED_FILENAMES_KEY).split(":")) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Not logging access to file: "
                            + blacklistedFilename);
                }
                fileDescriptorBlacklist
                        .addBlacklistedFilename(blacklistedFilename);
            }
        }

        // Transformer that adds log calls to read and write calls of certain
        // classes
        ClassFileTransformer ioClassTransformer = new ClassFileTransformer() {
            @Override
            public byte[] transform(ClassLoader loader, String className,
                    Class<?> classBeingRedefined,
                    ProtectionDomain protectionDomain, byte[] classfileBuffer)
                    throws IllegalClassFormatException {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Transforming I/O class: "
                            + classBeingRedefined.getName());
                }

                ClassReader cr = new ClassReader(classfileBuffer);
                ClassWriter cw = new ClassWriter(0);

                try {
                    switch (classBeingRedefined.getName()) {
                    case "java.io.FileInputStream":
                        cr.accept(new FileInputStreamAdapter(cw, fsLogger,
                                fileDescriptorBlacklist, "sfsa_native_"), 0);
                        break;
                    // case "java.io.FileOutputStream":
                    // cr.accept(new FileOutputStreamAdapter(cw), 0);
                    // break;
                    // case "sun.nio.ch.FileChannelImpl":
                    // cr.accept(new FileChannelImplAdapter(cw), 0);
                    // break;
                    }
                } catch (Exception e) {
                    LOG.warn("Could not transform I/O class: "
                            + classBeingRedefined.getName(), e);
                    cr.accept(cw, 0);
                }
                return cw.toByteArray();
            }
        };

        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin transforming I/O classes");
        }

        this.inst.addTransformer(ioClassTransformer, true);
        this.inst.setNativeMethodPrefix(ioClassTransformer, "sfsa_native_");
        this.inst.retransformClasses(FileInputStream.class,
                FileOutputStream.class, FileChannelImpl.class);
        this.inst.removeTransformer(ioClassTransformer);

        if (LOG.isDebugEnabled()) {
            LOG.debug("End transforming I/O classes");
        }
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
}
