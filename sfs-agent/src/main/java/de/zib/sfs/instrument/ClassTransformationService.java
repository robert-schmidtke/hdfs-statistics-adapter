/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClassTransformationService {

    public static void main(String args[]) {
        int i = 0;
        int serverPort = -1, agentPort = -1;
        int timeoutSeconds = 30;
        boolean verbose = false;
        while (i < args.length) {
            switch (args[i]) {
            case "--port":
                serverPort = Integer.parseInt(args[++i]);
                break;
            case "--communication-port-agent":
                agentPort = Integer.parseInt(args[++i]);
                break;
            case "--timeout":
                timeoutSeconds = Integer.parseInt(args[++i]);
                break;
            case "--verbose":
                verbose = "y".equals(args[++i]);
                break;
            }
            ++i;
        }

        if (serverPort < 0 && agentPort < 0) {
            System.err.println("Could not parse options: "
                    + Arrays.toString(args));
            System.err.println("Required options for standalone mode:");
            System.err.println("  --port port");
            System.err.println("Required options for slave mode:");
            System.err.println("  --communication-port-agent port");
            System.err.println("Optional options:");
            System.err.println("  --timeout seconds (default: 30)");
            System.err.println("  --verbose y|n (default: n)");
            System.exit(1);
        }

        LogUtil.enableStderrLogging(verbose);
        LogUtil.stderr("Starting class transformation service.\n");

        // quieten gRPC
        Logger.getLogger("io.grpc").setLevel(Level.SEVERE);

        // start the transformer server
        ClassTransformationServer classTransformationServer = null;

        if (serverPort < 0) {
            // the transformation server should find a port on its own
            LogUtil.stderr("Starting on random port.\n");

            Random random = new Random();
            int port = -1, tries = 0;
            boolean started = false;
            do {
                try {
                    ++tries;
                    port = random.nextInt(16384) + 49152;
                    LogUtil.stderr(
                            "Trying to start transformation server on port '%d'.\n",
                            port);
                    classTransformationServer = new ClassTransformationServer(
                            port);
                    classTransformationServer.start();
                    started = true;
                } catch (IOException e) {

                }
            } while (!started && tries < 10);
            if (!started) {
                System.err.println("Could not start transformer server after "
                        + tries + " tries.");
                System.exit(1);
            }
            LogUtil.stderr("Started transformation server on port '%d'.\n",
                    port);

            // signal to the agent that we are ready to receive transformation
            // requests
            LogUtil.stderr("Registering with agent on port '%d'.\n", agentPort);
            ClassTransformationClient classTransformationClient = new ClassTransformationClient(
                    agentPort);
            classTransformationClient.beginClassTransformations(port);
            try {
                LogUtil.stderr("Shutting down client.\n");
                classTransformationClient.shutdown();
            } catch (InterruptedException e) {
                System.err.println("Could not shut down transformer client");
                e.printStackTrace();

                try {
                    classTransformationServer.shutdown();
                } catch (InterruptedException e1) {
                    System.err
                            .println("Could not shut down transformer server");
                    e1.printStackTrace();
                }

                System.exit(1);
            }
        } else {
            // we have a dedicated port to run on
            LogUtil.stderr("Starting on dedicated port '%d'.\n", serverPort);
            classTransformationServer = new ClassTransformationServer(
                    serverPort);
            try {
                classTransformationServer.start();
            } catch (IOException e) {
                System.err
                        .println("Could not start transformer server on port "
                                + serverPort + ".");
                System.exit(1);
            }
        }
        LogUtil.stderr("Successfully started transformation server.\n");

        // shut down the server when this VM is shut down
        LogUtil.stderr("Registering shutdown hook.\n");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            private ClassTransformationServer classTransformationServer;

            public Thread setClassTransformationServer(
                    ClassTransformationServer classTransformationServer) {
                this.classTransformationServer = classTransformationServer;
                return this;
            }

            @Override
            public void run() {
                LogUtil.stderr("Running shutdown hook.\n");
                try {
                    if (classTransformationServer != null) {
                        LogUtil.stderr("Shutting down server.\n");
                        classTransformationServer.shutdown();
                    }
                } catch (InterruptedException e) {
                    System.err
                            .println("Could not shut down transformer server");
                    e.printStackTrace();
                }
            }
        }.setClassTransformationServer(classTransformationServer));

        // wait at most X seconds for the agent to signal it is done
        try {
            LogUtil.stderr("Wating '%s' for sthudown signal.\n",
                    timeoutSeconds > 0 ? (timeoutSeconds + " seconds")
                            : "indefinitely");
            if (!classTransformationServer
                    .awaitEndClassTransformations(timeoutSeconds)) {
                System.err
                        .println("Agent failed to finish class transformations within "
                                + timeoutSeconds + " seconds");
            }
        } catch (InterruptedException e) {
            System.err
                    .println("Error waiting for agent to finish class transformations");
            e.printStackTrace();
            System.exit(1);
        }

        LogUtil.stderr("Stopping class transformation service.\n");
    }
}
