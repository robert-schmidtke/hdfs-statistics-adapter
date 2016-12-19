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
            System.exit(1);
        }

        // quieten gRPC
        Logger.getLogger("io.grpc").setLevel(Level.SEVERE);

        // start the transformer server
        ClassTransformationServer classTransformationServer = null;

        if (serverPort < 0) {
            // the transformation server should find a port on its own
            Random random = new Random();
            int port = -1, tries = 0;
            boolean started = false;
            do {
                try {
                    ++tries;
                    port = random.nextInt(16384) + 49152;
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

            // signal to the agent that we are ready to receive transformation
            // requests
            ClassTransformationClient classTransformationClient = new ClassTransformationClient(
                    agentPort);
            classTransformationClient.beginClassTransformations(port);
            try {
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

        // shut down the server when this VM is shut down
        Runtime.getRuntime().addShutdownHook(new Thread() {
            private ClassTransformationServer classTransformationServer;

            public Thread setClassTransformationServer(
                    ClassTransformationServer classTransformationServer) {
                this.classTransformationServer = classTransformationServer;
                return this;
            }

            @Override
            public void run() {
                try {
                    if (classTransformationServer != null) {
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
    }
}
