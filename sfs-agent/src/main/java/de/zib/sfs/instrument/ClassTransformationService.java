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
        int agentPort = -1;
        int timeoutSeconds = 30;
        while (i < args.length) {
            switch (args[i]) {
            case "--communication-port-agent":
                agentPort = Integer.parseInt(args[++i]);
                break;
            case "--timeout":
                timeoutSeconds = Integer.parseInt(args[++i]);
                break;
            }
            ++i;
        }

        if (agentPort < 0) {
            System.err.println("Could not parse options: "
                    + Arrays.toString(args));
            System.err.println("Required options:");
            System.err.println("  --communication-port-agent port");
            System.err.println("Optional options:");
            System.err.println("  --timeout seconds (default: 30)");
            System.exit(1);
        }

        // quieten gRPC
        Logger.getLogger("io.grpc").setLevel(Level.SEVERE);

        // start the transformer server
        ClassTransformationServer classTransformationServer = null;

        // assume that all that can go wrong during startup is a port that is
        // already in use
        Random random = new Random();
        int port = -1, tries = 0;
        boolean started = false;
        do {
            try {
                ++tries;
                port = random.nextInt(16384) + 49152;
                classTransformationServer = new ClassTransformationServer(port);
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
                System.err.println("Could not shut down transformer server");
                e1.printStackTrace();
            }

            System.exit(1);
        }

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
        } finally {
            try {
                classTransformationServer.shutdown();
            } catch (InterruptedException e) {
                System.err.println("Could not shut down transformer server");
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
