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
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClassTransformationService {

    public static void main(String args[]) {
        int i = 0;
        int agentPort = -1, transformerPort = -1;
        while (i < args.length) {
            switch (args[i]) {
            case "--communication-port-agent":
                agentPort = Integer.parseInt(args[++i]);
                break;
            case "--communication-port-transformer":
                transformerPort = Integer.parseInt(args[++i]);
                break;
            }
            ++i;
        }

        if (agentPort < 0 || transformerPort < 0) {
            System.err.println("Could not parse options: "
                    + Arrays.toString(args));
            System.err.println("Required options:");
            System.err.println("  --communication-port-agent port");
            System.err.println("  --communication-port-transformer port");
            System.exit(1);
        }

        // quieten gRPC
        Logger.getLogger("io.grpc").setLevel(Level.SEVERE);

        // start the transformer server
        ClassTransformationServer classTransformationServer = new ClassTransformationServer(
                transformerPort);
        try {
            classTransformationServer.start();
        } catch (IOException e) {
            System.err.println("Could not start transformer server");
            e.printStackTrace();
            System.exit(1);
        }

        // signal to the agent that we are ready to receive transformation
        // requests
        ClassTransformationClient classTransformationClient = new ClassTransformationClient(
                agentPort);
        classTransformationClient.beginClassTransformations();
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

        // wait at most 30 seconds for the agent to signal it is done
        try {
            if (!classTransformationServer.awaitEndClassTransformations(30)) {
                System.err
                        .println("Agent failed to finish class transformations within 30 seconds");
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
