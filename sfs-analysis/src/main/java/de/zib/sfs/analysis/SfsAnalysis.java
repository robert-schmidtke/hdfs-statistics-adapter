/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class SfsAnalysis {

    private static final String INPUT_PATH_KEY = "inputPath";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String inputPath = params.get(INPUT_PATH_KEY);
        if (inputPath == null || inputPath.isEmpty()) {
            throw new IllegalArgumentException(INPUT_PATH_KEY
                    + " cannot be empty");
        }

        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        env.fromElements(inputPath)
                .flatMap(new FlatMapFunction<String, String>() {
                    private static final long serialVersionUID = -4182178352545729395L;

                    @Override
                    public void flatMap(String value,
                            final Collector<String> out) throws Exception {
                        Process hostnameProcess = Runtime.getRuntime().exec(
                                "hostname");
                        int exitCode = hostnameProcess.waitFor();
                        String hostname;
                        if (exitCode != 0) {
                            hostname = System.getenv("HOSTNAME");
                        } else {
                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(hostnameProcess
                                            .getInputStream()));

                            StringBuilder hostnameBuilder = new StringBuilder();
                            String line = "";
                            while ((line = reader.readLine()) != null) {
                                hostnameBuilder.append(line);
                            }
                            reader.close();
                            hostname = hostnameBuilder.toString();
                        }

                        out.collect(hostname);
                        Stream<Path> files = Files.list(Paths.get(value));
                        files.forEach(new Consumer<Path>() {
                            @Override
                            public void accept(Path t) {
                                out.collect(t.toString());
                            }
                        });
                        files.close();
                    }
                }).print();
    }
}
