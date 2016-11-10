/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class OperationInfoFactory {

    public static OperationInfo parseFromLogLine(String logLine) {
        String line = logLine;

        // line starts with hostname:
        int index = line.indexOf(":");
        String hostname = line.substring(0, index);
        line = line.substring(index + 1, line.length());

        // next is endtime-
        index = line.indexOf("-");
        long endTime = Long.parseLong(line.substring(0, index));
        line = line.substring(index + 1, line.length());

        // next is duration:
        index = line.indexOf(":");
        long duration = Long.parseLong(line.substring(0, index));
        line = line.substring(index + 1, line.length());

        // next is classname@
        index = line.indexOf("@");
        String className = line.substring(0, index);
        line = line.substring(index + 1, line.length());

        // next is instance.
        index = line.indexOf(".");
        String instance = line.substring(0, index);
        line = line.substring(index + 1, line.length());

        // next is operation(
        index = line.indexOf("(");
        String operation = line.substring(0, index);
        line = line.substring(index + 1, line.length());

        // next is arguments):
        index = line.indexOf("):");
        String args[] = line.substring(0, index).split(",");
        line = line.substring(index + 2, line.length());

        OperationInfo operationInfo;
        switch (operation) {
        case "read": {
            // next is result->
            index = line.indexOf("->");
            long data = Long.parseLong(line.substring(0, index));
            line = line.substring(index + 2, line.length());

            if (args.length == 0) {
                // 1 byte read, returns -1 if EOF
                data = data == -1 ? 0 : 1;
            } else if (args.length == 1 || args.length == 3 || args.length == 4) {
                // data is already correct
                data = data == -1 ? 0 : data;
            } else {
                // illegal number of arguments
                throw new IllegalArgumentException(
                        "Unrecognized read operation: " + logLine);
            }

            // line now only contains the remote host that was read from, if any
            operationInfo = new ReadDataOperationInfo(hostname, operation,
                    endTime - duration, endTime, data, line);
            break;
        }
        case "readFully": {
            // next is void->
            index = line.indexOf("->");
            line = line.substring(index + 2, line.length());

            long data;
            if (args.length == 2) {
                data = Long
                        .parseLong(args[1].substring(1, args[1].length() - 1));
            } else if (args.length == 4) {
                data = Long.parseLong(args[3]);
            } else {
                // illegal number of arguments
                throw new IllegalArgumentException(
                        "Unrecognized readFully operation: " + logLine);
            }

            // line now only contains the remote host that was read from, if any
            operationInfo = new ReadDataOperationInfo(hostname, operation,
                    endTime - duration, endTime, data, line);
            break;
        }
        case "write": {
            long data;
            if (args.length == 1) {
                if (args[0].startsWith("[") && args[0].endsWith("]")) {
                    data = Long.parseLong(args[0].substring(1,
                            args[0].length() - 1));
                } else {
                    // 1 byte write
                    data = 1;
                }
            } else if (args.length == 3) {
                data = Long.parseLong(args[2]);
            } else {
                // illegal number of arguments
                throw new IllegalArgumentException(
                        "Unrecognized write operation: " + logLine);
            }

            operationInfo = new DataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data);
            break;
        }
        default:
            operationInfo = new OperationInfo(hostname, operation, endTime
                    - duration, endTime);
            break;
        }

        return operationInfo;
    }

}
