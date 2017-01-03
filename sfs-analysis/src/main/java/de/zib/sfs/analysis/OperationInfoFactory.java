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

        // next is result->targetHostname or result
        index = line.indexOf("->");
        String result, targetHostname;
        if (index != -1) {
            result = line.substring(0, index);
            targetHostname = line.substring(index + 2, line.length());
        } else {
            result = line;
            targetHostname = null;
        }

        switch (className) {
        case "java.io.FileInputStream":
            return parseFileInputStreamOperationInfo(hostname, endTime,
                    duration, instance, operation, args, result, targetHostname);
        case "java.io.FileOutputStream":
            return parseFileOutputStreamOperationInfo(hostname, endTime,
                    duration, instance, operation, args, result, targetHostname);
        case "java.io.RandomAccessFile":
            return parseRandomAccessFileOperationInfo(hostname, endTime,
                    duration, instance, operation, args, result, targetHostname);
        case "sun.nio.ch.FileChannelImpl":
            return parseFileChannelImplOperationInfo(hostname, endTime,
                    duration, instance, operation, args, result, targetHostname);
        case "de.zib.sfs.StatisticsFileSystem":
            return parseStatisticsFileSystemOperationInfo(hostname, endTime,
                    duration, instance, operation, args, result, targetHostname);
        case "de.zib.sfs.WrappedFSDataInputStream":
            return parseWrappedFSDataInputStreamOperationInfo(hostname,
                    endTime, duration, instance, operation, args, result,
                    targetHostname);
        case "de.zib.sfs.WrappedFSDataOutputStream":
            return parseWrappedFSDataOutputStreamOperationInfo(hostname,
                    endTime, duration, instance, operation, args, result,
                    targetHostname);
        default:
            throw new IllegalArgumentException("Unknown class " + className
                    + " found in line " + logLine);
        }
    }

    private static OperationInfo parseFileInputStreamOperationInfo(
            String hostname, long endTime, long duration, String instance,
            String operation, String[] args, String result,
            String targetHostname) {
        switch (operation) {
        // {}:{}.open({}):void
        case "open": {
            return new OperationInfo(hostname, operation, endTime - duration,
                    endTime);
        }
        // {}:{}.read():{}->{}
        case "read": {
            // 1 byte read, -1 indicates EOF
            long data = Long.parseLong(result) == -1 ? 0 : 1;
            return new ReadDataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data, targetHostname);
        }
        // {}:{}.readBytes([{}],{},{}):{}->{}
        case "readBytes": {
            long data = Long.parseLong(result);
            data = data == -1 ? 0 : data;
            return new ReadDataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data, targetHostname);
        }
        default:
            throw new IllegalArgumentException("Unknown operation " + operation
                    + " for FileInputStream");
        }
    }

    private static OperationInfo parseFileOutputStreamOperationInfo(
            String hostname, long endTime, long duration, String instance,
            String operation, String[] args, String result,
            String targetHostname) {
        switch (operation) {
        // {}:{}.open({},{}):void
        case "open": {
            return new OperationInfo(hostname, operation, endTime - duration,
                    endTime);
        }
        // {}:{}.write({},{}):void
        case "write": {
            // 1 byte write
            return new DataOperationInfo(hostname, operation, endTime
                    - duration, endTime, 1);
        }
        // {}:{}.writeBytes([{}],{},{},{}):void
        case "writeBytes": {
            long data = Long.parseLong(args[2]);
            return new DataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data);
        }
        default:
            throw new IllegalArgumentException("Unknown operation " + operation
                    + " for FileOutputStream");
        }
    }

    private static OperationInfo parseRandomAccessFileOperationInfo(
            String hostname, long endTime, long duration, String instance,
            String operation, String[] args, String result,
            String targetHostname) {
        switch (operation) {
        // {}:{}.open({},{}):void
        case "open": {
            return new OperationInfo(hostname, operation, endTime - duration,
                    endTime);
        }
        // {}:{}.read():{}->{}
        case "read": {
            // 1 byte read, -1 indicates EOF
            long data = Long.parseLong(result) == -1 ? 0 : 1;
            return new ReadDataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data, targetHostname);
        }
        // {}:{}.readBytes([{}],{},{}):{}->{}
        case "readBytes": {
            long data = Long.parseLong(result);
            data = data == -1 ? 0 : data;
            return new ReadDataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data, targetHostname);
        }
        // {}:{}.write({}):void
        case "write": {
            // 1 byte write
            return new DataOperationInfo(hostname, operation, endTime
                    - duration, endTime, 1);
        }
        // {}:{}.writeBytes([{}],{},{}):void
        case "writeBytes": {
            long data = Long.parseLong(args[2]);
            return new DataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data);
        }
        default:
            throw new IllegalArgumentException("Unknown operation " + operation
                    + " for RandomAccessFile");
        }
    }

    private static OperationInfo parseFileChannelImplOperationInfo(
            String hostname, long endTime, long duration, String instance,
            String operation, String[] args, String result,
            String targetHostname) {
        switch (operation) {
        // {}:{}.read({}):{}->{}
        // {}:{}.read([{}],{},{}):{}->{}
        case "read": {
            long data = Long.parseLong(result);
            data = data == -1 ? 0 : data;
            return new ReadDataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data, targetHostname);
        }
        // {}:{}.write({}):{}
        // {}:{}.write([{}],{},{}):{}
        case "write": {
            long data = Long.parseLong(result);
            return new DataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data);
        }
        default:
            throw new IllegalArgumentException("Unknown operation " + operation
                    + " for FileChannelImpl");
        }
    }

    private static OperationInfo parseStatisticsFileSystemOperationInfo(
            String hostname, long endTime, long duration, String instance,
            String operation, String[] args, String result,
            String targetHostname) {
        switch (operation) {
        // {}:{}.append({},{}):{}
        case "append": {
            // fall through
        }
        // {}:{}.create({},{},{},{},{},{}):{}
        case "create": {
            // fall through
        }
        // {}:{}.delete({},{}):{}
        case "delete": {
            // fall through
        }
        // {}:{}.getFileBlockLocations({},{},{}):{}
        case "getFileBlockLocations": {
            // fall through
        }
        // {}:{}.getFileStatus({}):{}
        case "getFileStatus": {
            // fall through
        }
        // {}:{}.listStatus({}):{}
        case "listStatus": {
            // fall through
        }
        // {}:{}.mkdirs({},{}):{}
        case "mkdirs": {
            // fall through
        }
        // {}:{}.open({},{}):{}
        case "open": {
            // fall through
        }
        // {}:{}.rename({},{}):{}
        case "rename": {
            return new OperationInfo(hostname, operation, endTime - duration,
                    endTime);
        }
        default:
            throw new IllegalArgumentException("Unknown operation " + operation
                    + " for StatisticsFileSystem");
        }
    }

    private static OperationInfo parseWrappedFSDataInputStreamOperationInfo(
            String hostname, long endTime, long duration, String instance,
            String operation, String[] args, String result,
            String targetHostname) {
        switch (operation) {
        // {}:{}.read():{}->{}
        // {}:{}.read([{}],{},{}):{}->{}
        // {}:{}.read([{}]):{}->{}
        // {}:{}.read({},[{}],{},{}):{}->{}
        case "read": {
            long data = Long.parseLong(result);
            if (args.length == 0) {
                // 1 byte read, -1 indicates EOF
                data = data == -1 ? 0 : 1;
            } else {
                data = data == -1 ? 0 : data;
            }
            return new ReadDataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data, targetHostname);
        }
        // {}:{}.readFully({},[{}]):void->{}
        // {}:{}.readFully({},[{}],{},{}):void->{}
        case "readFully": {
            long data;
            if (args.length == 2) {
                data = Long
                        .parseLong(args[1].substring(1, args[1].length() - 1));
            } else {
                data = Long.parseLong(args[3]);
            }
            return new ReadDataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data, targetHostname);
        }
        // {}:{}.seek({}):void->{}
        case "seek": {
            // fall through
        }
        // {}:{}.seekToNewSource({}):{}->{}
        case "seekToNewSource": {
            return new OperationInfo(hostname, operation, endTime - duration,
                    endTime);
        }
        default:
            throw new IllegalArgumentException("Unknown operation " + operation
                    + " for WrappedFSDataInputStream");
        }
    }

    private static OperationInfo parseWrappedFSDataOutputStreamOperationInfo(
            String hostname, long endTime, long duration, String instance,
            String operation, String[] args, String result,
            String targetHostname) {
        switch (operation) {
        // {}:{}.write({}):void
        // {}:{}.write([{}]):void
        // {}:{}.write([{}],{},{}):void
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
            } else {
                data = Long.parseLong(args[2]);
            }
            return new DataOperationInfo(hostname, operation, endTime
                    - duration, endTime, data);
        }
        default:
            throw new IllegalArgumentException("Unknown operation " + operation
                    + " for WrappedFSDataOutputStream");
        }
    }

}
