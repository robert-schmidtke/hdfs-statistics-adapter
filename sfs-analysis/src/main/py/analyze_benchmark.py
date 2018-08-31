# Takes the path to an extracted benchmark run which contains BB files for each source/category.
# Parses them and plots them into a cumulative stacked chart.
# Parses the slurm.out file and prints I/O for each phase.

import math

import argparse
import datetime as dt
import matplotlib as mpl
import os

# use Agg backend if we're on a server
if os.getenv('DISPLAY') is None:
    # must be specified before pyplot is imported
    mpl.use('Agg')

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# get the benchmark's directory containing all files
parser = argparse.ArgumentParser()
parser.add_argument("bd", help="path to the benchmark directory")
args = parser.parse_args()

print("Processing {}".format(args.bd), flush=True)

# read all BB files into one DataFrame
os_raw_data = pd.DataFrame()
fd_raw_data = pd.DataFrame()

# assume operation statistics and file descriptor mappings data have been created together
if os.path.isfile("{}/raw_data.h5".format(args.bd)):
    try:
        print("Reading os_raw_data", flush=True)
        os_raw_data = pd.read_hdf("{}/raw_data.h5".format(args.bd), "os_raw_data")
    except KeyError:
        print("No os_raw_data found", flush=True)

    try:
        print("Reading fd_raw_data", flush=True)
        fd_raw_data = pd.read_hdf("{}/raw_data.h5".format(args.bd), "fd_raw_data")
    except KeyError:
        # for some reason empty dataframes are not written at all to hdf
        print("No fd_raw_data found", flush=True)
else:
    # === BB ===
    from multiprocessing import Pool
    import multiprocessing.sharedctypes as sct
    import ctypes as ct
    import osp
    import tarfile
    import time

    # first figure out memory requirements
    os_size, fd_size = 0, 0
    with tarfile.open("{}/bb.tar".format(args.bd), mode='r|') as bb:
        for entry in bb:
            if 'jvm' in entry.name or 'sfs' in entry.name:
                os_size += entry.size
            elif 'filedescriptormappings' in entry.name:
                fd_size += entry.size
    total_size = os_size + fd_size

    # for measuring progress
    current_size = 0
    progress = 0

    # read into shared memory

    # initialize shared counters here so they get their own shared memory region
    os_index = sct.Value(ct.c_int32, 0)
    fd_index = sct.Value(ct.c_int32, 0)

    # these will quite likely each have their own shared memory regions
    os_buffer, os_pos = sct.RawArray(ct.c_byte, os_size), 0
    fd_buffer, fd_pos = sct.RawArray(ct.c_byte, fd_size), 0

    # count operation statistics and file descriptor mappings
    os_count, fd_count = 0, 0
    os_offsets, fd_offsets = [], []
    with tarfile.open("{}/bb.tar".format(args.bd), mode='r|', bufsize=1048576) as bb:
        start_time = time.time()
        for entry in bb:
            current_size += entry.size
            new_progress = 100 * current_size / total_size
            if new_progress > .5:
                progress += new_progress
                current_speed = int(round(current_size / (1048576 * (time.time() - start_time))))
                current_size = 0
                start_time = time.time()
                print("Reading {}% at {} MiB/s".format(int(round(progress)), current_speed), flush=True)

            if entry.isfile():
                if 'jvm' in entry.name or 'sfs' in entry.name:
                    file = bb.extractfile(entry)
                    os_buffer[os_pos:os_pos + entry.size] = file.read()
                    os_offsets.append(os_pos)

                    # first 8 bytes (long) indicate number of elements
                    os_count += osp.ulong.unpack_from(os_buffer, os_pos)[0]
                    os_pos += entry.size

                    file.close()
                elif 'filedescriptormappings' in entry.name:
                    file = bb.extractfile(entry)
                    fd_buffer[fd_pos:fd_pos + entry.size] = file.read()
                    fd_offsets.append(fd_pos)

                    # first 4 bytes (int) indicate number of elements
                    fd_count += osp.uint.unpack_from(fd_buffer, fd_pos)[0]
                    fd_pos += entry.size

                    file.close()

    print("Setting up OS arrays", flush=True)

    # specify operation statistics array, again in separate memory region
    os_shared_arrays = (
        sct.RawArray(ct.c_wchar, os_count * 10),  # hostname: 10 * 4 bytes
        sct.RawArray(ct.c_int32, os_count),  # pid: 4 bytes
        sct.RawArray(ct.c_wchar, os_count * 7),  # key: 7 * 4 bytes
        sct.RawArray(ct.c_int64, os_count),  # time bin: 8 bytes
        sct.RawArray(ct.c_int64, os_count),  # count: 8 bytes
        sct.RawArray(ct.c_int64, os_count),  # CPU time: 8 bytes
        sct.RawArray(ct.c_wchar, os_count * 4),  # source: 4 * 4 bytes
        sct.RawArray(ct.c_wchar, os_count * 6),  # category: 6 * 4 bytes
        sct.RawArray(ct.c_int32, os_count),  # file descriptor: 4 bytes
        sct.RawArray(ct.c_int64, os_count),  # thread ID: 8 bytes
        sct.RawArray(ct.c_int64, os_count),  # data: 8 bytes
        sct.RawArray(ct.c_int64, os_count),  # remote count: 8 bytes
        sct.RawArray(ct.c_int64, os_count),  # remote CPU time: 8 bytes
        sct.RawArray(ct.c_int64, os_count)  # remote Data: 8 bytes
    )
    # os_count * 180 bytes

    print("Setting up FD arrays", flush=True)

    # specify file descriptor mappings array
    fd_shared_arrays = (
        sct.RawArray(ct.c_wchar, fd_count * 10),  # hostname: 10 * 4 bytes
        sct.RawArray(ct.c_int32, fd_count),  # pid: 4 bytes
        sct.RawArray(ct.c_wchar, fd_count * 7),  # key: 7 * 4 bytes
        sct.RawArray(ct.c_int32, fd_count),  # file descriptor 4 bytes
        sct.RawArray(ct.c_wchar, fd_count * 256)  # path: 256 * 4 bytes
    )
    # fd_count * 1100 bytes

    os_executor = Pool(processes=None, initializer=osp.os_init,
                       initargs=(os_shared_arrays, os_buffer, os_index))
    fd_executor = Pool(processes=None, initializer=osp.fd_init,
                       initargs=(fd_shared_arrays, fd_buffer, fd_index))

    print("Submitting tasks", flush=True)

    results = []
    for i in range(0, os_count, 100):
        results.append(os_executor.map_async(osp.parse_operation_statistics_bb, os_offsets[i:i + 100]))
    os_executor.close()
    for i in range(0, fd_count, 100):
        results.append(fd_executor.map_async(osp.parse_file_descriptor_mappings_bb, fd_offsets[i:i + 100]))
    fd_executor.close()

    del os_offsets
    del fd_offsets

    print("Waiting for tasks", flush=True)

    current_size = 0
    progress = 0
    start_time = time.time()
    for result in results:
        result.wait()
        current_size += sum(result.get())
        new_progress = 100 * current_size / total_size
        if int(round(new_progress)) > int(round(progress)):
            progress = new_progress
            current_speed = current_size / (time.time() - start_time)
            end_time = time.localtime(int(round(start_time + total_size / current_speed)))
            print("Processing {}% at {} KiB/s, ETA: {}".format(int(round(progress)), int(round(current_speed / 1024)),
                                                               time.strftime("%c", end_time)), flush=True)
    os_executor.join()
    fd_executor.join()

    del os_executor
    del fd_executor

    # we do not need the initial buffers any longer, close memory mapped files
    arenas = [
        os_buffer._wrapper._state[0][0],
        fd_buffer._wrapper._state[0][0],
        os_index._obj._wrapper._state[0][0],
        fd_index._obj._wrapper._state[0][0]]

    # need to remove all references...
    del os_buffer, fd_buffer, os_index, fd_index

    # ... before we can close the memory mapped files
    for arena in arenas:
        # since we have initialized the shared memory regions in order,
        # they do not share space with the regions we free below, which allows us to close regions here
        # do not close regions twice, as they may be shared
        if not arena.buffer.closed:
            arena.buffer.close()
            os.close(arena.fd)
    del arenas

    os_raw_data = pd.concat([
        pd.DataFrame(np.frombuffer(os_shared_arrays[0], dtype=[('hostname', np.unicode_, 10)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[1], dtype=[('pid', np.int32)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[2], dtype=[('key', np.unicode_, 7)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[3], dtype=[('timeBin', np.int64)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[4], dtype=[('count', np.int64)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[5], dtype=[('cpuTime', np.int64)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[6], dtype=[('source', np.unicode_, 4)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[7], dtype=[('category', np.unicode_, 6)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[8], dtype=[('fileDescriptor', np.int32)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[9], dtype=[('threadId', np.int64)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[10], dtype=[('data', np.int64)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[11], dtype=[('remoteCount', np.int64)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[12], dtype=[('remoteCpuTime', np.int64)])),
        pd.DataFrame(np.frombuffer(os_shared_arrays[13], dtype=[('remoteData', np.int64)]))
    ], axis=1, copy=False)

    fd_raw_data = pd.concat([
        pd.DataFrame(np.frombuffer(fd_shared_arrays[0], dtype=[('hostname', np.unicode_, 10)])),
        pd.DataFrame(np.frombuffer(fd_shared_arrays[1], dtype=[('pid', np.int32)])),
        pd.DataFrame(np.frombuffer(fd_shared_arrays[2], dtype=[('key', np.unicode_, 7)])),
        pd.DataFrame(np.frombuffer(fd_shared_arrays[3], dtype=[('fileDescriptor', np.int32)])),
        pd.DataFrame(np.frombuffer(fd_shared_arrays[4], dtype=[('path', np.unicode_, 256)]))
    ], axis=1, copy=False)

    # repeat for shared arrays
    arenas = [a._wrapper._state[0][0] for a in os_shared_arrays + fd_shared_arrays]
    del os_shared_arrays, fd_shared_arrays
    for arena in arenas:
        if not arena.buffer.closed:
            arena.buffer.close()
            os.close(arena.fd)
    del arenas
    # === BB ===

    # # === CSV ===
    # files = os.listdir(args.bd)
    # for f in files:
    #     if ('jvm' in f or 'sfs' in f) and f.endswith('csv'):
    #         print("Processing {}".format(f), flush=True)
    #         # drop NA rows within the new dataframe (= incomplete CSV line), fill NA values from concatenation with 0
    #         os_raw_data = pd.concat([os_raw_data, pd.read_csv("{}/{}".format(args.bd, f)).dropna()]).fillna(0)
    # # === CSV ===

    print("Writing os_raw_data", flush=True)
    os_raw_data.to_hdf("{}/raw_data.h5".format(args.bd), "os_raw_data", mode='w', format='table', append=True,
                       data_columns=["hostname"])

    print("Writing fd_raw_data", flush=True)
    fd_raw_data.to_hdf("{}/raw_data.h5".format(args.bd), "fd_raw_data", mode='a', format='table', append=True,
                       data_columns=["hostname"])

files = os.listdir(args.bd)
slurm_file = None
for f in files:
    if f.startswith('slurm') and f.endswith('out'):
        slurm_file = "{}/{}".format(args.bd, f)

if slurm_file is None:
    raise Exception("No slurm.out file found")

# parse start and end times from the log file, as well as interesting I/O
stats = dict()
with open(slurm_file) as f:
    for line in f:
        # removes leading spaces and trailing newline
        line = line.strip()

        # messages generated by us, extract start and end time for TeraGen/TeraSort from them
        if line.startswith("Using engine: "):
            stats['terasort.engine'] = line[len("Using engine: "):]
        elif line.endswith(": Generating TeraSort data on HDFS"):
            date = line[:-len(": Generating TeraSort data on HDFS")].replace('  ', ' ')
            stats['teragen.time.start'] = dt.datetime.strptime(date, '%a %b %d %H:%M:%S %Z %Y').timestamp()
        elif line.endswith(": Generating TeraSort data on HDFS done"):
            date = date = line[:-len(": Generating TeraSort data on HDFS done")].replace('  ', ' ')
            stats['teragen.time.end'] = dt.datetime.strptime(date, '%a %b %d %H:%M:%S %Z %Y').timestamp()
        elif line.endswith(": Running TeraSort"):
            date = line[:-len(": Running TeraSort")].replace('  ', ' ')
            stats['terasort.time.start'] = dt.datetime.strptime(date, '%a %b %d %H:%M:%S %Z %Y').timestamp()
        elif line.endswith(": Running TeraSort done: 0"):
            date = line[:-len(": Running TeraSort done: 0")].replace('  ', ' ')
            stats['terasort.time.end'] = dt.datetime.strptime(date, '%a %b %d %H:%M:%S %Z %Y').timestamp()
        # MapReduce framework counters
        elif line.startswith("HDFS: Number of bytes read="):
            # first encounter is TeraGen
            if stats.get('teragen.io.hdfs.read', None) is None:
                stats['teragen.io.hdfs.read'] = int(line.split('=')[1])
            # second encounter is TeraSort
            elif stats.get('terasort.io.hdfs.read', None) is None:
                stats['terasort.io.hdfs.read'] = int(line.split('=')[1])
        elif line.startswith("HDFS: Number of bytes written="):
            if stats.get('teragen.io.hdfs.write', None) is None:
                stats['teragen.io.hdfs.write'] = int(line.split('=')[1])
            elif stats.get('terasort.io.hdfs.write', None) is None:
                stats['terasort.io.hdfs.write'] = int(line.split('=')[1])
        elif line.startswith("FILE: Number of bytes read="):
            if stats.get('teragen.io.file.read', None) is None:
                stats['teragen.io.file.read'] = int(line.split('=')[1])
            elif stats.get('terasort.io.file.read', None) is None:
                stats['terasort.io.file.read'] = int(line.split('=')[1])
        elif line.startswith("FILE: Number of bytes written="):
            if stats.get('teragen.io.file.write', None) is None:
                stats['teragen.io.file.write'] = int(line.split('=')[1])
            elif stats.get('terasort.io.file.write', None) is None:
                stats['terasort.io.file.write'] = int(line.split('=')[1])
        elif line.startswith("Reduce shuffle bytes="):
            stats['terasort.io.shuffle.read'] = int(line.split('=')[1])
        elif line.startswith("Map output materialized bytes="):
            stats['terasort.io.shuffle.write'] = int(line.split('=')[1])
        elif line.startswith("Spilled Records="):
            if stats.get('teragen.io.spill', None) is None:
                stats['teragen.io.spill'] = int(line.split('=')[1])
            elif stats.get('terasort.io.spill', None) is None:
                stats['terasort.io.spill'] = int(line.split('=')[1])

# capture actual time in nanoseconds between TeraGen and TeraSort
teragen_terasort_border_ns = 1000000000 * int(
    round(stats['teragen.time.end'] + (stats['terasort.time.start'] - stats['teragen.time.end']) / 2.0))

# use beginning of TeraGen as zero and reset all times
stats['terasort.time.end'] -= stats['teragen.time.start']
stats['terasort.time.start'] -= stats['teragen.time.start']
stats['teragen.time.end'] -= stats['teragen.time.start']
stats['teragen.time.start'] = 0

# read XFS and ext4 statistics as well
mounts = {
    'xfs': {'all'},
    'ext4': {'all'}
}
for f in files:
    # file name is <jobid>-<hostname>.<filesystem>.<mount>.<phase>
    # e.g. 1234-cumu02-00.xfs.local.mid
    if 'xfs' in f:
        parts = f.split('.')
        mount = parts[2]
        mounts['xfs'].add(mount)
        phase = parts[3]
        with open("{}/{}".format(args.bd, f)) as lines:
            for line in lines:
                if line.startswith('xpc'):
                    # xpc <xs_xstrat_bytes> <xs_write_bytes> <xs_read_bytes>
                    xpc = line.split(' ')

                    # add artificial mount that receives all I/O as aggregate
                    for m in [mount, 'all']:
                        if phase == 'pre':
                            stats['teragen.io.xfs.' + m + '.read'] = stats.get('teragen.io.xfs.' + m + '.read',
                                                                               0) - int(xpc[3])
                            stats['teragen.io.xfs.' + m + '.write'] = stats.get('teragen.io.xfs.' + m + '.write',
                                                                                0) - int(xpc[2])
                        elif phase == 'mid':
                            stats['teragen.io.xfs.' + m + '.read'] = stats.get('teragen.io.xfs.' + m + '.read',
                                                                               0) + int(xpc[3])
                            stats['teragen.io.xfs.' + m + '.write'] = stats.get('teragen.io.xfs.' + m + '.write',
                                                                                0) + int(xpc[2])
                            stats['terasort.io.xfs.' + m + '.read'] = stats.get('terasort.io.xfs.' + m + '.read',
                                                                                0) - int(xpc[3])
                            stats['terasort.io.xfs.' + m + '.write'] = stats.get('terasort.io.xfs.' + m + '.write',
                                                                                 0) - int(xpc[2])
                        elif phase == 'post':
                            stats['terasort.io.xfs.' + m + '.read'] = stats.get('terasort.io.xfs.' + m + '.read',
                                                                                0) + int(xpc[3])
                            stats['terasort.io.xfs.' + m + '.write'] = stats.get('terasort.io.xfs.' + m + '.write',
                                                                                 0) + int(xpc[2])
                elif line.startswith('rw'):
                    # rw <xs_write_calls> <xs_read_calls>
                    rw = line.split(' ')

                    for m in [mount, 'all']:
                        if phase == 'pre':
                            stats['teragen.io.xfs.' + m + '.reads'] = stats.get('teragen.io.xfs.' + m + '.reads',
                                                                                0) - int(rw[2])
                            stats['teragen.io.xfs.' + m + '.writes'] = stats.get('teragen.io.xfs.' + m + '.writes',
                                                                                 0) - int(rw[1])
                        elif phase == 'mid':
                            stats['teragen.io.xfs.' + m + '.reads'] = stats.get('teragen.io.xfs.' + m + '.reads',
                                                                                0) + int(rw[2])
                            stats['teragen.io.xfs.' + m + '.writes'] = stats.get('teragen.io.xfs.' + m + '.writes',
                                                                                 0) + int(rw[1])
                            stats['terasort.io.xfs.' + m + '.reads'] = stats.get('terasort.io.xfs.' + m + '.reads',
                                                                                 0) - int(rw[2])
                            stats['terasort.io.xfs.' + m + '.writes'] = stats.get('terasort.io.xfs.' + m + '.writes',
                                                                                  0) - int(rw[1])
                        elif phase == 'post':
                            stats['terasort.io.xfs.' + m + '.reads'] = stats.get('terasort.io.xfs.' + m + '.reads',
                                                                                 0) + int(rw[2])
                            stats['terasort.io.xfs.' + m + '.writes'] = stats.get('terasort.io.xfs.' + m + '.writes',
                                                                                  0) + int(rw[1])
    elif 'ext4' in f:
        parts = f.split('.')
        mount = parts[2]
        mounts['ext4'].add(mount)
        phase = parts[3]
        with open("{}/{}".format(args.bd, f)) as lines:
            # just one line with writes in kilobytes
            w = lines.readline()
            for m in [mount, 'all']:
                if phase == 'pre':
                    stats['teragen.io.ext4.' + m + '.write'] = stats.get('teragen.io.ext4.' + m + '.write', 0) - int(w)
                elif phase == 'mid':
                    stats['teragen.io.ext4.' + m + '.write'] = stats.get('teragen.io.ext4.' + m + '.write', 0) + int(w)
                    stats['terasort.io.ext4.' + m + '.write'] = stats.get('terasort.io.ext4.' + m + '.write', 0) - int(
                        w)
                elif phase == 'post':
                    stats['terasort.io.ext4.' + m + '.write'] = stats.get('terasort.io.ext4.' + m + '.write', 0) + int(
                        w)

# read metrics if available
metrics = dict()
for f in files:
    # file name is <jobid>-<hostname>-<component>-metrics.out
    # e.g. 1234-cumu02-00-datanode-metrics.out
    if 'metrics' in f:
        component = f.split('-')[3]

        # present in all metrics: SentBytes=..., ReceivedBytes=... in rpc.rpc
        sent_bytes = dict()
        received_bytes = dict()

        # present in datanode: BytesWritten=..., BytesRead=..., RemoteBytesWritten=..., RemoteBytesRead=...
        bytes_written = 0
        bytes_read = 0
        remote_bytes_written = 0
        remote_bytes_read = 0
        port = 0

        # terribly inefficient since we only want the last occurrence of metrics, but alas.
        with open("{}/{}".format(args.bd, f)) as lines:
            for line in lines:
                if 'rpc.rpc' in line:
                    # sent and received bytes per port
                    parts = line.split(' ')
                    s = 0
                    r = 0
                    for part in parts:
                        if part.startswith('SentBytes'):
                            s = int(part.split('=')[1].split(',')[0])
                        elif part.startswith('ReceivedBytes'):
                            r = int(part.split('=')[1].split(',')[0])
                        elif part.startswith('port'):
                            port = int(part.split('=')[1].split(',')[0])
                    sent_bytes[port] = s
                    received_bytes[port] = r
                elif 'dfs.datanode' in line:
                    parts = line.split(' ')
                    for part in parts:
                        if part.startswith('BytesWritten'):
                            bytes_written = int(part.split('=')[1].split(',')[0])
                        elif part.startswith('BytesRead'):
                            bytes_read = int(part.split('=')[1].split(',')[0])
                        elif part.startswith('RemoteBytesWritten'):
                            remote_bytes_written = int(part.split('=')[1].split(',')[0])
                        elif part.startswith('RemoteBytesRead'):
                            remote_bytes_read = int(part.split('=')[1].split(',')[0])

        previous_metrics = metrics.get(component, dict())
        previous_metrics['SentBytes'] = previous_metrics.get('SentBytes', 0) + sum(sent_bytes.values())
        previous_metrics['ReceivedBytes'] = previous_metrics.get('ReceivedBytes', 0) + sum(received_bytes.values())
        previous_metrics['BytesWritten'] = previous_metrics.get('BytesWritten', 0) + bytes_written
        previous_metrics['BytesRead'] = previous_metrics.get('BytesRead', 0) + bytes_read
        previous_metrics['RemoteBytesWritten'] = previous_metrics.get('RemoteBytesWritten', 0) + remote_bytes_written
        previous_metrics['RemoteBytesRead'] = previous_metrics.get('RemoteBytesRead', 0) + remote_bytes_read
        metrics[component] = previous_metrics

# print stats
print()

# figure out longest file system + mount point name to align all output
max_mount_length = 0
for fs in ['xfs', 'ext4']:
    for mount in mounts[fs]:
        max_mount_length = max(max_mount_length, len(mount) + len(fs))

# maximum line length before the value is printed
# max_mount_length
#   +1 space in between file system and mount point
#   +1 space after the mount point
#   +7 for "Write: "
max_line_length = max_mount_length + 9


def rpad(s):
    return "{0: <{1}}".format(s, max_line_length)


print("TeraGen")
print("=======")
print(
    rpad("Duration:") + "{} minutes".format(
        int(round((stats['teragen.time.end'] - stats['teragen.time.start']) / 60.0))))
print(rpad("HDFS Read:") + "{} GiB".format(int(round(stats['teragen.io.hdfs.read'] / 1073741824.0))))
print(rpad("HDFS Write:") + "{} GiB".format(int(round(stats['teragen.io.hdfs.write'] / 1073741824.0))))
print(rpad("FILE Read:") + "{} GiB".format(int(round(stats['teragen.io.file.read'] / 1073741824.0))))
print(rpad("FILE Write:") + "{} GiB".format(int(round(stats['teragen.io.file.write'] / 1073741824.0))))
print(rpad("Spill:") + "{} GiB".format(int(round(stats['teragen.io.spill'] * 100.0 / 1073741824.0))))
for mount in sorted(mounts['xfs']):
    print(rpad("XFS {} Read:".format(mount)) + "{} GiB".format(
        int(round(stats['teragen.io.xfs.' + mount + '.read'] / 1073741824.0))))
    print(rpad("XFS {} Write:".format(mount)) + "{} GiB".format(
        int(round(stats['teragen.io.xfs.' + mount + '.write'] / 1073741824.0))))
for mount in sorted(mounts['ext4']):
    print(rpad("ext4 {} Write:".format(mount)) + "{} GiB".format(
        int(round(stats['teragen.io.ext4.' + mount + '.write'] / 1048576.0))))

print(flush=True)

print("TeraSort")
print("=======")
print(
    rpad("Duration:") + "{} minutes".format(
        int(round((stats['terasort.time.end'] - stats['terasort.time.start']) / 60.0))))
if stats['terasort.engine'] == 'hadoop':
    print(rpad("HDFS Read:") + "{} GiB".format(int(round(stats['terasort.io.hdfs.read'] / 1073741824.0))))
    print(rpad("HDFS Write:") + "{} GiB".format(int(round(stats['terasort.io.hdfs.write'] / 1073741824.0))))
    print(rpad("FILE Read:") + "{} GiB".format(int(round(stats['terasort.io.file.read'] / 1073741824.0))))
    print(rpad("FILE Write:") + "{} GiB".format(int(round(stats['terasort.io.file.write'] / 1073741824.0))))
    print(rpad("Shuffle Read:") + "{} GiB".format(int(round(stats['terasort.io.shuffle.read'] / 1073741824.0))))
    print(rpad("Shuffle Write:") + "{} GiB".format(int(round(stats['terasort.io.shuffle.write'] / 1073741824.0))))
    print(rpad("Spill:") + "{} GiB".format(int(round(stats['terasort.io.spill'] * 100.0 / 1073741824.0))))
for mount in sorted(mounts['xfs']):
    print(rpad("XFS {} Read:".format(mount)) + "{} GiB".format(
        int(round(stats['terasort.io.xfs.' + mount + '.read'] / 1073741824.0))))
    print(rpad("XFS {} Write:".format(mount)) + "{} GiB".format(
        int(round(stats['terasort.io.xfs.' + mount + '.write'] / 1073741824.0))))
for mount in sorted(mounts['ext4']):
    print(rpad("ext4 {} Write:".format(mount)) + "{} GiB".format(
        int(round(stats['terasort.io.ext4.' + mount + '.write'] / 1048576.0))))

print(flush=True)

print("Total")
print("=====")
print(rpad("Duration:") + "{} minutes".format(int(round((stats['teragen.time.end'] - stats['teragen.time.start'] +
                                                         stats['terasort.time.end'] - stats[
                                                             'terasort.time.start']) / 60.0))))

if stats['terasort.engine'] == 'hadoop':
    print(rpad("HDFS Read:") + "{} GiB".format(
        int(round((stats['teragen.io.hdfs.read'] + stats['terasort.io.hdfs.read']) / 1073741824.0))))
    print(rpad("HDFS Write:") + "{} GiB".format(
        int(round((stats['teragen.io.hdfs.write'] + stats['terasort.io.hdfs.write']) / 1073741824.0))))
    print(rpad("FILE Read:") + "{} GiB".format(
        int(round((stats['teragen.io.file.read'] + stats['terasort.io.file.read']) / 1073741824.0))))
    print(rpad("FILE Write:") + "{} GiB".format(
        int(round((stats['teragen.io.file.write'] + stats['terasort.io.file.write']) / 1073741824.0))))
    print(rpad("Shuffle Read:") + "{} GiB".format(int(round((stats['terasort.io.shuffle.read']) / 1073741824.0))))
    print(rpad("Shuffle Write:") + "{} GiB".format(int(round((stats['terasort.io.shuffle.write']) / 1073741824.0))))
    print(rpad("Spill:") + "{} GiB".format(
        int(round((stats['teragen.io.spill'] + stats['terasort.io.spill']) * 100.0 / 1073741824.0))))
for mount in sorted(mounts['xfs']):
    print(rpad("XFS {} Read:".format(mount)) + "{} GiB".format(int(round(
        (stats['teragen.io.xfs.' + mount + '.read'] + stats['terasort.io.xfs.' + mount + '.read']) / 1073741824.0))))
    print(rpad("XFS {} Write:".format(mount)) + "{} GiB".format(int(round(
        (stats['teragen.io.xfs.' + mount + '.write'] + stats['terasort.io.xfs.' + mount + '.write']) / 1073741824.0))))
for mount in sorted(mounts['ext4']):
    print(rpad("ext4 {} Write:".format(mount)) + "{} GiB".format(int(round(
        (stats['teragen.io.ext4.' + mount + '.write'] + stats['terasort.io.ext4.' + mount + '.write']) / 1048576.0))))

print(flush=True)

max_line_length = 36  # 'Jobhistoryserver RemoteBytesWritten'
print("Metrics")
print("=======")
for (component, m) in metrics.items():
    for (k, v) in m.items():
        print("{} {} GiB".format(rpad("{} {}:".format(component.capitalize(), k)), int(round(v / 1073741824.0))))

print(flush=True)

# write as LaTeX table as well, row by row
if stats['terasort.engine'] == 'hadoop':
    with open("{}/terasort-io-hdfs.tex".format(args.bd), "w") as f:
        f.write("\\texttt{{hdfs://}} & {:,} & {:,} & {:,} & {:,} & {:,} & {:,}".format(
            int(round(stats['teragen.io.hdfs.read'] / 1073741824.0)),
            int(round(stats['teragen.io.hdfs.write'] / 1073741824.0)),
            int(round(stats['terasort.io.hdfs.read'] / 1073741824.0)),
            int(round(stats['terasort.io.hdfs.write'] / 1073741824.0)),
            int(round((stats['teragen.io.hdfs.read'] + stats['terasort.io.hdfs.read']) / 1073741824.0)),
            int(round((stats['teragen.io.hdfs.write'] + stats['terasort.io.hdfs.write']) / 1073741824.0)))
        )
    with open("{}/terasort-io-file.tex".format(args.bd), "w") as f:
        f.write("\\texttt{{file://}} & {:,} & {:,} & {:,} & {:,} & {:,} & {:,}".format(
            int(round(stats['teragen.io.file.read'] / 1073741824.0)),
            int(round(stats['teragen.io.file.write'] / 1073741824.0)),
            int(round(stats['terasort.io.file.read'] / 1073741824.0)),
            int(round(stats['terasort.io.file.write'] / 1073741824.0)),
            int(round((stats['teragen.io.file.read'] + stats['terasort.io.file.read']) / 1073741824.0)),
            int(round((stats['teragen.io.file.write'] + stats['terasort.io.file.write']) / 1073741824.0)))
        )
    with open("{}/terasort-io-shuffle.tex".format(args.bd), "w") as f:
        f.write("Shuffle & -- & -- & {:,} & {:,} & {:,} & {:,}".format(
            int(round(stats['terasort.io.shuffle.read'] / 1073741824.0)),
            int(round(stats['terasort.io.shuffle.write'] / 1073741824.0)),
            int(round(stats['terasort.io.shuffle.read'] / 1073741824.0)),
            int(round(stats['terasort.io.shuffle.write'] / 1073741824.0)))
        )
    with open("{}/terasort-io-spill.tex".format(args.bd), "w") as f:
        f.write("Spill & {:,} & {:,} & {:,} & {:,} & {:,} & {:,}".format(
            int(round(stats['teragen.io.spill'] * 100.0 / 1073741824.0)),
            int(round(stats['teragen.io.spill'] * 100.0 / 1073741824.0)),
            int(round(stats['terasort.io.spill'] * 100.0 / 1073741824.0)),
            int(round(stats['terasort.io.spill'] * 100.0 / 1073741824.0)),
            int(round((stats['teragen.io.spill'] + stats['terasort.io.spill']) * 100.0 / 1073741824.0)),
            int(round((stats['teragen.io.spill'] + stats['terasort.io.spill']) * 100.0 / 1073741824.0)))
        )
elif stats['terasort.engine'] == 'flink' or stats['terasort.engine'] == 'spark':
    with open("{}/terasort-io-hdfs.tex".format(args.bd), "w") as f:
        f.write("\\texttt{{hdfs://}} & {:,} & {:,} & -- & -- & -- & --".format(
            int(round(stats['teragen.io.hdfs.read'] / 1073741824.0)),
            int(round(stats['teragen.io.hdfs.write'] / 1073741824.0)))
        )
    with open("{}/terasort-io-file.tex".format(args.bd), "w") as f:
        f.write("\\texttt{{file://}} & {:,} & {:,} & -- & -- & -- & --".format(
            int(round(stats['teragen.io.file.read'] / 1073741824.0)),
            int(round(stats['teragen.io.file.write'] / 1073741824.0)))
        )
    with open("{}/terasort-io-spill.tex".format(args.bd), "w") as f:
        f.write("Spill & {:,} & {:,} & -- & -- & -- & --".format(
            int(round(stats['teragen.io.spill'] * 100.0 / 1073741824.0)),
            int(round(stats['teragen.io.spill'] * 100.0 / 1073741824.0)))
        )
with open("{}/terasort-io-xfs.tex".format(args.bd), "w") as f:
    f.write("XFS & {:,} & {:,} & {:,} & {:,} & {:,} & {:,}".format(
        int(round(stats['teragen.io.xfs.all.read'] / 1073741824.0)),
        int(round(stats['teragen.io.xfs.all.write'] / 1073741824.0)),
        int(round(stats['terasort.io.xfs.all.read'] / 1073741824.0)),
        int(round(stats['terasort.io.xfs.all.write'] / 1073741824.0)),
        int(round((stats['teragen.io.xfs.all.read'] + stats['terasort.io.xfs.all.read']) / 1073741824.0)),
        int(round((stats['teragen.io.xfs.all.write'] + stats['terasort.io.xfs.all.write']) / 1073741824.0)))
    )
with open("{}/terasort-io-ext4.tex".format(args.bd), "w") as f:
    f.write("ext4 & n/a & {:,} & n/a & {:,} & n/a & {:,}".format(
        int(round(stats['teragen.io.ext4.all.write'] / 1048576.0)),
        int(round(stats['terasort.io.ext4.all.write'] / 1048576.0)),
        int(round((stats['teragen.io.ext4.all.write'] + stats['terasort.io.ext4.all.write']) / 1048576.0)))
    )

if os_raw_data.empty:
    print("No data imported, exiting.")
    print(flush=True)
    quit()

# sanity-check file descriptor mappings
# dups = fd_raw_data[fd_raw_data.duplicated(['hostname', 'pid', 'key', 'fileDescriptor'], keep=False)]
# if not dups.empty:
#    print("Found duplicates:")
#    print(dups)
#    print(flush=True)
# del dups

# t = os_raw_data[os_raw_data['category'] != 'other']
# t = t[t['source'] == 'jvm']
# t = t.groupby(['category', 'timeBin'], as_index=False).sum().set_index(['timeBin'], drop=False)
# t = t.groupby(['category'], as_index=False)
# for group, data in t:
#    d = data.assign(Bandwidth=lambda x: x['data'] / (x['cpuTime'] * 1048.576)) # bytes/ms => MiB/s
#    ax = d.plot(x='timeBin', y=['Bandwidth', 'count'], kind='line')
#    f = ax.get_figure()
#    f.savefig("{}.pdf".format(group))

# file_data = os_raw_data.join(fd_raw_data.set_index(['hostname', 'pid', 'key', 'fileDescriptor']),
#                             on=['hostname', 'pid', 'key', 'fileDescriptor'])
# file_data = file_data[file_data['source'] == 'jvm']
# file_data = file_data[file_data['category'] != 'other']
# file_data = file_data.drop(['source', 'fileDescriptor', 'remoteCount', 'remoteCpuTime', 'remoteData'], axis=1)
# file_data = file_data.groupby(['hostname', 'path'], as_index=False).sum().sort_values(['hostname', 'data'],
#                                                                                      ascending=[True, False])
# file_data.to_excel("{}/file_data.xlsx".format(args.bd))
# del file_data, fd_raw_data

# aggregate over all pids of desired hosts the metrics per time, key, source and category
hostnames = [
    'cumu01-00',
    'cumu01-01',
    'cumu01-02',
    'cumu01-03',
    'cumu01-04',
    'cumu01-05',
    'cumu01-06',
    'cumu01-07',
    'cumu01-08',
    'cumu01-09',
    'cumu01-10',
    'cumu01-11',
    'cumu01-12',
    'cumu01-13',
    'cumu01-14',
    'cumu01-15',
    'cumu02-00',
    'cumu02-01',
    'cumu02-02',
    'cumu02-03',
    'cumu02-04',
    'cumu02-05',
    'cumu02-06',
    'cumu02-07',
    'cumu02-08',
    'cumu02-09',
    'cumu02-10',
    'cumu02-11',
    'cumu02-12',
    'cumu02-13',
    'cumu02-14',
    'cumu02-15'
]
grouped_data = os_raw_data[os_raw_data['hostname'].isin(hostnames)].drop(['hostname', 'pid', 'fileDescriptor'],
                                                                         axis=1).groupby(
    ['timeBin', 'key', 'source', 'category'], as_index=False).sum().set_index(['timeBin'], drop=False)
# del os_raw_data

per_node_data_jvm = os_raw_data[os_raw_data['hostname'].isin(hostnames)].groupby(
    ['timeBin', 'hostname', 'source', 'category'], as_index=False).sum().set_index(['timeBin'], drop=False)
per_node_data_jvm = per_node_data_jvm[per_node_data_jvm['category'].isin(['read', 'write'])]
per_node_data_jvm = per_node_data_jvm[per_node_data_jvm['source'] == 'jvm']
per_node_data_jvm = per_node_data_jvm.groupby(['hostname', 'category'], as_index=False)

all_nodes = os_raw_data[os_raw_data['hostname'].isin(hostnames)].groupby(['timeBin', 'source', 'category'],
                                                                         as_index=False).sum().set_index(['timeBin'],
                                                                                                         drop=False)
all_nodes = all_nodes[all_nodes['category'].isin(['read', 'write'])]
all_nodes_sfs = all_nodes[all_nodes['source'] == 'sfs']
all_nodes_sfs = all_nodes_sfs.groupby(['category'], as_index=False)
all_nodes_jvm = all_nodes[all_nodes['source'] == 'jvm']
all_nodes_jvm = all_nodes_jvm.groupby(['category'], as_index=False)

fig, ax = plt.subplots(nrows=int(len(per_node_data_jvm.groups) / 2) + 2, ncols=1, sharex=True, sharey=True)
i = 0
for group in sorted(all_nodes_sfs.groups):
    data = all_nodes_sfs.get_group(group)
    data.fillna(0).plot(ax=ax[int(i)], y='data', kind='line', legend=False)
    i += 0.5
for group in sorted(all_nodes_jvm.groups):
    data = all_nodes_jvm.get_group(group)
    data.fillna(0).plot(ax=ax[int(i)], y='data', kind='line', legend=False)
    i += 0.5
for group in sorted(per_node_data_jvm.groups):
    data = per_node_data_jvm.get_group(group)
    data.fillna(0).plot(ax=ax[int(i)], y='data', kind='line', legend=False)
    i += 0.5
del i
fig.savefig("{}/io.pdf".format(args.bd))
plt.close(fig)

# per_node_index = None
# for group, group_data in per_node_data:
#    per_node_index = group_data.index if per_node_index is None else per_node_index.union(group_data.index)
# per_node_io = pd.DataFrame(index=per_node_index)
# hs = []
# for group in per_node_data.groups:
#    group_data = per_node_data.get_group(group)
#    per_node_io = per_node_io.assign(group=lambda x: x['data'] / x['cpuTime'])
#    hs.append("{}".format(group))
#    del group_data
# ax = per_node_io.plot(y=hs, kind='line')
# f = ax.get_figure()
# f.savefig("per_node.pdf")

# regroup to obtain a DataFrame per key/source/category tuple
current_data = grouped_data.groupby(['key', 'source', 'category'], as_index=False)
del grouped_data

# orders of plots when plotting SFS, smallest at the bottom, highest at the top
# same order for each key/category as below to ensure same color in graphs
sfs_orders = {
    ('map', 'sfs', 'write'): 4,
    ('map', 'sfs', 'read'): 5,
    ('reduce', 'sfs', 'write'): 6,
    ('reduce', 'sfs', 'read'): 7,
    ('flink', 'sfs', 'write'): 6,
    ('flink', 'sfs', 'read'): 7,
    ('spark', 'sfs', 'write'): 6,
    ('spark', 'sfs', 'read'): 7
}

# orders of plots when plotting JVM, smallest at the bottom, highest at the top
jvm_orders = {
    ('yarn', 'jvm', 'write'): 0,
    ('yarn', 'jvm', 'read'): 1,
    ('hdfs', 'jvm', 'write'): 2,
    ('hdfs', 'jvm', 'read'): 3,
    ('map', 'jvm', 'write'): 4,
    ('map', 'jvm', 'read'): 5,
    ('reduce', 'jvm', 'write'): 6,
    ('reduce', 'jvm', 'read'): 7,
    ('flink', 'jvm', 'write'): 6,
    ('flink', 'jvm', 'read'): 7,
    ('spark', 'jvm', 'write'): 6,
    ('spark', 'jvm', 'read'): 7
}


def prettyprint(name):
    if name == 'yarn':
        return 'YARN'
    elif name == 'hdfs':
        return 'HDFS'
    else:
        return name.capitalize()


# remember as many colors as necessary for JVM, as for SFS there are fewer
# we'll use this to give the same color for each key/category combination in JVM and SFS plots
base_colors = None

# same for hatches
base_hatches = ['o', '/', '\\', '|', '-', 'O', '+', 'x', '//', '\\\\']

# reset style
plt.clf()
mpl.rcParams.update(mpl.rcParamsDefault)
plt.style.use('seaborn-paper')
mpl.rcParams['hatch.linewidth'] = 2.0

# compute font size
# however this is not entirely accurate, as there is cropping involved at the end,
# and I can't seem to figure out the math, so the final font size in the paper will be slightly larger
latex_font_size = 7  # in pt, target font size in paper
latex_dpi = 72  # what \pdfimageresolution reports
latex_textwidth = 470  # in pt, what \textwidth reports
latex_textheight = 650  # in pt, what \textheight reports
latex_aspect_ratio = 16.0 / 10.0  # width / height, aspect ratio of the figure we're building
latex_figure_height = latex_textwidth / latex_aspect_ratio  # in pt, height of the figure in the paper
font_scale = latex_textheight / latex_figure_height  # scaling factor to reach the target font size when scaled down
font_size = int(round(latex_font_size * font_scale))  # font size in this figure to reach target font size

# all plots with same x and y axes
figure, (ax_top, ax_bottom) = plt.subplots(2, sharex=True, sharey=True)

# leave 50% of font size of vertical space between plots
figure.subplots_adjust(hspace=font_size / (2 * latex_figure_height))

# hide all x ticks except in the bottom plot
plt.setp([a.get_xticklabels() for a in figure.axes[:-1]], visible=False)

# point in time where TeraGen ends and TeraSort starts in seconds, relative to beginning
teragen_terasort_border = int(
    round(stats['teragen.time.end'] + (stats['terasort.time.start'] - stats['teragen.time.end']) / 2.0))

# count the number of operations for each category
operation_count = {
    'teragen': {
        'jvm': {'read': 0, 'write': 0, 'other': 0},
        'sfs': {'read': 0, 'write': 0, 'other': 0}
    },
    'terasort': {
        'jvm': {'read': 0, 'write': 0, 'other': 0},
        'sfs': {'read': 0, 'write': 0, 'other': 0}
    }
}
operation_data = {
    'teragen': {
        'jvm': {'read': 0, 'write': 0, 'other': 0},
        'sfs': {'read': 0, 'write': 0, 'other': 0}
    },
    'terasort': {
        'jvm': {'read': 0, 'write': 0, 'other': 0},
        'sfs': {'read': 0, 'write': 0, 'other': 0}
    }
}

# plot JVM first to scale out the y axis, since the I/O there is more than on SFS level,
# as well as to define the whole color map; plot JVM at the bottom
# only have a title for the top plot
for (orders, ax, title, subtitle) in [(jvm_orders, ax_bottom, None, "JVM Layer"),
                                      (sfs_orders, ax_top,
                                       "Hadoop TeraGen and {} TeraSort I/O".format(
                                           stats['terasort.engine'].capitalize()),
                                       "SFS Layer")]:
    # loop over all unique groups to build global index
    plot_index = None
    for group, group_data in current_data:
        plot_index = group_data.index if plot_index is None else plot_index.union(group_data.index)

    # dataframe used for plotting
    plot_data = pd.DataFrame(index=plot_index)

    # loop over all unique groups in previously defined order
    for group in sorted([g for g in current_data.groups if g in orders], key=lambda g: orders[g]):
        # group[0] contains the key, e.g. hadoop, spark, yarn, ...
        # group[1] contains jvm or sfs
        # group[2] contains read, write or other

        # get this group's data
        group_data = current_data.get_group(group)

        # sum of empty series is nan, replace with 0
        operation_count['teragen'][group[1]][group[2]] += np.nan_to_num(
            group_data['count'].loc[group_data['timeBin'] < teragen_terasort_border_ns].sum())
        operation_count['terasort'][group[1]][group[2]] += np.nan_to_num(
            group_data['count'].loc[group_data['timeBin'] > teragen_terasort_border_ns].sum())
        operation_data['teragen'][group[1]][group[2]] += np.nan_to_num(
            group_data['data'].loc[group_data['timeBin'] < teragen_terasort_border_ns].sum())
        operation_data['terasort'][group[1]][group[2]] += np.nan_to_num(
            group_data['data'].loc[group_data['timeBin'] > teragen_terasort_border_ns].sum())

        # give a nice name to each column
        plot_data = plot_data.assign(**{
            "{}: {}".format(prettyprint(group[0]), group[2].capitalize()): group_data['data'].cumsum() / 1073741824.0})

    # build base colors if necessary, not more than needed in the JVM plot (number of unique values)
    # i.e. for the SFS plot, which comes second in the iteration, the base_colors are already defined
    if base_colors is None:
        # imho viridis is best suited for gray-scale printing: https://matplotlib.org/users/colormaps.html
        base_colors = mpl.cm.get_cmap(name='viridis', lut=len(set(jvm_orders.values()))).colors

    # now build colors needed for this particular plot
    colors = []
    for group in sorted([g for g in current_data.groups if g in orders], key=lambda g: orders[g]):
        # same order receives same color in JVM and SFS plots
        colors.append(base_colors[orders[group]])

    # compute the minute value for each time bin so we can plot against it
    plot_data = plot_data.assign(Minutes=lambda x: (x.index - plot_index.values[0]) / 60000000000)

    # fill holes in data with previous valid value
    plot_data = plot_data.fillna(method='pad')

    # fill remaining holes at the beginning of each series
    plot_data = plot_data.fillna(0)

    # write out statistics as LaTeX files as well, remember total I/O for each phase
    tg_r = 0
    tg_w = 0
    ts_r = 0
    ts_w = 0
    for group in sorted([g for g in current_data.groups if g in orders], key=lambda g: orders[g]):
        if not os.path.isfile("{}/terasort-io-{}-{}.tex".format(args.bd, group[0], group[1])):
            with open("{}/terasort-io-{}-{}.tex".format(args.bd, group[0], group[1]), "w") as f:
                try:
                    r = plot_data["{}: Read".format(prettyprint(group[0]))]
                except KeyError:
                    r = pd.Series(index=plot_index, data=0)
                try:
                    w = plot_data["{}: Write".format(prettyprint(group[0]))]
                except KeyError:
                    w = pd.Series(index=plot_index, data=0)
                f.write("{} & {:,} & {:,} & {:,} & {:,} & {:,} & {:,}".format(
                    prettyprint(group[0]),
                    int(round(r.iloc[teragen_terasort_border])),
                    int(round(w.iloc[teragen_terasort_border])),
                    int(round(r.iloc[-1] - r.iloc[teragen_terasort_border])),
                    int(round(w.iloc[-1] - w.iloc[teragen_terasort_border])),
                    int(round(r.iloc[-1])),
                    int(round(w.iloc[-1])))
                )
                tg_r += r.iloc[teragen_terasort_border]
                tg_w += w.iloc[teragen_terasort_border]
                ts_r += r.iloc[-1] - r.iloc[teragen_terasort_border]
                ts_w += w.iloc[-1] - w.iloc[teragen_terasort_border]

    # write total I/O
    with open("{}/terasort-io-total-{}.tex".format(args.bd, group[1]), "w") as f:
        f.write("Total & {:,} & {:,} & {:,} & {:,} & {:,} & {:,}".format(
            int(round(tg_r)),
            int(round(tg_w)),
            int(round(ts_r)),
            int(round(ts_w)),
            int(round(tg_r + ts_r)),
            int(round(tg_w + ts_w)))
        )

    # finally plot the thing
    plot_data.plot.area(
        ax=ax,  # previously defined subplot
        figsize=(
            font_scale * latex_textwidth / latex_dpi,
            font_scale * latex_figure_height / latex_dpi),
        legend=False,  # we're adding one on our own
        colormap=mpl.colors.ListedColormap(colors),
        grid=False,
        x="Minutes",  # use computed minutes as x ticks
        linewidth=0  # to not show the line where the cumulative sum is still 0
    )
    if title is not None:
        ax.set_title(title, fontsize=font_size)

    # create legend with hatches in the patches and nice labels, similar lookup as for the colors
    patches = []
    labels = []
    for group in sorted([g for g in current_data.groups if g in orders], key=lambda g: orders[g]):
        patches.append(mpl.patches.Patch(edgecolor='white', facecolor=base_colors[orders[group]],
                                         hatch=base_hatches[orders[group]]))
        labels.append("{}: {}".format(prettyprint(group[0]), group[2].capitalize()))

    # reverse because we have added the plots bottom-up
    # no more than four patches in a column
    legend = ax.legend(reversed(patches), reversed(labels), loc='upper left', fontsize=font_size, framealpha=.85,
                       edgecolor='.5', ncol=math.ceil((plot_data.shape[1] - 1) / 4))
    legend.get_frame().set_linewidth(1)

    # set both y axes
    ax.tick_params(labelsize=font_size)
    ax.set_xlabel("Minutes", fontsize=font_size)
    ax.get_yaxis().set_major_formatter(mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.set_xlim(plot_data['Minutes'].iloc[0], plot_data['Minutes'].iloc[-1])
    ax2 = ax.twinx()
    ax2.set_ylim(ax.get_ylim())
    ax2.tick_params(labelsize=font_size)
    ax2.get_yaxis().set_major_formatter(mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax2.set_xlim(plot_data['Minutes'].iloc[0], plot_data['Minutes'].iloc[-1])

    # customly hacked stacked area plot with hatches,
    # actually the above area plot should not be necessary, but some styles are not easily set using fill_between
    lower_border = pd.Series(index=plot_index, data=np.zeros(plot_index.size))
    for group in sorted([g for g in current_data.groups if g in orders], key=lambda g: orders[g]):
        column_name = "{}: {}".format(prettyprint(group[0]), group[2].capitalize())
        column_index = plot_data.columns.get_loc(column_name)
        if column_index == 0:
            ax.fill_between(x=plot_data['Minutes'], y1=plot_data[column_name], y2=0, hatch=base_hatches[orders[group]],
                            edgecolor='white', facecolor='none', linewidth=0)
            lower_border += plot_data[column_name]
        elif column_index < plot_data.shape[1] - 1:
            ax.fill_between(x=plot_data['Minutes'], y1=plot_data[column_name] + lower_border, y2=lower_border,
                            hatch=base_hatches[orders[group]], edgecolor='white', facecolor='none', linewidth=0)
            lower_border += plot_data[column_name]

    # add arrows for annotations, bottom to top
    yfrac = 1.0 / (plot_data.shape[1] - 1)
    os_i = 0
    total_data = 0
    for column in plot_data:
        if column == "Minutes":
            continue

        total_data = total_data + plot_data[column].iloc[-1]
        ax.annotate('',
                    xy=(plot_data['Minutes'].iloc[-1], total_data), xycoords='data',
                    xytext=(.975, .5 / (plot_data.shape[1] - 1) + os_i * yfrac), textcoords='axes fraction',
                    size=font_size, verticalalignment='center', horizontalalignment='right',
                    arrowprops=dict(arrowstyle='fancy', facecolor='white', edgecolor='.5', alpha=.85, linewidth=1)
                    )
        os_i += 1

    # repeat for the text boxes to properly overlap the arrows
    os_i = 0
    total_data = 0
    for column in plot_data:
        if column == "Minutes":
            continue

        total_data = total_data + plot_data[column].iloc[-1]
        ax.annotate("{}: {:,} GiB".format(column, int(round(plot_data[column].iloc[-1]))),
                    xy=(plot_data['Minutes'].iloc[-1], total_data), xycoords='data',
                    xytext=(.975, .5 / (plot_data.shape[1] - 1) + os_i * yfrac), textcoords='axes fraction',
                    size=font_size, verticalalignment='center', horizontalalignment='right',
                    bbox=dict(boxstyle='round', facecolor='white', edgecolor='.5', alpha=.85, linewidth=1)
                    )
        os_i += 1

    ax.annotate(subtitle,
                xy=(.5, .925), xycoords='axes fraction',
                xytext=(.5, .925), textcoords='axes fraction',
                size=font_size, verticalalignment='center', horizontalalignment='center',
                bbox=dict(boxstyle='round', facecolor='white', edgecolor='.5', alpha=.85, linewidth=1)
                )

    del plot_data

# indicate teragen and terasort in top and bottom plot separately
ax_top.annotate('',
                xy=(teragen_terasort_border / (stats['terasort.time.end'] - stats['teragen.time.start']), 1),
                xycoords='axes fraction',
                xytext=(teragen_terasort_border / (stats['terasort.time.end'] - stats['teragen.time.start']), 0),
                textcoords='axes fraction',
                horizontalalignment='center',
                arrowprops=dict(arrowstyle='-', linewidth=1),
                )

# overdraw the line towards the bottom
ax_bottom.annotate('',
                   xy=(teragen_terasort_border / (stats['terasort.time.end'] - stats['teragen.time.start']), 1),
                   xycoords='axes fraction',
                   xytext=(teragen_terasort_border / (stats['terasort.time.end'] - stats['teragen.time.start']),
                           -3 * font_size / latex_figure_height), textcoords='axes fraction',
                   horizontalalignment='center',
                   arrowprops=dict(arrowstyle='-', linewidth=1),
                   )

# place the label on the overdrawn line
ax_bottom.annotate("TeraGen   TeraSort",
                   xy=(teragen_terasort_border / (stats['terasort.time.end'] - stats['teragen.time.start']), 1),
                   xycoords='axes fraction',
                   xytext=(teragen_terasort_border / (stats['terasort.time.end'] - stats['teragen.time.start']),
                           -3 * font_size / latex_figure_height), textcoords='axes fraction',
                   size=font_size, verticalalignment='bottom', horizontalalignment='center'
                   )

# just one label for both y axes
figure.text(0.05, 0.5, "Stacked Cumulative Data (GiB)", verticalalignment='center', rotation='vertical',
            fontsize=font_size)

# crop the figure
plt.savefig("{}/terasort-io.pdf".format(args.bd), dpi=latex_dpi, bbox_inches='tight')

# plot bandwidth per host
grouped_data = os_raw_data[os_raw_data['hostname'].isin(hostnames)].drop(['key', 'pid', 'fileDescriptor'],
                                                                         axis=1)
grouped_data = grouped_data[grouped_data['category'] == 'write']
grouped_data = grouped_data[grouped_data['source'] == 'jvm']
grouped_data = grouped_data.groupby(['timeBin', 'hostname'], as_index=False).sum().set_index(['timeBin'], drop=False)
del os_raw_data

current_data = grouped_data.groupby(['hostname'], as_index=False)
del grouped_data

figure, ax = plt.subplots(1)
# loop over all unique groups to build global index
plot_index = None
for group, group_data in current_data:
    plot_index = group_data.index if plot_index is None else plot_index.union(group_data.index)

# dataframe used for plotting
plot_data = pd.DataFrame(index=plot_index)

# loop over all unique groups in previously defined order
for group in current_data.groups:
    group_data = current_data.get_group(group)
    plot_data = plot_data.assign(**{
        "{}".format(group): group_data['data'].fillna(method='pad').fillna(0).rolling(120).sum() / (120 * 1048576)
    })

# compute the minute value for each time bin so we can plot against it
plot_data = plot_data.assign(Minutes=lambda x: (x.index - plot_index.values[0]) / 60000000000)

# fill holes in data with previous valid value
# plot_data = plot_data.fillna(method='pad')

# fill remaining holes at the beginning of each series
# plot_data = plot_data.fillna(0)

latex_textwidth = 235  # half the text width now
latex_textheight = 650  # in pt, what \textheight reports
latex_figure_height = latex_textwidth / latex_aspect_ratio  # in pt, height of the figure in the paper
font_scale = latex_textheight / latex_figure_height  # scaling factor to reach the target font size when scaled down
font_size = int(round(latex_font_size * font_scale))  # font size in this figure to reach target font size

# finally plot the thing
plot_data.plot.area(
    ax=ax,  # previously defined subplot
    figsize=(
        font_scale * latex_textwidth / latex_dpi,
        font_scale * latex_figure_height / latex_dpi),
    legend=True,  # we're adding one on our own
    # colormap=mpl.colors.ListedColormap(colors),
    grid=False,
    x="Minutes",  # use computed minutes as x ticks
    sort_columns=True,
    linewidth=0  # to not show the line where the cumulative sum is still 0
)

handles, labels = ax.get_legend_handles_labels()
l = [(x, y) for (y, x) in sorted(zip(labels, handles), key=lambda pair: pair[0])]
(a, b) = zip(*l)

legend = ax.legend(a, map(lambda h: "host-{}".format(h[7:]), b), loc='upper right', fontsize=font_size / 2,
                   framealpha=.85, edgecolor='.5', ncol=math.ceil((plot_data.shape[1] - 1) / 8))
legend.get_frame().set_linewidth(1)

# set both y axes
ax.tick_params(labelsize=font_size)
ax.set_xlabel("Minutes", fontsize=font_size)
ax.get_yaxis().set_major_formatter(mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
ax.set_xlim(plot_data['Minutes'].iloc[0], plot_data['Minutes'].iloc[-1])
ax.set_title("TeraGen/TeraSort Write Bandwidth per Host", fontsize=font_size)
ax.set_ylabel("Bandwidth (MiB/s)", fontsize=font_size)

plt.savefig("{}/terasort-bandwidth.pdf".format(args.bd), dpi=latex_dpi, bbox_inches='tight')

# print the number of operations
for phase in ['teragen', 'terasort']:
    for source in ['sfs', 'jvm']:
        count_r = operation_count[phase][source]['read']
        data_r = operation_data[phase][source]['read']
        count_w = operation_count[phase][source]['write']
        data_w = operation_data[phase][source]['write']
        print(
            "{} {}: {} read ops for {} bytes (avg. {} bytes/iop), {} write ops for {} bytes (avg. {} bytes/iop), {} other ops".format(
                phase, source, count_r, data_r, int(round(data_r / count_r)) if count_r != 0 else 0, count_w, data_w,
                int(round(data_w / count_w)) if count_w != 0 else 0, operation_count[phase][source]['other']))
    print(
        "{} xfs: {} read ops for {} bytes (avg. {} bytes/iop), {} MiB/s, {} write ops for {} bytes (avg. {} bytes/iop), {} MiB/s".format(
            phase, stats["{}.io.xfs.all.reads".format(phase)], stats["{}.io.xfs.all.read".format(phase)],
            int(round(stats["{}.io.xfs.all.read".format(phase)] / stats["{}.io.xfs.all.reads".format(phase)])), int(
                round(stats["{}.io.xfs.all.read".format(phase)] / (
                            (stats["{}.time.end".format(phase)] - stats["{}.time.start".format(phase)]) * 1048576))),
            stats["{}.io.xfs.all.writes".format(phase)], stats["{}.io.xfs.all.write".format(phase)],
            int(round(stats["{}.io.xfs.all.write".format(phase)] / stats["{}.io.xfs.all.writes".format(phase)])), int(
                round(stats["{}.io.xfs.all.write".format(phase)] / (
                            (stats["{}.time.end".format(phase)] - stats["{}.time.start".format(phase)]) * 1048576)))))
    print("{} ext4: {} MiB/s".format(phase, int(round(stats["{}.io.ext4.all.write".format(phase)] / (
                (stats["{}.time.end".format(phase)] - stats["{}.time.start".format(phase)]) * 1024)))))
print(flush=True)
