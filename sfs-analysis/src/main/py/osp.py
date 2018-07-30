import struct
import sys, traceback
from array import array
from enum import Enum

# primitive little endian unpackers
sbyte = struct.Struct('<b')

ubyte = struct.Struct('<B')
ushort = struct.Struct('<H')
uint = struct.Struct('<I')
ulong = struct.Struct('<Q')


class OperationStatisticsType(Enum):
    OS = 0
    DOS = 1
    RDOS = 2

    @classmethod
    def from_byte(cls, byte):
        for ost in OperationStatisticsType:
            if ost.value == byte:
                return ost
        raise ValueError(byte)


class OperationSource(Enum):
    JVM = (0, 'jvm')
    SFS = (1, 'sfs')

    @classmethod
    def from_byte(cls, byte):
        for os in OperationSource:
            if os.value[0] == byte:
                return os
        raise ValueError(byte)

    def text(self):
        return self.value[1]


class OperationCategory(Enum):
    READ = (0, 'read')
    WRITE = (1, 'write')
    OTHER = (2, 'other')
    ZIP = (3, 'zip')

    @classmethod
    def from_byte(cls, byte):
        for oc in OperationCategory:
            if oc.value[0] == byte:
                return oc
        raise ValueError(byte)

    def text(self):
        return self.value[1]


class NumberType(Enum):
    BYTE = (0, 1, ubyte)
    SHORT = (1, 2, ushort)
    INT = (2, 4, uint)
    LONG = (3, 8, ulong)
    EMPTY = (4, 0, None)

    @classmethod
    def from_byte(cls, byte):
        for nt in NumberType:
            if nt.value[0] == byte:
                return nt
        raise ValueError(byte)

    def get(self, mv, pos):
        if self.value[2] is not None:
            return self.value[2].unpack_from(mv, pos)[0], pos + self.value[1]
        else:
            return 0, pos


def get_number_type(header, header_offset):
    if (header & (0b100 << header_offset)) == 0:
        return NumberType.EMPTY
    else:
        return NumberType.from_byte((header & (0b011 << header_offset)) >> header_offset)


def os_init(os_shared_arrays, os_buffer, os_index):
    global os_sa, os_b, os_i
    os_sa, os_b, os_i = os_shared_arrays, os_buffer, os_index


def parse_operation_statistics_bb(os_index):
    # target array, source array, position in target array
    global os_sa, os_b, os_i

    # first 8 bytes are a long indicating the number of records
    count = ulong.unpack_from(os_b, os_index)[0]
    pos = os_index + 8

    # reserve right amount of memory
    os_i.acquire()
    index = os_i.value
    os_i.value += count
    os_i.release()

    n, init_pos = 0, pos
    while n < count:
        try:
            # 0 - 1: empty
            # 2 - 3: type(OS, DOS, RDOS)
            # 4: hasPid
            # 5 - 6: pidType
            # 7: hasCount
            # 8 - 9: countType
            # 10: hasCpuTime
            # 11 - 12: cpuTimeType
            # 13: hasFd
            # 14 - 15: fdType
            # 16: hasThreadId
            # 17 - 18: threadIdType
            # 19 - 31: empty
            header = uint.unpack_from(os_b, pos)[0]
            pos += 4

            ost = OperationStatisticsType.from_byte(header >> 28)
            nt_pid = get_number_type(header, 25)
            nt_count = get_number_type(header, 22)
            nt_cpu_time = get_number_type(header, 19)
            nt_fd = get_number_type(header, 16)
            nt_thread_id = get_number_type(header, 13)

            # DOS and RDOS have an extra header byte each
            ext_header = array('B', [0 for i in range(ost.value)])
            for i in range(ost.value):
                ext_header[i] = ubyte.unpack_from(os_b, pos)[0]
                pos += 1

            if ost.value > 0:
                # 32: empty
                # 33: hasData
                # 34 - 35: dataType
                # 36 - 39: empty
                nt_data = get_number_type(ext_header[0], 4)
            if ost.value > 1:
                # 36: hasRemoteCount
                # 37 - 38: remoteCountType
                # 39 - 40: empty
                # 41: hasRemoteCpuTime
                # 42 - 43: remoteCpuTimeType
                # 44: hasRemoteData
                # 45 - 46: remoteDataType
                # 47: empty
                nt_remote_count = get_number_type(ext_header[0], 1)
                nt_remote_time = get_number_type(ext_header[1], 4)
                nt_remote_data = get_number_type(ext_header[1], 1)

            # hostname
            hostname_length = sbyte.unpack_from(os_b, pos)[0] + 127
            pos += 1
            os_sa[0][index * 10:index * 10 + hostname_length] = bytearray(os_b[pos:pos + hostname_length]).decode(
                'us-ascii')
            pos += hostname_length

            # pid
            os_sa[1][index], pos = nt_pid.get(os_b, pos)

            # key
            key_length = sbyte.unpack_from(os_b, pos)[0] + 127
            pos += 1
            os_sa[2][index * 7:index * 7 + key_length] = bytearray(os_b[pos:pos + key_length]).decode('us-ascii')
            pos += key_length

            # time bin
            os_sa[3][index] = ulong.unpack_from(os_b, pos)[0]
            pos += 8

            # count
            os_sa[4][index], pos = nt_count.get(os_b, pos)

            # cpu time
            os_sa[5][index], pos = nt_cpu_time.get(os_b, pos)

            # source
            source = OperationSource.from_byte(ubyte.unpack_from(os_b, pos)[0]).text()
            os_sa[6][index * 4:index * 4 + len(source)] = source
            pos += 1

            # category
            category = OperationCategory.from_byte(ubyte.unpack_from(os_b, pos)[0]).text()
            os_sa[7][index * 6:index * 6 + len(category)] = category
            pos += 1

            # file descriptor
            os_sa[8][index], pos = nt_fd.get(os_b, pos)

            # thread ID
            os_sa[9][index], pos = nt_thread_id.get(os_b, pos)

            if ost.value > 0:
                # data
                os_sa[10][index], pos = nt_data.get(os_b, pos)
            else:
                os_sa[10][index] = 0
            if ost.value > 1:
                # remote count
                os_sa[11][index], pos = nt_remote_count.get(os_b, pos)

                # remote cpu time
                os_sa[12][index], pos = nt_remote_time.get(os_b, pos)

                # remote data
                os_sa[13][index], pos = nt_remote_data.get(os_b, pos)
            else:
                os_sa[11][index] = 0
                os_sa[12][index] = 0
                os_sa[13][index] = 0

            index += 1
            n += 1
        except struct.error as err:
            # not enough bytes in the buffer, was probably killed during writing
            print("Error parsing data: {} ({}/{}, {}/{})".format(err, pos, len(os_b), n, count), file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            n = count
        except ValueError as err:
            # invalid data in the buffer, because it was killed during writing?
            print("Error parsing data: {} ({}/{}, {}/{})".format(err, pos, len(os_b), n, count), file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            n = count

    return pos - init_pos


def fd_init(fd_shared_arrays, fd_buffer, fd_index):
    global fd_sa, fd_b, fd_i
    fd_sa, fd_b, fd_i = fd_shared_arrays, fd_buffer, fd_index


def parse_file_descriptor_mappings_bb(fd_index):
    # target array, source array, position in target array
    global fd_sa, fd_b, fd_i

    # first 4 bytes are an int indicating the number of records
    count = uint.unpack_from(fd_b, fd_index)[0]
    pos = fd_index + 4

    # reserve right amount of memory
    fd_i.acquire()
    index = fd_i.value
    fd_i.value += count
    fd_i.release()

    n, init_pos = 0, pos
    while n < count:
        try:
            # 0 - 1: empty
            # 2 - 3: pidType
            # 4 - 5: fdType
            # 6 - 7: lengthType
            header = ubyte.unpack_from(fd_b, pos)[0]
            pos += 1

            nt_pid = NumberType.from_byte((header & (0b11 << 4)) >> 4)
            nt_fd = NumberType.from_byte((header & (0b11 << 2)) >> 2)
            nt_length = NumberType.from_byte(header & 0b11)

            # hostname
            hostname_length = sbyte.unpack_from(fd_b, pos)[0] + 127
            pos += 1
            fd_sa[0][index * 10:index * 10 + hostname_length] = bytearray(fd_b[pos:pos + hostname_length]).decode(
                'us-ascii')
            pos += hostname_length

            # pid
            fd_sa[1][index], pos = nt_pid.get(fd_b, pos)

            # key
            key_length = sbyte.unpack_from(fd_b, pos)[0] + 127
            pos += 1
            fd_sa[2][index * 7:index * 7 + key_length] = bytearray(fd_b[pos:pos + key_length]).decode('us-ascii')
            pos += key_length

            # file descriptor
            fd_sa[3][index], pos = nt_fd.get(fd_b, pos)

            # path
            path_length, pos = nt_length.get(fd_b, pos)
            fd_sa[4][index * 256:index * 256 + path_length] = bytearray(fd_b[pos:pos + path_length]).decode('us-ascii')
            pos += path_length

            index += 1
            n += 1
        except struct.error as err:
            # not enough bytes in the buffer, was probably killed during writing
            print("Error parsing data: {} ({}/{}, {}/{})".format(err, pos, len(fd_b), n, count), file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            n = count
        except ValueError as err:
            # invalid data in the buffer, because it was killed during writing?
            print("Error parsing data: {} ({}/{}, {}/{})".format(err, pos, len(fd_b), n, count), file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            n = count

    return pos - init_pos
