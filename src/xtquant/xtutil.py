#coding:utf-8

from . import xtbson as _BSON_


def read_from_bson_buffer(buffer):
    import ctypes as ct
    result = []

    pos = 0
    while 1:
        if pos + 4 < len(buffer):
            dlen_buf = buffer[pos : pos + 4]
        else:
            break

        dlen = ct.cast(dlen_buf, ct.POINTER(ct.c_int32))[0]
        if dlen >= 5:
            try:
                data_buf = buffer[pos : pos + dlen]
                pos += dlen

                result.append(_BSON_.decode(data_buf))
            except Exception as e:
                pass
        else:
            break

    return result


def write_to_bson_buffer(data_list):
    buffer = b''

    for data in data_list:
        buffer += _BSON_.encode(data)

    return buffer


def read_from_feather_file(file):
    import feather as fe
    meta = {}
    return meta, fe.read_dataframe(file)


def write_to_feather_file(data, file, meta = None):
    import feather as fe
    if not meta:
        meta = {}
    return

