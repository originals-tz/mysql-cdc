#ifndef MYSQL_CDC_UTIL_H
#define MYSQL_CDC_UTIL_H

#include <cassert>
#include <math.h>

namespace binlog
{

struct Util
{
    template <typename T>
    static T ToBigEndianInteger(T data, int length)
    {
        assert(sizeof(T) >= length);
        char* ptr = (char*)&data;
        T result = 0;
        for (int i = 0; i < length; i++)
        {
            char c = *ptr;
            result = (result << 8) | (c >= 0 ? (int) c : (c+256));
            ptr++;
        }
        return result;
    }

    static int BitSlice(int value, int bitOffset, int numberOfBits, int payloadSize)
    {
        int result = value >> payloadSize - (bitOffset + numberOfBits);
        return (int) (result & ((1 << numberOfBits) - 1));
    }

    static int FractionalSeconds(int meta, ByteBuffer& buffer){
        int length = (meta + 1) / 2;
        if (length > 0) {
            int value = 0;
            buffer.Read(&value, length);
            value = Util::ToBigEndianInteger(value, length);
            return value * (int) pow(100, 3 - length);
        }
        return 0;
    }

    static std::vector<int> Split(uint64_t value, int divider, int length) {
        std::vector<int> result;
        result.resize(length, 0);
        for (int i = 0; i < length - 1; i++) {
            result[i] = (int) (value % divider);
            value /= divider;
        }
        result[length - 1] = (int) value;
        return result;
    }

};

}

#endif  // MYSQL_CDC_UTIL_H
