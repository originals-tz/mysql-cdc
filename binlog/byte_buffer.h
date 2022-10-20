#ifndef MYSQL_CDC_BYTE_BUFFER_H
#define MYSQL_CDC_BYTE_BUFFER_H
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace binlog
{
class ByteBuffer
{
public:
    void Init(const unsigned char* buffer, unsigned long size)
    {
        m_buffer = buffer;
        m_size = size;
        m_ptr = buffer;

        for (int i = 0; i < size; i++)
        {
            data_vect.emplace_back((uint8_t)m_buffer[i]);
        }
    }

    void Skip(uint32_t size)
    {
        m_ptr += size;
    }

    void ReadUint8(uint8_t& value)
    {
        value = *(uint8_t*)(m_ptr);
        m_ptr += 1;
    }

    void ReadInt8(int8_t& value)
    {
        value = *(int8_t*)(m_ptr);
        m_ptr += 1;
    }

    void ReadUint16(uint16_t& value)
    {
        value = *(uint16_t*)(m_ptr);
        m_ptr += 2;
    }

    void ReadInt16(int16_t& value)
    {
        value = *(int16_t*)(m_ptr);
        m_ptr += 2;
    }

    void ReadUint32(uint32_t& value)
    {
        value = *(uint32_t*)(m_ptr);
        m_ptr += 4;
    }

    void ReadInt32(int32_t& value)
    {
        value = *(int32_t*)(m_ptr);
        m_ptr += 4;
    }

    void ReadUint64(uint64_t& value)
    {
        value = *(uint64_t*)(m_ptr);
        m_ptr += 8;
    }

    void ReadInt64(int64_t& value)
    {
        value = *(int64_t*)(m_ptr);
        m_ptr += 8;
    }

    void ReadString(std::string& str, uint32_t size)
    {
        str.assign((char*)m_ptr, size);
        m_ptr += size;
    }

    void ReadString(std::string& str)
    {
        str.assign((char*)m_ptr);
        m_ptr += str.size() + 1;
    }

    void Read(uint8_t* buffer, uint32_t size)
    {
        memcpy(buffer, m_ptr, size);
        m_ptr += size;
    }

    void ReadBitSet(std::vector<uint8_t>& buffer, uint32_t size)
    {
        std::vector<uint8_t> tmp;
        uint32_t length = (size + 7) >> 3;
        tmp.resize(length);
        memcpy(&tmp.front(), m_ptr, length);
        m_ptr += length;

        buffer.resize(size, 0);
        for (int i = 0; i < size; i++)
        {
            if (tmp[i >> 3] & (1 << (i % 8))) {
                buffer[i] = 1;
            }
        }
    }

    uint32_t Available()
    {
        return m_size - (m_ptr - m_buffer);
    }

    void Read(void* ptr, uint32_t size)
    {
        memcpy(ptr, m_ptr, size);
        m_ptr += size;
    }

    uint64_t ReadPackedInteger()
    {
        uint64_t res = 0;
        uint8_t flag = 0;
        ReadUint8(flag);
        if (flag < 0xfb)
        {
            res = flag;
        }
        else if (flag == 0xfb)
        {
            assert(false);
        }
        else if (flag == 0xfc)
        {
            Read(&res, 2);
        }
        else if (flag == 0xfd)
        {
            Read(&res, 3);
        }
        else if (flag == 0xfe)
        {
            Read(&res, 8);
        }
        return res;
    }

    void HexStr()
    {
        uint32_t arr_len = Available();
        std::string hexstr;
        for (int i = 0; i < arr_len; i++)
        {
            char hex1;
            char hex2;
            int value = m_ptr[i];
            int v1 = value / 16;
            int v2 = value % 16;

            if (v1 >= 0 && v1 <= 9)
                hex1 = (char)(48 + v1);
            else
                hex1 = (char)(55 + v1);

            if (v2 >= 0 && v2 <= 9)
                hex2 = (char)(48 + v2);
            else
                hex2 = (char)(55 + v2);

            if (hex1 >= 'A' && hex1 <= 'F')
                hex1 += 32;

            if (hex2 >= 'A' && hex2 <= 'F')
                hex2 += 32;

            hexstr = hexstr + hex1 + hex2 + " ";
        }
        std::cout << hexstr << std::endl;
    }

private:
    std::vector<uint8_t> data_vect;
    unsigned long m_size = 0;
    const unsigned char* m_buffer = nullptr;
    const unsigned char* m_ptr = nullptr;
};
}
#endif  // MYSQL_CDC_BYTE_BUFFER_H
