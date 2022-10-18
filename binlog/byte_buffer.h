#ifndef MYSQL_CDC_BYTE_BUFFER_H
#define MYSQL_CDC_BYTE_BUFFER_H
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

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
    }

    unsigned char Peek(size_t size)
    {
        return *(m_ptr + size);
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
        str.assign((char*)m_ptr);
        m_ptr += size;
    }

    void ReadString(std::string& str)
    {
        str.assign((char*)m_ptr);
        m_ptr += str.size() + 1;
    }

    void ReadBitSet(uint8_t* buffer, uint32_t size)
    {
        memcpy(buffer, m_ptr, size);
        m_ptr += size;
    }


    uint32_t Available()
    {
        return m_size - (m_ptr - m_buffer);
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
    const unsigned char* m_buffer;
    unsigned long m_size;
    const unsigned char* m_ptr;
};
}
#endif  // MYSQL_CDC_BYTE_BUFFER_H
