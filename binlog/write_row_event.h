#ifndef MYSQL_CDC_WRITE_ROW_EVENT_H
#define MYSQL_CDC_WRITE_ROW_EVENT_H

#include <algorithm>
#include <sstream>
#include "bitset.h"
#include "byte_buffer.h"
#include "table_map_event.h"

namespace binlog
{
class WriteRowEvent
{
public:
    void Deserialize(ByteBuffer& buffer, FormatDescriptionEvent& format_description_event, TableMapEvent& table_map_event)
    {
        auto event_header_len = format_description_event.GetHeaderLen(WRITE_ROWS_EVENT);
        if (event_header_len == 6)
        {
            buffer.Read(&m_table_id, 4);
            buffer.Skip(2);
        }
        else
        {
            buffer.Read(&m_table_id, 6);
            buffer.Skip(2);
            uint16_t extra_data_len = 0;
            buffer.ReadUint16(extra_data_len);
            buffer.Skip(extra_data_len - 2);
        }

        m_number_of_column = buffer.ReadPackedInteger();
        buffer.ReadBitSet(m_include_columns, m_number_of_column);
        DeserializeRow(buffer, table_map_event);
    }

    void PrintValues()
    {
        for (size_t i = 0; i < m_value_vect.size(); i++)
        {
            std::cout << i << ": " << m_value_vect[i] << std::endl;
        }
    }
private:
    void DeserializeRow(ByteBuffer& buffer, TableMapEvent& table_map_event)
    {
        buffer.ReadBitSet(m_nullcolumns, m_include_columns.NumberOfSet());
        buffer.HexStr();
        auto column_type = table_map_event.GetColumnType();
        for (int i = 0, skip = 0; i < column_type.size(); i++)
        {
            if (!m_include_columns.Get(i))
            {
                skip++;
                continue;
            }

            if (!m_nullcolumns.Get(i - skip))
            {
                int meta = 0;
                int length = 0;
                uint8_t type = column_type[i];
                meta = table_map_event.GetMetaData(i);
                if (column_type[i] == MYSQL_TYPE_STRING)
                {
                    if (meta >= 256)
                    {
                        int byte0= meta >> 8;
                        int byte1= meta & 0xFF;
                        if ((byte0 & 0x30) != 0x30)
                        {
                            /* a long CHAR() field: see #37426 */
                            length= byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                            type = byte0 | 0x30;
                        }
                        else
                            length = meta & 0xFF;
                    }
                }
                DeserializeColumnValue(type, meta, length, buffer);
            }
        }
    }

    template <class T>
    void StoreValue(T& value)
    {
        m_value_vect.template emplace_back(std::to_string(value));
    }

    void DeserializeBit(int meta, ByteBuffer& buffer)
    {
        uint16_t len = (meta >> 8) * 8 + (meta & 0xFF);
        BitSet bitset;
        buffer.ReadBitSet(bitset, len);
    }

    void DeserializeTiny(ByteBuffer& buffer)
    {
        int8_t value = 0;
        buffer.ReadInt8(value);
        StoreValue(value);
    }

    void DeserializeShort(ByteBuffer& buffer)
    {
        int16_t value = 0;
        buffer.ReadInt16(value);
        StoreValue(value);
    }

    void DeserializeInt24(ByteBuffer& buffer)
    {
        int32_t value = 0;
        buffer.Read(&value, 3);
        StoreValue(value);
    }

    void DeserializeLong(ByteBuffer& buffer)
    {
        int32_t value = 0;
        buffer.ReadInt32(value);
        StoreValue(value);
    }

    void DeserializeLongLong(ByteBuffer& buffer)
    {
        int64_t value = 0;
        buffer.ReadInt64(value);
        StoreValue(value);
    }

    void DeserializeFloat(ByteBuffer& buffer)
    {
        float value = 0.0f;
        buffer.Read(&value, 4);
        StoreValue(value);
    }

    void DeserializeDouble(ByteBuffer& buffer)
    {
        double value = 0;
        buffer.Read(&value, 8);
        StoreValue(value);
    }

    void DeserializeNewDecimal(uint16_t meta, ByteBuffer& buffer)
    {
    }

    void DeserializeDate(ByteBuffer& buffer)
    {
        uint32_t value = 0;
        buffer.Read(&value, 3);
        uint day = value % 32;
        value >>=5;
        uint month = value % 16;
        uint year = value >> 4;
        std::stringstream ss;
        ss << year << "-" << month << "-" << day << std::endl;
        m_value_vect.emplace_back(ss.str());
    }

    void DeserializeTime(ByteBuffer& buffer)
    {
        uint32_t value = 0;
        buffer.Read(&value, 3);
    }

    void DeserializeTimeV2(ByteBuffer& buffer)
    {
        uint32_t value = 0;
        buffer.Read(&value, 3);
    }

    void DeserializeYear(ByteBuffer& buffer)
    {
        uint year = buffer.ReadUint8() + 1900;
        StoreValue(year);
    }

    void DeserializeString(uint32_t length, ByteBuffer& buffer)
    {
        uint string_length = length < 256 ? buffer.ReadUint8() : buffer.ReadUint16();
        std::string value;
        buffer.ReadString(value, string_length);
        m_value_vect.emplace_back(value);
    }

    void DeserializeVarString(int meta, ByteBuffer& buffer)
    {
        uint varcharLength = meta < 256 ? buffer.ReadUint8() : buffer.ReadUint16();
        std::string value;
        buffer.ReadString(value, varcharLength);
        m_value_vect.emplace_back(value);
    }

    void DeserializeBlob(int meta, ByteBuffer& buffer)
    {
        uint64_t len = 0;
        buffer.Read(&len, meta);
        std::string str;
        buffer.ReadString(str, len);
        m_value_vect.emplace_back(str);
    }

    void DeserializeEnum(uint32_t length, ByteBuffer& buffer)
    {
        uint64_t value = 0;
        buffer.Read(&value, length);
        StoreValue(value);
    }

    void DeserializeColumnValue(uint8_t field_type, int meta, int length, ByteBuffer& buffer)
    {
        switch (field_type)
        {
            case MYSQL_TYPE_BIT:
                DeserializeBit(meta, buffer);
                break;
            case MYSQL_TYPE_TINY:
                DeserializeTiny(buffer);
                break;
            case MYSQL_TYPE_SHORT:
                DeserializeShort(buffer);
                break;
            case MYSQL_TYPE_INT24:
                DeserializeInt24(buffer);
                break;
            case MYSQL_TYPE_LONG:
                DeserializeLong(buffer);
                break;
            case MYSQL_TYPE_LONGLONG:
                DeserializeLongLong(buffer);
                break;
            case MYSQL_TYPE_FLOAT:
                DeserializeFloat(buffer);
                break;
            case MYSQL_TYPE_DOUBLE:
                DeserializeDouble(buffer);
                break;
            case MYSQL_TYPE_NEWDECIMAL:
//                DeserializeNewDecimal(meta, buffer);
                break;
            case MYSQL_TYPE_DATE:
                DeserializeDate(buffer);
                break;
            case MYSQL_TYPE_TIME:
                DeserializeTime(buffer);
                break;
            case MYSQL_TYPE_TIME2:
                DeserializeTimeV2(buffer);
                break;
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_YEAR:
                DeserializeYear(buffer);
                break;
            case MYSQL_TYPE_STRING: // CHAR or BINARY
                DeserializeString(length, buffer);
                break;
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_VAR_STRING:
                DeserializeVarString(meta, buffer);
                break;
            case MYSQL_TYPE_BLOB:
                DeserializeBlob(meta, buffer);
                break;
            case MYSQL_TYPE_ENUM:
            case MYSQL_TYPE_SET:
            case MYSQL_TYPE_GEOMETRY:
            case MYSQL_TYPE_JSON:
            default:
                assert(false);
        }
    }

    std::vector<std::string> m_value_vect;
    uint64_t m_table_id = 0;
    uint64_t m_number_of_column = 0;
    BitSet m_include_columns;
    BitSet m_nullcolumns;
    static const int DIG_PER_DEC = 9;
    constexpr static const int DIG_TO_BYTES[] = {0,1,1,2,2,3,3,4,4,4};
};
}  // namespace binlog

#endif  // MYSQL_CDC_WRITE_ROW_EVENT_H
