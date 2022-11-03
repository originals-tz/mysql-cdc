#ifndef MYSQL_CDC_WRITE_ROW_EVENT_H
#define MYSQL_CDC_WRITE_ROW_EVENT_H

#include <algorithm>
#include "byte_buffer.h"
#include "table_map_event.h"
#include "bitset.h"

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
                uint16_t meta = 0;
                uint32_t length = 0;
                if (column_type[i] == MYSQL_TYPE_STRING)
                {
                    meta = table_map_event.GetMetaData(i);
                    if (meta >= 256)
                    {
                        auto meta_data = DeserializeMeta(meta);
                        uint16_t meta0 = meta_data.first;
                        uint16_t meta1 = meta_data.second;
                        length = meta1;
                    }
                }
                DeserializeColumnValue(column_type[i], meta, length, buffer);
            }
        }
    }

    std::pair<uint16_t, uint16_t> DeserializeMeta(uint16_t meta)
    {
        return std::make_pair(meta & 0xFF, meta >> 8);
    }

    template <class T>
    void StoreValue(T& value)
    {
        m_value_vect.template emplace_back(std::to_string(value));
    }

    void DeserializeBit(uint16_t meta, ByteBuffer& buffer)
    {
        auto meta_data = DeserializeMeta(meta);
        uint16_t len = meta_data.first * 8 + meta_data.second;
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

    void DeserializeString(uint32_t length, ByteBuffer& buffer)
    {
        if (length < 256)
        {
            uint8_t strlen = 0;
            buffer.ReadUint8(strlen);
            std::string value;
            buffer.ReadString(value, strlen);
            m_value_vect.emplace_back(value);
        }
    }

    void DeserializeColumnValue(uint8_t field_type, uint16_t meta, uint32_t length, ByteBuffer& buffer)
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
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_TIME2:
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_TIMESTAMP2:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATETIME2:
            case MYSQL_TYPE_YEAR:
            case MYSQL_TYPE_STRING: // CHAR or BINARY
                DeserializeString(length, buffer);
                break;
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_VAR_STRING: // VARCHAR or VARBINARY
            case MYSQL_TYPE_BLOB:
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
};
}  // namespace binlog

#endif  // MYSQL_CDC_WRITE_ROW_EVENT_H
