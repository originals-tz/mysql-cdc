#ifndef MYSQL_CDC_WRITE_ROW_EVENT_H
#define MYSQL_CDC_WRITE_ROW_EVENT_H

#include <algorithm>
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
        buffer.ReadBitSet(m_nullcolumns, NumberOfBitSet(m_include_columns));
        buffer.HexStr();
        auto column_type = table_map_event.GetColumnType();
        for (int i = 0, skip = 0; i < column_type.size(); i++)
        {
            if (!m_include_columns[i])
            {
                skip++;
                continue;
            }

            if (!m_nullcolumns[i - skip])
            {
                if (column_type[i] == MYSQL_TYPE_STRING)
                {
                    uint16_t meta = table_map_event.GetMetaData(i);
                    if (meta >= 256)
                    {
                        uint16_t meta0 = meta & 0xFF;
                        uint16_t meta1 = meta >> 8;
                        if (meta1 < 256)
                        {
                            uint8_t strlen = 0;
                            buffer.ReadUint8(strlen);
                            std::string value;
                            buffer.ReadString(value, strlen);
                            m_value_vect.emplace_back(value);
                        }
                    }
                }
                else if (column_type[i] == MYSQL_TYPE_LONG)
                {
                    int32_t value = 0;
                    buffer.ReadInt32(value);
                    m_value_vect.emplace_back(std::to_string(value));
                }
            }
        }
    }

    uint8_t NumberOfBitSet(const std::vector<uint8_t>& bit_set)
    {
        uint8_t res = 0;
        for (auto& data : bit_set)
        {
            if (data)
            {
                res++;
            }
        }
        return res;
    }

    std::vector<std::string> m_value_vect;
    uint64_t m_table_id = 0;
    uint64_t m_number_of_column = 0;
    std::vector<uint8_t> m_include_columns;
    std::vector<uint8_t> m_nullcolumns;
};
}  // namespace binlog

#endif  // MYSQL_CDC_WRITE_ROW_EVENT_H
